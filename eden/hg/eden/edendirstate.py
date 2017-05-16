#!/usr/bin/env python2
# Copyright (c) 2016-present, Facebook, Inc.
# All Rights Reserved.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

"""
Mercurial extension for supporting eden client checkouts.

This overrides the dirstate to check with the eden daemon for modifications,
instead of doing a normal scan of the filesystem.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

from mercurial import (
    error, pathutil, node, scmutil, util
)
from mercurial import dirstate as dirstatemod
from mercurial.i18n import _
from . import EdenThriftClient as thrift


class statobject(object):
    ''' this is a stat-like object to represent information from eden.'''
    __slots__ = ('st_mode', 'st_size', 'st_mtime')

    def __init__(self, mode=None, size=None, mtime=None):
        self.st_mode = mode
        self.st_size = size
        self.st_mtime = mtime


class edendirstate(object):
    '''
    edendirstate replaces mercurial's normal dirstate class.

    edendirstate generally avoids performing normal filesystem operations for
    computing the working directory state, and instead communicates directly to
    eden instead to ask for the status of the working copy.

    edendirstate currently does not derive from the normal dirstate class
    primarily just to ensure that we do not ever accidentally fall back to the
    default dirstate behavior.
    '''
    def __init__(self, repo, ui, root):
        self._repo = repo
        self.eden_client = thrift.EdenThriftClient(repo)
        self._ui = ui
        self._root = root
        self._rootdir = pathutil.normasprefix(root)
        # self._parents is a cache of the current parent node IDs.
        # This is a tuple of 2 20-byte binary commit IDs, or None when unset.
        self._parents = None

        # Store a vanilla dirstate object, so we can re-use some of its
        # functionality in a handful of cases.  Primarily this is just for cwd
        # and path computation.
        self._normaldirstate = dirstatemod.dirstate(
            opener=None, ui=self._ui, root=self._root, validate=None)

        self._parentwriters = 0

    def thrift_scm_add(self, paths):
        '''paths must be a normalized paths relative to the repo root.

        Note that each path in paths may refer to a file or a directory.

        Returns a possibly empty list of errors to present to the user.
        '''
        return self.eden_client.add(paths)

    def thrift_scm_remove(self, paths, force):
        '''paths must be normalized paths relative to the repo root.

        Note that each path in paths may refer to a file or a directory.

        Returns a possibly empty list of errors to present to the user.
        '''
        return self.eden_client.remove(paths, force)

    def beginparentchange(self):
        self._parentwriters += 1

    def endparentchange(self):
        if self._parentwriters <= 0:
            raise ValueError("cannot call dirstate.endparentchange without "
                             "calling dirstate.beginparentchange")
        self._parentwriters -= 1

    def pendingparentchange(self):
        return self._parentwriters > 0

    def dirs(self):
        raise NotImplementedError('edendirstate.dirs()')

    @property
    def _ignore(self):
        '''Returns a function that takes a file path (relative to the repo root)
        and returns a boolean indicating whether it is ignored.

        Note that this function may be called in the middle of a merge/histedit.
        '''
        # Even though this function starts with an underscore, it is directly
        # called from other parts of the mercurial code.

        # For the moment, we are only testing with repos that do not introduce
        # ignored files, so we categorically return False for now.
        # TODO(mbolin): Provide a legit implementation of this method.
        def never(filename):
            return False
        return never

    def _checklink(self):
        """
        check whether the given path is on a symlink-capable filesystem
        """
        # Even though this function starts with an underscore, it is directly
        # called from other parts of the mercurial code.
        return True

    def _checkexec(self):
        """
        Check whether the given path is on a filesystem with UNIX-like
        exec flags.
        """
        # Even though this function starts with an underscore, it is called
        # from other extensions and other parts of the mercurial code.
        return True

    def _join(self, f):
        # Use the same simple concatenation strategy as mercurial's
        # normal dirstate code.
        return self._rootdir + f

    def flagfunc(self, buildfallback):
        return self._flagfunc

    def _flagfunc(self, path):
        try:
            st = os.lstat(self._join(path))
            if util.statislink(st):
                return 'l'
            if util.statisexec(st):
                return 'x'
        except OSError:
            pass
        return ''

    def getcwd(self):
        # Use the vanilla mercurial dirstate.getcwd() implementation
        return self._normaldirstate.getcwd()

    def pathto(self, f, cwd=None):
        # Use the vanilla mercurial dirstate.pathto() implementation
        return self._normaldirstate.pathto(f, cwd)

    def __getitem__(self, key):
        # FIXME
        return '?'

    def __contains__(self, key):
        # FIXME
        return False

    def __iter__(self):
        # FIXME
        if False:
            yield None
        return

    def iteritems(self):
        raise NotImplementedError('edendirstate.iteritems()')

    def _getparents(self):
        if self._parents is None:
            p1, p2 = self.eden_client.getParentCommits()
            if p2 is None:
                p2 = node.nullid
            self._parents = (p1, p2)

    def parents(self):
        self._getparents()
        return list(self._parents)

    def p1(self):
        self._getparents()
        return self._parents[0]

    def p2(self):
        self._getparents()
        return self._parents[1]

    def branch(self):
        return 'default'

    def setparents(self, p1, p2=node.nullid):
        """Set dirstate parents to p1 and p2."""
        if self._parentwriters == 0:
            raise ValueError("cannot set dirstate parent without "
                             "calling dirstate.beginparentchange")

        # Normalize p1 and p2 to hashes in case either is passed in as a
        # revision number.
        if type(p1) is int:
            p1_node = self._repo.lookup(p1)
        else:
            p1_node = p1
        if type(p2) is int:
            p2_node = self._repo.lookup(p2)
        else:
            p2_node = p2

        self.eden_client.setHgParents(p1_node, p2_node)
        self._parents = (p1_node, p2_node)

    def setbranch(self, branch):
        raise NotImplementedError('edendirstate.setbranch()')

    def _opendirstatefile(self):
        # TODO: used by the journal extension
        raise NotImplementedError('edendirstate._opendirstatefile()')

    def invalidate(self):
        '''Clears local state such that it is forced to be recomputed the next
        time it is accessed.

        This method is invoked when the lock is acquired via
        localrepository.wlock(). In wlock(),
        localrepository.invalidatedirstate() is called when the lock is
        acquired, which calls dirstate.invalidate() (surprisingly, this is only
        because we have redefined localrepository.invalidatedirstate() to do so
        in extsetup(ui)).

        This method is also invoked when the lock is released if
        self.pendingparentchange() is True.
        '''
        self._parents = None

    def copy(self, source, dest):
        """Mark dest as a copy of source. Unmark dest if source is None."""
        raise NotImplementedError('edendirstate.copy()')

    def copied(self, file):
        # TODO(mbolin): Once we update edendirstate to properly store copy
        # information, we will have to return True if there are any
        # copies/renames.
        return False

    def copies(self):
        # TODO(mbolin): Once we update edendirstate to properly store copy
        # information, we will have to include it in the dict returned by this
        # method.
        return {}

    def normal(self, f):
        raise NotImplementedError('edendirstate.normal(%s)' % f)

    def normallookup(self, f):
        raise NotImplementedError('edendirstate.normallookup(%s)' % f)

    def otherparent(self, f):
        """Mark as coming from the other parent, always dirty."""
        if self.p2() == node.nullid:
            raise error.Abort(_('setting %r to other parent '
                                'only allowed in merges') % f)

    def add(self, f):
        """Mark a file added."""
        # TODO(mbolin): Eliminate add() in overrides.py and let `hg add` flow
        # into this command in the natural way. This should now be possible
        # due to @wez's matcher work.
        self.thrift_scm_add([f])

    def remove(self, f):
        """Mark a file removed."""
        # TODO(mbolin): Eliminate remove() in overrides.py and let `hg rm` flow
        # into this command in the natural way. This should now be possible
        # due to @wez's matcher work.
        self.thrift_scm_remove([f], force=False)

    def merge(self, f):
        """Mark a file merged."""
        raise NotImplementedError('edendirstate.merge()')

    def drop(self, f):
        """Drop a file from the dirstate"""
        raise NotImplementedError('edendirstate.drop()')

    def normalize(self, path, isknown=False, ignoremissing=False):
        """normalize the case of a pathname when on a casefolding filesystem"""
        # TODO: Should eden always be case-sensitive?
        return path

    def clear(self):
        raise NotImplementedError('edendirstate.clear()')

    def rebuild(self, parent, allfiles, changedfiles=None):
        # We don't ever need to rebuild file status with eden, all we need to
        # do is reset the parent commit of the working directory.
        #
        # TODO: It would be nicer if we could update the higher-level code so
        # it doesn't even bother computing allfiles and changedfiles.
        self.eden_client.setHgParents(parent, node.nullid)

    def write(self, tr):
        # TODO: write the data if it is dirty
        return

    def _dirignore(self, f):
        # Not used by core mercurial code; only internally by dirstate.walk
        # and by the hgview application
        raise NotImplementedError('edendirstate._dirignore()')

    def _ignorefileandline(self, f):
        # Only used by the "debugignore" command
        raise NotImplementedError('edendirstate._ignorefileandline()')

    def _eden_walk_helper(self, match, deleted, unknown, ignored):
        ''' Extract the matching information we collected from the
            match constructor and try to turn it into a list of
            glob expressions.  If we don't have enough information
            for this, make_glob_list() will raise an exception '''
        if not util.safehasattr(match, '_eden_match_info'):
            raise NotImplementedError(
                'match object is not eden compatible' + \
                '(_eden_match_info is missing)')
        info = match._eden_match_info
        globs = info.make_glob_list()

        # Expand the glob into a set of candidate files
        globbed_files = self.eden_client.glob(globs)

        # Run the results through the matcher object; this processes
        # any excludes that might be part of the matcher
        matched_files = [f for f in globbed_files if match(f)]

        if matched_files and (deleted or (not unknown) or (not ignored)):
            # !unknown as parameter means that we need to exclude
            # any files with an unknown status.
            # !ignored -> exclude any ignored files.
            # To get ignored files in the status list, we need to pass
            # True when !ignored is passed in to us.
            status = self.eden_client.getStatus(not ignored)
            elide = set()
            if not unknown:
                elide.update(status.unknown)
            if not ignored:
                elide.update(status.ignored)
            if deleted:
                elide.update(status.removed)
                elide.update(status.deleted)

            matched_files = [f for f in matched_files if f not in elide]

        return matched_files

    def walk(self, match, subrepos, unknown, ignored, full=True):
        '''
        Walk recursively through the directory tree, finding all files
        matched by match.

        If full is False, maybe skip some known-clean files.

        Return a dict mapping filename to stat-like object
        '''

        matched_files = self._eden_walk_helper(match,
                                               deleted=True,
                                               unknown=unknown,
                                               ignored=ignored)

        # Now we need to build a stat-like-object for each of these results
        file_info = self.eden_client.getFileInformation(matched_files)

        results = {}
        for index, info in enumerate(file_info):
            file_name = matched_files[index]
            if info.getType() == thrift.FileInformationOrError.INFO:
                finfo = info.get_info()
                results[file_name] = statobject(mode=finfo.mode,
                                                size=finfo.size,
                                                mtime=finfo.mtime)
            else:
                # Indicates that we knew of the file, but that is it
                # not present on disk; it has been removed.
                results[file_name] = None

        return results

    def status(self, match, subrepos, ignored, clean, unknown):
        # We should never have any files we are unsure about
        unsure = []

        edenstatus = self.eden_client.getStatus(ignored)

        status = scmutil.status(edenstatus.modified,
                                edenstatus.added,
                                edenstatus.removed,
                                edenstatus.deleted,
                                edenstatus.unknown,
                                edenstatus.ignored,
                                edenstatus.clean)
        return (unsure, status)

    def matches(self, match):
        return self._eden_walk_helper(match,
                                      deleted=False,
                                      unknown=False,
                                      ignored=False)

    def savebackup(self, tr, suffix='', prefix=''):
        '''
        Saves the current dirstate, using prefix/suffix to namespace the storage
        where the current dirstate is persisted.
        One of prefix or suffix must be set.

        The complement to this method is self.restorebackup(tr, suffix, prefix).

        Args:
            tr (transaction?): such as `repo.currenttransaction()` or None.
            suffix (str): If persisted to a file, suffix of file to use.
            prefix (str): If persisted to a file, prefix of file to use.
        '''
        assert len(suffix) > 0 or len(prefix) > 0
        # TODO(mbolin): Create a snapshot for the current dirstate and persist
        # it to a safe place.
        pass

    def restorebackup(self, tr, suffix='', prefix=''):
        '''
        Restores the saved dirstate, using prefix/suffix to namespace the
        storage where the dirstate was persisted.
        One of prefix or suffix must be set.

        The complement to this method is self.savebackup(tr, suffix, prefix).

        Args:
            tr (transaction?): such as `repo.currenttransaction()` or None.
            suffix (str): If persisted to a file, suffix of file to use.
            prefix (str): If persisted to a file, prefix of file to use.
        '''
        assert len(suffix) > 0 or len(prefix) > 0
        # TODO(mbolin): Restore the snapshot written by savebackup().
        pass

    def clearbackup(self, tr, suffix='', prefix=''):
        raise NotImplementedError('edendirstate.clearbackup()')
