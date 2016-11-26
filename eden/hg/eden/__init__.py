# Copyright (c) 2016, Facebook, Inc.
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

from mercurial import extensions, localrepo, pathutil, node, scmutil, util
from mercurial import dirstate as dirstate_mod

from .LameThriftClient import create_thrift_client

_requirement = 'eden'
_repoclass = localrepo.localrepository
_repoclass._basesupported.add(_requirement)


def extsetup(ui):
    # Wrap the localrepo.dirstate() function.
    #
    # The original dirstate attribute is a filecache object, and needs slightly
    # special handling to wrap properly.
    #
    # (The fsmonitor and sqldirstate extensions both already wrap it, and each
    # has slightly different mechanisms for doing so.  Here we wrap it more
    # like sqldirstate does.  Ideally code for wrapping filecache objects
    # should just get put into core mercurial.)
    orig = localrepo.localrepository.dirstate
    extensions.wrapfunction(orig, 'func', wrapdirstate)
    orig.paths = ()


def reposetup(ui, repo):
    # TODO: We probably need some basic sanity checking here:
    # - is this an eden client?
    # - are any conflicting extensions enabled?
    pass


def wrapdirstate(orig, repo):
    # Only override when actually inside an eden client directory.
    # TODO: Actually check this properly.
    is_eden = True
    if not is_eden:
        return orig(repo)

    # For now we intentionally do not derive from the original dirstate class.
    #
    # We want to make sure that we never accidentally fall back to the base
    # dirstate functionality; anything we do should be tailored for eden.

    # have the edendirstate class implementation more complete.
    return edendirstate(repo, repo.ui, repo.root)


class ClientStatus(object):
    def __init__(self):
        self.modified = []
        self.added = []
        self.removed = []
        self.deleted = []
        self.unknown = []
        self.ignored = []
        self.clean = []


class EdenThriftClient(object):
    def __init__(self, repo):
        self._root = repo.root
        # TODO: Find the correct socket path based on the mount point,
        # rather than assuming it is always ~/local/.eden
        eden_dir = os.path.join(os.environ['HOME'], 'local/.eden')
        self._client = create_thrift_client(eden_dir)
        # TODO: It would be nicer to use a context manager to make sure we
        # close the client appropriately.
        self._client.open()

    def _getMaterializedEntries(self):
        return self._client.getMaterializedEntries(self._root)

    def getCurrentNodeID(self):
        '''
        Returns the ID of the working directory's parent comment, as a
        20-byte binary value.

        Use mercurial.node.hex() to convert the return value into a
        40-character human-readable string.
        '''
        # TODO: Use a more specific thrift API for this.
        return self._getMaterializedEntries().currentPosition.snapshotHash

    def setHgParents(self, p1, p2):
        # TODO: update the eden snapshot pointer
        raise NotImplementedError('edendirstate.setparents()')

    def getStatus(self):
        status = ClientStatus()
        # TODO(mbolin): Currently, materialized entries are ~= to modified
        # files. Need to address corner cases.
        for filename in self._getMaterializedEntries().fileInfo:
            if (filename == '' or filename.startswith('.hg/')):
                continue
            if os.path.isdir(os.path.join(self._root, filename)):
                continue
            # TODO(mbolin): It is not correct to assume every file in this list
            # is a modified file.
            status.modified.append(filename)
        return status


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
        self._client = EdenThriftClient(repo)
        self._ui = ui
        self._root = root
        self._rootdir = pathutil.normasprefix(root)

        # Store a vanilla dirstate object, so we can re-use some of its
        # functionality in a handful of cases.  Primarily this is just for cwd
        # and path computation.
        self._normaldirstate = dirstate_mod.dirstate(
            opener=None, ui=self._ui, root=self._root, validate=None)

        self._parentwriters = 0

        # Unclear who writes this.
        self._filecache = {}

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

    def _ignore(self):
        # Even though this function starts with an underscore, it is directly
        # called from other parts of the mercurial code.
        raise NotImplementedError('edendirstate._ignore()')

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

    def parents(self):
        return [self.p1(), self.p2()]

    def p1(self):
        commit = self._client.getCurrentNodeID()
        return self._repo._dirstatevalidate(commit)

    def p2(self):
        return node.nullid

    def branch(self):
        return 'default'

    def setparents(self, p1, p2=node.nullid):
        """Set dirstate parents to p1 and p2."""
        if self._parentwriters == 0:
            raise ValueError("cannot set dirstate parent without "
                             "calling dirstate.beginparentchange")

        self._client.setHgParents(p1, p2)

    def setbranch(self, branch):
        raise NotImplementedError('edendirstate.setbranch()')

    def _opendirstatefile(self):
        # TODO: used by the journal extension
        raise NotImplementedError('edendirstate._opendirstatefile()')

    def invalidate(self):
        raise NotImplementedError('edendirstate.invalidate()')

    def copy(self, source, dest):
        """Mark dest as a copy of source. Unmark dest if source is None."""
        raise NotImplementedError('edendirstate.copy()')

    def copied(self, file):
        # FIXME
        return False

    def copies(self):
        # FIXME
        return {}

    def normal(self, f):
        # FIXME
        pass

    def normallookup(self, f):
        # FIXME
        pass

    def otherparent(self, f):
        """Mark as coming from the other parent, always dirty."""
        raise NotImplementedError('edendirstate.otherparent()')

    def add(self, f):
        """Mark a file added."""
        raise NotImplementedError('edendirstate.add()')

    def remove(self, f):
        """Mark a file removed."""
        raise NotImplementedError('edendirstate.remove()')

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
        # Probably don't ever need to rebuild the dirstate with eden?
        raise NotImplementedError('edendirstate.rebuild()')

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

    def walk(self, match, subrepos, unknown, ignored, full=True):
        # TODO:
        raise NotImplementedError('eden dirstate walk()')

    def status(self, match, subrepos, ignored, clean, unknown):
        # We should never have any files we are unsure about
        unsure = []

        edenstatus = self._client.getStatus()

        status = scmutil.status(edenstatus.modified,
                                edenstatus.added,
                                edenstatus.removed,
                                edenstatus.deleted,
                                edenstatus.unknown,
                                edenstatus.ignored,
                                edenstatus.clean)
        return (unsure, status)

    def matches(self, match):
        raise NotImplementedError('edendirstate.matches()')

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