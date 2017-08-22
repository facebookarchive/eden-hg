#!/usr/bin/env python2
# Copyright (c) 2016-present, Facebook, Inc.
# All Rights Reserved.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from mercurial import dirstate, node, policy, scmutil, util
try:
    # Check to see if this mercurial version has the sparse module.
    from mercurial import sparse as sparsemod
    # Unfortunately due to the demandimport module, the import above will
    # succeed even on older versions of mercurial that do not actually contain
    # sparse.  Access a member of the module to force it to be loaded.
    sparsemod.__name__
except ImportError:
    # Older versions of mercurial do not have sparse
    sparsemod = None

from . import EdenThriftClient as thrift
from . import eden_dirstate_map as eden_dirstate_map
import collections
import stat
import os

parsers = policy.importmod('parsers')

dirstatetuple = parsers.dirstatetuple


class statobject(object):
    ''' this is a stat-like object to represent information from eden.'''
    __slots__ = ('st_mode', 'st_size', 'st_mtime')

    def __init__(self, mode=None, size=None, mtime=None):
        self.st_mode = mode
        self.st_size = size
        self.st_mtime = mtime


class dummy_copymap(collections.MutableMapping):
    def __init__(self, thrift_client):
        # type(dummy_copymap, EdenThriftClient) -> None
        self._thrift_client = thrift_client

    def _get_mapping_thrift(self):
        # type(dummy_copymap) -> Dict[str, str]
        return self._thrift_client.hgCopyMapGetAll()

    def __getitem__(self, dest_filename):
        # type(str) -> str
        try:
            return self._thrift_client.hgCopyMapGet(dest_filename)
        except:  # noqa: B901
            raise KeyError(dest_filename)

    def __setitem__(self, dest_filename, source_filename):
        self._thrift_client.hgCopyMapPut(dest_filename, source_filename)

    def __delitem__(self, dest_filename):
        # TODO(mbolin): Setting the value to '' deletes it from the map. This
        # would be better as an explicit "remove" API.
        self._thrift_client.hgCopyMapPut(dest_filename, '')

    def __iter__(self):
        return iter(self._get_mapping_thrift())

    def __len__(self):
        raise Exception('Should not call __len__ on dummy_copymap!')

    def keys(self):
        # collections.MutableMapping implements keys(), but does so poorly--
        # it ends up calling __iter__() and then __len__(), and we want to
        # avoid making two separate thrift calls.
        return self._get_mapping_thrift().keys()

    def copy(self):
        # We return a new dict object, and not dummy_copymap() object.
        # Any mutations made to the returned copy should not affect the actual
        # dirstate, and should not be sent back to eden via thrift.
        return self._get_mapping_thrift().copy()


class eden_dirstate(dirstate.dirstate):
    def __init__(self, repo, ui, root):
        self.eden_client = thrift.EdenThriftClient(repo)
        self._eden_map_impl = eden_dirstate_map.eden_dirstate_map(
            self.eden_client
        )
        self._eden_copymap_impl = dummy_copymap(self.eden_client)

        # We should override any logic in dirstate that uses self._validate.
        validate = None

        opener = repo.vfs
        # Newer versions of mercurial require a sparsematchfn argument to the
        # dirstate.
        if sparsemod is not None:
            def sparsematchfn():
                return sparsemod.matcher(repo)
            super(eden_dirstate, self).__init__(opener, ui, root, validate,
                                                sparsematchfn)
        else:
            super(eden_dirstate, self).__init__(opener, ui, root, validate)

        self._repo = repo

        # self._parents is a cache of the current parent node IDs.
        # This is a tuple of 2 20-byte binary commit IDs, or None when unset.
        self._parents = None

    def __iter__(self):
        # FIXME: This appears to be called by `hg reset`, so we provide a dummy
        # response here, but really, we should outright prohibit this.
        # Most likely, we will have to replace the implementation of `hg reset`.
        if False:
            yield None
        return

    @property
    def _map(self):  # override
        return self._eden_map_impl

    @property
    def _copymap(self):  # override
        return self._eden_copymap_impl

    def _read(self):  # override
        pass

    def iteritems(self):  # override
        # This seems like the type of O(repo) operation that should not be
        # allowed. Or if it is, it should be through a separate, explicit
        # codepath.
        raise NotImplementedError('eden_dirstate.iteritems()')

    def dirs(self):  # override
        raise NotImplementedError('eden_dirstate.dirs()')

    @property
    def _nonnormalset(self):  # override
        return self._map.nonnormalentries()

    @property
    def _otherparentset(self):  # override
        result = set()
        for f, s in self._map.otherparententries():
            result.add(f)
        return result

    def _getparents(self):
        if self._parents is None:
            p1, p2 = self.eden_client.getParentCommits()
            if p2 is None:
                p2 = node.nullid
            self._parents = (p1, p2)

    def parents(self):  # override
        self._getparents()
        return list(self._parents)

    def p1(self):  # override
        self._getparents()
        return self._parents[0]

    def p2(self):  # override
        self._getparents()
        return self._parents[1]

    @property
    def _pl(self):
        '''I assume pl = "parents list"?'''
        return self.parents()

    def __setattr__(self, key, value):
        if key == '_pl':
            # self.rebuild() ends up calling this instead of self.setparents().
            # We should fix this upstream, but for now, we hack around this.
            # This is what sqldirstate does.
            p1 = value[0]
            p2 = value[1]
            self.setparents(p1, p2)
            self.__dict__['_p1'] = value
        else:
            return super(eden_dirstate, self).__setattr__(key, value)

    def setparents(self, p1, p2=node.nullid):  # override
        '''Set dirstate parents to p1 and p2.'''
        if self._parentwriters == 0:
            raise ValueError(
                'cannot set dirstate parent without '
                'calling dirstate.beginparentchange'
            )

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

        # If a transaction is currently in progress, make sure it has flushed
        # pending commit data to disk so that eden will be able to access it.
        txn = self._repo.currenttransaction()
        if txn is not None:
            txn.writepending()

        self.eden_client.setHgParents(p1_node, p2_node)
        self._parents = (p1_node, p2_node)

        # TODO(mbolin): `return copies` as the superclass does?

    def invalidate(self):  # override
        super(eden_dirstate, self).invalidate()
        self._parents = None

    def clear(self):  # override
        '''Intended to match superclass implementation except for changes to
        map_ and copymap_.'''
        self.eden_client.hgClearDirstate()
        self._pl = [node.nullid, node.nullid]
        self._lastnormaltime = 0
        self._updatedfiles.clear()

    def walk(self, match, subrepos, unknown, ignored, full=True):  # override
        '''
        Walk recursively through the directory tree, finding all files
        matched by match.

        If full is False, maybe skip some known-clean files.

        Return a dict mapping filename to stat-like object
        '''
        if unknown and not ignored and not full:
            # TODO(mbolin): Instead of assuming `hg add`, do something robust.
            # pre-add/post-add hooks might be appropriate.
            # We assume that this is being called from `hg add`, so we return
            # everything that is eligible for addition and filter by the
            # matcher.
            clean = False
            status = self.status(match, subrepos, ignored, clean, unknown)[1]
            modified, added, removed, deleted, unknown, ignored, clean = status
            candidates = unknown

            # If the file is marked for removal, but it exists on disk, then
            # include it in the list of files to add.
            for removed_file in removed:
                try:
                    mode = os.stat(os.path.join(self._root, removed_file)).st_mode
                    if stat.S_ISREG(mode) or stat.S_ISLNK(mode):
                        candidates.append(removed_file)
                except OSError as exception:
                    import errno
                    if exception.errno != errno.ENOENT:
                        raise

            return [f for f in candidates if match(f)]

        matched_files = self._eden_walk_helper(
            match, deleted=True, unknown=unknown, ignored=ignored
        )

        # Now we need to build a stat-like-object for each of these results
        file_info = self.eden_client.getFileInformation(matched_files)

        results = {}
        for index, info in enumerate(file_info):
            file_name = matched_files[index]
            if info.getType() == thrift.FileInformationOrError.INFO:
                finfo = info.get_info()
                results[file_name] = statobject(
                    mode=finfo.mode, size=finfo.size, mtime=finfo.mtime
                )
            else:
                # Indicates that we knew of the file, but that is it
                # not present on disk; it has been removed.
                results[file_name] = None

        return results

    def _eden_walk_helper(self, match, deleted, unknown, ignored):
        ''' Extract the matching information we collected from the
            match constructor and try to turn it into a list of
            glob expressions.  If we don't have enough information
            for this, make_glob_list() will raise an exception '''
        if not util.safehasattr(match, '_eden_match_info'):
            raise NotImplementedError(
                'match object is not eden compatible'
                '(_eden_match_info is missing)'
            )
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

    def status(self, match, subrepos, ignored, clean, unknown):  # override
        # We should never have any files we are unsure about
        unsure = []

        edenstatus = self.eden_client.getStatus(ignored)

        if clean:
            # By default, Eden's getStatus() will not return "clean" files.
            # Without any sort of filter, requesting the "clean" files is an
            # O(repo) operation, which we will not support.
            #
            # Note that remove() specifies clean=True with a matcher, which is
            # why we care about this use case. For now, we are doing
            # post-processing with the matching here on the client, but
            # ultimately, it would be best if this could be done on the server.
            if not match or match.always():
                raise Exception('Cannot request clean=True with no filter.')

            # For every file in matches that is not in edenstatus, assume that
            # it belongs in edenstatus.clean.
            classified_files = set(
                edenstatus.modified + edenstatus.added + edenstatus.removed +
                edenstatus.deleted + edenstatus.unknown + edenstatus.ignored
            )
            matches = self.matches(match)
            clean_files = []
            for filename in matches:
                if filename not in classified_files:
                    clean_files.append(filename)
            edenstatus.clean = clean_files

        status = scmutil.status(
            [f for f in edenstatus.modified if match(f)],
            [f for f in edenstatus.added if match(f)],
            [f for f in edenstatus.removed if match(f)],
            [f for f in edenstatus.deleted if match(f)],
            [f for f in edenstatus.unknown if match(f)],
            [f for f in edenstatus.ignored if match(f)],
            [f for f in edenstatus.clean if match(f)],
        )

        return (unsure, status)

    def matches(self, match):  # override
        return self._eden_walk_helper(
            match, deleted=False, unknown=False, ignored=False
        )

    def _droppath(self, f):  # override
        # This is a copy/paste of dirstate._droppath, but with the references to
        # self._dirs and self._filefoldmap removed.
        self._updatedfiles.add(f)

    def _addpath(self, f, state, mode, size, mtime):  # override
        # This is a copy/paste of dirstate._addpath, but with the references to
        # self._dirs removed.
        oldstate = self[f]
        if state == 'a' or oldstate == 'r':
            scmutil.checkfilename(f)

        self._updatedfiles.add(f)
        self._map[f] = dirstatetuple(state, mode, size, mtime)

    def write(self, tr):  # override
        '''This appears to be called from localrepo.'''
        pass

    def savebackup(self, tr, backupname):  # override
        '''Save current dirstate into backup file'''
        backup_file = self._opener(backupname, 'w', atomictemp=True)
        # TODO(mbolin): Notify _plchangecallbacks in setparents() even though
        # dirstate.py does it in this method.
        parents = self.parents()
        backup_file.write(parents[0] + parents[1])
        backup_file.close()

        if tr:
            # ensure that pending file written above is unlinked at
            # failure, even if tr.writepending isn't invoked until the
            # end of this transaction
            tr.registertmp(backupname, location='plain')

    def _writedirstate(self, st):  # override
        raise NotImplementedError(
            'No one should try to invoke _writedirstate() in eden_dirstate.')

    def restorebackup(self, tr, backupname):  # override
        '''
        Args:
            tr (transaction?): such as `repo.currenttransaction()` or None.
            backupname (str): Filename to pass to opener for reading data.
        '''
        # this "invalidate()" prevents "wlock.release()" from writing
        # changes of dirstate out after restoring from backup file
        self.invalidate()

        backup_data = self._opener.read(backupname).strip()
        p1 = node.nullid
        p2 = node.nullid
        if backup_data is not None:
            assert len(backup_data) == 40
            p1 = backup_data[0:20]
            p2 = backup_data[20:40]

        with self.parentchange():
            self.setparents(p1, p2)

        self._opener.tryunlink(backupname)

    def _opendirstatefile(self):  # override
        raise NotImplementedError(
            'No one should try to invoke _opendirstatefile() in eden_dirstate.')
