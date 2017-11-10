# Copyright (c) 2016-present, Facebook, Inc.
# All Rights Reserved.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

'''Eden implementation for the dirstatemap class.'''

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import errno
from mercurial import dirstate, util
from six import iteritems
from . import EdenThriftClient as thrift
import eden.dirstate

MERGE_STATE_NOT_APPLICABLE = eden.dirstate.MERGE_STATE_NOT_APPLICABLE
MERGE_STATE_BOTH_PARENTS = eden.dirstate.MERGE_STATE_BOTH_PARENTS
MERGE_STATE_OTHER_PARENT = eden.dirstate.MERGE_STATE_OTHER_PARENT
DUMMY_MTIME = 0


class eden_dirstate_map(dirstate.dirstatemap):
    def __init__(self, ui, opener, root, thrift_client, repo):
        # type(eden_dirstate_map, ui, opener, str, EdenThriftClient) -> None
        super(eden_dirstate_map, self).__init__(ui, opener, root)
        # Unlike the default self._map, our values in self._map are tuples of
        # the form: (status: char, mode: uint32, merge_state: int8).
        self._thrift_client = thrift_client
        self._repo = repo

    def setparents(self, p1, p2):
        # If a transaction is currently in progress, make sure it has flushed
        # pending commit data to disk so that eden will be able to access it.
        txn = self._repo.currenttransaction()
        if txn is not None:
            txn.writepending()

        super(eden_dirstate_map, self).setparents(p1, p2)
        # TODO(mbolin): Do not make this Thrift call to Eden until the
        # transaction is committed.
        self._thrift_client.setHgParents(p1, p2)

    def write(self, file, now):  # override
        # type(eden_dirstate_map, IO[str], float)
        parents = self.parents()

        # Remove all "clean" entries before writing. (It's possible we should
        # never allow these to be inserted into self._map in the first place.)
        to_remove = []
        for path, v in iteritems(self._map):
            if v[0] == 'n' and v[2] == MERGE_STATE_NOT_APPLICABLE:
                to_remove.append(path)
        for path in to_remove:
            self._map.pop(path)

        eden.dirstate.write(file, parents, self._map, self.copymap)
        file.close()
        self._dirtyparents = False
        self.nonnormalset, self.otherparentset = self.nonnormalentries()

    def read(self):  # override
        # ignore HG_PENDING because identity is used only for writing
        self.identity = util.filestat.frompath(
            self._opener.join(self._filename)
        )

        try:
            fp = self._opendirstatefile()
            try:
                parents, dirstate_tuples, copymap = eden.dirstate.read(
                    fp, self._filename
                )
            finally:
                fp.close()
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise
            else:
                # If the dirstate file does not exist, then we silently ignore
                # the error because that's what Mercurial's dirstate does.
                return

        if not self._dirtyparents:
            self.setparents(*parents)
        self._map = dirstate_tuples
        self.copymap = copymap

    def iteritems(self):
        raise Exception('Should not invoke iteritems() on eden_dirstate_map!')

    def __len__(self):
        raise Exception('Should not invoke __len__ on eden_dirstate_map!')

    def __iter__(self):
        raise Exception('Should not invoke __iter__ on eden_dirstate_map!')

    def keys(self):
        raise Exception('Should not invoke keys() on eden_dirstate_map!')

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def __contains__(self, key):
        return self.get(key) is not None

    def __getitem__(self, filename):
        # type(str) -> parsers.dirstatetuple
        entry = self._map.get(filename)
        if entry is not None:
            status, mode, merge_state = entry
            return [status, mode, merge_state, DUMMY_MTIME]
        # TODO: Support Hg submodules.
        # Mercurial has a bit of logic that depends on whether .hgsub or
        # .hgsubstate is in the dirstate. Currently, Eden does not attempt to
        # support submodules (and none of Hg's codepaths that use submodules
        # have been tested with Eden), so don't bother to go to the server when
        # either .hgsub or .hgsubstate is passed in.
        #
        # Because we know the Thrift call will fail, we throw the corresponding
        # KeyError in this case to avoid the overhead of the Thrift call as a
        # performance optimization.
        if filename == '.hgsub' or filename == '.hgsubstate':
            raise KeyError(filename)

        try:
            # TODO: Consider fetching this from the commit context rather than
            # querying Eden for this information.
            manifest_entry = self._thrift_client.getManifestEntry(filename)
            return [
                'n', manifest_entry.mode, MERGE_STATE_NOT_APPLICABLE,
                DUMMY_MTIME
            ]
        except thrift.NoValueForKeyError as e:
            raise KeyError(e.key)

    def hastrackeddir(self, d):  # override
        # TODO(mbolin): Unclear whether it is safe to hardcode this to False.
        return False

    def hasdir(self, d):  # override
        # TODO(mbolin): Unclear whether it is safe to hardcode this to False.
        return False

    def _insert_tuple(self, filename, state, mode, size, mtime):  # override
        if size != MERGE_STATE_BOTH_PARENTS and size != MERGE_STATE_OTHER_PARENT:
            merge_state = MERGE_STATE_NOT_APPLICABLE
        else:
            merge_state = size

        self._map[filename] = (state, mode, merge_state)

    def nonnormalentries(self):
        '''Returns a set of filenames.'''
        # type() -> Tuple[Set[str], Set[str]]
        nonnorm = set()
        otherparent = set()
        for path, entry in iteritems(self._map):
            if entry[0] != 'n':
                nonnorm.add(path)
            elif entry[2] == MERGE_STATE_OTHER_PARENT:
                otherparent.add(path)
        return nonnorm, otherparent

    def create_clone_of_internal_map(self):
        return dict(self._map)
