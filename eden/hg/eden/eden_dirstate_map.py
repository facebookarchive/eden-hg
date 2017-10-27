# Copyright (c) 2016-present, Facebook, Inc.
# All Rights Reserved.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

'''Eden implementation for the dirstate._map field.
In practice, this maintains the set of nonnormalfiles in the working copy.

TODO(mbolin): Refactor things so that we can do batch updates in a single Thrift
call rather than one per __setitem__ call.
'''

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from mercurial import node
from . import EdenThriftClient as thrift
import collections


class eden_dirstate_map(collections.MutableMapping):
    def __init__(self, thrift_client, repo):
        # type(eden_dirstate_map, EdenThriftClient, localrepo) -> None
        self._thrift_client = thrift_client
        self._repo = repo
        self.copymap = eden_dirstate_copymap(thrift_client)

        # self._parents is a cache of the current parent node IDs.
        # This is a tuple of 2 20-byte binary commit IDs, or None when unset.
        self._parents = None

    def invalidate(self):
        self._parents = None

    def parents(self):  # override
        self._getparents()
        return list(self._parents)

    def setparents(self, p1, p2):  # override
        '''Set dirstate parents to p1 and p2.'''

        # If a transaction is currently in progress, make sure it has flushed
        # pending commit data to disk so that eden will be able to access it.
        txn = self._repo.currenttransaction()
        if txn is not None:
            txn.writepending()

        self._thrift_client.setHgParents(p1, p2)
        self._parents = (p1, p2)

    def _getparents(self):
        if self._parents is None:
            p1, p2 = self._thrift_client.getParentCommits()
            if p2 is None:
                p2 = node.nullid
            self._parents = (p1, p2)

    def __getitem__(self, filename):
        # type(str) -> parsers.dirstatetuple
        # TODO: Support Hg submodules.
        # Mercurial has a bit of logic that depends on whether .hgsub or
        # .hgsubstate is in the dirstate. Currently, Eden does not attempt to
        # support submodules (and none of Hg's codepaths that use submodules
        # have been tested with Eden), so the server throws an exception when
        # either .hgsub or .hgsubstate is passed to hgGetDirstateTuple().
        #
        # Because we know the Thrift call will fail, we throw the corresponding
        # KeyError in this case to avoid the overhead of the Thrift call as a
        # performance optimization.
        if filename == '.hgsub' or filename == '.hgsubstate':
            raise KeyError(filename)

        try:
            thrift_dirstate_tuple = self._thrift_client.hgGetDirstateTuple(
                filename
            )
        except thrift.NoValueForKeyError as e:
            raise KeyError(e.key)

        return thrift_dirstate_tuple_to_parsers_dirstatetuple(
            thrift_dirstate_tuple
        )

    def __setitem__(self, filename, dirstatetuple):
        # type(str, parsers.dirstatetuple) -> None
        status, mode, size, mtime = dirstatetuple

        if size == -1:
            merge_state = thrift.DirstateMergeState.BothParents
        elif size == -2:
            merge_state = thrift.DirstateMergeState.OtherParent
        else:
            merge_state = thrift.DirstateMergeState.NotApplicable

        if status == 'n':
            file_status = thrift.DirstateNonnormalFileStatus.Normal
        elif status == 'm':
            file_status = thrift.DirstateNonnormalFileStatus.NeedsMerging
        elif status == 'r':
            file_status = thrift.DirstateNonnormalFileStatus.MarkedForRemoval
        elif status == 'a':
            file_status = thrift.DirstateNonnormalFileStatus.MarkedForAddition
        elif status == '?':
            file_status = thrift.DirstateNonnormalFileStatus.NotTracked
        else:
            raise Exception('Unrecognized status: %r' % status)

        thrift_dirstate_tuple = thrift.DirstateTuple(file_status, mode, merge_state)
        self._thrift_client.hgSetDirstateTuple(filename, thrift_dirstate_tuple)

    def __delitem__(self, filename):
        self._thrift_client.hgDeleteDirstateTuple(filename)

    def __iter__(self):
        raise Exception('Should not call __iter__ on eden_dirstate_map!')

    def __len__(self):
        raise Exception('Should not call __len__ on eden_dirstate_map!')

    def nonnormalentries(self):
        '''Returns a set of filenames.'''
        # type() -> Set[str]
        # It does not appear that we need to filter anything from this list.
        return set(self._get_all_nonnormal_entries().keys())

    def otherparententries(self):
        '''Returns an iterable of (filename, parsers.dirstatetuple) pairs.'''
        # Fetch the entries in the DirstateNonnormalFiles map
        # where entry.status == DirstateNonnormalFileStatus.Normal and
        # entry.mergeState == DirstateMergeState.OtherParent.
        for k, v in self._get_all_nonnormal_entries().items():
            if v[0] == 'n' and v[2] == -2:
                yield k, v

    def _get_all_nonnormal_entries(self):
        # type() -> Dict[str, parsers.dirstatetuple]
        entries = {}
        for t in self._thrift_client.hgGetNonnormalFiles():
            entries[t.relativePath
                    ] = thrift_dirstate_tuple_to_parsers_dirstatetuple(t.tuple)
        return entries

    @property
    def nonnormalset(self):
        return self.nonnormalentries()

    @property
    def otherparentset(self):  # override
        result = set()
        for f, _s in self.otherparententries():
            result.add(f)
        return result


def thrift_dirstate_tuple_to_parsers_dirstatetuple(thrift_dirstate_tuple):
    return [
        thrift_file_status_to_code(thrift_dirstate_tuple.status),
        thrift_dirstate_tuple.mode,
        thrift_merge_status_to_code(thrift_dirstate_tuple.mergeState),
        0,  # fake mtime
    ]


def thrift_file_status_to_code(thrift_file_status):
    tfs = thrift_file_status
    if tfs == thrift.DirstateNonnormalFileStatus.Normal:
        return 'n'
    elif tfs == thrift.DirstateNonnormalFileStatus.NeedsMerging:
        return 'm'
    elif tfs == thrift.DirstateNonnormalFileStatus.MarkedForRemoval:
        return 'r'
    elif tfs == thrift.DirstateNonnormalFileStatus.MarkedForAddition:
        return 'a'
    elif tfs == thrift.DirstateNonnormalFileStatus.NotTracked:
        return '?'
    else:
        raise Exception('Unrecognized status: %r' % thrift_file_status)


def thrift_merge_status_to_code(thrift_merge_status):
    tms = thrift_merge_status
    if tms == thrift.DirstateMergeState.NotApplicable:
        return 0
    elif tms == thrift.DirstateMergeState.BothParents:
        return -1
    elif tms == thrift.DirstateMergeState.OtherParent:
        return -2


class eden_dirstate_copymap(collections.MutableMapping):
    def __init__(self, thrift_client):
        # type(eden_dirstate_copymap, EdenThriftClient) -> None
        self._thrift_client = thrift_client

    def _get_mapping_thrift(self):
        # type(eden_dirstate_copymap) -> Dict[str, str]
        return self._thrift_client.hgCopyMapGetAll()

    def __getitem__(self, dest_filename):
        # type(str) -> str
        try:
            return self._thrift_client.hgCopyMapGet(dest_filename)
        except thrift.NoValueForKeyError as e:
            raise KeyError(e.key)

    def __setitem__(self, dest_filename, source_filename):
        self._thrift_client.hgCopyMapPut(dest_filename, source_filename)

    def __delitem__(self, dest_filename):
        # TODO(mbolin): Setting the value to '' deletes it from the map. This
        # would be better as an explicit "remove" API.
        self._thrift_client.hgCopyMapPut(dest_filename, '')

    def __iter__(self):
        return iter(self._get_mapping_thrift())

    def __len__(self):
        raise Exception('Should not call __len__ on eden_dirstate_copymap!')

    def keys(self):
        # collections.MutableMapping implements keys(), but does so poorly--
        # it ends up calling __iter__() and then __len__(), and we want to
        # avoid making two separate thrift calls.
        return self._get_mapping_thrift().keys()

    def copy(self):
        # We return a new dict object, and not eden_dirstate_copymap() object.
        # Any mutations made to the returned copy should not affect the actual
        # dirstate, and should not be sent back to eden via thrift.
        return self._get_mapping_thrift().copy()
