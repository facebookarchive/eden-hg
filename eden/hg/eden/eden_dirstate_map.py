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

from . import EdenThriftClient as thrift
import collections


class eden_dirstate_map(collections.MutableMapping):
    def __init__(self, thrift_client):
        # type(eden_dirstate_map, EdenThriftClient) -> None
        self._thrift_client = thrift_client

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
        except:  # noqa: B901
            raise KeyError(filename)

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
