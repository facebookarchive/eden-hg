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
import sys

from mercurial import node
from mercurial import demandimport

# Disable demandimport while importing thrift files.
#
# The thrift modules try importing modules which may or may not exist, and they
# handle the ImportError generated if the modules aren't present.  demandimport
# breaks this behavior by making it appear like the modules were successfully
# loaded, and only throwing ImportError later when you actually try to use
# them.
with demandimport.deactivated():
    try:
        if sys.version_info < (2, 7, 6):
            # 2.7.6 was the first version to allow unicode format strings in
            # struct.{pack,unpack}; our devservers have 2.7.5, so let's
            # monkey patch in support for unicode format strings.
            import struct

            orig_pack = struct.pack
            orig_unpack = struct.unpack

            def wrap_pack(fmt, *args):
                if isinstance(fmt, unicode):
                    fmt = fmt.encode('utf-8')
                return orig_pack(fmt, *args)

            def wrap_unpack(fmt, data):
                if isinstance(fmt, unicode):
                    fmt = fmt.encode('utf-8')
                return orig_unpack(fmt, data)

            struct.pack = wrap_pack
            struct.unpack = wrap_unpack

        # Look for the native thrift client relative to our local file.
        #
        # Our file should be "hgext3rd/eden/__init__.py", inside a directory
        # that also contains the other thrift modules required to talk to eden.
        archive_root = os.path.normpath(os.path.join(__file__, '../../..'))
        sys.path.insert(0, archive_root)

        import eden.thrift as eden_thrift_module
        import facebook.eden.ttypes as eden_ttypes
        import facebook.hgdirstate.ttypes as hg_ttypes
        thrift_client_type = 'native'
    except Exception:
        # If we fail to import eden.thrift, fall back to using the
        # LameThriftClient module for now.  At the moment we build the
        # eden.thrift modules with fairly recent versions of gcc and glibc, but
        # mercurial is often invoked with the system version of python, which
        # cannot import modules compiled against newer glibc versions.
        #
        # Eventually this fallback should be removed once we make sure
        # mercurial is always deployed to use our newer python builds.  For now
        # it is in place to ease development.
        from . import LameThriftClient as eden_thrift_module
        eden_ttypes = eden_thrift_module
        from . import LameHgTypes as hg_ttypes
        thrift_client_type = 'lame'

create_thrift_client = eden_thrift_module.create_thrift_client
StatusCode = eden_ttypes.StatusCode
ConflictType = eden_ttypes.ConflictType
FileInformationOrError = eden_ttypes.FileInformationOrError
HgNonnormalFile = eden_ttypes.HgNonnormalFile
NoValueForKeyError = eden_ttypes.NoValueForKeyError

DirstateCopymap = hg_ttypes.DirstateCopymap
DirstateMergeState = hg_ttypes.DirstateMergeState
DirstateNonnormalFileStatus = hg_ttypes.DirstateNonnormalFileStatus
DirstateNonnormalFile = hg_ttypes.DirstateNonnormalFile
DirstateNonnormalFiles = hg_ttypes.DirstateNonnormalFiles
DirstateTuple = hg_ttypes.DirstateTuple


class ClientStatus(object):
    def __init__(self):
        self.modified = []
        self.added = []
        self.removed = []
        self.deleted = []
        self.unknown = []
        self.ignored = []
        self.clean = []

    def __repr__(self):
        return ('ClientStatus(modified={modified}; added={added}; '
                'removed={removed}; deleted={deleted}; unknown={unknown}; '
                'ignored={ignored}; clean={clean}').format(
            modified=self.modified,
            added=self.added,
            removed=self.removed,
            deleted=self.deleted,
            unknown=self.unknown,
            ignored=self.ignored,
            clean=self.clean)


class EdenThriftClient(object):
    def __init__(self, repo):
        self._root = repo.root

    def _get_client(self):
        '''
        Create a new client instance for each call because we may be idle
        (from the perspective of the server) between calls and have our
        connection snipped by the server.
        We could potentially try to speculatively execute a call and
        reconnect on transport failure, but for the moment this strategy
        is a reasonable compromise.
        '''
        return create_thrift_client(mounted_path=self._root)

    def getParentCommits(self):
        '''
        Returns a tuple containing the IDs of the working directory's parent
        commits.

        The first element of the tuple is always a 20-byte binary value
        containing the commit ID.

        The second element of the tuple is None if there is only one parent,
        or the second parent ID as a 20-byte binary value.
        '''
        with self._get_client() as client:
            parents = client.getParentCommits(self._root)
        return (parents.parent1, parents.parent2)

    def setHgParents(self, p1, p2):
        if p2 == node.nullid:
            p2 = None

        parents = eden_ttypes.WorkingDirectoryParents(parent1=p1, parent2=p2)
        with self._get_client() as client:
            client.resetParentCommits(self._root, parents)

    def getStatus(self, list_ignored):
        status = ClientStatus()
        with self._get_client() as client:
            thrift_hg_status = client.scmGetStatus(self._root, list_ignored)

        for path, code in thrift_hg_status.entries.iteritems():
            if code == StatusCode.MODIFIED:
                status.modified.append(path)
            elif code == StatusCode.ADDED:
                status.added.append(path)
            elif code == StatusCode.REMOVED:
                status.removed.append(path)
            elif code == StatusCode.MISSING:
                status.deleted.append(path)
            elif code == StatusCode.NOT_TRACKED:
                status.unknown.append(path)
            elif code == StatusCode.IGNORED:
                status.ignored.append(path)
            elif code == StatusCode.CLEAN:
                status.clean.append(path)
            else:
                raise Exception('Unexpected status code: %s' % code)
        return status

    def checkout(self, node, force):
        with self._get_client() as client:
            return client.checkOutRevision(self._root, node, force)

    def glob(self, globs):
        with self._get_client() as client:
            return client.glob(self._root, globs)

    def getFileInformation(self, files):
        with self._get_client() as client:
            return client.getFileInformation(self._root, files)

    def hgClearDirstate(self):
        with self._get_client() as client:
            client.hgClearDirstate(self._root)

    def hgGetDirstateTuple(self, relativePath):
        with self._get_client() as client:
            return client.hgGetDirstateTuple(self._root, relativePath)

    def hgSetDirstateTuple(self, relativePath, dirstateTuple):
        with self._get_client() as client:
            return client.hgSetDirstateTuple(self._root, relativePath,
                                               dirstateTuple)

    def hgDeleteDirstateTuple(self, relativePath):
        with self._get_client() as client:
            return client.hgDeleteDirstateTuple(self._root, relativePath)

    def hgGetNonnormalFiles(self):
        # type() -> List[HgNonnormalFile]
        with self._get_client() as client:
            return client.hgGetNonnormalFiles(self._root)

    def hgCopyMapPut(self, relativePathDest, relativePathSource):
        # type(str, str) -> None
        with self._get_client() as client:
            return client.hgCopyMapPut(self._root, relativePathDest,
                                         relativePathSource)

    def hgCopyMapGet(self, relativePathDest):
        # type(str) -> str
        with self._get_client() as client:
            return client.hgCopyMapGet(self._root, relativePathDest)

    def hgCopyMapGetAll(self):
        # type(str) -> Dict[str, str]
        with self._get_client() as client:
            return client.hgCopyMapGetAll(self._root)
