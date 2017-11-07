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

import sys

from mercurial import node
from mercurial import demandimport

import eden.dirstate

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

# Disable demandimport while importing thrift files.
#
# The thrift modules try importing modules which may or may not exist, and they
# handle the ImportError generated if the modules aren't present.  demandimport
# breaks this behavior by making it appear like the modules were successfully
# loaded, and only throwing ImportError later when you actually try to use
# them.
with demandimport.deactivated():
    import eden.thrift as eden_thrift_module
    import facebook.eden.ttypes as eden_ttypes

create_thrift_client = eden_thrift_module.create_thrift_client
ScmFileStatus = eden_ttypes.ScmFileStatus
ConflictType = eden_ttypes.ConflictType
FileInformationOrError = eden_ttypes.FileInformationOrError
ManifestEntry = eden_ttypes.ManifestEntry
NoValueForKeyError = eden_ttypes.NoValueForKeyError


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
        return (
            'ClientStatus(modified={modified}; added={added}; '
            'removed={removed}; deleted={deleted}; unknown={unknown}; '
            'ignored={ignored}; clean={clean}'
        ).format(
            modified=self.modified,
            added=self.added,
            removed=self.removed,
            deleted=self.deleted,
            unknown=self.unknown,
            ignored=self.ignored,
            clean=self.clean
        )


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

    def getManifestEntry(self, relativePath):
        with self._get_client() as client:
            return client.getManifestEntry(self._root, relativePath)

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

    def getStatus(self, list_ignored, dirstate_map, ui):  # noqa: C901
        # type(bool, eden_dirstate_map, ui) -> ClientStatus
        status = ClientStatus()
        with self._get_client() as client:
            thrift_hg_status = client.getScmStatus(self._root, list_ignored)

        # The Eden getStatus() Thrift call is not SCM-aware. We must incorporate
        # the information in our eden_dirstate_map to determine the overall
        # status.

        dirstates = dirstate_map.create_clone_of_internal_map()

        for path, code in thrift_hg_status.entries.iteritems():
            if code == ScmFileStatus.MODIFIED:
                # It is possible that the user can mark a file for removal, but
                # then modify it. If it is marked for removal, it should be
                # reported as such by `hg status` even though it is still on
                # disk.
                dirstate = dirstates.pop(path, None)
                if dirstate and dirstate[0] == 'r':
                    status.removed.append(path)
                else:
                    status.modified.append(path)
            elif code == ScmFileStatus.REMOVED:
                # If the file no longer exits, we must check to see whether the
                # user explicitly marked it for removal.
                dirstate = dirstates.pop(path, None)
                if dirstate and dirstate[0] == 'r':
                    status.removed.append(path)
                else:
                    status.deleted.append(path)
            elif code == ScmFileStatus.ADDED:
                dirstate = dirstates.pop(path, None)
                if dirstate:
                    state = dirstate[0]
                    if state == 'a' or (
                        state == 'n' and
                        dirstate[2] == eden.dirstate.MERGE_STATE_OTHER_PARENT
                    ):
                        status.added.append(path)
                    else:
                        status.unknown.append(path)
                else:
                    status.unknown.append(path)
            elif code == ScmFileStatus.IGNORED:
                # Although Eden may think the file should be ignored as per
                # .gitignore, it is possible the user has overridden that
                # default behavior by marking it for addition.
                dirstate = dirstates.pop(path, None)
                if dirstate and dirstate[0] == 'a':
                    status.added.append(path)
                else:
                    status.ignored.append(path)
            else:
                raise Exception('Unexpected status code: %s' % code)

        for path, entry in dirstates.iteritems():
            state = entry[0]
            if state == 'm':
                if entry[2] == 0:
                    ui.warn(
                        'Unexpected Nonnormal file ' + path + ' has a '
                        'merge state of NotApplicable while its has been '
                        'marked as "needs merging".'
                    )
                else:
                    status.modified.append(path)
            elif state == 'a':
                # TODO(mbolin): If `list_ignored` is `False`, we must verify
                # that `path` is on disk before reporting it as added. If it is
                # not on disk, then it should be reported as missing. This could
                # happen if the user has done an `hg add` to override an
                # `.hgignore` and then deleted the file.
                status.deleted.append(path)
            elif state == 'r':
                status.removed.append(path)

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
