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

from __future__ import absolute_import, division, print_function

import os
import sys

import eden.dirstate
from mercurial import demandimport, node


if sys.version_info < (2, 7, 6):
    # 2.7.6 was the first version to allow unicode format strings in
    # struct.{pack,unpack}; our devservers have 2.7.5, so let's
    # monkey patch in support for unicode format strings.
    import struct

    orig_pack = struct.pack
    orig_unpack = struct.unpack

    def wrap_pack(fmt, *args):
        if isinstance(fmt, unicode):
            fmt = fmt.encode("utf-8")
        return orig_pack(fmt, *args)

    def wrap_unpack(fmt, data):
        if isinstance(fmt, unicode):
            fmt = fmt.encode("utf-8")
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
CheckoutMode = eden_ttypes.CheckoutMode
ConflictType = eden_ttypes.ConflictType
FileInformationOrError = eden_ttypes.FileInformationOrError
ManifestEntry = eden_ttypes.ManifestEntry
NoValueForKeyError = eden_ttypes.NoValueForKeyError


class EdenThriftClient(object):

    def __init__(self, repo):
        self._repo = repo
        self._root = repo.root

        self._socket_path = os.readlink(os.path.join(self._root, ".eden", "socket"))
        # Read the .eden/root symlink to see what eden thinks the name of this
        # mount point is.  This might not match self._root in some cases.  In
        # particular, a parent directory of the eden mount might be bind
        # mounted somewhere else, resulting in it appearing at multiple
        # separate locations.
        self._eden_root = os.readlink(os.path.join(self._root, ".eden", "root"))

    def _get_client(self):
        """
        Create a new client instance for each call because we may be idle
        (from the perspective of the server) between calls and have our
        connection snipped by the server.
        We could potentially try to speculatively execute a call and
        reconnect on transport failure, but for the moment this strategy
        is a reasonable compromise.
        """
        return create_thrift_client(socket_path=self._socket_path)

    def getManifestEntry(self, relativePath):
        with self._get_client() as client:
            return client.getManifestEntry(self._eden_root, relativePath)

    def setHgParents(self, p1, p2):
        if p2 == node.nullid:
            p2 = None

        self._flushPendingTransactions()

        parents = eden_ttypes.WorkingDirectoryParents(parent1=p1, parent2=p2)
        with self._get_client() as client:
            client.resetParentCommits(self._eden_root, parents)

    def getStatus(self, parent, list_ignored):  # noqa: C901
        # type(str, bool) -> Dict[str, int]
        with self._get_client() as client:
            return client.getScmStatus(self._eden_root, list_ignored, parent)

    def checkout(self, node, checkout_mode):
        self._flushPendingTransactions()
        with self._get_client() as client:
            return client.checkOutRevision(self._eden_root, node, checkout_mode)

    def glob(self, globs):
        with self._get_client() as client:
            return client.glob(self._eden_root, globs)

    def getFileInformation(self, files):
        with self._get_client() as client:
            return client.getFileInformation(self._eden_root, files)

    def _flushPendingTransactions(self):
        # If a transaction is currently in progress, make sure it has flushed
        # pending commit data to disk so that eden will be able to access it.
        txn = self._repo.currenttransaction()
        if txn is not None:
            txn.writepending()
