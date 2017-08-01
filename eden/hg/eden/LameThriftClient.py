# Copyright (c) 2016-present, Facebook, Inc.
# All Rights Reserved.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

'''
This is a lame thrift client that contains hand-generated versions of the
Python classes for the API defined in eden.thrift. This is used for testing the
Mercurial extension for Eden until we have a version of Mercurial that is built
using the same toolchain as fbthrift.
'''

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import os
import subprocess


from . import LameHgTypes as hg_ttypes
DirstateTuple = hg_ttypes.DirstateTuple


class LameThriftClient(object):
    def __init__(self, pyremote, eden_dir=None, mounted_path=None):
        self._pyremote = pyremote
        if mounted_path:
            eden_socket = os.path.join(mounted_path, '.eden', 'socket')
        else:
            eden_socket = os.path.join(eden_dir, 'socket')
        # Note that eden_socket is often a symlink. We resolve this up front to
        # avoid any potential overhead when reading this path later.
        self._eden_socket = os.path.realpath(eden_socket)

    def open(self):
        pass

    def close(self):
        pass

    def getParentCommits(self, mountPoint):
        return self._call('getParentCommits', mountPoint)

    def checkOutRevision(self, mountPoint, snapshotHash, force):
        return self._call('checkOutRevision', mountPoint, snapshotHash, force)

    def resetParentCommits(self, mountPoint, parents):
        return self._call('resetParentCommits', mountPoint, parents)

    def scmGetStatus(self, mountPoint, listIgnored):
        return self._call('scmGetStatus', mountPoint, listIgnored)

    def glob(self, mountPoint, globs):
        return self._call('glob', mountPoint, globs)

    def getFileInformation(self, mountPoint, files):
        return self._call('getFileInformation', mountPoint, files)

    def hgGetDirstateTuple(self, mountPoint, relativePath):
        return self._call('hgGetDirstateTuple', mountPoint, relativePath)

    def hgSetDirstateTuple(self, mountPoint, relativePath, dirstateTuple):
        return self._call('hgSetDirstateTuple', mountPoint, relativePath,
                          dirstateTuple)

    def hgGetNonnormalFiles(self, mountPoint):
        return self._call('hgGetNonnormalFiles', mountPoint)

    def hgCopyMapPut(self, mountPoint, relativePathDest, relativePathSource):
        return self._call('hgCopyMapPut', mountPoint, relativePathDest,
                          relativePathSource)

    def hgCopyMapGet(self, mountPoint, relativePathDest):
        return self._call('hgCopyMapGet', mountPoint, relativePathDest)

    def hgCopyMapGetAll(self, mountPoint):
        return self._call('hgCopyMapGetAll', mountPoint)

    def _call_binary(self, function, *function_args):
        arg_data = json.dumps([repr(arg) for arg in function_args])
        cmd = [
            self._pyremote, '--path', self._eden_socket, '-f', '--stdin',
            function,
        ]
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            close_fds=True,
        )
        output, error = proc.communicate(arg_data)
        if proc.returncode != 0:
            raise Exception('error making eden thrift call via pyremote: %r' %
                            (error,))
        return output

    def _call(self, function, *api_args):
        output = self._call_binary(function, *api_args)
        if output.startswith('Exception:\n'):
            msg = output[len('Exception:\n'):]
            raise Exception(msg)

        # Make sure we compile without inheriting the flags used by the current
        # source file.  In particular we want to make sure the unicode_literals
        # flag is disabled.
        code = compile(output, 'thrift_result', 'eval', 0, True)
        return eval(code)


def _find_pyremote_path():
    path = os.environ.get('EDENFS_LAME_THRIFT_PAR')
    if path:
        if not os.path.exists(path):
            raise Exception('Specified pyremote does not exist: ' + path)
        return path

    # Check in the current directory
    this_dir = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(this_dir, 'thrift-EdenService-pyremote.par')
    if os.path.exists(path):
        return path

    # Look upwards to find it in a build directory
    build_path = 'buck-out/gen/eden/fs/service/thrift-EdenService-pyremote.par'
    while True:
        path = os.path.join(this_dir, build_path)
        if os.path.exists(path):
            return path

        parent = os.path.dirname(this_dir)
        if parent == this_dir:
            raise Exception('Could not find eden pyremote binary')
        this_dir = parent


_pyremote_path = None


def create_thrift_client(eden_dir=None, mounted_path=None):
    global _pyremote_path
    if _pyremote_path is None:
        _pyremote_path = _find_pyremote_path()

    return LameThriftClient(_pyremote_path,
                            eden_dir=eden_dir,
                            mounted_path=mounted_path)


# !!! HAND-GENERATED PYTHON CLASSES BASED ON eden.thrift !!!
# See buck-out/gen/eden/fs/service/thrift-py-eden.thrift/gen-py/facebook/eden/ttypes.py
# for real Python codegen.
class WorkingDirectoryParents(object):
    def __init__(self, parent1=None, parent2=None,):
        self.parent1 = parent1
        self.parent2 = parent2

    def __repr__(self):
        return ('WorkingDirectoryParents(parent1=%r, parent2=%r)' %
                (self.parent1, self.parent2))


class JournalPosition(object):
    def __init__(self, snapshotHash, mountGeneration, sequenceNumber):
        self.snapshotHash = snapshotHash
        self.mountGeneration = mountGeneration
        self.sequenceNumber = sequenceNumber

    def __str__(self):
        return repr(
            {
                'snapshotHash': self.snapshotHash,
                'mountGeneration': self.mountGeneration,
                'sequenceNumber': self.sequenceNumber
            }
        )


class FileInformation(object):
    def __init__(self, size, mtime, mode):
        self.size = size
        self.mtime = mtime
        self.mode = mode


class FileInformationOrError(object):
    """
    Holds information about a file, or an error in retrieving that info.
    The most likely error will be ENOENT, implying that the file doesn't exist.

    Attributes:
    - info
    - error
    """

    __EMPTY__ = 0
    INFO = 1
    ERROR = 2

    def __init__(self, info=None, error=None):
        if info:
            self.set_info(info)
        else:
            self.set_error(error)

    def get_info(self):
        assert self.field == 1
        return self.value

    def get_error(self):
        assert self.field == 2
        return self.value

    def set_info(self, value):
        self.field = 1
        self.value = value

    def set_error(self, value):
        self.field = 2
        self.value = value

    def getType(self):
        return self.field


class CheckoutConflict(object):
    def __init__(self, path, type, message):
        self.path = path
        self.type = type
        self.message = message


def _define_enum(*args, **kwargs):
    class EnumClass(object):
        _NAMES_TO_VALUES = {}
        _VALUES_TO_NAMES = {}

    def define_value(k, v):
        assert v not in EnumClass._VALUES_TO_NAMES
        setattr(EnumClass, k, v)
        EnumClass._NAMES_TO_VALUES[k] = v
        EnumClass._VALUES_TO_NAMES[v] = k

    auto_value = 0
    for k in args:
        define_value(k, auto_value)
        auto_value += 1

    for k, v in kwargs.items():
        define_value(k, v)

    return EnumClass


ConflictType = _define_enum(
    'ERROR',
    'MODIFIED_REMOVED',
    'UNTRACKED_ADDED',
    'REMOVED_MODIFIED',
    'MISSING_REMOVED',
    'MODIFIED')


class TimeSpec(object):
    def __init__(self, seconds, nanoSeconds):
        self._seconds = seconds
        self._nanoSeconds = nanoSeconds


class ThriftHgStatus(object):
    def __init__(self, entries):
        self.entries = entries


StatusCode = _define_enum(
    'CLEAN',
    'MODIFIED',
    'ADDED',
    'REMOVED',
    'MISSING',
    'NOT_TRACKED',
    'IGNORED')


class HgNonnormalFile(object):
    __slots__ = ('relativePath', 'tuple')

    def __init__(self, relativePath, tuple):
        self.relativePath = relativePath
        self.tuple = tuple

    def __repr__(self):
        return ('HgNonnormalFile(relativePath=%r, tuple=%r)' %
                (self.relativePath, self.tuple))
