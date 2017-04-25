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

import os
import pickle
import subprocess


class LameThriftClient(object):
    def __init__(self, pyremote, eden_dir=None, mounted_path=None):
        self._pyremote = pyremote
        if mounted_path:
            self._eden_socket = os.path.join(mounted_path, '.eden', 'socket')
        else:
            self._eden_socket = os.path.join(eden_dir, 'socket')

    def open(self):
        pass

    def close(self):
        pass

    def getCurrentSnapshot(self, mountPoint):
        result = self._call_binary(['getCurrentSnapshot', mountPoint])
        # There will be a trailing newline on the output.  Strip it off
        if len(result) != 21:
            raise Exception('unexpected output from getCurrentSnapshot(): %r' %
                            result)
        return result[:20]

    def checkOutRevision(self, mountPoint, snapshotHash, force):
        return self._call(['checkOutRevision', mountPoint, snapshotHash,
                           str(force)])

    def resetParentCommit(self, mountPoint, snapshotHash):
        return self._call(['resetParentCommit', mountPoint, snapshotHash])

    def scmAdd(self, mountPoint, paths):
        return self._call(['scmAdd', mountPoint, repr(paths)])

    def scmRemove(self, mountPoint, paths, force):
        return self._call(['scmRemove', mountPoint, repr(paths), str(force)])

    def scmGetStatus(self, mountPoint, listIgnored):
        return self._call(['scmGetStatus', mountPoint, repr(listIgnored)])

    def _call_binary(self, api_args):

        proc = subprocess.Popen(
            [self._pyremote, '--path', self._eden_socket, '-f', '--stdin', api_args[0]],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
        )
        output, error = proc.communicate(pickle.dumps(api_args[1:]))
        if proc.returncode != 0:
            raise Exception('error making eden thrift call via pyremote: %r' %
                            (error,))
        return output

    def _call(self, api_args):
        output = self._call_binary(api_args)
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


class ScmAddRemoveError(object):
    def __init__(self, path, errorMessage):
        self.path = path
        self.errorMessage = errorMessage
