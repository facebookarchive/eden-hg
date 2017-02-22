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
import subprocess


class LameThriftClient(object):
    def __init__(self, pyremote, eden_dir):
        self._pyremote = pyremote
        self._eden_socket = os.path.join(eden_dir, 'socket')

    def open(self):
        pass

    def close(self):
        pass

    def getCurrentSnapshot(self, mount_point):
        result = self._call_binary(['getCurrentSnapshot', mount_point])
        # There will be a trailing newline on the output.  Strip it off
        if len(result) != 21:
            raise Exception('unexpected output from getCurrentSnapshot(): %r' %
                            result)
        return result[:20]

    def getMaterializedEntries(self, mount_point):
        return self._call(['getMaterializedEntries', mount_point])

    def scmAdd(self, mount_point, paths):
        return self._call(['scmAdd', mount_point, repr(paths)])

    def scmRemove(self, mount_point, paths, force):
        return self._call(['scmRemove', mount_point, repr(paths), str(force)])

    def scmGetStatus(self, mount_point):
        return self._call(['scmGetStatus', mount_point])

    def scmMarkCommitted(self, mount_point, node, paths_to_clear,
                         paths_to_drop):
        return self._call(['scmMarkCommitted', mount_point, node,
                           repr(paths_to_clear), repr(paths_to_drop)])

    def _call_binary(self, api_args):
        proc = subprocess.Popen(
            [self._pyremote, '--path', self._eden_socket, '-f'] + api_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        output, error = proc.communicate()
        if proc.returncode != 0:
            raise Exception('error making eden thrift call via pyremote: %r' %
                            (error,))
        return output

    def _call(self, api_args):
        output = self._call_binary(api_args)
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


def create_thrift_client(eden_dir):
    global _pyremote_path
    if _pyremote_path is None:
        _pyremote_path = _find_pyremote_path()

    return LameThriftClient(_pyremote_path, eden_dir)


# !!! HAND-GENERATED PYTHON CLASSES BASED ON eden.thrift !!!
# See buck-out/gen/eden/fs/service/thrift-py-eden.thrift/gen-py/facebook/eden/ttypes.py
# for real Python codegen.
class MaterializedResult(object):
    def __init__(self, currentPosition, fileInfo):
        self.currentPosition = currentPosition
        self.fileInfo = fileInfo  # map<string, FileInformation>

    def __str__(self):
        return repr(
            {
                'currentPosition': str(self.currentPosition),
                'fileInfo': self.fileInfo,
            }
        )


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


class TimeSpec(object):
    def __init__(self, seconds, nanoSeconds):
        self._seconds = seconds
        self._nanoSeconds = nanoSeconds


class ThriftHgStatus(object):
    def __init__(self, entries):
        self.entries = entries


class StatusCode(object):
    CLEAN = 0
    MODIFIED = 1
    ADDED = 2
    REMOVED = 3
    MISSING = 4
    NOT_TRACKED = 5
    IGNORED = 6

    _VALUES_TO_NAMES = {
        0: "CLEAN",
        1: "MODIFIED",
        2: "ADDED",
        3: "REMOVED",
        4: "MISSING",
        5: "NOT_TRACKED",
        6: "IGNORED",
    }

    _NAMES_TO_VALUES = {
        "CLEAN": 0,
        "MODIFIED": 1,
        "ADDED": 2,
        "REMOVED": 3,
        "MISSING": 4,
        "NOT_TRACKED": 5,
        "IGNORED": 6,
    }


class ScmAddRemoveError(object):
    def __init__(self, path, errorMessage):
        self.path = path
        self.errorMessage = errorMessage


if __name__ == '__main__':
    '''This takes a single arg, which is an absolute path to an Eden mount.'''
    import sys
    eden_mount = sys.argv[1]
    eden_dir = os.path.join(os.environ['HOME'], 'local/.eden')
    client = create_thrift_client(eden_dir)
    materialized_result = client.getMaterializedEntries(eden_mount)

    quote_fn = None
    try:
        import pipes
        quote_fn = pipes.quote
    except (ImportError, AttributeError):
        import shlex
        quote_fn = shlex.quote

    for filename in materialized_result.fileInfo:
        # Use quoting to make it obvious that the empty string is an entry.
        print(quote_fn(filename))
