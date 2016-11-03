# Copyright (c) 2016, Facebook, Inc.
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

    def getMaterializedEntries(self, mount_point):
        return self._call(['getMaterializedEntries', mount_point])

    def _call(self, api_args):
        proc = subprocess.Popen(
            [self._pyremote, '--path', self._eden_socket, '-f'] + api_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        output, error = proc.communicate()
        if proc.returncode != 0:
            raise Exception('error making eden thrift call via pyremote: %r' %
                            (error,))

        # Make sure we compile without inheriting the flags used by the current
        # source file.  In particular we want to make sure the unicode_literals
        # flag is disabled.
        code = compile(output, 'thrift_result', 'eval', 0, True)
        return eval(code)


def create_thrift_client(eden_dir):
    this_dir = os.path.dirname(os.path.realpath(__file__))
    while True:
        if os.path.exists(os.path.join(this_dir, '.buckconfig')):
            src_repo = this_dir
            break
        parent = os.path.dirname(this_dir)
        if parent == this_dir:
            raise Exception('repository root not found')
        this_dir = parent

    pyremote = os.path.join(
        src_repo,
        'buck-out/gen/eden/fs/service/thrift-EdenService-pyremote.par'
    )

    if not os.path.isfile(pyremote):
        subprocess.check_call(
            ['buck', 'build', 'eden/fs/service/...'],
            cwd=src_repo
        )

    return LameThriftClient(pyremote, eden_dir)


# !!! HAND-GENERATED PYTHON CLASSES BASED ON eden.thrift !!!
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

if __name__ == '__main__':
    '''This takes a single arg, which is an absolute path to an Eden mount.'''
    import sys
    eden_client_root = sys.argv[1]
    client = create_lame_thrift_client(eden_client_root)
    materialized_result = client.getMaterializedEntries()
    for filename in materialized_result.fileInfo:
        print(filename)
