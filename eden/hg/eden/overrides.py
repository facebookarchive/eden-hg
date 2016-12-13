# Copyright (c) 2016, Facebook, Inc.
# All Rights Reserved.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import errno
import os
import stat

from mercurial import pathutil


def add(orig, ui, repo, *paths, **opts):
    """Mark a file added."""
    if opts['dry_run']:
        raise NotImplementedError('--dry-run is not supported')
    if opts['exclude']:
        raise NotImplementedError('--exclude is not supported')
    if opts['include']:
        raise NotImplementedError('--include is not supported')
    if opts['subrepos']:
        raise NotImplementedError('--subrepos is not supported')
    _add(repo.root, repo.dirstate, paths, repo.getcwd())


def _add(root, dirstate, paths, cwd):
    # Expand all of the paths to yield the set of files to add.
    # TODO: Send the canonical paths to the server and let it decide which can
    # be `hg add`ed. Doing the walk here via the FUSE APIs could be considerably
    # more expensive, particularly for the `hg add .` case, which is common.
    files_to_add = set()
    for path in paths:
        canonical_path = pathutil.canonpath(root, cwd, path)
        try:
            mode = os.stat(os.path.join(root, canonical_path)).st_mode
            if stat.S_ISREG(mode) or stat.S_ISLNK(mode):
                files_to_add.add(canonical_path)
                break
            elif stat.S_ISDIR(mode):
                dir_to_search = os.path.join(root, canonical_path)
                for current_dir, dirs, files in os.walk(dir_to_search):
                    current = os.path.relpath(current_dir, root)
                    for name in files:
                        files_to_add.add(os.path.join(current, name))
            else:
                raise Exception('%s: unsupported mode %s' % (path, mode))
        except OSError as error:
            if error.errno == errno.ENOENT:
                raise Exception('%s: No such file or directory' % path)
            else:
                raise

    # TODO(mbolin): Sort files_to_add before iterating over it?
    # TODO(mbolin): Consider supporting a batch API for thrift_scm_add().
    for path in files_to_add:
        # TODO(mbolin): If this individual Thrift call fails, should the rest
        # succeed? Perhaps it's time to consider transactions?
        dirstate.thrift_scm_add(path)

    return 0


def remove(orig, ui, repo, *paths, **opts):
    '''Mark files removed.'''
    if opts['after']:
        raise NotImplementedError('--after is not supported')
    if opts['exclude']:
        raise NotImplementedError('--exclude is not supported')
    if opts['include']:
        raise NotImplementedError('--include is not supported')
    if opts['subrepos']:
        raise NotImplementedError('--subrepos is not supported')
    force = opts['force'] or False
    return _remove(repo.root, repo.dirstate, paths, force, repo.getcwd())


def _remove(root, dirstate, paths, force, cwd):
    paths_to_remove = map(lambda p: pathutil.canonpath(root, cwd, p), paths)
    errors = dirstate.thrift_scm_remove(paths_to_remove, force)
    if len(errors):
        for error in errors:
            print(error.errorMessage)
        return 1
    return 0
