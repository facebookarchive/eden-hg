# Copyright (c) 2016-present, Facebook, Inc.
# All Rights Reserved.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

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
    return _add(repo.root, repo.dirstate, paths, repo.getcwd())


def _add(root, dirstate, paths, cwd):
    paths_to_add = map(lambda p: pathutil.canonpath(root, cwd, p), paths)
    if len(paths_to_add) == 0:
        paths_to_add.append(pathutil.canonpath(root, cwd, root))
    errors = dirstate.thrift_scm_add(paths_to_add)
    if len(errors):
        for error in errors:
            print(error.errorMessage)
        return 1
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
