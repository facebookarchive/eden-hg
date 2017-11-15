#!/usr/bin/env python2
# Copyright (c) 2016-present, Facebook, Inc.
# All Rights Reserved.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from mercurial import dirstate, policy, scmutil, sparse as sparsemod, util
from . import EdenThriftClient as thrift
from . import eden_dirstate_map as eden_dirstate_map
import errno
import stat
import os

parsers = policy.importmod('parsers')
propertycache = util.propertycache


class statobject(object):
    ''' this is a stat-like object to represent information from eden.'''
    __slots__ = ('st_mode', 'st_size', 'st_mtime')

    def __init__(self, mode=None, size=None, mtime=None):
        self.st_mode = mode
        self.st_size = size
        self.st_mtime = mtime


class eden_dirstate(dirstate.dirstate):
    def __init__(self, repo, ui, root):
        self.eden_client = thrift.EdenThriftClient(repo)

        # We should override any logic in dirstate that uses self._validate.
        validate = repo._dirstatevalidate

        opener = repo.vfs

        # Newer versions of mercurial require a sparsematchfn argument to the
        # dirstate.
        def sparsematchfn():
            return sparsemod.matcher(repo)
        super(eden_dirstate, self).__init__(opener, ui, root, validate,
                                            sparsematchfn)

        def create_eden_dirstate(ui, opener, root):
            return eden_dirstate_map.eden_dirstate_map(
                ui, opener, root, self.eden_client, repo
            )
        self._mapcls = create_eden_dirstate

    def __iter__(self):
        # FIXME: This appears to be called by `hg reset`, so we provide a dummy
        # response here, but really, we should outright prohibit this.
        # Most likely, we will have to replace the implementation of `hg reset`.
        if False:
            yield None
        return

    def iteritems(self):  # override
        # This seems like the type of O(repo) operation that should not be
        # allowed. Or if it is, it should be through a separate, explicit
        # codepath.
        #
        # We do provide edeniteritems() for users to iterate through only the
        # files explicitly tracked in the eden dirstate.
        raise NotImplementedError('eden_dirstate.iteritems()')

    def dirs(self):  # override
        raise NotImplementedError('eden_dirstate.dirs()')

    def edeniteritems(self):
        '''
        Walk over all items tracked in the eden dirstate.

        This includes non-normal files (e.g., files marked for addition or
        removal), as well as normal files that have merge state information.
        '''
        return self._map._map.iteritems()

    def walk(self, match, subrepos, unknown, ignored, full=True):  # override
        '''
        Walk recursively through the directory tree, finding all files
        matched by match.

        If full is False, maybe skip some known-clean files.

        Return a dict mapping filename to stat-like object
        '''
        if unknown and not ignored and not full:
            # We assume that this is being called from `hg add`, so we return
            # everything that is eligible for addition and filtered by the
            # matcher.
            # TODO(mbolin): Instead of assuming `hg add`, do something robust.
            # pre-add/post-add hooks might be appropriate.
            clean = False
            status = self.status(match, subrepos, ignored, clean, unknown)[1]
            modified, added, removed, deleted, unknown, ignored, clean = status
            candidates = unknown

            # If the file is marked for removal, but it exists on disk, then
            # include it in the list of files to add.
            for removed_file in removed:
                try:
                    mode = os.stat(os.path.join(self._root, removed_file)).st_mode
                    if stat.S_ISREG(mode) or stat.S_ISLNK(mode):
                        candidates.append(removed_file)
                except OSError as exception:
                    err = exception.errno
                    # As the file has likely been removed, it's expected that
                    # the path may not exist, or one of the components of the
                    # path prefix is not a directory.
                    if err != errno.ENOENT and err != errno.ENOTDIR:
                        raise

            matched_files = [f for f in candidates if match(f)]

            # Finally, we have to account for the case where the user has
            # explicitly tried to add an ignored file. We don't want to
            # request ignored files when we call status() because the result set
            # could be extremely large. Instead, we ask the matcher if any of
            # its patterns are for a specific file, and if so, we add such files
            # as potential candidates for addition.
            #
            # Note that users can explicitly add ignored *files*, but they
            # cannot explicitly add ignored *directories* (each file in the
            # directory would have to be added individually).
            if not util.safehasattr(match, '_eden_match_info'):
                raise NotImplementedError(
                    'match object is not eden compatible'
                    '(_eden_match_info is missing)'
                )
            info = match._eden_match_info
            files = info.get_explicitly_matched_files()
            if files:
                matched_files_set = set(matched_files)
                matched_files_set.update(files)
                matched_files = list(matched_files_set)
        else:
            matched_files = self._eden_walk_helper(
                match, deleted=True, unknown=unknown, ignored=ignored
            )

        # Now we need to build a stat-like-object for each of these results
        file_info = self.eden_client.getFileInformation(matched_files)

        results = {}
        for index, info in enumerate(file_info):
            file_name = matched_files[index]
            if info.getType() == thrift.FileInformationOrError.INFO:
                finfo = info.get_info()
                results[file_name] = statobject(
                    mode=finfo.mode, size=finfo.size, mtime=finfo.mtime
                )
            else:
                # Indicates that we knew of the file, but that is it
                # not present on disk; it has been removed.
                results[file_name] = None

        return results

    def _eden_walk_helper(self, match, deleted, unknown, ignored):
        ''' Extract the matching information we collected from the
            match constructor and try to turn it into a list of
            glob expressions.  If we don't have enough information
            for this, make_glob_list() will raise an exception '''
        if not util.safehasattr(match, '_eden_match_info'):
            raise NotImplementedError(
                'match object is not eden compatible'
                '(_eden_match_info is missing)'
            )
        info = match._eden_match_info
        globs = info.make_glob_list()

        # Expand the glob into a set of candidate files
        globbed_files = self.eden_client.glob(globs)

        # Run the results through the matcher object; this processes
        # any excludes that might be part of the matcher
        matched_files = [f for f in globbed_files if match(f)]

        if matched_files and (deleted or (not unknown) or (not ignored)):
            # !unknown as parameter means that we need to exclude
            # any files with an unknown status.
            # !ignored -> exclude any ignored files.
            # To get ignored files in the status list, we need to pass
            # True when !ignored is passed in to us.
            status = self._getStatus(not ignored)
            elide = set()
            if not unknown:
                elide.update(status.unknown)
            if not ignored:
                elide.update(status.ignored)
            if deleted:
                elide.update(status.removed)
                elide.update(status.deleted)

            matched_files = [f for f in matched_files if f not in elide]

        return matched_files

    def status(self, match, subrepos, ignored, clean, unknown):  # override
        # We should never have any files we are unsure about
        unsure = []

        edenstatus = self._getStatus(ignored)

        if clean:
            # By default, Eden's getStatus() will not return "clean" files.
            # Without any sort of filter, requesting the "clean" files is an
            # O(repo) operation, which we will not support.
            #
            # Note that remove() specifies clean=True with a matcher, which is
            # why we care about this use case. For now, we are doing
            # post-processing with the matching here on the client, but
            # ultimately, it would be best if this could be done on the server.
            if not match or match.always():
                raise Exception('Cannot request clean=True with no filter.')

            # For every file in matches that is not in edenstatus, assume that
            # it belongs in edenstatus.clean.
            classified_files = set(
                edenstatus.modified + edenstatus.added + edenstatus.removed +
                edenstatus.deleted + edenstatus.unknown + edenstatus.ignored
            )
            matches = self.matches(match)
            clean_files = []
            for filename in matches:
                if filename not in classified_files:
                    clean_files.append(filename)
            edenstatus.clean = clean_files

        status = scmutil.status(
            [f for f in edenstatus.modified if match(f)],
            [f for f in edenstatus.added if match(f)],
            [f for f in edenstatus.removed if match(f)],
            [f for f in edenstatus.deleted if match(f)],
            [f for f in edenstatus.unknown if match(f)],
            [f for f in edenstatus.ignored if match(f)],
            [f for f in edenstatus.clean if match(f)],
        )

        return (unsure, status)

    def _getStatus(self, list_ignored):
        return self.eden_client.getStatus(list_ignored, self._map, self._ui)

    def matches(self, match):  # override
        return self._eden_walk_helper(
            match, deleted=False, unknown=False, ignored=False
        )
