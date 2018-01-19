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
from six import iteritems

# Update sys.path so that we can find modules that we need.
#
# Our file should be "hgext3rd/eden/__init__.py", inside a directory
# that also contains the other eden library modules.
archive_root = os.path.normpath(os.path.join(__file__, '../../..'))
sys.path.insert(0, archive_root)

from mercurial import (error, extensions, hg, localrepo, util)
from mercurial import merge as mergemod
from mercurial.i18n import _
from mercurial import match as matchmod
from . import EdenThriftClient as thrift
from . import constants
CheckoutMode = thrift.CheckoutMode
ConflictType = thrift.ConflictType

_repoclass = localrepo.localrepository

# Import the "cmdtable" variable from our commands module.
# Note that this is not unused even though we do not appear to use it:
# the mercurial extension framework automatically looks for this variable
# and will register all commands it contains.
from .commands import cmdtable  # noqa: F401


def extsetup(ui):
    orig = localrepo.localrepository.dirstate
    # For some reason, localrepository.invalidatedirstate() does not call
    # dirstate.invalidate() by default, so we must wrap it.
    extensions.wrapfunction(
        localrepo.localrepository, 'invalidatedirstate', invalidatedirstate
    )
    extensions.wrapfunction(mergemod, 'update', merge_update)
    extensions.wrapfunction(hg, '_showstats', update_showstats)
    extensions.wrapfunction(orig, 'func', wrapdirstate)
    extensions.wrapfunction(matchmod, 'match', wrap_match)
    extensions.wrapfunction(matchmod, 'exact', wrap_match_exact)
    orig.paths = ()

    _repoclass._basesupported.add(constants.requirement)


def invalidatedirstate(orig, self):
    if constants.requirement in self.requirements:
        self.dirstate.invalidate()
    else:
        # In Eden, we do not want the original behavior of
        # localrepository.invalidatedirstate because it operates on the private
        # _filecache property of dirstate, which is not a field we provide in
        # edendirstate.
        orig(self)


# This function replaces the update() function in mercurial's mercurial.merge
# module.   It's signature must match the original mercurial.merge.update()
# function.
def merge_update(
    orig,
    repo,
    node,
    branchmerge,
    force,
    ancestor=None,
    mergeancestor=False,
    labels=None,
    matcher=None,
    mergeforce=False,
    updatecheck=None,
    wc=None
):
    '''Apparently node can be a 20-byte hash or an integer referencing a
    revision number.
    '''
    assert node is not None

    if not util.safehasattr(repo.dirstate, 'eden_client'):
        why_not_eden = 'This is not an eden repository.'
    elif matcher is not None and not matcher.always():
        why_not_eden = (
            'We don\'t support doing a partial update through '
            'eden yet.'
        )
    elif branchmerge:
        # TODO: We potentially should support handling this scenario ourself in
        # the future.  For now we simply haven't investigated what the correct
        # semantics are in this case.
        why_not_eden = 'branchmerge is "truthy:" %s.' % branchmerge
    elif ancestor is not None:
        # TODO: We potentially should support handling this scenario ourself in
        # the future.  For now we simply haven't investigated what the correct
        # semantics are in this case.
        why_not_eden = 'ancestor is not None: %s.' % ancestor
    elif wc is not None and wc.isinmemory():
        # In memory merges do not operate on the working directory,
        # so we don't need to ask eden to change the working directory state
        # at all, and can use the vanilla merge logic in this case.
        why_not_eden = 'merge is in-memory'
    else:
        # TODO: We probably also need to set why_not_eden if there are
        # subrepositories.  (Personally I might vote for just not supporting
        # subrepos in eden.)
        why_not_eden = None

    if why_not_eden:
        repo.ui.debug(
            'falling back to non-eden update code path: %s\n' % why_not_eden
        )
        return orig(
            repo,
            node,
            branchmerge,
            force,
            ancestor=ancestor,
            mergeancestor=mergeancestor,
            labels=labels,
            matcher=matcher,
            mergeforce=mergeforce,
            updatecheck=updatecheck,
            wc=wc
        )
    else:
        repo.ui.debug('using eden update code path\n')

    with repo.wlock():
        wctx = repo[None]
        parents = wctx.parents()

        p1ctx = parents[0]
        destctx = repo[node]
        deststr = str(destctx)

        if not force:
            # Make sure there isn't an outstanding merge or unresolved files.
            if len(parents) > 1:
                raise error.Abort(_('outstanding uncommitted merge'))
            ms = mergemod.mergestate.read(repo)
            if list(ms.unresolved()):
                raise error.Abort(_('outstanding merge conflicts'))

            # The vanilla merge code disallows updating between two unrelated
            # branches if the working directory is dirty.  I don't really see a
            # good reason to disallow this; it should be treated the same as if
            # we committed the changes, checked out the other branch then tried
            # to graft the changes here.

        # Invoke the preupdate hook
        repo.hook('preupdate', throw=True, parent1=deststr, parent2='')
        # note that we're in the middle of an update
        repo.vfs.write('updatestate', destctx.hex())

        stats = 0, 0, 0, 0
        actions = {}

        # Ask eden to perform the checkout
        if force:
            # eden_client.checkout() returns the list of conflicts here,
            # but since this is a force update it will have already replaced
            # the conflicts with the destination file state, so we don't have
            # to do anything with them here.
            conflicts = repo.dirstate.eden_client.checkout(
                destctx.node(), CheckoutMode.FORCE
            )
            # We do still need to make sure to update the merge state though.
            # In the non-force code path the merge state is updated in
            # _handle_update_conflicts().
            ms = mergemod.mergestate.clean(repo, p1ctx.node(), destctx.node(),
                                           labels)
            ms.commit()
        elif p1ctx == destctx:
            # No update to perform.
            pass
        else:
            # If we are in noconflict mode, then we must do a DRY_RUN first to
            # see if there are any conflicts that should prevent us from
            # attempting the update.
            if updatecheck == 'noconflict':
                conflicts = repo.dirstate.eden_client.checkout(
                    destctx.node(), CheckoutMode.DRY_RUN
                )
                if conflicts:
                    actions = _determine_actions_for_conflicts(
                        repo, p1ctx, conflicts
                    )
                    _check_actions_and_raise_if_there_are_conflicts(actions)

            conflicts = repo.dirstate.eden_client.checkout(
                destctx.node(), CheckoutMode.NORMAL
            )
            # TODO(mbolin): Add a warning if we did a DRY_RUN and the conflicts
            # we get here do not match. Only in the event of a race would we
            # expect them to differ from when the DRY_RUN was done (or if we
            # decide that DIRECTORY_NOT_EMPTY conflicts do not need to be
            # reported during a DRY_RUN).

            stats, actions = _handle_update_conflicts(
                repo, wctx, p1ctx, destctx, labels, conflicts, force
            )

        with repo.dirstate.parentchange():
            if force:
                # If the user has done an `update --clean`, then we should
                # remove all entries from the dirstate. Note this call to
                # clear() will also remove the parents, but we set them on the
                # next line, so we'll be OK.
                repo.dirstate.clear()
            # TODO(mbolin): Set the second parent, if appropriate.
            repo.setparents(destctx.node())
            mergemod.recordupdates(repo, actions, branchmerge)

            # Clear the update state
            util.unlink(repo.vfs.join('updatestate'))

    # Invoke the update hook
    repo.hook('update', parent1=deststr, parent2='', error=stats[3])

    return stats


def update_showstats(orig, repo, stats, quietempty=False):
    # We hide the updated and removed counts, because they are not accurate
    # with eden.  One of the primary goals of eden is that the entire working
    # directory does not need to be accessed or traversed on update operations.
    (updated, merged, removed, unresolved) = stats
    if merged or unresolved:
        repo.ui.status(
            _('%d files merged, %d files unresolved\n') % (merged, unresolved)
        )
    elif not quietempty:
        repo.ui.status(_('update complete\n'))


def _handle_update_conflicts(repo, wctx, src, dest, labels, conflicts, force):
    # When resolving conflicts during an update operation, the working
    # directory (wctx) is one side of the merge, the destination commit (dest)
    # is the other side of the merge, and the source commit (src) is treated as
    # the common ancestor.
    #
    # This is what we want with respect to the graph topology.  If we are
    # updating from commit A (src) to B (dest), and the real ancestor is C, we
    # effectively treat the update operation as reverting all commits from A to
    # C, then applying the commits from C to B.  We are then trying to re-apply
    # the local changes in the working directory (against A) to the new
    # location B.  Using A as the common ancestor in this operation is the
    # desired behavior.
    actions = _determine_actions_for_conflicts(repo, src, conflicts)
    return _applyupdates(repo, actions, wctx, dest, labels, conflicts)


def _determine_actions_for_conflicts(repo, src, conflicts):
    '''Calculate the actions for _applyupdates().'''
    # Build a list of actions to pass to mergemod.applyupdates()
    actions = dict(
        (m, [])
        for m in [
            'a',
            'am',
            'cd',
            'dc',
            'dg',
            'dm',
            'e',
            'f',
            'g',  # create or modify
            'k',
            'm',
            'p',  # path conflicts
            'pr',  # files to rename
            'r',
        ]
    )

    for conflict in conflicts:
        # The action tuple is:
        # - path_in_1, path_in_2, path_in_ancestor, move, ancestor_node

        if conflict.type == ConflictType.ERROR:
            # We don't record this as a conflict for now.
            # We will report the error, but the file will show modified in
            # the working directory status after the update returns.
            repo.ui.write_err(
                _('error updating %s: %s\n') %
                (conflict.path, conflict.message)
            )
            continue
        elif conflict.type == ConflictType.MODIFIED_REMOVED:
            action_type = 'cd'
            action = (conflict.path, None, conflict.path, False, src.node())
            prompt = "prompt changed/deleted"
        elif conflict.type == ConflictType.UNTRACKED_ADDED:
            # In core Mercurial, this is the case where the file does not exist
            # in the manifest of the common ancestor for the merge.
            # TODO(mbolin): Check for the "both renamed from " case in
            # manifestmerge(), which is the other possibility when the file
            # does not exist in the manifest of the common ancestor for the
            # merge.
            action_type = 'm'
            action = (conflict.path, conflict.path, None, False, src.node())
            prompt = 'both created'
        elif conflict.type == ConflictType.REMOVED_MODIFIED:
            action_type = 'dc'
            action = (None, conflict.path, conflict.path, False, src.node())
            prompt = "prompt deleted/changed"
        elif conflict.type == ConflictType.MISSING_REMOVED:
            # Nothing to do here really.  The file was already removed
            # locally in the working directory before, and it was removed
            # in the new commit.
            continue
        elif conflict.type == ConflictType.MODIFIED_MODIFIED:
            action_type = 'm'
            action = (
                conflict.path, conflict.path, conflict.path, False, src.node()
            )
            prompt = "versions differ"
        elif conflict.type == ConflictType.DIRECTORY_NOT_EMPTY:
            # This is a file in a directory that Eden would have normally
            # removed as part of the checkout, but it could not because this
            # untracked file was here. Just leave it be.
            continue
        else:
            raise Exception(
                'unknown conflict type received from eden: '
                '%r, %r, %r' % (conflict.type, conflict.path, conflict.message)
            )

        actions[action_type].append((conflict.path, action, prompt))

    return actions


def _check_actions_and_raise_if_there_are_conflicts(actions):
    # In stock Hg, update() performs this check once it gets the set of actions.
    for action_type, list_of_tuples in iteritems(actions):
        if len(list_of_tuples) == 0:
            continue  # Note `actions` defaults to [] for all keys.
        if action_type not in ('g', 'k', 'e', 'r', 'pr'):
            msg = _('conflicting changes')
            hint = _('commit or update --clean to discard changes')
            raise error.Abort(msg, hint=hint)


def _applyupdates(repo, actions, wctx, dest, labels, conflicts):
    numerrors = sum(1 for c in conflicts if c.type == ConflictType.ERROR)

    # Call applyupdates
    # Note that applyupdates may mutate actions.
    stats = mergemod.applyupdates(
        repo, actions, wctx, dest, overwrite=False, labels=labels
    )

    # Add the error count to the number of unresolved files.
    # This ensures we exit unsuccessfully if there were any errors
    return (stats[0], stats[1], stats[2], stats[3] + numerrors), actions


def reposetup(ui, repo):
    pass


def wrapdirstate(orig, repo):
    if constants.requirement not in repo.requirements:
        return orig(repo)
    else:
        from . import eden_dirstate as dirstate_reimplementation
        return dirstate_reimplementation.eden_dirstate(repo, repo.ui, repo.root)


class EdenMatchInfo(object):
    ''' Holds high fidelity information about a matching operation '''

    def __init__(self, root, cwd, exact, patterns, includes, excludes):
        self._root = root
        self._cwd = cwd
        self._includes = includes + patterns
        self._excludes = excludes
        self._exact = exact

    def make_glob_list(self):
        ''' run through the list of includes and transform it into
            a list of glob expressions. '''
        globs = []
        for kind, pat, raw in self._includes:
            if kind == 'glob':
                globs.append(pat)
                continue
            if kind in ('relpath', 'path'):
                # When a 'relpath' pattern is passed to matchmod.match(), pat is
                # relative to cwd. However, before it is passed to
                # EdenMatchInfo, it is normalized via matchmod._donormalize(),
                # so now pat is relative to self._root.
                #
                # An "exact" matcher should always match files only.
                if not self._exact and os.path.isdir(
                    os.path.join(self._root, pat)
                ):
                    if pat == '':
                        # In general, we should be wary if the user is
                        # attempting something that will match all of the files
                        # in the repo. Nevertheless, we should let it go
                        # through.
                        globs.append('**/*')
                    else:
                        globs.append(pat + '/**/*')
                else:
                    globs.append(pat)
                continue
            if kind == 'relglob':
                globs.append('**/' + pat)
                continue

            raise NotImplementedError(
                'match pattern %r is not supported by Eden' % (kind, pat, raw)
            )
        return globs

    def get_explicitly_matched_files(self):
        '''Returns a set of files (as paths relative to the repo root) that
        are identified by 'path' or 'relpath' includes. This should be the set
        of files that the user has enumerated explicitly (as opposed to
        implicitly via a directory/glob) in creating the matcher associated with
        this EdenMatchInfo.

        Note that each returned path will correspond to an *existing* file. If
        the 'path' or 'relpath' identifies either a directory or a non-existent
        file, then it will not be included in the set.
        '''
        files = set()
        for kind, pat, raw in self._includes:
            if (kind in ('relpath', 'path') and
                    os.path.isfile(os.path.join(self._root, pat))):
                files.add(pat)
        return files

    def __str__(self):
        return str(self.make_glob_list())


def wrap_match(
    orig,
    root,
    cwd,
    patterns,
    include=None,
    exclude=None,
    default='glob',
    exact=False,
    auditor=None,
    ctx=None,
    listsubrepos=False,
    warn=None,
    badfn=None,
    icasefs=False
):
    ''' Wrapper around matcher.match.__init__
        The goal is to capture higher fidelity information about the matcher
        being created than we would otherwise be able to extract from the
        object once it has been created.

        arguments:
        root - the canonical root of the tree you're matching against
        cwd - the current working directory, if relevant
        patterns - patterns to find
        include - patterns to include (unless they are excluded)
        exclude - patterns to exclude (even if they are included)
        default - if a pattern in patterns has no explicit type, assume this one
        exact - patterns are actually filenames (include/exclude still apply)
        warn - optional function used for printing warnings
        badfn - optional bad() callback for this matcher instead of the default

        a pattern is one of:
        'glob:<glob>' - a glob relative to cwd
        're:<regexp>' - a regular expression
        'path:<path>' - a path relative to repository root, which is matched
                        recursively
        'rootfilesin:<path>' - a path relative to repository root, which is
                        matched non-recursively (will not match subdirectories)
        'relglob:<glob>' - an unrooted glob (*.c matches C files in all dirs)
        'relpath:<path>' - a path relative to cwd
        'relre:<regexp>' - a regexp that needn't match the start of a name
        'set:<fileset>' - a fileset expression
        'include:<path>' - a file of patterns to read and include
        'subinclude:<path>' - a file of patterns to match against files under
                              the same directory
        '<something>' - a pattern of the specified default type
    '''
    res = orig(
        root, cwd, patterns, include, exclude, default, exact, auditor, ctx,
        listsubrepos, warn, badfn, icasefs
    )

    info = EdenMatchInfo(
        root, cwd, exact,
        matchmod._donormalize(
            patterns or [], default, root, cwd, auditor, False
        ),
        matchmod._donormalize(include or [], 'glob', root, cwd, auditor, False),
        matchmod._donormalize(exclude or [], 'glob', root, cwd, auditor, False)
    )

    res._eden_match_info = info

    return res


def wrap_match_exact(orig, root, cwd, files, badfn=None):
    res = orig(root, cwd, files, badfn)

    # Note that files could be a dict. In this case, the keys are the names of
    # the files to match.
    if not isinstance(files, list):
        files = list(files)

    # Normalize the patterns for the EdenMatchInfo and create it.
    patterns = matchmod._donormalize(
        files, 'path', root, cwd, auditor=None, warn=False
    )
    res._eden_match_info = EdenMatchInfo(
        root, cwd, exact=True, patterns=patterns, includes=[], excludes=[]
    )
    return res
