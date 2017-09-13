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

from mercurial import (
    error, extensions, hg, localrepo, util
)
from mercurial import merge as mergemod
from mercurial.i18n import _
from mercurial import match as matchmod
from . import EdenThriftClient as thrift
ConflictType = thrift.ConflictType
import os

_requirement = 'eden'
_repoclass = localrepo.localrepository
_repoclass._basesupported.add(_requirement)


def extsetup(ui):
    # Wrap the localrepo.dirstate() function.
    #
    # The original dirstate attribute is a filecache object, and needs slightly
    # special handling to wrap properly.
    #
    # (The fsmonitor and sqldirstate extensions both already wrap it, and each
    # has slightly different mechanisms for doing so.  Here we wrap it more
    # like sqldirstate does.  Ideally code for wrapping filecache objects
    # should just get put into core mercurial.)
    orig = localrepo.localrepository.dirstate
    # For some reason, localrepository.invalidatedirstate() does not call
    # dirstate.invalidate() by default, so we must wrap it.
    extensions.wrapfunction(localrepo.localrepository, 'invalidatedirstate',
                            invalidatedirstate)
    extensions.wrapfunction(mergemod, 'update', merge_update)
    extensions.wrapfunction(hg, '_showstats', update_showstats)
    extensions.wrapfunction(orig, 'func', wrapdirstate)
    extensions.wrapfunction(matchmod, 'match', wrap_match)
    extensions.wrapfunction(matchmod, 'exact', wrap_match_exact)
    orig.paths = ()


def invalidatedirstate(orig, self):
    if _requirement in self.requirements:
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
def merge_update(orig, repo, node, branchmerge, force, ancestor=None,
                 mergeancestor=False, labels=None, matcher=None,
                 mergeforce=False, updatecheck=None):
    '''Apparently node can be a 20-byte hash or an integer referencing a
    revision number.
    '''
    assert node is not None

    if not util.safehasattr(repo.dirstate, 'eden_client'):
        why_not_eden = 'This is not an eden repository.'
    if matcher is not None and not matcher.always():
        why_not_eden = 'We don\'t support doing a partial update through eden yet.'
    elif branchmerge:
        why_not_eden = 'branchmerge is "truthy:" %s.' % branchmerge
    elif ancestor is not None:
        why_not_eden = 'ancestor is not None: %s.' % ancestor
    else:
        # TODO: We probably also need to set why_not_eden if there are
        # subrepositories.  (Personally I might vote for just not supporting
        # subrepos in eden.)
        why_not_eden = None

    if why_not_eden:
        repo.ui.debug('falling back to non-eden update code path: %s\n' % why_not_eden)
        return orig(repo, node, branchmerge, force, ancestor=ancestor,
                    mergeancestor=mergeancestor, labels=labels, matcher=matcher,
                    mergeforce=mergeforce)
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
                raise error.Abort(_("outstanding uncommitted merge"))
            ms = mergemod.mergestate.read(repo)
            if list(ms.unresolved()):
                raise error.Abort(_("outstanding merge conflicts"))

            # The vanilla merge code disallows updating between two unrelated
            # branches if the working directory is dirty.  I don't really see a
            # good reason to disallow this; it should be treated the same as if
            # we committed the changes, checked out the other branch then tried
            # to graft the changes here.

        # Invoke the preupdate hook
        repo.hook('preupdate', throw=True, parent1=deststr, parent2='')
        # note that we're in the middle of an update
        repo.vfs.write('updatestate', destctx.hex())

        # Ask eden to perform the checkout
        if force or p1ctx != destctx:
            # Write out pending transaction data if there is a transaction in
            # progress, so eden will be able to access the destination node.
            tr = repo.currenttransaction()
            if tr is not None:
                tr.writepending()

            conflicts = repo.dirstate.eden_client.checkout(
                destctx.node(), force=force)
        else:
            conflicts = None

        # Handle any conflicts
        # The stats returned are numbers of files affected:
        #   (updated, merged, removed, unresolved)
        # The updated and removed file counts will always be 0 in our case.
        if conflicts and not force:
            stats = _handleupdateconflicts(repo, wctx, p1ctx, destctx, labels,
                                           conflicts)
        else:
            stats = 0, 0, 0, 0

        with repo.dirstate.parentchange():
            # TODO(mbolin): Set the second parent, if appropriate.
            repo.setparents(destctx.node())

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
        repo.ui.status(_('%d files merged, %d files unresolved\n') %
                       (merged, unresolved))
    elif not quietempty:
        repo.ui.status(_('update complete\n'))


def _handleupdateconflicts(repo, wctx, src, dest, labels, conflicts):
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

    # Build a list of actions to pass to mergemod.applyupdates()
    actions = dict((m, []) for m in 'a am f g cd dc r dm dg m e k'.split())
    numerrors = 0
    for conflict in conflicts:
        # The action tuple is:
        # - path_in_1, path_in_2, path_in_ancestor, move, ancestor_node

        if conflict.type == ConflictType.ERROR:
            # We don't record this as a conflict for now.
            # We will report the error, but the file will show modified in
            # the working directory status after the update returns.
            repo.ui.write_err(_('error updating %s: %s\n') %
                              (conflict.path, conflict.message))
            numerrors += 1
            continue
        elif conflict.type == ConflictType.MODIFIED_REMOVED:
            action_type = 'cd'
            action = (conflict.path, None, conflict.path, False, src.node())
            prompt = "prompt changed/deleted"
        elif conflict.type == ConflictType.UNTRACKED_ADDED:
            action_type = 'c'
            action = (dest.manifest().flags(conflict.path),)
            prompt = "remote created"
        elif conflict.type == ConflictType.REMOVED_MODIFIED:
            action_type = 'dc'
            action = (None, conflict.path, conflict.path, False, src.node())
            prompt = "prompt deleted/changed"
        elif conflict.type == ConflictType.MISSING_REMOVED:
            # Nothing to do here really.  The file was already removed
            # locally in the working directory before, and it was removed
            # in the new commit.
            continue
        elif conflict.type == ConflictType.MODIFIED:
            action_type = 'm'
            action = (conflict.path, conflict.path, conflict.path,
                      False, src.node())
            prompt = "versions differ"
        else:
            raise Exception('unknown conflict type received from eden: '
                            '%r, %r, %r' % (conflict.type, conflict.path,
                                            conflict.message))

        actions[action_type].append((conflict.path, action, prompt))

    # Call applyupdates
    stats = mergemod.applyupdates(repo, actions, wctx, dest,
                                  overwrite=False, labels=labels)

    # Add the error count to the number of unresolved files.
    # This ensures we exit unsuccessfully if there were any errors
    return (stats[0], stats[1], stats[2], stats[3] + numerrors)


def reposetup(ui, repo):
    # TODO: We probably need some basic sanity checking here:
    # - is this an eden client?
    # - are any conflicting extensions enabled?
    pass


def wrapdirstate(orig, repo):
    # Only override when actually inside an eden client directory.
    if _requirement not in repo.requirements:
        return orig(repo)

    # have the edendirstate class implementation more complete.
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
                if not self._exact and os.path.isdir(os.path.join(self._root,
                                                                  pat)):
                    globs.append(pat + '/**/*')
                else:
                    globs.append(pat)
                continue
            if kind == 'relglob':
                globs.append('**/' + pat)
                continue

            raise NotImplementedError(
                'match pattern %r is not supported by Eden' % (kind, pat, raw))
        return globs

    def __str__(self):
        return str(self.make_glob_list())


def wrap_match(orig, root, cwd, patterns, include=None, exclude=None,
                    default='glob', exact=False, auditor=None, ctx=None,
                    listsubrepos=False, warn=None, badfn=None, icasefs=False):
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
    res = orig(root, cwd, patterns, include, exclude, default,
               exact, auditor, ctx, listsubrepos, warn, badfn, icasefs)

    info = EdenMatchInfo(root, cwd, exact,
                         matchmod._donormalize(patterns or [],
                                               default, root, cwd, auditor, False),
                         matchmod._donormalize(include or [],
                                               'glob', root, cwd, auditor, False),
                         matchmod._donormalize(exclude or [],
                                               'glob', root, cwd, auditor, False))

    res._eden_match_info = info

    return res


def wrap_match_exact(orig, root, cwd, files, badfn=None):
    res = orig(root, cwd, files, badfn)

    # Note that files could be a dict. In this case, the keys are the names of
    # the files to match.
    if not isinstance(files, list):
        files = list(files)

    # Normalize the patterns for the EdenMatchInfo and create it.
    patterns = matchmod._donormalize(files, 'path', root, cwd, auditor=None,
                                     warn=False)
    res._eden_match_info = EdenMatchInfo(root, cwd, exact=True,
                                         patterns=patterns, includes=[],
                                         excludes=[])
    return res
