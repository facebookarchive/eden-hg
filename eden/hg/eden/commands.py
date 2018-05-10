#!/usr/bin/env python2
# Copyright (c) 2016-present, Facebook, Inc.
# All Rights Reserved.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

from __future__ import absolute_import, division, print_function

import json

import eden.dirstate
from mercurial import commands, debugcommands, registrar, util
from mercurial.i18n import _

from . import constants


cmdtable = {}
command = registrar.command(cmdtable)

# Remove the default debugdirstate implementation, since we will replace it
# with our own version.
del commands.table["debugdirstate|debugstate"]


@command(
    "debugdirstate|debugstate",
    [("j", "json", None, _("print the output in JSON format"))],
    _("[OPTION]..."),
)
def debugdirstate(ui, repo, **opts):
    if constants.requirement not in repo.requirements:
        return debugcommands.debugstate(ui, repo, **opts)

    def get_merge_string(value):
        if value == eden.dirstate.MERGE_STATE_NOT_APPLICABLE:
            return ""
        elif value == eden.dirstate.MERGE_STATE_OTHER_PARENT:
            return "MERGE_OTHER"
        elif value == eden.dirstate.MERGE_STATE_BOTH_PARENTS:
            return "MERGE_BOTH"
        # We don't expect any other merge state values; we probably had a bug
        # if the dirstate file contains other values.
        # However, just return the integer value as a string so we can use
        # debugdirstate to help debug this situation if it does occur.
        return str(value)

    if opts.get("json"):
        data = {}
        for path, dirstate_tuple in repo.dirstate.edeniteritems():
            status, mode, merge_state = dirstate_tuple
            data[path] = {
                "status": status,
                "mode": mode,
                "merge_state": merge_state,
                "merge_state_string": get_merge_string(merge_state),
            }

        ui.write(json.dumps(data, indent=2, sort_keys=True))
        ui.write("\n")
        return

    for path, dirstate_tuple in sorted(repo.dirstate._map._map.iteritems()):
        status, mode, merge_state = dirstate_tuple
        if mode & 0o20000:
            display_mode = "lnk"
        else:
            display_mode = "%3o" % (mode & 0o777 & ~util.umask)

        merge_str = get_merge_string(merge_state)
        ui.write("%s %s %12s %s\n" % (status, display_mode, merge_str, path))
