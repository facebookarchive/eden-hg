# Copyright (c) 2016-present, Facebook, Inc.
# All Rights Reserved.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


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


DirstateNonnormalFileStatus = _define_enum(
    'Normal', 'NeedsMerging', 'MarkedForRemoval', 'MarkedForAddition',
    'NotTracked'
)

DirstateMergeState = _define_enum('NotApplicable', 'BothParents', 'OtherParent')


class DirstateNonnormalFile(object):
    __slots__ = ('status', 'mergeState')

    def __init__(self, status, mergeState):
        self.status = status
        self.mergeState = mergeState


class DirstateNonnormalFiles(object):
    __slots__ = ('entries')

    def __init__(self, entries):
        self.entries = entries


class DirstateCopymap(object):
    __slots__ = ('entries')

    def __init__(self, entries):
        self.entries = entries


class DirstateTuple(object):
    __slots__ = ('status', 'mode', 'mergeState')

    def __init__(self, status, mode, mergeState):
        self.status = status
        self.mode = mode
        self.mergeState = mergeState

    def __repr__(self):
        return (
            'facebook.hgdirstate.ttypes.DirstateTuple'
            '(status=%r, mode=%r, mergeState=%r)' %
            (self.status, self.mode, self.mergeState)
        )
