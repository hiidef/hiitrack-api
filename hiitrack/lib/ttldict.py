#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Dictionary with TTL
"""

import time


PROTECTED = set(["ttl", "timestamps", "_prune"])


class TTLDict(dict):

    def __init__(self, *args, **kwargs):
        try:
            self.ttl = kwargs["ttl"]
        except KeyError:
            raise TypeError("TTLDict requires keyword arg 'ttl'")
        del kwargs["ttl"]
        self.timestamps = []
        super(TTLDict, self).__init__(*args, **kwargs)
        self.timestamps.extend([(x, time.time()) for x in self])

    def __setitem__(self, key, value):
        self.timestamps.append((key, time.time()))
        return super(TTLDict, self).__setitem__(key, value)

    def __getitem__(self, key):
        now = time.time()
        while self.timestamps and self.timestamps[0][1] + self.ttl < now:
            key = self.timestamps.pop()[0]
            del self[key]
        return super(TTLDict, self).__getitem__(key)
