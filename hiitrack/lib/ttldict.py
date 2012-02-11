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

    def __getattribute__(self, name):
        if name not in PROTECTED:
            self._prune()
        return super(TTLDict, self).__getattribute__(name)

    def __setitem__(self, key, value):
        self.timestamps.append((key, time.time()))
        return super(TTLDict, self).__setitem__(key, value)

    def __getitem__(self, key):
        self._prune()
        return super(TTLDict, self).__getitem__(key)

    def _prune(self):        
        now = time.time()
        while self.timestamps and self.timestamps[0][1] + self.ttl < now:
            key = self.timestamps.pop()[0]
            del self[key]

    def __str__(self):
        self._prune()
        return super(TTLDict, self).__str__()


if __name__ == '__main__':
    ttldict = TTLDict(a=1, b=2, ttl=3)
    print ttldict["b"]
    time.sleep(1)
    print "One second: %s" % ttldict
    time.sleep(1)
    print "Two seconds: %s" % ttldict
    time.sleep(1)
    print "Three seconds: %s" % ttldict
    time.sleep(1)
    print "Four seconds: %s" % ttldict
    time.sleep(1)
    print "Five seconds: %s" % ttldict
