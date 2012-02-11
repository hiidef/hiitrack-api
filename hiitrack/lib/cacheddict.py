#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Dictionary with TTL
"""

class TTLDict(dict):

    def __init__(self, *args, **kwargs):
        super(TTLDict, self).__init__(*args, **kwargs)

    def __set__(self, key, value):
        return super(TTLDict, self).__set__(key, value)

    def __get__(self, key, default=None):
        return super(TTLDict, self).__get__(key, default=None)


if __name__ == '__main__':
    ttldict = TTLDict([1,2,3])