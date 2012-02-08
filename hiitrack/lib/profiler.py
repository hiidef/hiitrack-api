#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Profiler.
"""

from twisted.internet.defer import inlineCallbacks, returnValue
from collections import defaultdict
import time


EXECUTION_TIME = defaultdict(lambda:0.0)
EXECUTION_COUNT = defaultdict(lambda:0)


def profile(method):
    """
    Decorator.
    """
    @inlineCallbacks
    def wrapper(*args, **kwargs):
        """
        Performs basic profiling functions.
        """
        name = "%s/%s" % (method.__module__, method.__name__)
        start = time.time()
        data = yield method(*args, **kwargs)
        EXECUTION_TIME[name] += time.time() - start
        EXECUTION_COUNT[name] += 1
        returnValue(data)
    return wrapper
