#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
CityHash128 based hashing/packing function. See:

http://code.google.com/p/cityhash/
https://github.com/hiidef/cityhash
"""

from cityhash import ch128
import struct


def pack_hash(args):
    """Takes several strings and returns a unique 16 byte hash string."""
    return struct.pack(">2Q", *ch128(":".join(args)).digest())
