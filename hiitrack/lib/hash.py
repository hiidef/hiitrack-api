#!/usr/bin/env python
# -*- coding: utf-8 -*-

from hashlib import sha1
from cityhash import ch128
import struct


def pack_hash(args):
    """
    CityHash128 based hashing/packing function. See:

    http://code.google.com/p/cityhash/
    https://github.com/hiidef/cityhash

    Takes several strings and returns a unique 16 byte hash string."""
    return struct.pack(">2Q", *ch128(":".join(args)).digest())


def password_hash(user_name, password):
    """
    Returns a password hash.
    """
    return sha1("%s:%s" % (user_name, password)).digest()