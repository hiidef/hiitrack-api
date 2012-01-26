#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Functions to that provide a shortened base64 encoding of hash values.
"""

from base64 import urlsafe_b64decode, urlsafe_b64encode


def uri_b64encode(value):
    """
    URL safe base64 encoding with no trailing equals.
    """
    return urlsafe_b64encode(value).strip('=')


def uri_b64decode(value):
    """
    URL safe base64 decoding with no trailing equals.
    """
    return urlsafe_b64decode(value + '=' * (4 - len(value) % 4))


def b64encode_keys(dictionary):
    """
    Make a dictionary's keys url safe.
    """
    return dict([(uri_b64encode(x[0]), x[1]) for x in dictionary.items()])


def b64encode_nested_keys(dictionary):
    """
    Nested version of b64encode_keys.
    """
    return dict([(uri_b64encode(x[0]), b64encode_keys(x[1]))
        for x in dictionary.items()])


def b64encode_double_nested_keys(dictionary):
    """
    Double-nested version of b64encode_keys.
    """
    return dict([(uri_b64encode(x[0]), b64encode_nested_keys(x[1]))
        for x in dictionary.items()])


def b64encode_values(dictionary):
    """
    Make a dictionary's values url safe.
    """
    return dict([(x[0], uri_b64encode(x[1])) for x in dictionary.items()])


def b64encode_nested_values(dictionary):
    """
    Nested version of b64encode_values.
    """
    return dict([(x[0], b64encode_values(x[1]))
        for x in dictionary.items()])
