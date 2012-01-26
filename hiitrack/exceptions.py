#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Exceptions used by various components. Here to avoid circular imports.
"""


class HiiTrackException(Exception):
    """
    Abstract base exception.
    """
    pass


class HTTPAuthenticationRequired(HiiTrackException):
    """
    HTTP Authenticaton failure
    """
    pass


class UserException(HiiTrackException):
    """
    User failure
    """
    pass


class BucketException(HiiTrackException):
    """
    Bucket failure
    """
    pass


class MissingParameterException(HiiTrackException):
    """
    Missing HTTP parameter
    """
    pass
