#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Checks for required parameters.
"""

from ..exceptions import MissingParameterException
from functools import wraps


def require(*required_parameters):
    """
    Decorator.
    """
    def decorator(method):
        """
        Decorator.
        """
        def wrapper(*args, **kwargs):
            """
            Checks for required parameters.
            """
            request = args[1]
            try:
                for parameter in required_parameters:
                    assert request.args[parameter][0]
            except (KeyError, IndexError, AssertionError):
                request.setResponseCode(403)
                raise MissingParameterException("Parameter '%s' is "
                    "required." % parameter)
            return method(*args, **kwargs)
        return wraps(method)(wrapper)
    return decorator
