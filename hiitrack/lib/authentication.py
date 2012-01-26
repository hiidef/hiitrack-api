#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Request authentication decorator.
"""

import base64
from telephus.cassandra.c08.ttypes import NotFoundException
from twisted.internet.defer import inlineCallbacks, returnValue
from ..exceptions import HTTPAuthenticationRequired
from ..models import UserModel


def authenticate(method):
    """
    Decorator.
    """
    @inlineCallbacks
    def wrapper(*args, **kwargs):
        """
        Checks basic authentication.
        """
        request = args[1]
        try:
            auth_header = request.getHeader("Authorization")
            assert auth_header
            auth_type, auth_data = auth_header.split()
            assert auth_type == "Basic"
            user_name, password = base64.b64decode(auth_data).split(":", 1)
            user = UserModel(user_name)
            stored_password = yield user.get_password()
            assert stored_password == password
        except (AssertionError, NotFoundException):
            request.setResponseCode(401)
            request.setHeader('WWW-Authenticate', 'Basic')
            raise HTTPAuthenticationRequired("Authentication required.")
        else:
            request.username = user_name
            data = yield method(*args, **kwargs)
            returnValue(data)
    return wrapper
