#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Request authentication decorator.
"""

import base64
from telephus.cassandra.c08.ttypes import NotFoundException
from twisted.internet.defer import inlineCallbacks, returnValue
from ..exceptions import HTTPAuthenticationRequired
from ..models import UserModel, BucketModel
from ..lib.ttldict import TTLDict

TTL_CACHE = TTLDict(ttl=30)

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
        except:
            request.setResponseCode(401)
            request.setHeader('WWW-Authenticate', 'Basic')
            raise HTTPAuthenticationRequired("Authentication required.")
        try:
            auth_type, auth_data = auth_header.split()
            assert auth_type == "Basic"
            user_name, password = base64.b64decode(auth_data).split(":", 1)
            assert user_name
            assert password
            user = UserModel(user_name)
            try:
                assert TTL_CACHE[user_name] == password
            except (AssertionError, KeyError):
                pass
            valid = yield user.validate_password(password)
            try:
                assert valid
                TTL_CACHE[user_name] = password
            except AssertionError:
                if "bucket_name" in kwargs:
                    bucket = BucketModel(user_name, kwargs["bucket_name"])
                    valid = yield bucket.validate_password(password)
                    assert valid
                raise
        except AssertionError:
            request.setResponseCode(401)
            if request.getHeader("X-Requested-With") != "XMLHttpRequest":
                request.setHeader('WWW-Authenticate', 'Basic')
            raise HTTPAuthenticationRequired("Authentication required.")
        request.username = user_name
        data = yield method(*args, **kwargs)
        returnValue(data)
    return wrapper
