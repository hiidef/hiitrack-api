#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Users have usernames, passwords, and buckets.
"""

import ujson
from twisted.internet.defer import inlineCallbacks, returnValue
from telephus.cassandra.c08.ttypes import NotFoundException
from ..lib.cassandra import get_relation, get_user, set_user, delete_user
from ..exceptions import HTTPAuthenticationRequired
from ..models import BucketModel
from hashlib import sha1
from ..lib.profiler import profile


def user_authorize(method):
    """
    Decorator.
    """
    def wrapper(*args, **kwargs):
        """
        Checks that the user associated with the request can perform operations
        on the bucket.
        """
        request = args[1]
        # Dispatcher makes some args into kwargs.
        username = kwargs["user_name"]
        if request.username != username:
            request.setResponseCode(401)
            raise HTTPAuthenticationRequired("Invalid user")
        else:
            return method(*args, **kwargs)
    return wrapper


def password_hash(user_name, password):
    """
    Returns a password hash.
    """
    return sha1("%s:%s" % (user_name, password)).digest()


class UserModel(object):
    """
    Users have usernames, passwords, and buckets.
    """

    def __init__(self, user_name):
        self.user_name = user_name
    
    @profile
    @inlineCallbacks
    def exists(self):
        """
        Returns boolean indicating whether user exists.
        """
        try:
            yield get_user(self.user_name, "hash")
            returnValue(True)
        except NotFoundException:
            returnValue(False)
    
    @profile
    @inlineCallbacks
    def validate_password(self, password):
        """
        Returns the password associated with the username.
        """
        _password_hash = yield get_user(self.user_name, "hash")
        returnValue(_password_hash == password_hash(self.user_name, password))

    @profile
    @inlineCallbacks
    def create(self, password):
        """
        Creates a user with the associated username and password.
        """
        yield set_user(
            self.user_name, 
            "hash", 
            password_hash(self.user_name, password))

    @profile
    @inlineCallbacks
    def get_buckets(self):
        """
        Return buckets associated with a user.
        """
        key = (self.user_name, "bucket")
        data = yield get_relation(key)
        response = dict([(x, ujson.decode(data[x])) for x in data])
        returnValue(response)

    @profile
    @inlineCallbacks
    def delete(self):
        """
        Delete a user.
        """
        buckets = yield self.get_buckets()
        for bucket_name in buckets:
            yield BucketModel(self.user_name, bucket_name).delete()
        yield delete_user(self.user_name)
