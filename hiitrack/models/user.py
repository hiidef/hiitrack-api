#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Users have usernames, passwords, and buckets.
"""

import ujson
from twisted.internet.defer import inlineCallbacks, returnValue
from telephus.cassandra.c08.ttypes import NotFoundException
from ..lib.cassandra import get_relation, insert_relation, delete_relation
from ..exceptions import HTTPAuthenticationRequired
from ..models import BucketModel


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


class UserModel(object):
    """
    Users have usernames, passwords, and buckets.
    """

    def __init__(self, user_name):
        self.user_name = user_name

    @inlineCallbacks
    def exists(self):
        """
        Returns boolean indicating whether user exists.
        """
        try:
            yield self.get_password()
            returnValue(True)
        except NotFoundException:
            returnValue(False)

    @inlineCallbacks
    def get_password(self):
        """
        Returns the password associated with the username.
        """
        key = (self.user_name,)
        column = ("password",)
        value = yield get_relation(key, column)
        returnValue(value)

    @inlineCallbacks
    def create(self, password):
        """
        Creates a user with the associated username and password.
        """
        key = (self.user_name,)
        column = ("password",)
        value = password
        yield insert_relation(key, column, value)

    @inlineCallbacks
    def get_buckets(self):
        """
        Return buckets associated with a user.
        """
        key = (self.user_name, "bucket")
        data = yield get_relation(key)
        result = {}
        for key, value in data.items():
            name, description = ujson.loads(value)
            result[name] = {
                "id": key,
                "description": description}
        returnValue(result)

    @inlineCallbacks
    def delete(self):
        """
        Delete a user.
        """
        buckets = yield self.get_buckets()
        for bucket_name in buckets:
            yield BucketModel(self.user_name, bucket_name).delete()
        key = (self.user_name,)
        yield delete_relation(key)
