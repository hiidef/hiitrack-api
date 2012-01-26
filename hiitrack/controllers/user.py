#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Users have usernames, passwords, and buckets.
"""

from twisted.internet.defer import inlineCallbacks, returnValue
from ..lib.authentication import authenticate
from ..lib.parameters import require
from ..exceptions import UserException
from ..models import UserModel, user_authorize
from ..lib.b64encode import uri_b64encode


class User(object):
    """
    User controller.
    """
    def __init__(self, dispatcher):
        dispatcher.connect(
            name='user',
            route='/{user_name}',
            controller=self,
            action='put',
            conditions={"method": "PUT"})
        dispatcher.connect(
            name='user',
            route='/{user_name}',
            controller=self,
            action='get',
            conditions={"method": "GET"})
        dispatcher.connect(
            name='user',
            route='/{user_name}',
            controller=self,
            action='delete',
            conditions={"method": "DELETE"})

    @require("password")
    @inlineCallbacks
    def put(self, request, user_name):
        """
        Create a new user.
        """
        user = UserModel(user_name)
        exists = yield user.exists()
        if exists:
            request.setResponseCode(403)
            raise UserException("Username exists.")
        else:
            yield user.create(request.args["password"][0])
        request.setResponseCode(201)

    @authenticate
    @user_authorize
    @inlineCallbacks
    def get(self, request, user_name):
        """
        Information about the user.
        """
        user = UserModel(user_name)
        buckets = yield user.get_buckets()
        for name in buckets:
            buckets[name]["id"] = uri_b64encode(buckets[name]["id"])
        returnValue({"buckets": buckets})

    @authenticate
    @user_authorize
    @inlineCallbacks
    def delete(self, request, user_name):
        """
        Delete a user.
        """
        user = UserModel(user_name)
        yield user.delete()
