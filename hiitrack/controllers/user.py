#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Users have usernames, passwords, and buckets.
"""

from twisted.internet.defer import inlineCallbacks, returnValue
from ..lib.authentication import authenticate
from ..lib.parameters import require
from ..exceptions import UserException
from ..models import UserModel, user_authorize, BucketModel
from ..lib.b64encode import uri_b64encode, b64encode_values, \
    b64encode_nested_values
from ..lib.profiler import profile

@profile
def get_user(user_name):
    """
    Returns basic user information.
    """
    user = UserModel(user_name)
    returnValue({})

class User(object):
    """
    User controller.
    """
    def __init__(self, dispatcher):
        dispatcher.connect(
            name='user',
            route='/batch',
            controller=self,
            action='batch',
            conditions={"method": "POST"})
        dispatcher.connect(
            name='user',
            route='/{user_name}',
            controller=self,
            action='post',
            conditions={"method": "POST"})
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
    @profile
    @inlineCallbacks
    def post(self, request, user_name):
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
        response = yield get_user(user_name)
        returnValue(response)

    @authenticate
    @user_authorize
    @profile
    @inlineCallbacks
    def get(self, request, user_name):
        """
        Information about the user.
        """
        response = yield get_user(user_name)
        returnValue(response)

    @authenticate
    @user_authorize
    @profile
    @inlineCallbacks
    def delete(self, request, user_name):
        """
        Delete a user.
        """
        user = UserModel(user_name)
        yield user.delete()

