#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Buckets are a collection of events, properties, and funnels belonging to a
user.
"""

from twisted.internet.defer import inlineCallbacks, returnValue
from ..lib.authentication import authenticate
from ..exceptions import BucketException
from ..models import bucket_check, BucketModel, user_authorize
from ..lib.b64encode import b64encode_values, b64encode_nested_values
from ..lib.parameters import require


class Bucket(object):
    """
    Bucket controller.
    """
    def __init__(self, dispatcher):
        dispatcher.connect(
            name='bucket',
            route='/{user_name}/{bucket_name}',
            controller=self,
            action='post',
            conditions={"method": "POST"})
        dispatcher.connect(
            name='bucket',
            route='/{user_name}/{bucket_name}',
            controller=self,
            action='get',
            conditions={"method": "GET"})
        dispatcher.connect(
            name='bucket',
            route='/{user_name}/{bucket_name}',
            controller=self,
            action='delete',
            conditions={"method": "DELETE"})

    @authenticate
    @user_authorize
    @require("description")
    @inlineCallbacks
    def post(self, request, user_name, bucket_name):
        """
        Create a new bucket.
        """
        bucket = BucketModel(user_name, bucket_name)
        exists = yield bucket.exists()
        if exists:
            request.setResponseCode(403)
            raise BucketException("Bucket already exists.")
        description = request.args["description"][0]
        yield bucket.create(description)
        request.setResponseCode(201)

    @authenticate
    @user_authorize
    @bucket_check
    @inlineCallbacks
    def get(self, request, user_name, bucket_name):
        """
        Information about the bucket.
        """
        bucket = BucketModel(user_name, bucket_name)
        name, description = yield bucket.get_name_and_description()
        data = yield bucket.get_properties()
        properties = b64encode_nested_values(data)
        data = yield bucket.get_events()
        events = b64encode_values(data)
        returnValue({
            "description": description,
            "properties": properties,
            "events": events})

    @authenticate
    @user_authorize
    @bucket_check
    @inlineCallbacks
    def delete(self, request, user_name, bucket_name):
        """
        Delete bucket.
        """
        yield BucketModel(user_name, bucket_name).delete()
