#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Users have usernames, passwords, and buckets.
"""

from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList
from ..lib.authentication import authenticate
from ..lib.parameters import require
from ..exceptions import UserException, MissingParameterException
from ..models import UserModel, user_authorize, EventModel, \
    PropertyValueModel, VisitorModel
from ..lib.b64encode import uri_b64encode
from base64 import b64decode
import ujson


@inlineCallbacks
def get_user(user_name):
    """
    Returns basic user information.
    """
    user = UserModel(user_name)
    buckets = yield user.get_buckets()
    for name in buckets:
        buckets[name]["id"] = uri_b64encode(buckets[name]["id"])
    returnValue({"buckets": buckets})

def encode(value):
    """
    Confirms string, encodes as utf-8
    """
    if not value or not isinstance(value, basestring):
        raise MissingParameterException("Parameter must be string or unicode")
    return value.encode("utf-8")

def encode_tuple(values):
    """
    Applies 'encode' to tuples.
    """
    return encode(values[0]), encode(values[1])

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
            conditions={"method": "GET"})
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
    @inlineCallbacks
    def get(self, request, user_name):
        """
        Information about the user.
        """
        response = yield get_user(user_name)
        returnValue(response)

    @authenticate
    @user_authorize
    @inlineCallbacks
    def delete(self, request, user_name):
        """
        Delete a user.
        """
        user = UserModel(user_name)
        yield user.delete()

    @require("id", "message")
    @inlineCallbacks
    def batch(self, request):
        """
        Batched events and properties.
        """
        request_id = request.args["id"][0]
        data = ujson.loads(b64decode(request.args["message"][0]))
        if len(data) != 5:                
            returnValue({
                "id":request_id,
                "error": "Batch request must contain base64 encoded list"
                    " of five values: user_name, bucket_name, event_names"
                    " properties, visitor_id"})
        user_name, bucket_name, event_names, properties, visitor_id = data
        if not isinstance(event_names, list):
            returnValue({
                "id":request_id,
                "error": "event_names must be a list."})        
        if not isinstance(properties, list):
            returnValue({
                "id":request_id,
                "error": "properties must be a list of tuples."})    
        if not all([len(x) == 2 for x in properties]):
            returnValue({
                "id":request_id,
                "error": "properties must be in [key, value] format."})
        try:
            user_name = encode(user_name)
            bucket_name = encode(bucket_name)
            visitor_id = encode(visitor_id)
            event_names = [encode(x) for x in event_names]
            properties = [encode_tuple(x) for x in properties]
        except:
            returnValue({
                "id":request_id,
                "error": "Batch request must contain base64 encoded list"
                    " of five values: user_name, bucket_name, event_names"
                    " properties, visitor_id"})
        visitor = VisitorModel(user_name, bucket_name, visitor_id)
        deferreds = []
        for event_name in event_names:
            event = EventModel(user_name, bucket_name, event_name)
            deferreds.append(event.add(visitor))
        yield DeferredList(deferreds)
        for key, value in properties:
            pv = PropertyValueModel(user_name, bucket_name, key, value)
            deferreds.append(pv.add(visitor))
        yield DeferredList(deferreds)
        returnValue({"id":request.args["id"][0]})
