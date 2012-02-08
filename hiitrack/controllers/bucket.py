#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Buckets are a collection of events, properties, and funnels belonging to a
user.
"""

from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList
from ..lib.authentication import authenticate
from ..exceptions import BucketException, MissingParameterException
from ..models import bucket_check, BucketModel, user_authorize, EventModel, \
    PropertyValueModel, VisitorModel, bucket_create
from ..lib.b64encode import b64encode_values, b64encode_nested_values
from ..lib.parameters import require
from base64 import b64decode
import ujson
from ..lib.profiler import profile


def encode(value):
    """
    Confirms string, encodes as utf-8
    """
    if not value or not isinstance(value, basestring):
        raise MissingParameterException("Parameter must be string or unicode")
    return value.encode("utf-8")


class Bucket(object):
    """
    Bucket controller.
    """
    def __init__(self, dispatcher):
        dispatcher.connect(
            name='bucket',
            route='/{user_name}/{bucket_name}/batch',
            controller=self,
            action='batch',
            conditions={"method": "GET"})
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
    @profile
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
    @profile
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
    @profile
    @inlineCallbacks
    def delete(self, request, user_name, bucket_name):
        """
        Delete bucket.
        """
        yield BucketModel(user_name, bucket_name).delete()

    @require("id", "message", "visitor_id")
    @bucket_create
    @profile
    @inlineCallbacks
    def batch(self, request, user_name, bucket_name):
        """
        Batched events and properties.
        """
        request_id = request.args["id"][0]
        visitor_id = request.args["visitor_id"][0]
        data = ujson.loads(b64decode(request.args["message"][0]))
        if len(data) != 2:                
            returnValue({
                "id":request_id,
                "error": "Batch request must contain base64 encoded list"
                    " of two values: event_names, properties"})
        event_names, properties = data
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
            event_names = [encode(x) for x in event_names]
            properties = [(encode(x[0]), x[1]) for x in properties]
        except Exception, e:
            returnValue({
                "id":request_id,
                "error": "Batch request must contain base64 encoded list"
                    " of two values: event_names, properties"})
        bucket = BucketModel(user_name, bucket_name)
        exists = yield bucket.exists()
        if not exists:
            yield bucket.create("")
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
