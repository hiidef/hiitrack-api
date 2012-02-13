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
from ..lib.b64encode import b64encode_values, uri_b64encode
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
        description = yield bucket.get_description()
        properties = yield bucket.get_properties()
        for key, values in properties.items():
            for value in values:
                value["id"] = uri_b64encode(value["id"])
        events = yield bucket.get_events()
        for key, value in events.items():
            value["id"] = uri_b64encode(value["id"])
        returnValue({
            "bucket_name": bucket_name,
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
#        event_ids, path, property_ids = yield visitor.get_metadata()
#        deferreds = []
#        new_properties = [PropertyValueModel(user_name, bucket_name, key, value) for key, value in properties]
#        new_events = [EventModel(user_name, bucket_name, event_name) for event_name in event_names]
#        for pv in new_properties:
#            if pv.id not in property_ids:
#                deferreds.append(pv.create())
#                deferreds.append(visitor.add_property(pv))
#        property_ids = set(property_ids + [x.id for x in properties])
#        for event in new_events:
#            unique = event.id not in event_ids
#            if unique:
#                deferreds.append(event.create())
#            deferreds.append(event.increment_total(unique))
#
#
#        event_ids = set(event_ids + )
#        for property_id in property_ids + :
#            deferreds.append(self.increment_total(unique, property_id))
#        deferreds.append(visitor.increment_total(self.id))
#        for event_id in event_ids:
#            _unique = unique or event_id not in path[self.id]
#            deferreds.append(visitor.increment_path(event_id, self.id))
#            deferreds.append(self.increment_path(event_id, _unique))
#            for property_id in property_ids:
#                deferreds.append(self.increment_path(
#                    event_id, 
#                    _unique, 
#                    property_id))
#        BUFFER.flush()
#        yield DeferredList(deferreds)


