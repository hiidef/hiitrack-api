#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Buckets are a collection of events, properties, and funnels belonging to a
user.
"""

from twisted.internet.defer import inlineCallbacks, returnValue
from ..lib.authentication import authenticate
from ..exceptions import BucketException
from ..models import bucket_check, BucketModel, user_authorize, EventModel, \
    PropertyValueModel, VisitorModel, bucket_create
from ..lib.b64encode import b64encode_keys, uri_b64encode
from ..lib.parameters import require
from base64 import b64decode
import ujson
from ..lib.profiler import profile
from ..lib.cassandra import BUFFER
from uuid import uuid4
from datetime import datetime, timedelta


def set_cookie(request, visitor_id):
    """
    Sets a visitor cookie.
    """
    expiration = datetime.now() + timedelta(days=100*365)
    request.addCookie(
        "v",
        visitor_id,
        path="/",
        expires=expiration.strftime("%a, %d-%b-%Y %H:%M:%S PST"))


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
        events = yield bucket.get_events()
        for value in events.values():
            value["id"] = uri_b64encode(value["id"])
        returnValue({
            "bucket_name": bucket_name,
            "description": description,
            "properties": b64encode_keys(properties),
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

    @require("message")
    @bucket_create
    @profile
    @inlineCallbacks
    def batch(self, request, user_name, bucket_name):
        """
        Batched events and properties.
        """
        cookied_visitor_id = request.getCookie("v")
        if "visitor_id" in request.args:
            visitor_id = request.args["visitor_id"][0]
            if visitor_id != cookied_visitor_id:
                set_cookie(request, visitor_id)
        else:
            if cookied_visitor_id:
                visitor_id = cookied_visitor_id
            else:
                visitor_id = uuid4().hex
                set_cookie(request, visitor_id)
        data = ujson.loads(b64decode(request.args["message"][0]))
        if len(data) != 2:
            returnValue({
                "visitor_id": visitor_id,
                "error": "Batch request must contain base64 encoded list"
                    " of two values: event_names, properties"})
        event_names, properties = data
        if not isinstance(event_names, list):
            returnValue({
                "visitor_id": visitor_id,
                "error": "event_names must be a list."})
        if not isinstance(properties, list):
            returnValue({
                "visitor_id": visitor_id,
                "error": "properties must be a list of tuples."})
        if not all([len(x) == 2 for x in properties]):
            returnValue({
                "visitor_id": visitor_id,
                "error": "properties must be in [key, value] format."})
        try:
            event_names = [x.encode("utf-8") for x in event_names]
            properties = [(x[0].encode("utf-8"), x[1]) for x in properties]
        except AttributeError:
            returnValue({
                "visitor_id": visitor_id,
                "error": "Batch request must contain base64 encoded list"
                    " of two values: event_names, properties"})
        visitor = VisitorModel(user_name, bucket_name, visitor_id)
        total, path, property_ids = yield visitor.get_metadata()
        for key, value in properties:
            pv = PropertyValueModel(user_name, bucket_name, key, value)
            pv.batch_add(visitor, total, path, property_ids)
            property_ids.append(pv.id)
        for event_name in event_names:
            event = EventModel(user_name, bucket_name, event_name)
            event.batch_add(visitor, total, path, property_ids)
        yield BUFFER.flush()
        returnValue({"visitor_id":visitor_id})


