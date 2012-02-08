#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Events are name/timestamp pairs linked to a visitor and stored in buckets.
"""

from twisted.internet.defer import inlineCallbacks, returnValue
from ..models import bucket_check, user_authorize, bucket_create
from ..models import VisitorModel, EventModel
from ..lib.authentication import authenticate
from ..lib.b64encode import b64encode_keys, b64encode_nested_keys, \
    uri_b64encode
from ..lib.parameters import require
from ..lib.profiler import profile


class Event(object):
    """
    Event controller.
    """

    def __init__(self, dispatcher):
        dispatcher.connect(
            name='event',
            route='/{user_name}/{bucket_name}/event/{event_name}',
            controller=self,
            action='post',
            conditions={"method": "POST"})
        dispatcher.connect(
            name='event',
            route='/{user_name}/{bucket_name}/event/{event_name}/jsonp',
            controller=self,
            action='post',
            conditions={"method": "GET"})
        dispatcher.connect(
            name='event',
            route='/{user_name}/{bucket_name}/event/{event_name}',
            controller=self,
            action='get',
            conditions={"method": "GET"})

    @authenticate
    @user_authorize
    @bucket_check
    @profile
    @inlineCallbacks
    def get(self, request, user_name, bucket_name, event_name):
        """
        Information about the event.
        """
        event = EventModel(user_name, bucket_name, event_name)
        total = yield event.get_total()
        unique_total = yield event.get_unique_total()
        path = yield event.get_path()
        unique_path = yield event.get_unique_path()
        returnValue({
            "id": uri_b64encode(event.id),
            "unique_total": b64encode_keys(unique_total),
            "total": b64encode_keys(total),
            "path": b64encode_nested_keys(path),
            "unique_path": b64encode_nested_keys(unique_path),
            "name": event_name})

    @require("visitor_id")
    @bucket_create
    @profile
    @inlineCallbacks
    def post(self, request, user_name, bucket_name, event_name):
        """
        Create event.
        """
        event = EventModel(user_name, bucket_name, event_name)
        visitor = VisitorModel(
            user_name,
            bucket_name,
            request.args["visitor_id"][0])
        yield event.add(visitor)




