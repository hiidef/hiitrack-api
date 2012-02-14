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
    uri_b64encode, uri_b64decode
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
            route='/{user_name}/{bucket_name}/event/{event_name}',
            controller=self,
            action='get',
            conditions={"method": "GET"})
        dispatcher.connect(
            name='event',
            route='/{user_name}/{bucket_name}/event_id/{event_id}',
            controller=self,
            action='get_by_id',
            conditions={"method": "GET"})

    @authenticate
    @user_authorize
    @bucket_check
    @profile
    def get(self, request, user_name, bucket_name, event_name):
        """
        Information about the event.
        """
        event = EventModel(user_name, bucket_name, event_name)
        return self._get(request, event, event_name)

    @authenticate
    @user_authorize
    @bucket_check
    @profile
    @inlineCallbacks
    def get_by_id(self, request, user_name, bucket_name, event_id):
        """
        Information about the event by id.
        """
        event_id = uri_b64decode(event_id)
        event = EventModel(user_name, bucket_name, event_id=event_id)
        event_name = yield event.get_name()
        data = yield self._get(request, event, event_name)
        returnValue(data)

    @inlineCallbacks
    def _get(self, request, event, event_name):
        args = request.args
        if "start" in args:
            start = int(args["start"][0])
        else:
            start = None
        response = {"id": uri_b64encode(event.id), "name": event_name}
        if start:
            if "finish" in args:
                finish = int(args["finish"][0])
            else:
                finish = time.time()
            if "interval" in args and args["interval"][0] == "hour":
                total = yield event.get_hourly_total(start, finish)
                unique_total = yield event.get_hourly_unique_total(start, finish)
                path = yield event.get_hourly_path(start, finish)
                unique_path = yield event.get_hourly_unique_path(start, finish)
            else:
                total = yield event.get_daily_total(start, finish)
                unique_total = yield event.get_daily_unique_total(start, finish)
                path = yield event.get_daily_path(start, finish)
                unique_path = yield event.get_daily_unique_path(start, finish)
            response.update({
                "total": b64encode_keys(total),
                "unique_total": b64encode_keys(unique_total),
                "path": b64encode_nested_keys(path),
                "unique_path": b64encode_nested_keys(unique_path)})
        else:
            total = yield event.get_total()
            unique_total = yield event.get_unique_total()
            path = yield event.get_path()
            unique_path = yield event.get_unique_path()
            response.update({   
                "total": b64encode_keys(total),
                "unique_total": b64encode_keys(unique_total),            
                "path": b64encode_nested_keys(path),
                "unique_path": b64encode_nested_keys(unique_path)})
        returnValue(response)

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




