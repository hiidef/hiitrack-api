#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Events are name/timestamp pairs linked to a visitor and stored in buckets.
"""

import time
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList
from ..models import bucket_check, user_authorize, bucket_create
from ..models import VisitorModel, EventModel, PropertyModel
from ..lib.authentication import authenticate
from ..lib.b64encode import b64encode_keys, b64encode_nested_keys, \
    uri_b64encode, uri_b64decode
from ..lib.parameters import require
from ..lib.profiler import profile
from ..exceptions import EventException


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
        Information about the event by name.
        """
        event = EventModel(user_name, bucket_name, event_name)
        return _get(request, event)

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
        yield event.get_name()
        data = yield _get(request, event)
        returnValue(data)

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


@inlineCallbacks
def _get(request, event):
    """
    Information about the event.
    """
    if "property" in request.args:
        _property = PropertyModel(
            event.user_name,
            event.bucket_name,
            property_name=request.args["property"][0])
    else:
        _property = None
    response = {"id": uri_b64encode(event.id), "name": event.event_name}
    if "start" in request.args:
        yield _get_interval(request, event, _property, response)
    else:
        yield _get_total(event, _property, response)
    returnValue(response)


@inlineCallbacks
def _get_total(event, _property, response):
    """
    Gets total counts.
    """
    if _property:
        deferreds = [
            _property.get_values(),
            event.get_total(_property),
            event.get_unique_total(_property),
            event.get_path(_property),
            event.get_unique_path(_property),
            event.get_properties()]
        data = yield DeferredList(deferreds)
        response.update({
            "values": b64encode_keys(data[0][1]),
            "totals": b64encode_keys(data[1][1]),
            "unique_totals": b64encode_keys(data[2][1]),
            "paths": b64encode_nested_keys(data[3][1]),
            "unique_paths": b64encode_nested_keys(data[4][1]),
            "properties": b64encode_keys(data[5][1])})
    else:
        deferreds = [
            event.get_total(),
            event.get_unique_total(),
            event.get_path(),
            event.get_unique_path(),
            event.get_properties()]
        data = yield DeferredList(deferreds)
        response.update({
            "total": data[0][1][event.id],
            "unique_total": data[1][1][event.id],
            "path": b64encode_keys(data[2][1][event.id]),
            "unique_path": b64encode_keys(data[3][1][event.id]),
            "properties": b64encode_keys(data[4][1])})


@inlineCallbacks
def _get_interval(request, event, _property, response):
    """
    Gets counts split by time interval.
    """
    start = int(request.args["start"][0])
    if "finish" in request.args:
        finish = int(request.args["finish"][0])
    else:
        finish = time.time()
    if "interval" in request.args:
        interval = request.args["interval"][0]
    else:
        interval = "day"
    if interval not in ["hour", "day"]:
        raise EventException("Interval must be 'hour' or 'day'")
    if _property:
        deferreds = [
            event.get_timed_total(start, finish, interval, _property),
            event.get_timed_unique_total(start, finish, interval, _property),
            event.get_timed_path(start, finish, interval, _property),
            event.get_timed_unique_path(start, finish, interval, _property),
            _property.get_values(),
            event.get_properties()]
        data = yield DeferredList(deferreds)
        response.update({
            "totals": b64encode_keys(data[0][1]),
            "unique_totals": b64encode_keys(data[1][1]),
            "paths": b64encode_nested_keys(data[2][1]),
            "unique_paths": b64encode_nested_keys(data[3][1]),
            "values": b64encode_keys(data[4][1]),
            "properties": b64encode_keys(data[5][1])})
    else:
        deferreds = [
            event.get_timed_total(start, finish, interval),
            event.get_timed_unique_total(start, finish, interval),
            event.get_timed_path(start, finish, interval),
            event.get_timed_unique_path(start, finish, interval),
            event.get_properties()]
        data = yield DeferredList(deferreds)
        response.update({
            "total": data[0][1][event.id],
            "unique_total": data[1][1][event.id],
            "path": b64encode_keys(data[2][1][event.id]),
            "unique_path": b64encode_keys(data[3][1][event.id]),
            "properties": b64encode_keys(data[4][1])})

