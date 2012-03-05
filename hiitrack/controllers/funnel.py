#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Funnels are a collection of events within a bucket.
"""

from itertools import chain
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList
from telephus.cassandra.c08.ttypes import NotFoundException
from ..models import bucket_check, user_authorize
from ..models import FunnelModel, EventModel, PropertyModel
from ..lib.authentication import authenticate
from ..exceptions import MissingParameterException
from ..lib.b64encode import uri_b64decode, uri_b64encode, \
    b64encode_nested_keys, b64encode_double_nested_keys, b64encode_keys
from ..lib.hash import pack_hash
from ..lib.parameters import require
from ..lib.profiler import profile


def _parse(data, event_ids):
    """Zip responses by data[offset::step]"""
    data = [x[1] for x in data]
    return (
        dict(zip(event_ids, data[0::4])),
        dict(zip(event_ids, data[1::4])),
        dict(zip(event_ids, data[2::4])),
        dict(zip(event_ids, data[3::4])))


def encode_nested_lists(dictionary):
    """
    Base64 encodes nested lists.
    """
    return dict([(uri_b64encode(k), [(uri_b64encode(x[0]), x[1]) for x in v]) \
        for k, v in dictionary.items()])


class Funnel(object):
    """
    Funnel.
    """

    def __init__(self, dispatcher):
        dispatcher.connect(
            name='funnel',
            route='/{user_name}/{bucket_name}/funnel/{funnel_name}',
            controller=self,
            action='post',
            conditions={"method": "POST"})
        dispatcher.connect(
            name='funnel',
            route='/{user_name}/{bucket_name}/funnel',
            controller=self,
            action='preview_funnel',
            conditions={"method": "GET"})
        dispatcher.connect(
            name='funnel',
            route='/{user_name}/{bucket_name}/funnel/{funnel_name}',
            controller=self,
            action='get_saved_funnel',
            conditions={"method": "GET"})
        dispatcher.connect(
            name='funnel',
            route='/{user_name}/{bucket_name}/funnel/{funnel_name}',
            controller=self,
            action='delete',
            conditions={"method": "DELETE"})

    @authenticate
    @user_authorize
    @bucket_check
    @require("event_id", "description")
    @profile
    @inlineCallbacks
    def post(self, request, user_name, bucket_name, funnel_name):
        """
        Create a new funnel.
        """
        if len(request.args["event_id"]) < 2:
            request.setResponseCode(403)
            raise MissingParameterException("Parameter 'event_id' requires "
                "at least two values.")
        event_ids = [uri_b64decode(x) for x in request.args["event_id"]]
        description = request.args["description"][0]
        if "property" in request.args:
            property_name = request.args["property"][0]
        else:
            property_name = None
        funnel = FunnelModel(user_name, bucket_name, funnel_name)
        yield funnel.create(description, event_ids, property_name)
        request.setResponseCode(201)

    @authenticate
    @user_authorize
    @bucket_check
    @profile
    @inlineCallbacks
    def preview_funnel(self, request, user_name, bucket_name):
        """
        Information about an unsaved funnel.
        """
        if "event_id" in request.args:
            if len(request.args["event_id"]) < 2:
                request.setResponseCode(403)
                raise MissingParameterException("Parameter 'event_id' requires"
                    " at least two values.")
            event_ids = [uri_b64decode(x) for x in request.args["event_id"]]
        elif "event" in request.args:
            if len(request.args["event"]) < 2:
                request.setResponseCode(403)
                raise MissingParameterException("Parameter 'event' requires"
                    " at least two values.")
            event_ids = [pack_hash((x,)) for x in request.args["event"]]
        else:
            request.setResponseCode(403)
            raise MissingParameterException("Parameters 'event' or 'event_id'"
                " required.")
        if "property" in request.args:
            _property = PropertyModel(
                user_name,
                bucket_name,
                property_name=request.args["property"][0])
        else:
            _property = None
        data = yield _get(user_name, bucket_name, event_ids, _property)
        returnValue(data)

    @authenticate
    @user_authorize
    @bucket_check
    @profile
    @inlineCallbacks
    def get_saved_funnel(self, request, user_name, bucket_name, funnel_name):
        """
        Information about a saved funnel.
        """
        funnel = FunnelModel(user_name, bucket_name, funnel_name)
        try:
            yield funnel.get()
        except NotFoundException:
            request.setResponseCode(404)
            raise
        data = yield _get(user_name, bucket_name, funnel.event_ids, funnel.property)
        data["description"] = funnel.description
        returnValue(data)

    @authenticate
    @user_authorize
    @bucket_check
    @profile
    @inlineCallbacks
    def delete(self, request, user_name, bucket_name, funnel_name):
        """
        Delete funnel.
        """
        funnel = FunnelModel(user_name, bucket_name, funnel_name)
        yield funnel.delete()


@inlineCallbacks
def _get(user_name, bucket_name, event_ids, _property):
    """
    Information about a funnel.
    """
    # Combine requests for event data.
    deferreds = []
    for event_id in event_ids:
        event = EventModel(user_name, bucket_name, event_id=event_id)
        deferreds.extend([
            event.get_total(_property),
            event.get_unique_total(_property),
            event.get_path(_property),
            event.get_unique_path(_property)])
    if _property:
        deferreds.append(_property.get_values())
    data = yield DeferredList(deferreds)
    response = {"event_ids": [uri_b64encode(x) for x in event_ids]}
    if _property:
        property_values = data.pop()[1]
        response.update({
            "property":{
                "name":_property.property_name,
                "id": uri_b64encode(_property.id),
                "values": b64encode_keys(property_values)}})
        _get_with_property(data, event_ids, response)
    else:
        _get_without_property(data, event_ids, response)
    returnValue(response)


def _get_with_property(data, event_ids, response):
    """
    Information about a funnel on a property.
    """
    totals, unique_totals, paths, unique_paths = _parse(data, event_ids)
    property_ids = set(chain(*[x.keys() for x in totals.values()]))
    funnels = {}
    unique_funnels = {}
    for property_id in property_ids - set(event_ids):
        event_id = event_ids[0]
        _funnel = [(event_id, totals[event_id][property_id])]
        unique_funnel = [(event_id, unique_totals[event_id][property_id])]
        for i in range(1, len(event_ids)):
            event_id = event_ids[i - 1]
            new_event_id = event_ids[i]
            if event_id not in paths[new_event_id][property_id]:
                continue
            _funnel.append((
                new_event_id,
                paths[new_event_id][property_id][event_id]))
            unique_funnel.append((
                new_event_id,
                unique_paths[new_event_id][property_id][event_id]))
        funnels[property_id] = _funnel
        unique_funnels[property_id] = unique_funnel
    response.update({
        "totals": b64encode_nested_keys(totals),
        "unique_totals": b64encode_nested_keys(unique_totals),
        "paths": b64encode_double_nested_keys(paths),
        "unique_paths": b64encode_double_nested_keys(unique_paths),
        "funnels": encode_nested_lists(funnels),
        "unique_funnels": encode_nested_lists(unique_funnels)})


def _get_without_property(data, event_ids, response):
    """
    Information about a funnel without a property.
    """
    totals, unique_totals, paths, unique_paths = _parse(data, event_ids)
    # Full funnel, no properties.
    event_id = event_ids[0]
    totals = dict([(x, totals[x][x]) for x in totals])
    unique_totals = dict([(x, unique_totals[x][x]) for x in unique_totals])
    _funnel = [(event_id, totals[event_id])]
    unique_funnel = [(event_id, unique_totals[event_id])]
    paths = dict([(x, paths[x][x]) for x in paths])
    unique_paths = dict([(x, unique_paths[x][x]) for x in unique_paths])
    for i in range(1, len(event_ids)):
        event_id = event_ids[i - 1]
        new_event_id = event_ids[i]
        _funnel.append((new_event_id, paths[new_event_id][event_id]))
        unique_funnel.append((
            new_event_id,
            unique_paths[new_event_id][event_id]))
    response.update({
        "total": b64encode_keys(totals),
        "unique_total": b64encode_keys(unique_totals),
        "path": b64encode_nested_keys(paths),
        "unique_path": b64encode_nested_keys(unique_paths),
        "funnel": [(uri_b64encode(x[0]), x[1]) for x in _funnel],
        "unique_funnel": [(uri_b64encode(x[0]), x[1]) \
            for x in unique_funnel]})
