#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Funnels are a collection of events within a bucket.
"""

from itertools import chain
from twisted.internet.defer import inlineCallbacks, returnValue
from telephus.cassandra.c08.ttypes import NotFoundException
from ..models import bucket_check, user_authorize
from ..models import FunnelModel, EventModel
from ..lib.authentication import authenticate
from ..exceptions import MissingParameterException
from ..lib.b64encode import uri_b64decode, uri_b64encode, \
    b64encode_nested_keys, b64encode_double_nested_keys
from ..lib.parameters import require
from ..lib.profiler import profile


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
        funnel = FunnelModel(user_name, bucket_name, funnel_name)
        yield funnel.create(description, event_ids)
        request.setResponseCode(201)

    @authenticate
    @user_authorize
    @bucket_check
    @profile
    @inlineCallbacks
    def get_saved_funnel(self, request, user_name, bucket_name, funnel_name):
        """
        Get funnel details.
        """
        funnel = FunnelModel(user_name, bucket_name, funnel_name)
        try:
            description, event_ids = yield funnel.get_description_event_ids()
        except NotFoundException:
            request.setResponseCode(404)
            raise
        totals = {}
        unique_totals = {}
        paths = {}
        unique_paths = {}
        for event_id in event_ids:
            event = EventModel(user_name, bucket_name, event_id=event_id)
            total = yield event.get_total()
            totals[event.id] = total
            unique_totals[event.id] = yield event.get_unique_total()
            paths[event.id] = yield event.get_path()
            unique_paths[event.id] = yield event.get_unique_path()
        property_ids = chain(*[x.keys() for x in totals.values()])
        property_ids = set(property_ids) - set(event_ids)
        # Full funnel, no properties.
        event_id = event_ids[0]
        base_funnel = [(event_id, totals[event_id][event_id])]
        base_unique_funnel = [(event_id, unique_totals[event_id][event_id])]
        for i in range(1, len(event_ids)):
            event_id = event_ids[i - 1]
            new_event_id = event_ids[i]
            base_funnel.append((
                new_event_id,
                paths[new_event_id][new_event_id][event_id]))
            try:
                base_unique_funnel.append((
                    new_event_id,
                    unique_paths[new_event_id][new_event_id][event_id]))
            except KeyError:
                base_unique_funnel.append((new_event_id, 0))
        funnels = {}
        unique_funnels = {}
        for property_id in property_ids:
            event_id = event_ids[0]
            try:
                _funnel = [(event_id, totals[event_id][property_id])]
            except KeyError:
                _funnel = [(event_id, 0)]
            try:
                unique_funnel = [(
                    event_id,
                    unique_totals[event_id][property_id])]
            except KeyError:
                unique_funnel = [(event_id, 0)]
            for i in range(1, len(event_ids)):
                event_id = event_ids[i - 1]
                new_event_id = event_ids[i]
                try:
                    _funnel.append((
                        new_event_id,
                        paths[new_event_id][property_id][event_id]))
                except KeyError:
                    _funnel.append((new_event_id, 0))
                try:
                    unique_funnel.append((
                        new_event_id,
                        unique_paths[new_event_id][property_id][event_id]))
                except KeyError:
                    unique_funnel.append((new_event_id, 0))
            funnels[property_id] = _funnel
            unique_funnels[property_id] = unique_funnel
        returnValue({
            "description": description,
            "event_ids": [uri_b64encode(x) for x in event_ids],
            "totals": b64encode_nested_keys(totals),
            "unique_totals": b64encode_nested_keys(unique_totals),
            "paths": b64encode_double_nested_keys(paths),
            "unique_paths": b64encode_double_nested_keys(unique_paths),
            "funnel": [(uri_b64encode(x[0]), x[1]) for x in base_funnel],
            "unique_funnel": [(uri_b64encode(x[0]), x[1]) \
                for x in base_unique_funnel],
            "funnels": encode_nested_lists(funnels),
            "unique_funnels": encode_nested_lists(unique_funnels)})

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
