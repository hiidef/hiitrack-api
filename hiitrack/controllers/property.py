#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Properties are key/value pairs linked to a visitor and stored in buckets.
"""

from twisted.internet.defer import inlineCallbacks, returnValue
from ..models import bucket_check, user_authorize
from ..models import PropertyValueModel, VisitorModel, EventModel
from ..lib.authentication import authenticate
from ..lib.b64encode import b64encode_keys
from ..lib.parameters import require


class Property(object):
    """
    Property controller.
    """

    def __init__(self, dispatcher):
        dispatcher.connect(
            name='property',
            route=('/{user_name}/{bucket_name}/property/'
                '{property_name}/{property_value}'),
            controller=self,
            action='post',
            conditions={"method": "POST"})
        dispatcher.connect(
            name='property',
            route=('/{user_name}/{bucket_name}/property/'
                '{property_name}/{property_value}/jsonp'),
            controller=self,
            action='post',
            conditions={"method": "GET"})
        dispatcher.connect(
            name='property',
            route=('/{user_name}/{bucket_name}/property/'
                '{property_name}/{property_value}'),
            controller=self,
            action='get',
            conditions={"method": "GET"})

    @authenticate
    @user_authorize
    @bucket_check
    @inlineCallbacks
    def get(self,
            request,
            user_name,
            bucket_name,
            property_name,
            property_value):
        """
        Information about the property.
        """
        property_value = PropertyValueModel(
            user_name,
            bucket_name,
            property_name,
            property_value)
        name, value = property_value.get_name_and_value()
        total = yield property_value.get_total()
        returnValue({
            "name": name,
            "value": value,
            "total": b64encode_keys(total)})

    @bucket_check
    @require("visitor_id")
    @inlineCallbacks
    def post(self,
            request,
            user_name,
            bucket_name,
            property_name,
            property_value):
        """
        Record property for visitor.
        """
        property_value = PropertyValueModel(
            user_name,
            bucket_name,
            property_name,
            property_value)
        visitor_id = request.args["visitor_id"][0]
        visitor = VisitorModel(
            user_name,
            bucket_name,
            visitor_id)
        yield property_value.create()
        property_ids = yield visitor.get_property_ids()
        if property_value.id in property_ids:
            return
        yield property_value.add_to_visitor(visitor)
        event_total = yield visitor.get_total()
        event_path = yield visitor.get_path()
        for event_id in event_total:
            event = EventModel(user_name, bucket_name, event_id=event_id)
            yield event.increment_total(
                True,
                property_id=property_value.id,
                value=event_total[event_id])
        for new_event_id in event_path:
            event = EventModel(user_name, bucket_name, event_id=new_event_id)
            for event_id in event_path[new_event_id]:
                yield event.increment_path(event_id,
                    True,  # Unique
                    property_id=property_value.id,
                    value=event_path[event.id][event_id])
