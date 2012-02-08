#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Properties are key/value pairs linked to a visitor and stored in buckets.
"""

import ujson
from twisted.internet.defer import inlineCallbacks, returnValue
from ..lib.hash import pack_hash
from ..lib.cassandra import insert_relation, get_counter, pack_timestamp, \
    insert_relation_by_id
from .event import EventModel
from ..lib.profiler import profile


class PropertyValueModel(object):
    """
    Properties are key/value pairs linked to a visitor and stored in buckets.
    """

    def __init__(self, user_name, bucket_name, property_name, property_value):
        self.user_name = user_name
        self.bucket_name = bucket_name
        self.property_name = property_name
        self.property_value = property_value
        self.id = pack_hash((property_name,)) + pack_hash((ujson.dumps(self.property_value),))

    @profile
    @inlineCallbacks
    def create(self):
        """
        Create property in a bucket.
        """
        key = (self.user_name, self.bucket_name, "property")
        column_id = self.id
        value = ujson.dumps((self.property_name, self.property_value))
        yield insert_relation_by_id(key, column_id, value)

    def get_name_and_value(self):
        """
        Return the name and value of the property associated with property_id.
        """
        return (self.property_name, self.property_value)

    @profile
    @inlineCallbacks
    def add_to_visitor(self, visitor):
        """
        Add property to visitor.
        """
        key = (self.user_name, self.bucket_name, "visitor_property")
        column_id = "".join([visitor.id, self.id])
        value = pack_timestamp()
        yield insert_relation_by_id(key, column_id, value)

    @profile
    @inlineCallbacks
    def get_total(self):
        """
        Get the events associated with this property.
        """
        key = (self.user_name, self.bucket_name, "property")
        prefix = self.id
        data = yield get_counter(key, prefix=prefix)
        returnValue(data)

    @profile
    @inlineCallbacks
    def add(self, visitor):
        """
        Add the property/value to the visitor and increment global counters.
        """    
        yield self.create()
        property_ids = yield visitor.get_property_ids()
        if self.id in property_ids:
            return
        yield self.add_to_visitor(visitor)
        event_total = yield visitor.get_total()
        event_path = yield visitor.get_path()
        for event_id in event_total:
            event = EventModel(
                self.user_name, 
                self.bucket_name, 
                event_id=event_id)
            yield event.increment_total(
                True,
                property_id=self.id,
                value=event_total[event_id])
        for new_event_id in event_path:
            event = EventModel(
                self.user_name, 
                self.bucket_name, 
                event_id=new_event_id)
            for event_id in event_path[new_event_id]:
                yield event.increment_path(event_id,
                    True,  # Unique
                    property_id=self.id,
                    value=event_path[event.id][event_id])
