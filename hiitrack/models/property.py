#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Properties are key/value pairs linked to a visitor and stored in buckets.
"""

import ujson
from itertools import chain
from twisted.internet.defer import inlineCallbacks, returnValue
from collections import defaultdict
from ..lib.hash import pack_hash
from ..lib.cassandra import get_counter, insert_relation_by_id, BUFFER, \
    get_relation
from .event import EventModel
from ..lib.profiler import profile


class PropertyModel(object):
    """
    Properties are key/value pairs linked to a visitor and stored in buckets.
    """
    def __init__(
            self,
            user_name,
            bucket_name,
            property_name=None,
            property_prefix_id=None):
        self.user_name = user_name
        self.bucket_name = bucket_name
        self.property_name = property_name
        if property_name:
            self.id = pack_hash((property_name,))
        elif property_prefix_id:
            self.id = property_prefix_id
        else:
            raise ValueError("PropertyModel requires 'property_name' "
                "or 'property_id'.")

    @inlineCallbacks
    def get_name(self):
        """
        Return the name of the property.
        """
        key = (self.user_name, self.bucket_name, "property_name")
        column_id = self.id
        data = yield get_relation(key, column_id=column_id)
        returnValue(ujson.loads(data))

    @inlineCallbacks
    def get_values(self):
        """
        Get the values associated with the property.
        """
        key = (self.user_name, self.bucket_name, "property")
        prefix = self.id
        data = yield get_relation(key, prefix=prefix)
        returnValue(dict([(self.id + x[0], ujson.loads(x[1])[1]) 
            for x in data.items()]))

    @inlineCallbacks
    def get_totals(self):
        """
        Get the counts associated with the property values.
        """
        key = (self.user_name, self.bucket_name, "property", self.id[0])
        prefix = self.id
        data = yield get_counter(key, prefix=prefix)
        response = defaultdict(lambda:defaultdict(lambda:0))
        for column_id in data:
            property_id = self.id + column_id[0:16]
            event_id = column_id[16:]
            response[property_id][event_id] = data[column_id]
        returnValue(response)

    @inlineCallbacks
    def get_events(self):
        key = (self.user_name, self.bucket_name, "property", self.id[0])
        prefix = self.id
        data = yield get_counter(key, prefix=prefix)
        column_ids = set([column_id[16:] for column_id in data])
        key = (self.user_name, self.bucket_name, "event")
        events = yield get_relation(key, column_ids=column_ids)
        returnValue(events)


class PropertyValueModel(object):
    """
    Properties are key/value pairs linked to a visitor and stored in buckets.
    """

    def __init__(self, user_name, bucket_name, property_name, property_value):
        self.user_name = user_name
        self.bucket_name = bucket_name
        self.property_name = property_name
        self.property_value = property_value
        self.id = "".join([
            pack_hash((property_name,)),
            pack_hash((ujson.dumps(self.property_value),))])

    @profile
    def create(self):
        """
        Create property in a bucket.
        """
        key = (self.user_name, self.bucket_name, "property")
        column_id = self.id
        value = ujson.dumps((self.property_name, self.property_value))
        insert_relation_by_id(key, column_id, value)
        key = (self.user_name, self.bucket_name, "property_name")
        column_id = self.id[0:16]
        value = ujson.dumps(self.property_name)
        insert_relation_by_id(key, column_id, value)

    def get_name_and_value(self):
        """
        Return the name and value of the property.
        """
        return (self.property_name, self.property_value)

    @profile
    def get_total(self):
        """
        Get the events associated with this property.
        """
        key = (self.user_name, self.bucket_name, "property", self.id[0])
        prefix = self.id
        return get_counter(key, prefix=prefix)

    def batch_add(self, visitor, total, path, property_ids):
        """
        Add property/value to the visitor and return a list of deferreds.
        """
        if self.id in property_ids:
            return []
        for event_id in total:
            event = EventModel(
                self.user_name,
                self.bucket_name,
                event_id=event_id)
            event.increment_total(
                True,
                property_id=self.id,
                value=total[event_id])
        for new_event_id in path:
            event = EventModel(
                self.user_name,
                self.bucket_name,
                event_id=new_event_id)
            for event_id in path[new_event_id]:
                event.increment_path(event_id,
                    True,  # Unique
                    property_id=self.id,
                    value=path[event.id][event_id])
        self.create()
        visitor.add_property(self)

    @profile
    @inlineCallbacks
    def add(self, visitor):
        """
        Add the property/value to the visitor and increment global counters.
        """
        total, path, property_ids = yield visitor.get_metadata()
        self.batch_add(visitor, total, path, property_ids)
        yield BUFFER.flush()
