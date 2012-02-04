#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Events are name/timestamp pairs linked to a visitor and stored in buckets.
"""

from ..lib.hash import pack_hash
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList
from ..lib.cassandra import insert_relation, increment_counter, get_counter
from collections import defaultdict


class EventModel(object):
    """
    Events are name/timestamp pairs linked to a visitor and stored in buckets.
    """

    def __init__(self, user_name, bucket_name, event_name=None, event_id=None):
        self.user_name = user_name
        self.bucket_name = bucket_name
        self.event_name = event_name
        if event_name:
            self.id = pack_hash((user_name, bucket_name, "event", event_name))
        elif event_id:
            self.id = event_id
        else:
            raise ValueError("EventModel requires 'event_name' or 'event_id'.")

    @inlineCallbacks
    def create(self):
        """
        Bucket event.
        """
        key = (self.user_name, self.bucket_name, "event")
        column = (self.user_name, self.bucket_name, "event", self.event_name)
        value = self.event_name
        yield insert_relation(key, column, value)

    @inlineCallbacks
    def increment_total(self, unique, property_id=None, value=1):
        """
        Increment the total count of event_id.
        """
        key = (self.user_name, self.bucket_name, "event")
        column_id = "".join([self.id, property_id or self.id])
        yield increment_counter(key, column_id=column_id, value=value)
        if not unique:
            return
        key = (self.user_name, self.bucket_name, "unique_event")
        yield increment_counter(key, column_id=column_id)
        if property_id:
            key = (self.user_name, self.bucket_name, "property")
            column_id = "".join([property_id, self.id])
            yield increment_counter(key, column_id=column_id)

    @inlineCallbacks
    def get_total(self):
        """
        Get the total count of event_id.
        """
        key = (self.user_name, self.bucket_name, "event")
        data = yield get_counter(key, prefix=self.id)
        returnValue(data)

    @inlineCallbacks
    def get_unique_total(self):
        """
        Get the total unique count of event_id.
        """
        key = (self.user_name, self.bucket_name, "unique_event")
        data = yield get_counter(key, prefix=self.id)
        returnValue(data)

    @inlineCallbacks
    def increment_path(self, event_id, unique, property_id=None, value=1):
        """
        Increment the path of events from event_id -> new_event_id.
        """
        key = (self.user_name, self.bucket_name, "path")
        column_id = "".join([
            self.id,
            property_id or self.id,
            event_id])
        yield increment_counter(key, column_id=column_id, value=value)
        if not unique:
            return
        key = (self.user_name, self.bucket_name, "unique_path")
        yield increment_counter(key, column_id=column_id)

    @inlineCallbacks
    def get_path(self):
        """
        Get the path of events.
        """
        key = (self.user_name, self.bucket_name, "path")
        prefix = self.id
        data = yield get_counter(key, prefix=prefix)
        result = defaultdict(dict)
        for column_id in data:
            event_id = column_id[0:16]
            property_id = column_id[16:]
            result[event_id][property_id] = data[column_id]
        returnValue(result)

    @inlineCallbacks
    def get_unique_path(self):
        """
        Get the unique path of visitor events.
        """
        key = (self.user_name, self.bucket_name, "unique_path")
        prefix = self.id
        data = yield get_counter(key, prefix=prefix)
        result = defaultdict(dict)
        for column_id in data:
            event_id = column_id[0:16]
            property_id = column_id[16:]
            result[event_id][property_id] = data[column_id]
        returnValue(result)
    
    @inlineCallbacks
    def add(self, visitor):
        """
        Add the event to the visitor and increment global counters.
        """         
        event_ids = yield visitor.get_event_ids()
        path = yield visitor.get_path()
        property_ids = yield visitor.get_property_ids()
        unique = self.id not in event_ids
        deferreds = [
            self.create(),
            self.increment_total(unique)]
        for property_id in property_ids:
            deferreds.append(self.increment_total(unique, property_id))
        deferreds.append(visitor.increment_total(self.id))
        for event_id in event_ids:
            _unique = unique or event_id not in path[self.id]
            deferreds.append(visitor.increment_path(event_id, self.id))
            deferreds.append(self.increment_path(event_id, _unique))
            for property_id in property_ids:
                deferreds.append(self.increment_path(event_id, _unique, property_id))
        yield DeferredList(deferreds)
