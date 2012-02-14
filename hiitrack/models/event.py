#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Events are name/timestamp pairs linked to a visitor and stored in buckets.
"""

from ..lib.hash import pack_hash
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList
from ..lib.cassandra import insert_relation_by_id, increment_counter, get_counter, \
    BUFFER, get_relation
from collections import defaultdict
from ..lib.b64encode import uri_b64encode
from ..lib.profiler import profile


_32_BYTE_FILLER = chr(0)*32


class EventModel(object):
    """
    Events are name/timestamp pairs linked to a visitor and stored in buckets.
    """

    def __init__(self, user_name, bucket_name, event_name=None, event_id=None):
        self.user_name = user_name
        self.bucket_name = bucket_name
        self.event_name = event_name
        if event_name:
            self.id = pack_hash((event_name,))
        elif event_id:
            self.id = event_id
        else:
            raise ValueError("EventModel requires 'event_name' or 'event_id'.")

    @profile
    def create(self):
        """
        Bucket event.
        """
        key = (self.user_name, self.bucket_name, "event")
        column_id = self.id
        value = self.event_name
        return insert_relation_by_id(key, column_id, value)

    @profile
    def get_name(self):   
        key = (self.user_name, self.bucket_name, "event")
        column_id = self.id
        return get_relation(key, column_id=column_id)

    @profile
    @inlineCallbacks
    def increment_total(self, unique, property_id=None, value=1):
        """
        Increment the total count of event_id.
        """
        key = (self.user_name, self.bucket_name, "event", self.id[0])
        column_id = "".join([self.id, property_id or self.id])
        deferreds = [increment_counter(key, column_id=column_id, value=value)]
        if unique:        
            key = (self.user_name, self.bucket_name, "unique_event", self.id[0])
            deferreds.append(increment_counter(key, column_id=column_id))
            if property_id:
                key = (self.user_name, self.bucket_name, "property", property_id[0])
                column_id = "".join([property_id, self.id])
                deferreds.append(increment_counter(key, column_id=column_id))
        yield DeferredList(deferreds)

    @profile
    def get_total(self):
        """
        Get the total count of event_id.
        """
        key = (self.user_name, self.bucket_name, "event", self.id[0])
        return get_counter(key, prefix=self.id)

    @profile
    def get_unique_total(self):
        """
        Get the total unique count of event_id.
        """
        key = (self.user_name, self.bucket_name, "unique_event", self.id[0])
        return get_counter(key, prefix=self.id)

    @profile
    @inlineCallbacks
    def increment_path(self, event_id, unique, property_id=None, value=1):
        """
        Increment the path of events from event_id -> new_event_id.
        """
        key = (self.user_name, self.bucket_name, "path", self.id[0])
        column_id = "".join([
            self.id,
            property_id or _32_BYTE_FILLER,
            event_id])
        deferreds = [increment_counter(key, column_id=column_id, value=value)]
        if unique:
            key = (self.user_name, self.bucket_name, "unique_path", self.id[0])
            deferreds.append(increment_counter(key, column_id=column_id))
        yield DeferredList(deferreds)
        
    @profile
    @inlineCallbacks
    def get_path(self):
        """
        Get the path of events.
        """
        key = (self.user_name, self.bucket_name, "path", self.id[0])
        prefix = self.id
        data = yield get_counter(key, prefix=prefix)
        result = defaultdict(dict)
        for column_id in data:
            property_id = column_id[0:32]
            event_id = column_id[32:]
            if property_id == _32_BYTE_FILLER:
                property_id = self.id
            result[property_id][event_id] = data[column_id]
        returnValue(result)

    @profile
    @inlineCallbacks
    def get_unique_path(self):
        """
        Get the unique path of visitor events.
        """
        key = (self.user_name, self.bucket_name, "unique_path", self.id[0])
        prefix = self.id
        data = yield get_counter(key, prefix=prefix)
        result = defaultdict(dict)
        for column_id in data:
            property_id = column_id[0:32]
            event_id = column_id[32:]
            if property_id == _32_BYTE_FILLER:
                property_id = self.id
            result[property_id][event_id] = data[column_id]
        returnValue(result)

    def batch_add(self, visitor, total, path, property_ids):
        """
        Add the event to the visitor, increment global counters, and return
        a list of deferreds.
        """
        unique = self.id not in total
        deferreds = [
            self.create(),
            self.increment_total(unique)]
        for property_id in property_ids:
            deferreds.append(self.increment_total(unique, property_id))
        deferreds.append(visitor.increment_total(self.id))
        for event_id in total:
            _unique = unique or event_id not in path[self.id]
            deferreds.append(visitor.increment_path(event_id, self.id))
            deferreds.append(self.increment_path(event_id, _unique))
            for property_id in property_ids:
                deferreds.append(self.increment_path(
                    event_id, 
                    _unique, 
                    property_id))
            path[event_id][self.id] += 1 #Update the visitor path for batch
        total[self.id] += 1 # Update the visitor total for batch
        return deferreds

    @profile 
    @inlineCallbacks
    def add(self, visitor):
        """
        Add the event to the visitor and increment global counters.
        """
        total, path, property_ids = yield visitor.get_metadata()
        deferreds = self.batch_add(visitor, total, path, property_ids)
        BUFFER.flush()
        yield DeferredList(deferreds)
