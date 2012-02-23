#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Visitors are stored in buckets and can have properties and events.
"""

from twisted.internet.defer import inlineCallbacks, returnValue
from ..lib.hash import pack_hash
from ..lib.cassandra import get_counter, increment_counter, get_counters
from collections import defaultdict
from ..lib.profiler import profile


class VisitorModel(object):
    """
    Visitors are stored in buckets and can have properties and events.
    """

    def __init__(self, user_name, bucket_name, visitor_id):
        self.user_name = user_name
        self.bucket_name = bucket_name
        self.id = pack_hash((user_name, bucket_name, visitor_id))
        self.shard = self.id[0]

    @profile
    @inlineCallbacks
    def get_metadata(self):
        """
        Returns a visitor's events, path, and properties.
        """
        keys = [
            (self.user_name, self.bucket_name, "visitor_event", self.shard),
            (self.user_name, self.bucket_name, "visitor_path", self.shard),
            (self.user_name, self.bucket_name, "visitor_property", self.shard)]
        prefix = self.id
        events, paths, properties = yield get_counters(keys, prefix=prefix)
        events_result = defaultdict(lambda:0)
        for column_id in events:
            event_id = column_id[0:16]
            events_result[event_id] += events[column_id]
        path_result = defaultdict(lambda:defaultdict(lambda:0))
        for column_id in paths:
            new_event_id = column_id[0:16]
            event_id = column_id[16:32]
            path_result[new_event_id][event_id] += paths[column_id]
        returnValue((events_result, path_result, properties.keys()))

    @profile
    def add_property(self, _property):
        """
        Add property to visitor.
        """
        key = (self.user_name, self.bucket_name, "visitor_property", self.shard)
        column_id = "".join([self.id, _property.id])
        increment_counter(key, column_id=column_id)

    @profile
    def increment_path(self, event_id, new_event_id):
        """
        Increment the path of visitor events from event_id -> new_event_id.
        """
        key = (self.user_name, self.bucket_name, "visitor_path", self.shard)
        column_id = "".join([self.id, new_event_id, event_id])
        increment_counter(key, column_id=column_id)

    @profile
    @inlineCallbacks
    def get_path(self):
        """
        Get the path of visitor events.
        """
        key = (self.user_name, self.bucket_name, "visitor_path", self.shard)
        prefix = self.id
        data = yield get_counter(key, prefix=prefix)
        result = defaultdict(lambda:defaultdict(lambda:0))
        for column_id in data:
            new_event_id = column_id[0:16]
            event_id = column_id[16:32]
            result[new_event_id][event_id] += data[column_id]
        returnValue(result)

    @profile
    def increment_total(self, event_id):
        """
        Increment the count of visitor events.
        """
        key = (self.user_name, self.bucket_name, "visitor_event", self.shard)
        column_id = "".join([self.id, event_id])
        increment_counter(key, column_id=column_id)

    @profile
    @inlineCallbacks
    def get_total(self):
        """
        Get the count of visitor events.
        """
        key = (self.user_name, self.bucket_name, "visitor_event", self.shard)
        prefix = self.id
        data = yield get_counter(key, prefix=prefix)
        result = defaultdict(lambda:0)
        for column_id in data:
            event_id = column_id[0:16]
            result[event_id] += data[column_id]
        returnValue(result)

