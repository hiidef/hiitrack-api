#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Visitors are stored in buckets and can have properties and events.
"""

from twisted.internet.defer import inlineCallbacks, returnValue
from ..lib.hash import pack_hash
from ..lib.cassandra import get_relation, get_counter, increment_counter
from collections import defaultdict


class VisitorModel(object):
    """
    Visitors are stored in buckets and can have properties and events.
    """

    def __init__(self, user_name, bucket_name, visitor_id):
        self.user_name = user_name
        self.bucket_name = bucket_name
        self.id = pack_hash((user_name, bucket_name, visitor_id))

    @inlineCallbacks
    def get_property_ids(self):
        """
        Return ids of visitor properties.
        """
        key = (self.user_name, self.bucket_name, "visitor_property")
        prefix = self.id
        data = yield get_relation(key, prefix=prefix)
        returnValue(data.keys())

    @inlineCallbacks
    def get_event_ids(self):
        """
        Return ids of visitor events.
        """
        key = (self.user_name, self.bucket_name, "visitor_event")
        prefix = self.id
        data = yield get_counter(key, prefix=prefix)
        returnValue(data.keys())

    @inlineCallbacks
    def increment_path(self, event_id, new_event_id):
        """
        Increment the path of visitor events from event_id -> new_event_id.
        """
        key = (self.user_name, self.bucket_name, "visitor_path")
        column_id = "".join([self.id, new_event_id, event_id])
        yield increment_counter(key, column_id=column_id)

    @inlineCallbacks
    def get_path(self):
        """
        Get the path of visitor events.
        """
        key = (self.user_name, self.bucket_name, "visitor_path")
        prefix = self.id
        data = yield get_counter(key, prefix=prefix)
        result = defaultdict(dict)
        for column_id in data:
            new_event_id = column_id[0:16]
            event_id = column_id[16:]
            result[new_event_id][event_id] = data[column_id]
        returnValue(result)

    @inlineCallbacks
    def increment_total(self, event_id):
        """
        Increment the count of visitor events.
        """
        key = (self.user_name, self.bucket_name, "visitor_event")
        column_id = "".join([self.id, event_id])
        yield increment_counter(key, column_id=column_id)

    @inlineCallbacks
    def get_total(self):
        """
        Get the count of visitor events.
        """
        key = (self.user_name, self.bucket_name, "visitor_event")
        prefix = self.id
        data = yield get_counter(key, prefix=prefix)
        returnValue(data)
