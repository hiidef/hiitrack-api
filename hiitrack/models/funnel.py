#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Funnels are a collection of events within a bucket.
"""

import ujson
from twisted.internet.defer import inlineCallbacks
from ..lib.cassandra import get_relation, insert_relation, delete_relation
from ..lib.b64encode import uri_b64encode, uri_b64decode
from ..lib.profiler import profile
from .property import PropertyModel

class FunnelModel(object):
    """
    Funnels are a collection of events within a bucket.
    """



    def __init__(self, user_name, bucket_name, funnel_name):
        self.user_name = user_name
        self.bucket_name = bucket_name
        self.funnel_name = funnel_name
        self.description = None
        self.property = None
        self.event_ids = []

    @profile
    def create(self, description, event_ids, property_name=None):
        """
        Create funnel.
        """
        key = (self.user_name, self.bucket_name, "funnel")
        column = (self.funnel_name,)
        self.description = description
        if property_name:
            self.property = PropertyModel(
                self.user_name,
                self.bucket_name,
                property_name)
        else:
            self.property = None
        self.event_ids = event_ids
        value = ujson.dumps((
            description,
            property_name,
            [uri_b64encode(x) for x in event_ids]))
        return insert_relation(key, column, value, commit=True)

    @profile
    @inlineCallbacks
    def get(self):
        """
        Get a saved funnel.
        """
        key = (self.user_name, self.bucket_name, "funnel")
        column = (self.funnel_name,)
        value = yield get_relation(key, column)
        description, property_name, encoded_event_ids = ujson.loads(value)
        self.description = description
        if property_name:
            self.property = PropertyModel(
                self.user_name,
                self.bucket_name,
                property_name.encode("utf-8"))
        else:
            self.property = None
        self.event_ids = [uri_b64decode(str(x)) for x in encoded_event_ids]

    @profile
    def delete(self):
        """
        Delete the funnel.
        """
        key = (self.user_name, self.bucket_name, "funnel")
        column = (self.funnel_name,)
        return delete_relation(key, column)
