#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Funnels are a collection of events within a bucket.
"""

import ujson
from twisted.internet.defer import inlineCallbacks, returnValue
from ..lib.cassandra import get_relation, insert_relation, delete_relation
from ..lib.b64encode import uri_b64encode, uri_b64decode
from ..lib.profiler import profile


class FunnelModel(object):
    """
    Funnels are a collection of events within a bucket.
    """

    def __init__(self, user_name, bucket_name, funnel_name):
        self.user_name = user_name
        self.bucket_name = bucket_name
        self.funnel_name = funnel_name

    @profile
    @inlineCallbacks
    def create(self, description, event_ids):
        """
        Create funnel.
        """
        key = (self.user_name, self.bucket_name, "funnel")
        column = (self.funnel_name,)
        value = ujson.dumps((
            description,
            [uri_b64encode(x) for x in event_ids]))
        funnel_id = yield insert_relation(key, column, value)
        returnValue(funnel_id)

    @profile
    @inlineCallbacks
    def get_description_event_ids(self):
        """
        Get the description and event ids associated with funnel_id.
        """
        key = (self.user_name, self.bucket_name, "funnel")
        column = (self.funnel_name,)
        value = yield get_relation(key, column)
        description, encoded_event_ids = ujson.loads(value)
        returnValue((
            description,
            [uri_b64decode(str(x)) for x in encoded_event_ids]))

    @profile
    @inlineCallbacks
    def delete(self):
        """
        Delete the funnel.
        """
        key = (self.user_name, self.bucket_name, "funnel")
        column = (self.funnel_name,)
        yield delete_relation(key, column)
