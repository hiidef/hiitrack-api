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


class PropertyValueModel(object):
    """
    Properties are key/value pairs linked to a visitor and stored in buckets.
    """

    def __init__(self, user_name, bucket_name, property_name, property_value):
        self.user_name = user_name
        self.bucket_name = bucket_name
        self.property_name = property_name
        self.property_value = property_value
        self.id = pack_hash((
            user_name,
            bucket_name,
            "property",
            property_name,
            property_value))

    @inlineCallbacks
    def create(self):
        """
        Create property in a bucket.
        """
        key = (self.user_name, self.bucket_name, "property")
        column = (
            self.user_name,
            self.bucket_name,
            "property",
            self.property_name,
            self.property_value)
        value = ujson.dumps((self.property_name, self.property_value))
        yield insert_relation(key, column, value)

    def get_name_and_value(self):
        """
        Return the name and value of the property associated with property_id.
        """
        return (self.property_name, self.property_value)

    @inlineCallbacks
    def add_to_visitor(self, visitor):
        """
        Add property to visitor.
        """
        key = (self.user_name, self.bucket_name, "visitor_property")
        column_id = "".join([visitor.id, self.id])
        value = pack_timestamp()
        yield insert_relation_by_id(key, column_id, value)

    @inlineCallbacks
    def get_total(self):
        """
        Get the events associated with this property.
        """
        key = (self.user_name, self.bucket_name, "property")
        prefix = self.id
        data = yield get_counter(key, prefix=prefix)
        returnValue(data)
