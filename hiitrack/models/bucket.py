#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Buckets are a collection of events, properties, and funnels belonging to
a user.
"""

from collections import defaultdict
import ujson
from twisted.internet.defer import inlineCallbacks, returnValue
from telephus.cassandra.c08.ttypes import NotFoundException
from ..lib.cassandra import get_relation, insert_relation, delete_relation, \
    delete_counter, get_user
from ..exceptions import BucketException, UserException


def bucket_check(method):
    """
    Decorator.
    """
    @inlineCallbacks
    def wrapper(*args, **kwargs):
        """
        Verifies bucket exists.
        """
        request = args[1]
        # Dispatcher makes some args into kwargs.
        user_name = kwargs["user_name"]
        bucket_name = kwargs["bucket_name"]
        _exists = yield BucketModel(user_name, bucket_name).exists()
        if not _exists:
            request.setResponseCode(404)
            raise BucketException("Bucket %s does not exist." % bucket_name)
        data = yield method(*args, **kwargs)
        returnValue(data)
    return wrapper


def bucket_create(method):
    """
    Decorator.
    """
    @inlineCallbacks
    def wrapper(*args, **kwargs):
        """
        Creates new bucket if bucket does not exist.
        """
        request = args[1]
        # Dispatcher makes some args into kwargs.
        user_name = kwargs["user_name"]
        bucket_name = kwargs["bucket_name"]
        bucket = BucketModel(user_name, bucket_name)
        _exists = yield bucket.exists()
        if not _exists:
            _user_exists = yield bucket.user_exists()
            if not _user_exists:
                request.setResponseCode(404)
                raise UserException("User %s does not exist." % user_name)                
            yield BucketModel(user_name, bucket_name).create("")
        data = yield method(*args, **kwargs)
        returnValue(data)
    return wrapper


class BucketModel(object):
    """
    Buckets are a collection of events, properties, and funnels belonging to
    a user.
    """

    def __init__(self, user_name, bucket_name):
        self.user_name = user_name
        self.bucket_name = bucket_name

    @inlineCallbacks
    def user_exists(self):
        """
        Returns boolean indicating whether user exists.
        """
        try:
            yield get_user(self.user_name, "hash")
            returnValue(True)
        except NotFoundException:
            returnValue(False)

    @inlineCallbacks
    def exists(self):
        """
        Verify bucket exists.
        """
        key = (self.user_name, "bucket")
        column = (self.bucket_name,)
        try:
            yield get_relation(key, column)
        except NotFoundException:
            returnValue(False)
        returnValue(True)

    @inlineCallbacks
    def create(self, description):
        """
        Create bucket for username.
        """
        key = (self.user_name, "bucket")
        column = (self.bucket_name,)
        value = ujson.dumps((self.bucket_name, description))
        yield insert_relation(key, column, value)

    @inlineCallbacks
    def get_property_ids(self):
        """
        Return property ids in bucket.
        """
        key = (self.user_name, self.bucket_name, "property")
        data = yield get_relation(key)
        returnValue(data.keys())

    @inlineCallbacks
    def get_properties(self):
        """
        Return nested dictionary of
        property_name -> property_value -> property_id in bucket.
        """
        key = (self.user_name, self.bucket_name, "property")
        data = yield get_relation(key)
        properties = defaultdict(dict)
        for property_id in data:
            key, value = ujson.loads(data[property_id])
            properties[key][value] = property_id
        returnValue(properties)

    @inlineCallbacks
    def get_events(self):
        """
        Return event_name/event_id pairs for the bucket.
        """
        key = (self.user_name, self.bucket_name, "event")
        data = yield get_relation(key)
        returnValue(dict([(v, k) for k, v in data.items()]))

    @inlineCallbacks
    def get_name_and_description(self):
        """
        Return bucket description.
        """
        key = (self.user_name, "bucket")
        column = (self.bucket_name,)
        data = yield get_relation(key, column)
        bucket_name, description = ujson.loads(data)
        returnValue((bucket_name, description))

    @inlineCallbacks
    def delete(self):
        """
        Delete the bucket.
        """
        key = (self.user_name, "bucket")
        column = (self.bucket_name,)
        yield delete_relation(key, column)
        keys = [
            (self.user_name, self.bucket_name, "property"),
            (self.user_name, self.bucket_name, "event"),
            (self.user_name, self.bucket_name, "funnel"),
            (self.user_name, self.bucket_name, "visitor_property")]
        for key in keys:
            yield delete_relation(key)
        keys = [
            (self.user_name, self.bucket_name, "property"),
            (self.user_name, self.bucket_name, "event"),
            (self.user_name, self.bucket_name, "unique_event"),
            (self.user_name, self.bucket_name, "path"),
            (self.user_name, self.bucket_name, "unique_path"),
            (self.user_name, self.bucket_name, "visitor_event"),
            (self.user_name, self.bucket_name, "visitor_path")]
        for key in keys:
            yield delete_counter(key)
