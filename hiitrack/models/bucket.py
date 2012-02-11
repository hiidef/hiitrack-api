#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Buckets are a collection of events, properties, and funnels belonging to
a user.
"""

from collections import defaultdict
import ujson
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList
from telephus.cassandra.c08.ttypes import NotFoundException
from pylru import lrucache
from ..lib.cassandra import get_relation, insert_relation, delete_relation, \
    delete_counter, get_user, insert_relation_by_id
from ..exceptions import BucketException, UserException
from ..lib.profiler import profile
from .property import PropertyValueModel


LRU_CACHE = lrucache(1000)


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
        bucket = BucketModel(kwargs["user_name"],  kwargs["bucket_name"])
        if not bucket.cached():
            _exists = yield bucket.exists()
            if not _exists:
                request.setResponseCode(404)
                raise BucketException("Bucket %s does not "
                    "exist." % bucket.bucket_name)
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
        if not bucket.cached():
            _exists = yield bucket.exists()
            if not _exists:
                _user_exists = yield bucket.user_exists()
                if not _user_exists:
                    request.setResponseCode(404)
                    raise UserException("User %s does not exist." % user_name)               
                yield bucket.create(bucket_name)
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
        self.cache_key = "|".join((user_name, bucket_name))

    @profile
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

    def cached(self):
        return self.cache_key in LRU_CACHE

    @profile
    @inlineCallbacks
    def exists(self):
        """
        Verify bucket exists.
        """
        key = (self.user_name, "bucket")
        column_id = self.bucket_name
        try:
            yield get_relation(key, column_id=column_id)
        except NotFoundException:
            returnValue(False)
        returnValue(True)

    @profile
    def create(self, description):
        """
        Create bucket for username.
        """
        LRU_CACHE[self.cache_key] = None
        key = (self.user_name, "bucket")
        column_id = self.bucket_name
        value = ujson.dumps({"description":description})
        return insert_relation_by_id(key, column_id, value, commit=True)

    @profile
    @inlineCallbacks
    def get_property_ids(self):
        """
        Return property ids in bucket.
        """
        key = (self.user_name, self.bucket_name, "property")
        data = yield get_relation(key)
        returnValue(data.keys())

    @profile
    @inlineCallbacks
    def get_properties(self):
        """
        Return nested dictionary of
        property_name -> property_value -> property_id in bucket.
        """
        key = (self.user_name, self.bucket_name, "property")
        data = yield get_relation(key)
        properties = defaultdict(list)
        for property_id in data:
            name, value = ujson.loads(data[property_id])
            properties[name].append({
                "value":value, 
                "id":property_id})
        returnValue(properties)

    @profile
    @inlineCallbacks
    def get_events(self):
        """
        Return event_name/event_id pairs for the bucket.
        """
        key = (self.user_name, self.bucket_name, "event")
        data = yield get_relation(key)
        returnValue(dict([(data[i], {"id":i}) for i in data]))

    @profile
    @inlineCallbacks
    def get_description(self):
        """
        Return bucket description.
        """
        key = (self.user_name, "bucket")
        column_id = self.bucket_name
        data = yield get_relation(key, column_id=column_id)
        returnValue(ujson.loads(data)["description"])

    @profile
    @inlineCallbacks
    def delete(self):
        """
        Delete the bucket.
        """
        del LRU_CACHE[self.cache_key]
        key = (self.user_name, "bucket")
        column_id = self.bucket_name
        deferreds = [delete_relation(key, column_id=column_id)]
        keys = [
            (self.user_name, self.bucket_name, "property"),
            (self.user_name, self.bucket_name, "event"),
            (self.user_name, self.bucket_name, "funnel"),
            (self.user_name, self.bucket_name, "visitor_property")]
        for key in keys:
            deferreds.append(delete_relation(key))
        keys = [
            (self.user_name, self.bucket_name, "property"),
            (self.user_name, self.bucket_name, "event"),
            (self.user_name, self.bucket_name, "unique_event"),
            (self.user_name, self.bucket_name, "path"),
            (self.user_name, self.bucket_name, "unique_path"),
            (self.user_name, self.bucket_name, "visitor_event"),
            (self.user_name, self.bucket_name, "visitor_path")]
        for key in keys:
            deferreds.append(delete_counter(key))
        yield DeferredList(deferreds)