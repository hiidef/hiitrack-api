#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Buckets are a collection of events, properties, and funnels belonging to
a user.
"""

import ujson
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList
from telephus.cassandra.c08.ttypes import NotFoundException
from pylru import lrucache
from ..lib.cassandra import get_relation, delete_relation, get_user, \
    insert_relation_by_id, delete_relations, delete_counters
from ..exceptions import BucketException, UserException
from ..lib.profiler import profile
from ..lib.hash import password_hash
from base64 import b64encode


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
        bucket = BucketModel(kwargs["user_name"], kwargs["bucket_name"])
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
        """
        Check local cache for bucket's existence
        """
        return self.cache_key in LRU_CACHE

    @profile
    @inlineCallbacks
    def validate_password(self, password):
        try:
            _hash = yield get_user(self.user_name, "hash")
        except NotFoundException:
            returnValue(False)
        _bucket_hash = b64encode(password_hash(self.bucket_name, _hash))
        returnValue(password == _bucket_hash)

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
            LRU_CACHE[self.cache_key] = None
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
    def get_properties(self):
        """
        Return nested dictionary of
        property_name -> property_value -> property_id in bucket.
        """
        key = (self.user_name, self.bucket_name, "property_name")
        data = yield get_relation(key)
        returnValue(dict([(x, ujson.loads(data[x])) for x in data]))

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
        deferreds = []
        deferreds.append(delete_relation(key, column_id=column_id))
        keys = [
                (self.user_name, self.bucket_name, "event"),
                (self.user_name, self.bucket_name, "funnel"),
                (self.user_name, self.bucket_name, "property"),
                (self.user_name, self.bucket_name, "property_name")]
        for i in range(0, 256):
            shard = chr(i)
            keys.extend([(self.user_name, self.bucket_name, "visitor_property", shard)])
        deferreds.append(delete_relations(keys))
        keys = []
        hash_keys = ["property", "event", "hourly_event", "daily_event", 
                     "unique_event", "hourly_unique_event", 
                     "daily_unique_event", "path", "hourly_path", 
                     "daily_path", "unique_path", "hourly_unique_path", 
                     "daily_unique_path", "visitor_event", "visitor_path"]
        for i in range(0, 256):
            keys.extend([(self.user_name, self.bucket_name, x, chr(i)) 
                for x in hash_keys])
        deferreds.append(delete_counters(keys))
        yield DeferredList(deferreds)