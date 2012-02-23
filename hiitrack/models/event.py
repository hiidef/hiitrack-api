#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Events are name/timestamp pairs linked to a visitor and stored in buckets.
"""

from ..lib.hash import pack_hash
from twisted.internet.defer import inlineCallbacks, returnValue
from ..lib.cassandra import insert_relation_by_id, increment_counter, \
    get_counter, BUFFER, get_relation, pack_hour, pack_day, pack_timestamp, \
    unpack_timestamp
from collections import defaultdict
from ..lib.profiler import profile
import ujson


_16_BYTE_FILLER = chr(0)*16
_32_BYTE_FILLER = chr(0)*32


def sort_nested_dict(d):
    """
    Sorts nested dictionary d[x][y] = sorted([])
    """
    for x in d:
        for y in d[x]:
            d[x][y] = sorted(d[x][y].items(), key=lambda z:z[0])
    return d


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
        self.shard = self.id[0]

    @profile
    def create(self):
        """
        Bucket event.
        """
        key = (self.user_name, self.bucket_name, "event")
        column_id = self.id
        value = self.event_name
        insert_relation_by_id(key, column_id, value)

    @profile
    @inlineCallbacks
    def get_properties(self):
        """
        Returns dictionary of id:name pairs for properties associated with the
        event.
        """
        key = (self.user_name, self.bucket_name, "event", self.shard)
        data = yield get_counter(key, prefix=self.id)
        property_prefix_ids = set([column_id[0:16] for column_id in data])
        key = (self.user_name, self.bucket_name, "property_name")
        column_ids = property_prefix_ids
        data = yield get_relation(key, column_ids=column_ids)
        returnValue(dict([(x, ujson.loads(data[x])) for x in data]))

    @profile
    @inlineCallbacks
    def get_name(self):
        """
        Gets event name.
        """
        key = (self.user_name, self.bucket_name, "event")
        column_id = self.id
        name = yield get_relation(key, column_id=column_id)
        self.event_name = name
        returnValue(name)

    @profile
    def increment_total(self, unique, property_id=None, value=1):
        """
        Increment the total count of event_id.
        """
        key = (self.user_name, self.bucket_name, "event", self.shard)
        column_id = "".join([self.id, property_id or _32_BYTE_FILLER])
        increment_counter(key, column_id=column_id, value=value)
        if unique:
            key = (
                self.user_name, 
                self.bucket_name, 
                "unique_event", 
                self.shard)
            increment_counter(key, column_id=column_id)
            if property_id:
                key = (
                    self.user_name, 
                    self.bucket_name, 
                    "property", 
                    property_id[0])
                column_id = "".join([property_id, self.id])
                increment_counter(key, column_id=column_id)

    @profile
    def increment_hourly_total(self, unique, property_id=None, value=1):
        """
        Increment the total hourly count of event_id.
        """
        key = (self.user_name, self.bucket_name, "hourly_event", self.shard)
        property_id = property_id or _32_BYTE_FILLER
        column_id = "".join([
            self.id,
            property_id[0:16],
            pack_hour(),
            property_id[16:32]])
        increment_counter(key, column_id=column_id, value=value)
        if unique:
            key = (
                self.user_name, 
                self.bucket_name, 
                "hourly_unique_event", 
                self.shard)
            increment_counter(key, column_id=column_id)

    @profile
    def increment_daily_total(self, unique, property_id=None, value=1):
        """
        Increment the total daily count of event_id.
        """
        key = (self.user_name, self.bucket_name, "daily_event", self.shard)
        property_id = property_id or _32_BYTE_FILLER
        column_id = "".join([
            self.id,
            property_id[0:16],
            pack_day(),
            property_id[16:32]])
        increment_counter(key, column_id=column_id, value=value)
        if unique:
            key = (
                self.user_name, 
                self.bucket_name, 
                "daily_unique_event", 
                self.shard)
            increment_counter(key, column_id=column_id)

    @profile
    def get_total(self, _property=None):
        """
        Get the count of event_id.
        """
        return self._get_total("event", _property)

    @profile
    def get_unique_total(self, _property=None):
        """
        Get the unique count of event_id.
        """
        return self._get_total("unique_event", _property)

    @inlineCallbacks
    def _get_total(self, hash_value, _property):
        """
        Get the count of event_id.
        """
        key = (self.user_name, self.bucket_name, hash_value, self.shard)
        if _property:
            property_prefix_id = _property.id
        else:
            property_prefix_id = _16_BYTE_FILLER
        prefix = self.id + property_prefix_id
        data = yield get_counter(key, prefix=prefix)
        result = defaultdict(lambda:defaultdict(lambda:0))
        for column_id in data:
            property_id = property_prefix_id + column_id[0:16]
            if property_id == _32_BYTE_FILLER:
                property_id = self.id
            result[property_id] = data[column_id]
        returnValue(result)

    @profile
    def get_timed_total(self, start, finish, interval="day", _property=None):
        """
        Get the timed counts.
        """
        if interval == "day":
            hash_value = "daily_event"
        elif interval == "hour":
            hash_value = "hourly_event"
        return self._get_timed_total(start, finish, hash_value, _property)

    @profile
    def get_timed_unique_total(
            self, 
            start, 
            finish, 
            interval="day", 
            _property=None):
        """
        Get the timed unique counts.
        """
        if interval == "day":
            hash_value = "daily_unique_event"
        elif interval == "hour":
            hash_value = "hourly_unique_event"
        return self._get_timed_total(start, finish, hash_value, _property)

    @inlineCallbacks
    def _get_timed_total(self, start, finish, hash_value, _property):
        """
        Get the regular or unique timed counts.
        """
        start = pack_timestamp(start)
        finish = pack_timestamp(finish)
        key = (self.user_name, self.bucket_name, hash_value, self.shard)
        if _property:
            property_prefix_id = _property.id
        else:
            property_prefix_id = _16_BYTE_FILLER
        prefix = self.id + property_prefix_id
        data = yield get_counter(key, prefix=prefix, start=start, finish=finish)
        result = defaultdict(list)
        for column_id in data:
            timestamp = unpack_timestamp(column_id[0:4])
            property_id = property_prefix_id + column_id[4:20]
            if property_id == _32_BYTE_FILLER:
                property_id = self.id
            result[property_id].append((timestamp, data[column_id]))
        for property_id in result:
            result[property_id] = sorted(result[property_id], key=lambda x:x[0])
        returnValue(result)

    @profile
    def increment_hourly_path(
            self, 
            event_id, 
            unique, 
            property_id=None, 
            value=1):
        """
        Increments the path by hour.
        """
        key = (self.user_name, self.bucket_name, "hourly_path", self.shard)
        property_id = property_id or _32_BYTE_FILLER
        column_id = "".join([
            self.id,
            property_id[0:16],
            pack_hour(),
            property_id[16:32],
            event_id])
        increment_counter(key, column_id=column_id, value=value)
        if unique:
            key = (
                self.user_name, 
                self.bucket_name, 
                "hourly_unique_path", 
                self.shard)
            increment_counter(key, column_id=column_id)

    @profile
    def increment_daily_path(self, event_id, unique, property_id=None, value=1):
        """
        Increments the path by day.
        """
        key = (self.user_name, self.bucket_name, "daily_path", self.shard)
        property_id = property_id or _32_BYTE_FILLER
        column_id = "".join([
            self.id,
            property_id[0:16],
            pack_day(),
            property_id[16:32],
            event_id])
        increment_counter(key, column_id=column_id, value=value)
        if unique:
            key = (
                self.user_name, 
                self.bucket_name, 
                "daily_unique_path", 
                self.shard)
            increment_counter(key, column_id=column_id)

    @profile
    def increment_path(self, event_id, unique, property_id=None, value=1):
        """
        Increment the path of events from self.id -> event_id.
        """
        key = (self.user_name, self.bucket_name, "path", self.shard)
        column_id = "".join([
            self.id,
            property_id or _32_BYTE_FILLER,
            event_id])
        increment_counter(key, column_id=column_id, value=value)
        if unique:
            key = (self.user_name, self.bucket_name, "unique_path", self.shard)
            increment_counter(key, column_id=column_id)

    @profile
    def get_path(self, _property=None):
        """
        Get the path of events.
        """
        return self._get_path("path", _property)

    @profile
    def get_unique_path(self, _property=None):
        """
        Get the unique path of visitor events.
        """
        return self._get_path("unique_path", _property)

    @inlineCallbacks
    def _get_path(self, hash_value, _property):
        """
        Get the regular or unique path of visitor events.
        """
        key = (self.user_name, self.bucket_name, hash_value, self.shard)
        if _property:
            property_prefix_id = _property.id
        else:
            property_prefix_id = _16_BYTE_FILLER
        prefix = self.id + property_prefix_id
        data = yield get_counter(key, prefix=prefix)
        result = defaultdict(lambda:defaultdict(lambda:0))
        for column_id in data:
            property_id = property_prefix_id + column_id[0:16]
            event_id = column_id[16:32]
            if property_id == _32_BYTE_FILLER:
                property_id = self.id
            result[property_id][event_id] = data[column_id]
        returnValue(result)

    @profile
    def get_timed_path(self, start, finish, interval="day", _property=None):
        """
        Get the hourly path of events.
        """
        if interval == "day":
            hash_value = "daily_path"
        elif interval == "hour":
            hash_value = "hourly_path"
        return self._get_timed_path(start, finish, hash_value, _property)

    @profile
    def get_timed_unique_path(
            self, 
            start, 
            finish, 
            interval="day", 
            _property=None):
        """
        Get the hourly path of events.
        """
        if interval == "day":
            hash_value = "daily_unique_path"
        elif interval == "hour":
            hash_value = "hourly_unique_path"
        return self._get_timed_path(start, finish, hash_value, _property)

    @inlineCallbacks
    def _get_timed_path(self, start, finish, hash_value, _property):
        """
        Get the regular or unique path of timed visitor events.
        """
        key = (self.user_name, self.bucket_name, hash_value, self.shard)
        if _property:
            property_prefix_id = _property.id
        else:
            property_prefix_id = _16_BYTE_FILLER
        start = pack_timestamp(start)
        finish = pack_timestamp(finish)
        prefix = self.id + property_prefix_id
        data = yield get_counter(
            key, 
            prefix=prefix, 
            start=start, 
            finish=finish)
        result = defaultdict(lambda:defaultdict(lambda:defaultdict(lambda:0)))
        for column_id in data:
            property_id = property_prefix_id + column_id[4:20]
            timestamp = unpack_timestamp(column_id[0:4])
            event_id = column_id[20:36]
            if property_id == _32_BYTE_FILLER:
                property_id = self.id
            result[property_id][event_id][timestamp] += data[column_id]
        returnValue(sort_nested_dict(result))

    @profile
    def batch_add(self, visitor, total, path, property_ids):
        """
        Add the event to the visitor, increment global counters, and return
        a list of deferreds.
        """
        unique = self.id not in total
        self.create()
        self.increment_total(unique)
        self.increment_hourly_total(unique)
        self.increment_daily_total(unique)
        visitor.increment_total(self.id)
        for property_id in property_ids:
            self.increment_total(unique, property_id)
            self.increment_hourly_total(unique, property_id)
            self.increment_daily_total(unique, property_id)
        for event_id in total:
            _unique = unique or event_id not in path[self.id]
            self.increment_path(event_id, _unique)
            self.increment_hourly_path(event_id, _unique)
            self.increment_daily_path(event_id, _unique)
            visitor.increment_path(event_id, self.id)
            for property_id in property_ids:
                self.increment_path(event_id, _unique, property_id)
                self.increment_hourly_path(event_id, _unique, property_id)
                self.increment_daily_path(event_id, _unique, property_id)
            path[event_id][self.id] += 1 # Update the visitor path for batch
        total[self.id] += 1 # Update the visitor total for batch

    @profile
    @inlineCallbacks
    def add(self, visitor):
        """
        Add the event to the visitor and increment global counters.
        """
        total, path, property_ids = yield visitor.get_metadata()
        self.batch_add(visitor, total, path, property_ids)
        yield BUFFER.flush()
