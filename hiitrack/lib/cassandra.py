#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Cassandra methods for dealing with hashed keys.
"""

from ..lib.hash import pack_hash
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred, \
    DeferredList
import struct
import time
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from ..lib.profiler import profile
from collections import defaultdict


CLIENT = None
HIGH_ID = chr(255) * 16

class Batched(object):

    def __init__(self):
        self.relation = {}

    def get_relation(self, key, column_id):
        pass

    def flush(self):
        pass


class Buffer(object):

    def __init__(self):
        self.relation = defaultdict(dict)
        self.counter = defaultdict(lambda:defaultdict(lambda:0))
        self.relation_deferreds = []
        self.counter_deferreds = []

    @inlineCallbacks
    def flush_relation(self):
        relation, self.relation = self.relation, defaultdict(dict)
        relation_deferreds, self.relation_deferreds = self.relation_deferreds, []
        try:
            yield CLIENT.batch_multikey_insert("relation", relation)
            for deferred in relation_deferreds:
                deferred.callback(True)
        except Exception, error:
            for deferred in relation_deferreds:
                deferred.errback(error)
            raise

    @inlineCallbacks
    def flush_counter(self):
        counter, self.counter = self.counter, defaultdict(lambda:defaultdict(lambda:0))
        counter_deferreds, self.counter_deferreds = self.counter_deferreds, []
        try:
            yield CLIENT.batch_multikey_add("counter", counter)
            for deferred in counter_deferreds:
                deferred.callback(True)
        except Exception, error:
            for deferred in counter_deferreds:
                deferred.errback(error)
            raise

    def insert_relation(self, key, column_id, value):
        self.relation[key][column_id] = value
        deferred = Deferred()
        self.relation_deferreds.append(deferred)
        return deferred

    def increment_counter(self, key, column_id, value):    
        self.counter[key][column_id] += value
        deferred = Deferred()
        self.counter_deferreds.append(deferred)
        return deferred

    def flush(self):
        self.flush_relation()
        self.flush_counter()


BUFFER = Buffer()


def pack_timestamp():
    """
    Return a packed byte string representing a timestamp.
    """
    return struct.pack(">1d", time.time())


def cols_to_dict(columns, prefix=None):
    """
    Convert a Cassandra row into a dictionary.
    """
    if prefix:
        prefix_length = len(prefix)
        return OrderedDict([
            (x.column.name[prefix_length:], x.column.value)
                for x in columns])
    else:
        return OrderedDict([(x.column.name, x.column.value)
            for x in columns])


def counter_cols_to_dict(columns, prefix=None):
    """
    Convert a Cassandra row into a dictionary.
    """
    if prefix:
        prefix_length = len(prefix)
        return OrderedDict([
            (x.counter_column.name[prefix_length:], x.counter_column.value)
                for x in columns])
    else:
        return OrderedDict([(x.counter_column.name, x.counter_column.value)
            for x in columns])


@profile
def set_user(key, column, value, consistency=None):
    """
    Sets a key and column in the user column family.
    """
    return CLIENT.insert(
        key=key,
        column_family="user",
        consistency=consistency,
        column=column,
        value=value)


@profile
@inlineCallbacks
def get_user(key, column, consistency=None):
    """
    Gets a key and column in the user column family.
    """
    result = yield CLIENT.get(
        key=key,
        column_family="user",
        consistency=consistency,
        column=column)
    returnValue(result.column.value)


@profile
def delete_user(key, consistency=None):
    """
    Deletes a key and column in the user column family.
    """
    return CLIENT.remove(
        key=key,
        column_family="user",
        consistency=consistency)


@profile
@inlineCallbacks
def get_relation(
        key,
        column=None,
        column_id=None,
        prefix=None,
        consistency=None):
    """
    Get a row, column, or slice from the relation column family.
    """
    key = pack_hash(key)
    if column_id:
        result = yield CLIENT.get(
            key=key,
            column_family="relation",
            consistency=consistency,
            column=column_id)
        returnValue(result.column.value)
    elif column:
        result = yield CLIENT.get(
            key=key,
            column_family="relation",
            consistency=consistency,
            column=pack_hash(column))
        returnValue(result.column.value)
    else:
        if prefix:
            start = prefix
            finish = prefix + HIGH_ID
        else:
            start = ''
            finish = ''
        result = yield CLIENT.get_slice(
            key=key,
            column_family="relation",
            start=start,
            finish=finish,
            consistency=consistency)
        returnValue(cols_to_dict(result, prefix=prefix))


@profile
def insert_relation(key, column, value, commit=False):
    """
    Insert a column into the relation column family using a hashed
    column tuple.
    """
    key = pack_hash(key)
    column_id = pack_hash(column)
    return _insert_relation(key, column_id, value, commit)


@profile
def insert_relation_by_id(key, column_id, value, commit=False):
    """
    Insert a column into the relation column family using a column ID.
    """
    key = pack_hash(key)
    return _insert_relation(key, column_id, value, commit)


def _insert_relation(key, column_id, value, commit):
    deferred = BUFFER.insert_relation(key, column_id, value)
    if commit:
        BUFFER.flush_relation()
    return deferred


@profile
def delete_relation(key, column=None, column_id=None, consistency=None):
    """
    Delete a row or column from the relation CF.
    """
    key = pack_hash(key)
    if column_id:
        return CLIENT.remove(
            key=key,
            column_family="relation",
            column=column_id,
            consistency=consistency)
    elif column:
        return CLIENT.remove(
            key=key,
            column_family="relation",
            column=pack_hash(column),
            consistency=consistency)
    else:
        return CLIENT.remove(
            key=key,
            column_family="relation",
            consistency=consistency)


@profile
def delete_relations(keys, consistency=None):
    keys = [pack_hash(key) for key in keys]
    return CLIENT.batch_remove_rows({"relation":keys}, consistency=consistency)


@profile
@inlineCallbacks
def get_counter(key, consistency=None, prefix=None):
    """
    Get all columns from a row of counters.
    """
    key = pack_hash(key)
    if prefix:
        start = prefix
        finish = prefix + HIGH_ID
    else:
        start = ''
        finish = ''
    result = yield CLIENT.get_slice(
        key=key,
        column_family="counter",
        consistency=consistency,
        start=start,
        finish=finish,
        count=10000)
    returnValue(counter_cols_to_dict(result, prefix=prefix))


@profile
@inlineCallbacks
def get_counters(keys, consistency=None, prefix=None):
    keys = [pack_hash(key) for key in keys]
    if prefix:
        start = prefix
        finish = prefix + HIGH_ID
    else:
        start = ''
        finish = ''
    data = yield CLIENT.multiget_slice(
        keys=keys,
        column_family="counter",
        consistency=consistency,
        start=start,
        finish=finish,
        count=10000)
    returnValue([counter_cols_to_dict(data[key], prefix=prefix) for key in keys])


@profile
def increment_counter(
        key,
        column=None,
        column_id=None,
        value=1,
        commit=False):
    """
    Increment a counter specified by a hashed column tuple or a column_id.
    """
    key = pack_hash(key)
    column_id = column_id or pack_hash(column)
    deferred = BUFFER.increment_counter(key, column_id, value)
    if commit:
        BUFFER.flush_counter()
    return deferred


@profile
def delete_counter(key, column=None, column_id=None, consistency=None):
    """
    Delete a row or column from the counter CF.
    """
    key = pack_hash(key)
    if column_id:
        return CLIENT.remove_counter(
            key=key,
            column_family="counter",
            column=column_id,
            consistency=consistency)
    elif column:
        return CLIENT.remove_counter(
            key=key,
            column_family="counter",
            column=pack_hash(column),
            consistency=consistency)
    else:
        return CLIENT.remove_counter(
            key=key,
            column_family="counter",
            consistency=consistency)

@profile
def delete_counters(keys, consistency=None):
    keys = [pack_hash(key) for key in keys]
    return CLIENT.batch_remove_rows({"counter":keys}, consistency=consistency)
