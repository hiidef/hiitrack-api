#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks, returnValue
from lib.agent import request
from hiitrack import HiiTrack
import uuid
import ujson
from base64 import b64encode
from random import randint
from urllib import urlencode
from urllib import quote
from collections import defaultdict
from pprint import pprint


class BatchTestCase(unittest.TestCase):

    @inlineCallbacks
    def setUp(self):
        self.hiitrack = HiiTrack(8080)
        self.hiitrack.startService()
        self.user_name = uuid.uuid4().hex
        self.password = uuid.uuid4().hex
        self.bucket_name = uuid.uuid4().hex
        yield request(
            "POST",
            "http://127.0.0.1:8080/%s" % self.user_name,
            data={"password":self.password}) 
        self.description = uuid.uuid4().hex
        self.url =  "http://127.0.0.1:8080/%s/%s" % (
            self.user_name, 
            self.bucket_name)
        result = yield request(
            "POST",
            self.url,
            username=self.user_name,
            password=self.password,
            data={"description":self.description})

    @inlineCallbacks
    def tearDown(self):
        yield request(
            "DELETE",
            self.url,
            username=self.user_name,
            password=self.password) 
        yield request(
            "DELETE",
            "http://127.0.0.1:8080/%s" % self.user_name,
            username=self.user_name,
            password=self.password) 
        self.hiitrack.stopService()

    @inlineCallbacks
    def get_event_dict(self):
        result = yield request(
            "GET",
            self.url,
            username=self.user_name,
            password=self.password)
        self.assertEqual(result.code, 200)
        events = ujson.loads(result.body)["events"].items()
        result = dict([(k, v["id"]) for k,v in events])
        returnValue(result)

    @inlineCallbacks
    def get_event(self, name, _property=None, start=None, finish=None, interval="day"):
        url = str("%s/event/%s" % (self.url, quote(name)))
        qs = {}
        if start:
            qs["start"] = start
            qs["interval"] = interval
            if finish:
                qs["finish"] = finish
        if _property:
            qs["property"] = _property
        if qs:
            url += "?%s" % urlencode(qs)
        result = yield request(
            "GET",
            url,
            username=self.user_name,
            password=self.password)
        self.assertEqual(result.code, 200)
        returnValue(ujson.loads(result.body))

    @inlineCallbacks
    def get_property(self, name):
        result = yield request(
            "GET",
            str("%s/property/%s" % (self.url, quote(name))),
            username=self.user_name,
            password=self.password)
        self.assertEqual(result.code, 200)
        data = ujson.loads(result.body)
        data["value_ids"] = dict([(data["values"][x]["value"], x) for x in data["values"]]) 
        returnValue(data)

    @inlineCallbacks
    def test_batch_insert(self):
        event_name_1 = "Event 1 %s" % uuid.uuid4().hex
        event_name_2 = "Event 2 %s" % uuid.uuid4().hex
        event_name_3 = "Event 3 %s" % uuid.uuid4().hex
        visitor_id_1 = uuid.uuid4().hex
        visitor_id_2 = uuid.uuid4().hex
        property_1_key = uuid.uuid4().hex
        property_1_value = "Property 1 %s" %  uuid.uuid4().hex
        property_2_key = uuid.uuid4().hex
        property_2_value = "Property 2 %s" % uuid.uuid4().hex
        property_3_key = uuid.uuid4().hex
        property_3_value = "Property 3 %s" % uuid.uuid4().hex
        # VISITOR 1
        events = [event_name_1, event_name_2, event_name_3]
        properties = [
            [property_1_key, property_1_value],
            [property_2_key, property_2_value],
            [property_3_key, property_3_value]]
        message = b64encode(ujson.dumps([
            events,
            properties]))
        request_id = randint(0, 100000)
        callback = uuid.uuid4().hex[0:10];
        qs = urlencode({
            "message": message, 
            "request_id": request_id,
            "callback": callback,
            "visitor_id": visitor_id_1})
        result = yield request(
            "GET",
            "%s/batch?%s" % (self.url, qs))
        self.assertEqual(result.body[0:11], "%s(" % callback)
        self.assertEqual(result.body[-2:], ");")
        payload = result.body[11:-2]
        self.assertEqual(ujson.loads(payload)["request_id"], str(request_id))
        # VISITOR 2
        events = [event_name_1, event_name_3]
        properties = []
        message = b64encode(ujson.dumps([
            events,
            properties]))
        request_id = randint(0, 100000)
        callback = uuid.uuid4().hex[0:10];
        qs = urlencode({
            "message": message, 
            "request_id": request_id,
            "callback": callback,
            "visitor_id": visitor_id_2})
        message = b64encode(ujson.dumps([
            events,
            properties]))
        result = yield request(
            "GET",
            "%s/batch?%s" % (self.url, qs))
        self.assertEqual(result.body[0:11], "%s(" % callback)
        self.assertEqual(result.body[-2:], ");")
        payload = result.body[11:-2]
        self.assertEqual(ujson.loads(payload)["request_id"], str(request_id))
        # Check events
        event_1 = yield self.get_event(event_name_1)
        event_2 = yield self.get_event(event_name_2)
        event_3 = yield self.get_event(event_name_3)
        event_1_property_1 = yield self.get_event(event_name_1, _property=property_1_key)
        event_2_property_1 = yield self.get_event(event_name_2, _property=property_1_key)
        event_3_property_1 = yield self.get_event(event_name_3, _property=property_1_key)
        event_1_property_2 = yield self.get_event(event_name_1, _property=property_2_key)
        event_2_property_2 = yield self.get_event(event_name_2, _property=property_2_key)
        event_3_property_2 = yield self.get_event(event_name_3, _property=property_2_key)
        event_1_property_3 = yield self.get_event(event_name_1, _property=property_3_key)
        event_2_property_3 = yield self.get_event(event_name_2, _property=property_3_key)
        event_3_property_3 = yield self.get_event(event_name_3, _property=property_3_key)
        events = yield self.get_event_dict()
        event_1_id = events[event_name_1]
        event_2_id = events[event_name_2]
        event_3_id = events[event_name_3]
        property_1 = yield self.get_property(property_1_key)
        property_2 = yield self.get_property(property_2_key)
        property_3 = yield self.get_property(property_3_key)
        property_1_id = property_1["value_ids"][property_1_value]
        property_2_id = property_2["value_ids"][property_2_value]
        property_3_id = property_3["value_ids"][property_3_value]
        # Event totals
        self.assertEqual(event_1["total"], 2)
        self.assertEqual(event_2["total"], 1)
        self.assertEqual(event_3["total"], 2)
        # Event property totals
        self.assertEqual(event_1_property_1["totals"][property_1_id], 1)
        self.assertEqual(event_2_property_1["totals"][property_1_id], 1)
        self.assertEqual(event_3_property_1["totals"][property_1_id], 1)
        self.assertEqual(event_1_property_2["totals"][property_2_id], 1)
        self.assertEqual(event_2_property_2["totals"][property_2_id], 1)
        self.assertEqual(event_3_property_2["totals"][property_2_id], 1)
        self.assertEqual(event_1_property_3["totals"][property_3_id], 1)
        self.assertEqual(event_2_property_3["totals"][property_3_id], 1)
        self.assertEqual(event_3_property_3["totals"][property_3_id], 1)
        # Property event totals
        self.assertEqual(property_1["values"][property_1_id]["total"][event_1_id], 1)
        self.assertEqual(property_1["values"][property_1_id]["total"][event_2_id], 1)
        self.assertEqual(property_1["values"][property_1_id]["total"][event_3_id], 1)
        self.assertEqual(property_2["values"][property_2_id]["total"][event_1_id], 1)
        self.assertEqual(property_2["values"][property_2_id]["total"][event_2_id], 1)
        self.assertEqual(property_2["values"][property_2_id]["total"][event_3_id], 1)
        self.assertEqual(property_3["values"][property_3_id]["total"][event_1_id], 1)
        self.assertEqual(property_3["values"][property_3_id]["total"][event_2_id], 1)
        self.assertEqual(property_3["values"][property_3_id]["total"][event_3_id], 1)
        # Event paths
        self.assertEqual(len(event_1["path"]), 0)
        # Event paths
        self.assertEqual(event_2["path"][event_1_id], 1)
        self.assertEqual(event_2_property_1["paths"][property_1_id][event_1_id], 1)
        self.assertEqual(event_2_property_2["paths"][property_2_id][event_1_id], 1)
        self.assertEqual(event_2_property_3["paths"][property_3_id][event_1_id], 1)
        # Event paths
        self.assertEqual(event_3["path"][event_1_id], 2)
        self.assertEqual(event_3_property_1["paths"][property_1_id][event_1_id], 1)
        self.assertEqual(event_3_property_2["paths"][property_2_id][event_1_id], 1)
        self.assertEqual(event_3_property_3["paths"][property_3_id][event_1_id], 1)
        self.assertEqual(event_3["path"][event_2_id], 1)
        self.assertEqual(event_3_property_1["paths"][property_1_id][event_2_id], 1)
        self.assertEqual(event_3_property_2["paths"][property_2_id][event_2_id], 1)
        self.assertEqual(event_3_property_3["paths"][property_3_id][event_2_id], 1)
