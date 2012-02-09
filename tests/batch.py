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
    def get_property_dict(self):
        result = yield request(
            "GET",
            self.url,
            username=self.user_name,
            password=self.password)
        self.assertEqual(result.code, 200)
        result = ujson.loads(result.body)["properties"]
        response = defaultdict(dict)
        for property_name in result:
            for property_value in result[property_name]:
                response[property_name][property_value["value"]] = property_value["id"]
        returnValue(response)

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
    def get_event(self, name):
        result = yield request(
            "GET",
            str("%s/event/%s" % (self.url, quote(name))),
            username=self.user_name,
            password=self.password)
        self.assertEqual(result.code, 200)
        returnValue(ujson.loads(result.body))

    @inlineCallbacks
    def get_property(self, name, value):
        properties = yield self.get_property_dict()
        result = yield request(
            "GET",
            str("%s/property/%s/%s" % (self.url, quote(name), quote(value))),
            username=self.user_name,
            password=self.password)
        self.assertEqual(result.code, 200)
        returnValue(ujson.loads(result.body))

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
            "id": request_id,
            "callback": callback,
            "visitor_id": visitor_id_1})
        result = yield request(
            "GET",
            "%s/batch?%s" % (self.url, qs))
        self.assertEqual(result.body[0:11], "%s(" % callback)
        self.assertEqual(result.body[-2:], ");")
        payload = result.body[11:-2]
        self.assertEqual(ujson.loads(payload)["id"], str(request_id))
        event_1 = yield self.get_event(event_name_1)
        event_2 = yield self.get_event(event_name_2)
        event_3 = yield self.get_event(event_name_3)
        events = yield self.get_event_dict()
        event_1_id = events[event_name_1]
        event_2_id = events[event_name_2]
        event_3_id = events[event_name_3]
        properties = yield self.get_property_dict()
        property_1 = yield self.get_property(property_1_key, property_1_value)
        property_2 = yield self.get_property(property_2_key, property_2_value)
        property_3 = yield self.get_property(property_3_key, property_3_value)
        property_1_id = properties[property_1_key][property_1_value]
        property_2_id = properties[property_2_key][property_2_value]
        property_3_id = properties[property_3_key][property_3_value]
        # Event totals
        self.assertEqual(event_1["total"][event_1_id], 1)
        self.assertEqual(event_2["total"][event_2_id], 1)
        self.assertEqual(event_3["total"][event_3_id], 1)
        # Event property totals
        self.assertEqual(event_1["total"][property_1_id], 1)
        self.assertEqual(event_2["total"][property_1_id], 1)
        self.assertEqual(event_3["total"][property_1_id], 1)
        self.assertEqual(event_1["total"][property_2_id], 1)
        self.assertEqual(event_2["total"][property_2_id], 1)
        self.assertEqual(event_3["total"][property_2_id], 1)
        self.assertEqual(event_1["total"][property_3_id], 1)
        self.assertEqual(event_2["total"][property_3_id], 1)
        self.assertEqual(event_3["total"][property_3_id], 1)
        # Property totals
        self.assertEqual(property_1["total"][event_1_id], 1)
        self.assertEqual(property_2["total"][event_2_id], 1)
        self.assertEqual(property_3["total"][event_3_id], 1)
        # Property event totals
        self.assertEqual(property_1["total"][event_1_id], 1)
        self.assertEqual(property_1["total"][event_2_id], 1)
        self.assertEqual(property_1["total"][event_3_id], 1)
        self.assertEqual(property_2["total"][event_1_id], 1)
        self.assertEqual(property_2["total"][event_2_id], 1)
        self.assertEqual(property_2["total"][event_3_id], 1)
        self.assertEqual(property_3["total"][event_1_id], 1)
        self.assertEqual(property_3["total"][event_2_id], 1)
        self.assertEqual(property_3["total"][event_3_id], 1)