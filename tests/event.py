#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks, returnValue
from lib.agent import request
from hiitrack import HiiTrack
import uuid
import ujson
from pprint import pprint
from urllib import quote
from collections import defaultdict
from base64 import b64encode
from urllib import urlencode

class EventTestCase(unittest.TestCase):
    
    @inlineCallbacks
    def setUp(self):
        self.hiitrack = HiiTrack(8080)
        self.hiitrack.startService()
        self.username = uuid.uuid4().hex
        self.password = uuid.uuid4().hex
        yield request(
            "POST",
            "http://127.0.0.1:8080/%s" % self.username,
            data={"password":self.password}) 
        self.description = uuid.uuid4().hex
        self.url =  "http://127.0.0.1:8080/%s/%s" % (
            self.username, 
            uuid.uuid4().hex)
        result = yield request(
            "POST",
            self.url,
            username=self.username,
            password=self.password,
            data={"description":self.description})

    @inlineCallbacks
    def tearDown(self):
        yield request(
            "DELETE",
            self.url,
            username=self.username,
            password=self.password) 
        yield request(
            "DELETE",
            "http://127.0.0.1:8080/%s" % self.username,
            username=self.username,
            password=self.password) 
        self.hiitrack.stopService()

    @inlineCallbacks
    def get_property_dict(self):
        result = yield request(
            "GET",
            self.url,
            username=self.username,
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
            username=self.username,
            password=self.password)
        self.assertEqual(result.code, 200)
        events = ujson.loads(result.body)["events"].items()
        result = dict([(k, v["id"]) for k,v in events])
        returnValue(result)

    @inlineCallbacks
    def post_property(self, visitor_id, name, value):
        qs = urlencode({"value":b64encode(ujson.dumps(value))})
        result = yield request(
            "POST",
            "%s/property/%s?%s" % (self.url, quote(name), qs),
            data={"visitor_id":visitor_id})
        self.assertEqual(result.code, 200)
        returnValue(result)

    @inlineCallbacks
    def post_event(self, visitor_id, name):
        result = yield request(
            "POST",
            "%s/event/%s" % (self.url, quote(name)),
            data={"visitor_id":visitor_id})
        self.assertEqual(result.code, 200)
        returnValue(result)

    @inlineCallbacks
    def get_event(self, name):
        result = yield request(
            "GET",
            str("%s/event/%s" % (self.url, quote(name))),
            username=self.username,
            password=self.password)
        self.assertEqual(result.code, 200)
        returnValue(ujson.loads(result.body))

    @inlineCallbacks
    def get_property(self, name):
        properties = yield self.get_property_dict()
        result = yield request(
            "GET",
            str("%s/property/%s" % (self.url, quote(name))),
            username=self.username,
            password=self.password)
        self.assertEqual(result.code, 200)
        returnValue(ujson.loads(result.body))

    @inlineCallbacks
    def test_bucket(self):    
        NAME = uuid.uuid4().hex
        visitor_id_1 = uuid.uuid4().hex
        result = yield self.post_event(visitor_id_1, NAME)
        result = yield self.get_event(NAME)

    @inlineCallbacks
    def test_get(self):          
        event_name_1 = "Event 1 %s" % uuid.uuid4().hex
        event_name_2 = "Event 2 %s" % uuid.uuid4().hex
        event_name_3 = "Event 3 %s" % uuid.uuid4().hex
        visitor_id_1 = uuid.uuid4().hex
        yield self.post_event(visitor_id_1, event_name_1)
        yield self.post_event(visitor_id_1, event_name_2)
        yield self.post_event(visitor_id_1, event_name_3)
        yield self.post_event(visitor_id_1, event_name_1)
        yield self.post_event(visitor_id_1, event_name_2)
        yield self.post_event(visitor_id_1, event_name_3)
        yield self.post_event(visitor_id_1, event_name_1)
        yield self.post_event(visitor_id_1, event_name_2)
        yield self.post_event(visitor_id_1, event_name_2)
        yield self.post_event(visitor_id_1, event_name_1)
        event_1 = yield self.get_event(event_name_1)
        event_2 = yield self.get_event(event_name_2)
        event_3 = yield self.get_event(event_name_3)
        events = yield self.get_event_dict()
        event_1_id = events[event_name_1]
        event_2_id = events[event_name_2]
        event_3_id = events[event_name_3]
        self.assertEqual(event_1["total"][event_1_id], 4)
        self.assertEqual(event_2["total"][event_2_id], 4)
        self.assertEqual(event_3["total"][event_3_id], 2)
        self.assertEqual(event_1["path"][event_1_id][event_1_id], 3)
        self.assertEqual(event_1["path"][event_1_id][event_2_id], 3)
        self.assertEqual(event_1["path"][event_1_id][event_3_id], 3)
        self.assertEqual(event_2["path"][event_2_id][event_1_id], 4)
        self.assertEqual(event_2["path"][event_2_id][event_2_id], 3)
        self.assertEqual(event_2["path"][event_2_id][event_3_id], 3)
        self.assertEqual(event_3["path"][event_3_id][event_1_id], 2)
        self.assertEqual(event_3["path"][event_3_id][event_2_id], 2)
        self.assertEqual(event_3["path"][event_3_id][event_3_id], 1)
        self.assertEqual(event_1["unique_path"][event_1_id][event_1_id], 1)
        self.assertEqual(event_1["unique_path"][event_1_id][event_2_id], 1)
        self.assertEqual(event_1["unique_path"][event_1_id][event_3_id], 1)
        self.assertEqual(event_2["unique_path"][event_2_id][event_1_id], 1)
        self.assertEqual(event_2["unique_path"][event_2_id][event_2_id], 1)
        self.assertEqual(event_2["unique_path"][event_2_id][event_3_id], 1)
        self.assertEqual(event_3["unique_path"][event_3_id][event_1_id], 1)
        self.assertEqual(event_3["unique_path"][event_3_id][event_2_id], 1)
        self.assertEqual(event_3["unique_path"][event_3_id][event_3_id], 1)
         
    @inlineCallbacks
    def test_property_get(self):   
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
        yield self.post_property(visitor_id_1, property_1_key, property_1_value)
        yield self.post_event(visitor_id_1, event_name_1)
        yield self.post_event(visitor_id_1, event_name_1)
        yield self.post_event(visitor_id_2, event_name_1)
        yield self.post_event(visitor_id_1, event_name_2)
        yield self.post_event(visitor_id_2, event_name_2)
        yield self.post_property(visitor_id_1, property_2_key, property_2_value)
        yield self.post_property(visitor_id_2, property_2_key, property_2_value)
        yield self.post_event(visitor_id_1, event_name_3)
        yield self.post_event(visitor_id_2, event_name_3)
        yield self.post_event(visitor_id_2, event_name_3)
        yield self.post_property(visitor_id_1, property_3_key, property_3_value)
        event_1 = yield self.get_event(event_name_1)
        event_2 = yield self.get_event(event_name_2)
        event_3 = yield self.get_event(event_name_3)
        events = yield self.get_event_dict()
        event_1_id = events[event_name_1]
        event_2_id = events[event_name_2]
        event_3_id = events[event_name_3]
        properties = yield self.get_property_dict()
        property_1 = yield self.get_property(property_1_key)
        property_2 = yield self.get_property(property_2_key)
        property_3 = yield self.get_property(property_3_key)
        property_1_id = properties[property_1_key][property_1_value]
        property_2_id = properties[property_2_key][property_2_value]
        property_3_id = properties[property_3_key][property_3_value]
        # Paths
        self.assertEqual(event_1["path"][event_1_id][event_1_id], 1)
        self.assertTrue(event_2_id not in event_1["path"][event_1_id])
        self.assertTrue(event_3_id not in event_1["path"][event_1_id])
        self.assertEqual(event_2["path"][event_2_id][event_1_id], 2)
        self.assertTrue(event_3_id not in event_2["path"][event_2_id])
        self.assertTrue(event_3_id not in event_2["path"][event_2_id])
        self.assertEqual(event_3["path"][event_3_id][event_1_id], 3)
        self.assertEqual(event_3["path"][event_3_id][event_2_id], 3)
        self.assertEqual(event_3["path"][event_3_id][event_3_id], 1)
        # Unique paths
        self.assertEqual(event_1["unique_path"][event_1_id][event_1_id], 1)
        self.assertTrue(event_2_id not in event_1["unique_path"][event_1_id])
        self.assertTrue(event_3_id not in event_1["unique_path"][event_1_id])
        self.assertEqual(event_2["unique_path"][event_2_id][event_1_id], 2)
        self.assertTrue(event_3_id not in event_2["unique_path"][event_2_id])
        self.assertTrue(event_3_id not in event_2["unique_path"][event_2_id])
        self.assertEqual(event_3["unique_path"][event_3_id][event_1_id], 2)
        self.assertEqual(event_3["unique_path"][event_3_id][event_2_id], 2)
        self.assertEqual(event_3["unique_path"][event_3_id][event_3_id], 1)
        # Property paths
        # Property 1
        self.assertEqual(event_1["path"][property_1_id][event_1_id], 1)
        self.assertTrue(event_2_id not in event_1["path"][property_1_id])
        self.assertTrue(event_3_id not in event_1["path"][property_1_id])
        self.assertEqual(event_2["path"][property_1_id][event_1_id], 1)
        self.assertTrue(event_2_id not in event_2["path"][property_1_id])
        self.assertTrue(event_3_id not in event_2["path"][property_1_id])
        self.assertEqual(event_3["path"][property_1_id][event_1_id], 1)
        self.assertEqual(event_3["path"][property_1_id][event_2_id], 1)
        self.assertTrue(event_3_id not in event_3["path"][property_1_id])
        # Property 2  
        self.assertEqual(event_1["path"][property_2_id][event_1_id], 1)
        self.assertTrue(event_2_id not in event_1["path"][property_2_id])
        self.assertTrue(event_3_id not in event_1["path"][property_2_id])
        self.assertEqual(event_2["path"][property_2_id][event_1_id], 2)
        self.assertTrue(event_3_id not in event_2["path"][property_2_id])
        self.assertTrue(event_3_id not in event_2["path"][property_2_id])
        self.assertEqual(event_3["path"][property_2_id][event_1_id], 3)
        self.assertEqual(event_3["path"][property_2_id][event_2_id], 3)
        self.assertEqual(event_3["path"][property_2_id][event_3_id], 1)
        # Property 3
        self.assertEqual(event_1["path"][property_3_id][event_1_id], 1)
        self.assertTrue(event_2_id not in event_1["path"][property_3_id])
        self.assertTrue(event_3_id not in event_1["path"][property_3_id])
        self.assertEqual(event_2["path"][property_3_id][event_1_id], 1)
        self.assertTrue(event_2_id not in event_2["path"][property_3_id])
        self.assertTrue(event_3_id not in event_2["path"][property_3_id])
        self.assertEqual(event_3["path"][property_3_id][event_1_id], 1)
        self.assertEqual(event_3["path"][property_3_id][event_2_id], 1)
        self.assertTrue(event_3_id not in event_3["path"][property_3_id])    
        # Property unique paths 
        # Property 1
        self.assertEqual(event_1["unique_path"][property_1_id][event_1_id], 1)
        self.assertTrue(event_2_id not in event_1["unique_path"][property_1_id])
        self.assertTrue(event_3_id not in event_1["unique_path"][property_1_id])
        self.assertEqual(event_2["unique_path"][property_1_id][event_1_id], 1)
        self.assertTrue(event_2_id not in event_2["unique_path"][property_1_id])
        self.assertTrue(event_3_id not in event_2["unique_path"][property_1_id])
        self.assertEqual(event_3["unique_path"][property_1_id][event_1_id], 1)
        self.assertEqual(event_3["unique_path"][property_1_id][event_2_id], 1)
        self.assertTrue(event_3_id not in event_3["unique_path"][property_1_id])
        # Property 2  
        self.assertEqual(event_1["unique_path"][property_2_id][event_1_id], 1)
        self.assertTrue(event_2_id not in event_1["unique_path"][property_2_id])
        self.assertTrue(event_3_id not in event_1["unique_path"][property_2_id])
        self.assertEqual(event_2["unique_path"][property_2_id][event_1_id], 2)
        self.assertTrue(event_3_id not in event_2["unique_path"][property_2_id])
        self.assertTrue(event_3_id not in event_2["unique_path"][property_2_id])
        self.assertEqual(event_3["unique_path"][property_2_id][event_1_id], 2)
        self.assertEqual(event_3["unique_path"][property_2_id][event_2_id], 2)
        self.assertEqual(event_3["unique_path"][property_2_id][event_3_id], 1)
        # Property 3
        self.assertEqual(event_1["unique_path"][property_3_id][event_1_id], 1)
        self.assertTrue(event_2_id not in event_1["unique_path"][property_3_id])
        self.assertTrue(event_3_id not in event_1["unique_path"][property_3_id])
        self.assertEqual(event_2["unique_path"][property_3_id][event_1_id], 1)
        self.assertTrue(event_2_id not in event_2["unique_path"][property_3_id])
        self.assertTrue(event_3_id not in event_2["unique_path"][property_3_id])
        self.assertEqual(event_3["unique_path"][property_3_id][event_1_id], 1)
        self.assertEqual(event_3["unique_path"][property_3_id][event_2_id], 1)
        self.assertTrue(event_3_id not in event_3["unique_path"][property_3_id])   
        # Event names
        self.assertEqual(event_1["name"], event_name_1)
        self.assertEqual(event_2["name"], event_name_2)
        self.assertEqual(event_3["name"], event_name_3)
        # Property names and values
        self.assertEqual(property_1["name"], property_1_key)
        self.assertEqual(property_2["name"], property_2_key)
        self.assertEqual(property_3["name"], property_3_key)
        self.assertEqual(property_1["values"][property_1_id]["value"], property_1_value)
        self.assertEqual(property_2["values"][property_2_id]["value"], property_2_value)
        self.assertEqual(property_3["values"][property_3_id]["value"], property_3_value)
        # Event totals
        self.assertEqual(event_1["total"][event_1_id], 3)
        self.assertEqual(event_2["total"][event_2_id], 2)
        self.assertEqual(event_3["total"][event_3_id], 3)
        # Event unique totals
        self.assertEqual(event_1["unique_total"][event_1_id], 2)
        self.assertEqual(event_2["unique_total"][event_2_id], 2)
        self.assertEqual(event_3["unique_total"][event_3_id], 2)
        # Event property totals
        self.assertEqual(event_1["total"][property_1_id], 2)
        self.assertEqual(event_2["total"][property_1_id], 1)
        self.assertEqual(event_3["total"][property_1_id], 1)
        self.assertEqual(event_1["total"][property_2_id], 3)
        self.assertEqual(event_2["total"][property_2_id], 2)
        self.assertEqual(event_3["total"][property_2_id], 3)
        self.assertEqual(event_1["total"][property_3_id], 2)
        self.assertEqual(event_2["total"][property_3_id], 1)
        self.assertEqual(event_3["total"][property_3_id], 1)
        # Event property unique totals
        self.assertEqual(event_1["unique_total"][property_1_id], 1)
        self.assertEqual(event_2["unique_total"][property_1_id], 1)
        self.assertEqual(event_3["unique_total"][property_1_id], 1)
        self.assertEqual(event_1["unique_total"][property_2_id], 2)
        self.assertEqual(event_2["unique_total"][property_2_id], 2)
        self.assertEqual(event_3["unique_total"][property_2_id], 2)
        self.assertEqual(event_1["unique_total"][property_3_id], 1)
        self.assertEqual(event_2["unique_total"][property_3_id], 1)
        self.assertEqual(event_3["unique_total"][property_3_id], 1)
        # Property event totals

        self.assertEqual(property_1["values"][property_1_id]["total"][event_1_id], 1)
        self.assertEqual(property_1["values"][property_1_id]["total"][event_2_id], 1)
        self.assertEqual(property_1["values"][property_1_id]["total"][event_3_id], 1)
        self.assertEqual(property_2["values"][property_2_id]["total"][event_1_id], 2)
        self.assertEqual(property_2["values"][property_2_id]["total"][event_2_id], 2)
        self.assertEqual(property_2["values"][property_2_id]["total"][event_3_id], 2)
        self.assertEqual(property_3["values"][property_3_id]["total"][event_1_id], 1)
        self.assertEqual(property_3["values"][property_3_id]["total"][event_2_id], 1)
        self.assertEqual(property_3["values"][property_3_id]["total"][event_3_id], 1)



