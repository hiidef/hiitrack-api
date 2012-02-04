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

class FunnelTestCase(unittest.TestCase):
    
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
        returnValue(result)

    @inlineCallbacks
    def get_event_dict(self):
        result = yield request(
            "GET",
            self.url,
            username=self.username,
            password=self.password)
        self.assertEqual(result.code, 200)
        result = ujson.loads(result.body)["events"]
        returnValue(result)

    @inlineCallbacks
    def post_property(self, visitor_id, name, value):
        result = yield request(
            "POST",
            "%s/property/%s/%s" % (self.url, quote(name), quote(value)),
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
    def test_create(self):
        VISITOR_ID_1 = uuid.uuid4().hex
        EVENT_1 = "Event 1 %s" % uuid.uuid4().hex
        EVENT_2 = "Event 2 %s" % uuid.uuid4().hex
        yield self.post_event(VISITOR_ID_1, EVENT_1)
        yield self.post_event(VISITOR_ID_1, EVENT_2)
        events = yield self.get_event_dict()
        event_id_1 = events[EVENT_1]
        event_id_2 = events[EVENT_2]
        FUNNEL_NAME = uuid.uuid4().hex
        DESCRIPTION = uuid.uuid4().hex
        result = yield request(
            "POST",
            "%s/funnel/%s" % (self.url, FUNNEL_NAME),
            username=self.username,
            password=self.password,
            data=[
                ("description", DESCRIPTION),
                ("event_id",event_id_1),
                ("event_id",event_id_2)])
        self.assertEqual(result.code, 201)
        result = yield request(
            "GET",
            "%s/funnel/%s" % (self.url, FUNNEL_NAME),
            username=self.username,
            password=self.password)        
        self.assertEqual(result.code, 200)
        returned_description = ujson.decode(result.body)["description"]
        self.assertEqual(DESCRIPTION, returned_description)
        returned_event_ids = ujson.decode(result.body)["event_ids"]
        self.assertTrue(event_id_1 in returned_event_ids)
        self.assertTrue(event_id_2 in returned_event_ids)
        funnel = ujson.decode(result.body)["funnel"]
        unique_funnel = ujson.decode(result.body)["unique_funnel"]
        self.assertEqual(funnel[0][0], event_id_1)
        self.assertEqual(funnel[1][0], event_id_2)
        self.assertEqual(funnel[0][1], 1)
        self.assertEqual(funnel[1][1], 1)
        self.assertEqual(unique_funnel[0][0], event_id_1)
        self.assertEqual(unique_funnel[1][0], event_id_2)
        self.assertEqual(unique_funnel[0][1], 1)
        self.assertEqual(unique_funnel[1][1], 1)
        result = yield request(
            "DELETE",
            "%s/funnel/%s" % (self.url, FUNNEL_NAME),
            username=self.username,
            password=self.password)
        self.assertEqual(result.code, 200)
  
    @inlineCallbacks
    def test_add(self):
        VISITOR_ID_1 = uuid.uuid4().hex
        VISITOR_ID_2 = uuid.uuid4().hex
        VISITOR_ID_3 = uuid.uuid4().hex
        EVENT_1 = "Event 1 %s" % uuid.uuid4().hex
        EVENT_2 = "Event 2 %s" % uuid.uuid4().hex
        EVENT_3 = "Event 3 %s" % uuid.uuid4().hex
        PROPERTY_1 = "Property 1 %s" % uuid.uuid4().hex
        PROPERTY_2 = "Property 2 %s" % uuid.uuid4().hex
        VALUE_1 = "Value 1 %s" % uuid.uuid4().hex
        VALUE_2 = "Value 2 %s" % uuid.uuid4().hex
        yield self.post_event(VISITOR_ID_1, EVENT_3)
        yield self.post_event(VISITOR_ID_1, EVENT_3)
        yield self.post_event(VISITOR_ID_1, EVENT_1)
        yield self.post_event(VISITOR_ID_1, EVENT_1)
        yield self.post_event(VISITOR_ID_2, EVENT_1)
        yield self.post_event(VISITOR_ID_3, EVENT_1)
        yield self.post_event(VISITOR_ID_1, EVENT_2)
        yield self.post_event(VISITOR_ID_2, EVENT_2)
        yield self.post_event(VISITOR_ID_1, EVENT_2)
        yield self.post_event(VISITOR_ID_1, EVENT_3)
        yield self.post_event(VISITOR_ID_1, EVENT_3)
        yield self.post_property(VISITOR_ID_1, PROPERTY_1, VALUE_1)
        yield self.post_property(VISITOR_ID_1, PROPERTY_2, VALUE_2)
        yield self.post_property(VISITOR_ID_2, PROPERTY_2, VALUE_2)
        events = yield self.get_event_dict()
        properties = yield self.get_property_dict()
        event_id_1 = events[EVENT_1]
        event_id_2 = events[EVENT_2]
        event_id_3 = events[EVENT_3]
        property_id_1 = properties[PROPERTY_1][VALUE_1]
        property_id_2 = properties[PROPERTY_2][VALUE_2]
        FUNNEL_NAME = uuid.uuid4().hex
        DESCRIPTION = uuid.uuid4().hex
        result = yield request(
            "POST",
            "%s/funnel/%s" % (self.url, FUNNEL_NAME),
            username=self.username,
            password=self.password,
            data=[
                ("description", DESCRIPTION),
                ("event_id", event_id_1),
                ("event_id", event_id_2),
                ("event_id", event_id_3)])
        self.assertEqual(result.code, 201)
        result = yield request(
            "GET",
            "%s/funnel/%s" % (self.url, FUNNEL_NAME),
            username=self.username,
            password=self.password)        
        self.assertEqual(result.code, 200)
        funnel = ujson.decode(result.body)["funnel"]
        unique_funnel = ujson.decode(result.body)["unique_funnel"]
        funnels = ujson.decode(result.body)["funnels"]
        unique_funnels = ujson.decode(result.body)["unique_funnels"]
        self.assertEqual(funnel[0][0], event_id_1)
        self.assertEqual(funnel[1][0], event_id_2)
        self.assertEqual(funnel[2][0], event_id_3)
        self.assertEqual(funnels[property_id_1][0][0], event_id_1)
        self.assertEqual(funnels[property_id_1][1][0], event_id_2)
        self.assertEqual(funnels[property_id_1][2][0], event_id_3)
        self.assertEqual(funnels[property_id_2][0][0], event_id_1)
        self.assertEqual(funnels[property_id_2][1][0], event_id_2)
        self.assertEqual(funnels[property_id_2][2][0], event_id_3)
        self.assertEqual(funnel[0][1], 4)
        self.assertEqual(funnel[1][1], 3)
        self.assertEqual(funnel[2][1], 2)
        self.assertEqual(funnels[property_id_1][0][1], 2)
        self.assertEqual(funnels[property_id_1][1][1], 2)
        self.assertEqual(funnels[property_id_1][2][1], 2)
        self.assertEqual(funnels[property_id_2][0][1], 3)
        self.assertEqual(funnels[property_id_2][1][1], 3)
        self.assertEqual(funnels[property_id_2][2][1], 2)
        self.assertEqual(unique_funnel[0][0], event_id_1)
        self.assertEqual(unique_funnel[1][0], event_id_2)
        self.assertEqual(unique_funnel[2][0], event_id_3)
        self.assertEqual(unique_funnels[property_id_1][0][0], event_id_1)
        self.assertEqual(unique_funnels[property_id_1][1][0], event_id_2)
        self.assertEqual(unique_funnels[property_id_1][2][0], event_id_3)
        self.assertEqual(unique_funnels[property_id_2][0][0], event_id_1)
        self.assertEqual(unique_funnels[property_id_2][1][0], event_id_2)
        self.assertEqual(unique_funnels[property_id_2][2][0], event_id_3)
        self.assertEqual(unique_funnel[0][1], 3)
        self.assertEqual(unique_funnel[1][1], 2)
        self.assertEqual(unique_funnel[2][1], 1)
        self.assertEqual(unique_funnels[property_id_1][0][1], 1)
        self.assertEqual(unique_funnels[property_id_1][1][1], 1)
        self.assertEqual(unique_funnels[property_id_1][2][1], 1)
        self.assertEqual(unique_funnels[property_id_2][0][1], 2)
        self.assertEqual(unique_funnels[property_id_2][1][1], 2)
        self.assertEqual(unique_funnels[property_id_2][2][1], 1)