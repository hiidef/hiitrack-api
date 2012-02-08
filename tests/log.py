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


class LogTestCase(unittest.TestCase):
    
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
    def get_event(self, name):
        result = yield request(
            "GET",
            str("%s/event/%s" % (self.url, quote(name))),
            username=self.username,
            password=self.password)
        self.assertEqual(result.code, 200)
        returnValue(ujson.loads(result.body))

    @inlineCallbacks
    def get_property(self, name, value):
        properties = yield self.get_property_dict()
        result = yield request(
            "GET",
            str("%s/property/%s/%s" % (self.url, quote(name), quote(value))),
            username=self.username,
            password=self.password)
        self.assertEqual(result.code, 200)
        returnValue(ujson.loads(result.body))

    @inlineCallbacks
    def test_logging(self): 
        event_name_1 = "Event 1 %s" % uuid.uuid4().hex
        visitor_id_1 = uuid.uuid4().hex
        property_1_key = uuid.uuid4().hex
        property_1_value = "Property 1 %s" %  uuid.uuid4().hex
        yield self.post_property(visitor_id_1, property_1_key, property_1_value)
        yield self.post_event(visitor_id_1, event_name_1)
        