#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks
from lib.agent import request
from hiitrack import HiiTrack
import uuid
import ujson
from urllib import quote
from base64 import b64encode
from urllib import urlencode

class PropertyTestCase(unittest.TestCase):
    
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
    def test_post(self):
        NAME = uuid.uuid4().hex
        VALUE = uuid.uuid4().hex
        VISITOR_ID = uuid.uuid4().hex
        qs = urlencode({"value":b64encode(ujson.dumps(VALUE))})
        result = yield request(
            "POST",
            "%s/property/%s?%s" % (self.url, quote(NAME), qs),
            data={"visitor_id":VISITOR_ID})
        self.assertEqual(result.code, 200)

    @inlineCallbacks
    def test_bucket(self):    
        NAME = uuid.uuid4().hex
        VALUE = uuid.uuid4().hex
        VISITOR_ID = uuid.uuid4().hex
        qs = urlencode({"value":b64encode(ujson.dumps(VALUE))})
        result = yield request(
            "POST",
            "%s/property/%s?%s" % (self.url, quote(NAME), qs),
            data={"visitor_id":VISITOR_ID})
        result = yield request(
            "GET",
            self.url,
            username=self.username,
            password=self.password)
        self.assertTrue(NAME in ujson.loads(result.body)["properties"].values())

