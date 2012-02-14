#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks
from lib.agent import request
from hiitrack import HiiTrack
import uuid
import ujson
from base64 import b64encode
from urllib import urlencode
from random import randint

class CookieTestCase(unittest.TestCase):
    
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
    def test_set(self):
        event_name_1 = "Event 1 %s" % uuid.uuid4().hex
        event_name_2 = "Event 2 %s" % uuid.uuid4().hex
        event_name_3 = "Event 2 %s" % uuid.uuid4().hex
        visitor_id_2 = uuid.uuid4().hex
        events = [event_name_1]
        properties = []
        message = b64encode(ujson.dumps([
            events,
            properties]))
        qs = urlencode({"message": message})
        response = yield request(
            "GET",
            "%s/batch?%s" % (self.url, qs))
        for cookie in response.cookies:
            if cookie.name == "v":
                break
        self.assertEqual(ujson.loads(response.body)["visitor_id"], cookie.value)
        visitor_id_1 = cookie.value
        qs = urlencode({
            "message": message, 
            "visitor_id": visitor_id_1})
        response = yield request(
            "GET",
            "%s/batch?%s" % (self.url, qs))   
        for cookie in response.cookies:
            if cookie.name == "v":
                break
        self.assertEqual(cookie.value, visitor_id_1)
        self.assertEqual(ujson.loads(response.body)["visitor_id"], visitor_id_1)
        qs = urlencode({
            "message": message, 
            "visitor_id": visitor_id_2})
        response = yield request(
            "GET",
            "%s/batch?%s" % (self.url, qs)) 
        for cookie in response.cookies:
            if cookie.name == "v":
                break
        self.assertEqual(cookie.value, visitor_id_2)
        self.assertEqual(ujson.loads(response.body)["visitor_id"], visitor_id_2)
        qs = urlencode({
            "message": message, 
            "visitor_id": visitor_id_1})
        response = yield request(
            "GET",
            "%s/batch?%s" % (self.url, qs))   
        for cookie in response.cookies:
            if cookie.name == "v":
                break
        self.assertEqual(cookie.value, visitor_id_1)
        self.assertEqual(ujson.loads(response.body)["visitor_id"], visitor_id_1)