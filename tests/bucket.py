#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks
from lib.agent import request
from hiitrack import HiiTrack
import uuid
import ujson

class BucketTestCase(unittest.TestCase):
    
    @inlineCallbacks
    def setUp(self):
        self.hiitrack = HiiTrack(8080)
        self.hiitrack.startService()
        self.username = uuid.uuid4().hex
        self.password = uuid.uuid4().hex
        self.username2 = uuid.uuid4().hex
        self.password2 = uuid.uuid4().hex
        yield request(
            "PUT",
            "http://127.0.0.1:8080/%s" % self.username,
            data={"password":self.password}) 
        self.url =  "http://127.0.0.1:8080/%s" % self.username
        yield request(
            "PUT",
            "http://127.0.0.1:8080/%s" % self.username2,
            data={"password":self.password2}) 

    @inlineCallbacks
    def tearDown(self):
        yield request(
            "DELETE",
            "http://127.0.0.1:8080/%s" % self.username,
            username=self.username,
            password=self.password) 
        yield request(
            "DELETE",
            "http://127.0.0.1:8080/%s" % self.username2,
            username=self.username2,
            password=self.password2) 
        # Despite beforeShutdown hooks, Twisted complains.
        self.hiitrack.stopService() 
 
    @inlineCallbacks
    def test_unauthorized(self):
        BUCKETNAME = uuid.uuid4().hex
        DESCRIPTION = uuid.uuid4().hex
        result = yield request(
            "PUT",
            "%s/%s" % (self.url, BUCKETNAME),
            username=self.username,
            password=self.password,
            data={"description":DESCRIPTION})
        self.assertEqual(result.code, 201)
        result = yield request(
            "GET",
            "%s/%s" % (self.url, BUCKETNAME),
            username="INVALID_USER",
            password="INVALID_PASSWORD")        
        self.assertEqual(result.code, 401)
        result = yield request(
            "GET",
            "%s/%s" % (self.url, BUCKETNAME),
            username=self.username2,
            password=self.password2)        
        self.assertEqual(result.code, 401)
        result = yield request(
            "DELETE",
            "%s/%s" % (self.url, BUCKETNAME),
            username=self.username,
            password=self.password)
        self.assertEqual(result.code, 200)

    @inlineCallbacks
    def test_create(self):
        BUCKETNAME = uuid.uuid4().hex
        DESCRIPTION = uuid.uuid4().hex
        result = yield request(
            "PUT",
            "%s/%s" % (self.url, BUCKETNAME),
            username=self.username,
            password=self.password,
            data={"description":DESCRIPTION})
        self.assertEqual(result.code, 201)
        result = yield request(
            "GET",
            "%s/%s" % (self.url, BUCKETNAME),
            username=self.username,
            password=self.password)        
        self.assertEqual(result.code, 200)
        returned_description = ujson.decode(result.body)["description"]
        result = yield request(
            "GET",
            self.url,
            username=self.username,
            password=self.password)        
        self.assertEqual(result.code, 200)
        buckets = ujson.decode(result.body)["buckets"]
        self.assertTrue(BUCKETNAME in buckets)
        self.assertEqual(buckets[BUCKETNAME]["description"], DESCRIPTION)
        self.assertEqual(DESCRIPTION, returned_description)
        result = yield request(
            "DELETE",
            "%s/%s" % (self.url, BUCKETNAME),
            username=self.username,
            password=self.password)
        self.assertEqual(result.code, 200)

    @inlineCallbacks
    def test_missing(self):
        BUCKETNAME = uuid.uuid4().hex
        result = yield request(
            "GET",
            "%s/%s" % (self.url, BUCKETNAME),
            username=self.username,
            password=self.password)        
        self.assertEqual(result.code, 404)
    