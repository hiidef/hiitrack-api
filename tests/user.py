#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks
from lib.agent import request
from hiitrack import HiiTrack
import uuid

class UserTestCase(unittest.TestCase):
    
    def setUp(self):
        self.hiitrack = HiiTrack(8080)
        
    def tearDown(self):
        # Despite beforeShutdown hooks, Twisted complains.
        self.hiitrack.shutdown() 

    @inlineCallbacks
    def test_create(self):
        CORRECT_PASSWORD = "qwerty"
        INCORRECT_PASSWORD = "123456"
        USERNAME = uuid.uuid4().hex
        result = yield request(
            "PUT",
            "http://127.0.0.1:8080/%s" % USERNAME,
            data={"password":CORRECT_PASSWORD})
        self.assertEqual(result.code, 201)
        result = yield request(
            "GET",
            "http://127.0.0.1:8080/%s" % USERNAME) 
        self.assertEqual(result.code, 401)
        result = yield request(
            "GET",
            "http://127.0.0.1:8080/%s" % USERNAME,
            username=USERNAME,
            password=CORRECT_PASSWORD) 
        self.assertEqual(result.code, 200)
        result = yield request(
            "GET",
            "http://127.0.0.1:8080/%s" % USERNAME,
            username=USERNAME,
            password=INCORRECT_PASSWORD) 
        self.assertEqual(result.code, 401)
        result = yield request(
            "DELETE",
            "http://127.0.0.1:8080/%s" % USERNAME,
            username=USERNAME,
            password=INCORRECT_PASSWORD) 
        self.assertEqual(result.code, 401)
        result = yield request(
            "DELETE",
            "http://127.0.0.1:8080/%s" % USERNAME,
            username=USERNAME,
            password=CORRECT_PASSWORD) 
        self.assertEqual(result.code, 200)
        
    @inlineCallbacks
    def test_duplicate(self):
        USERNAME = uuid.uuid4().hex
        PASSWORD = "qwerty"
        result = yield request(
            "PUT",
            "http://127.0.0.1:8080/%s" % USERNAME,
            data={"password":PASSWORD})
        self.assertEqual(result.code, 201)
        result = yield request(
            "PUT",
            "http://127.0.0.1:8080/%s" % USERNAME,
            data={"password":PASSWORD})
        self.assertEqual(result.code, 403)
        result = yield request(
            "DELETE",
            "http://127.0.0.1:8080/%s" % USERNAME,
            username=USERNAME,
            password=PASSWORD) 
        self.assertEqual(result.code, 200)
                        
    @inlineCallbacks
    def test_access(self):
        USERNAME_A = uuid.uuid4().hex
        USERNAME_B = uuid.uuid4().hex
        PASSWORD_A = "poiuy"
        PASSWORD_B = "mnbvc"
        result = yield request(
            "PUT",
            "http://127.0.0.1:8080/%s" % USERNAME_A,
            data={"password":PASSWORD_A})
        self.assertEqual(result.code, 201)
        result = yield request(
            "PUT",
            "http://127.0.0.1:8080/%s" % USERNAME_B,
            data={"password":PASSWORD_B})
        self.assertEqual(result.code, 201)
        result = yield request(
            "GET",
            "http://127.0.0.1:8080/%s" % USERNAME_A,
            username=USERNAME_A,
            password=PASSWORD_B) 
        self.assertEqual(result.code, 401)  
        result = yield request(
            "GET",
            "http://127.0.0.1:8080/%s" % USERNAME_B,
            username=USERNAME_A,
            password=PASSWORD_A) 
        self.assertEqual(result.code, 401) 
        result = yield request(
            "GET",
            "http://127.0.0.1:8080/%s" % USERNAME_A,
            username=USERNAME_A,
            password=PASSWORD_B) 
        self.assertEqual(result.code, 401)     
        result = yield request(
            "DELETE",
            "http://127.0.0.1:8080/%s" % USERNAME_A,
            username=USERNAME_A,
            password=PASSWORD_A) 
        self.assertEqual(result.code, 200)
        result = yield request(
            "DELETE",
            "http://127.0.0.1:8080/%s" % USERNAME_B,
            username=USERNAME_B,
            password=PASSWORD_B) 
        self.assertEqual(result.code, 200)       