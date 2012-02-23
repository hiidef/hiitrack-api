#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""HiiTrack HTTP interface."""

from twisted.application.service import Service
from telephus.pool import CassandraClusterPool
from twisted.internet import reactor
from twisted.web.server import Site
from twisted.python import log
from .lib.dispatcher import Dispatcher
from .controllers.user import User
from .controllers.bucket import Bucket
from .controllers.event import Event
from .controllers.property import Property
from .controllers.funnel import Funnel
from .lib import cassandra
from .lib.profiler import EXECUTION_TIME, EXECUTION_COUNT
from twisted.internet.task import LoopingCall


class HiiTrack(Service):
    """
    HiiTrack HTTP interface.
    """

    listener = None

    def __init__(
            self,
            port=8080,
            cassandra_settings=None):
        if not cassandra_settings:
            cassandra_settings = {}
        cassandra.CLIENT = CassandraClusterPool(
            cassandra_settings.get("servers", ["127.0.0.1"]),
            keyspace=cassandra_settings.get("keyspace", "HiiTrack"),
            pool_size=cassandra_settings.get("pool_size", None))
        dispatcher = Dispatcher()
        dispatcher.connect(
            name='index',
            route='/',
            controller=self,
            action='index')
        self.logloop = LoopingCall(self.log)
        User(dispatcher)
        Bucket(dispatcher)
        Event(dispatcher)
        Property(dispatcher)
        Funnel(dispatcher)
        self.dispatcher = dispatcher
        self.port = port

    def log(self):
        """
        Periodically logs running speed of various methods.
        """
        timers = sorted([(EXECUTION_TIME[name], EXECUTION_COUNT[name], name)
            for name in EXECUTION_TIME], key=lambda x:x[0], reverse=True)
        for timer in timers:
            time, count, name = timer
            mean = time / count
            log.msg("%s: %s, %sx%ss" % (name, time, count, mean))

    def startService(self):
        """
        Start HiiTrack.
        """
        self.logloop.start(60*5, False)
        Service.startService(self)
        cassandra.CLIENT.startService()
        self.listener = reactor.listenTCP(self.port, Site(self.dispatcher))

    def stopService(self):
        """
        Shutdown HiiTrack.
        """
        self.log()
        self.logloop.stop()
        Service.stopService(self)
        cassandra.CLIENT.stopService()
        if self.listener:
            self.listener.stopListening()
