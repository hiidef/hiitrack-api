#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""HiiTrack HTTP interface."""

from twisted.application.service import Service
from telephus.pool import CassandraClusterPool
from twisted.python import log
from twisted.internet import reactor
from twisted.web.server import Site
from .lib.dispatcher import Dispatcher
from .controllers.user import User
from .controllers.bucket import Bucket
from .controllers.event import Event
from .controllers.property import Property
from .controllers.funnel import Funnel
from .lib import cassandra


class HiiTrack(Service):
    """
    HiiTrack HTTP interface.
    """

    listener = None

    def __init__(self, port=8080, cassandra_settings=None):
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
        User(dispatcher)
        Bucket(dispatcher)
        Event(dispatcher)
        Property(dispatcher)
        Funnel(dispatcher)
        self.dispatcher = dispatcher
        self.port = port

    def startService(self):
        """
        Start HiiTrack.
        """
        Service.startService(self)
        cassandra.CLIENT.startService()
        self.listener = reactor.listenTCP(self.port, Site(self.dispatcher))

    def stopService(self):
        """
        Shutdown HiiTrack.
        """
        Service.stopService(self)
        cassandra.CLIENT.stopService()
        if self.listener:
            self.listener.stopListening()
        log.msg("Shut down.")
