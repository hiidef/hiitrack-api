#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""HiiTrack HTTP interface."""

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
from telephus.pool import CassandraClusterPool


class HiiTrack(object):
    """
    HiiTrack HTTP interface.
    """

    port = None

    def __init__(self, port=8080, cassandra_settings=None):
        if not cassandra_settings:
            cassandra_settings = {}
        cassandra.CLIENT = CassandraClusterPool(
            cassandra_settings.get("servers", ["127.0.0.1"]),
            keyspace=cassandra_settings.get("keyspace", "HiiTrack"),
            pool_size=cassandra_settings.get("pool_size", None))
        cassandra.CLIENT.startService()
        reactor.addSystemEventTrigger('before', 'shutdown', self.shutdown)
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
        self.port = reactor.listenTCP(port, Site(dispatcher))

    def shutdown(self):
        """
        Shutdown HiiTrack.
        """
        if self.port:
            self.port.stopListening()
        cassandra.CLIENT.stopService()
        log.msg("Shut down.")
