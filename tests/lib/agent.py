from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.web.client import Agent, CookieAgent
from twisted.internet.protocol import Protocol
from zope.interface import implements
from twisted.internet.defer import succeed
from twisted.web.iweb import IBodyProducer
import urllib
from twisted.web.http_headers import Headers
from twisted.cred._digest import calcResponse, calcHA1, calcHA2
import uuid
from urlparse import urlparse
import re
from base64 import b64encode
from cookielib import CookieJar

AUTH_REGEX = re.compile('(\w+)[=] ?"?(\w+)"?')

def produce_credential_headers(username, password):
    header = "Basic %s" % b64encode("%s:%s" % (username, password))
    return {"Authorization":[header]}

class StringProducer(object):

    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass

class BodyDeliverProtocol(Protocol):

    def __init__(self, finished):
        self.finished = finished
        self.remaining = 1024 * 10
        self.data = []

    def dataReceived(self, bytes):
        if self.remaining:
            display = bytes[:self.remaining]
            self.data.append(display)
            self.remaining -= len(display)

    def connectionLost(self, reason):
        self.finished.callback("".join(self.data))

COOKIEJAR = CookieJar()
AGENT = CookieAgent(Agent(reactor), COOKIEJAR)

@inlineCallbacks
def request(*args, **kwargs):
    headers = kwargs.get("headers", {})
    if "data" in kwargs:
        body = StringProducer(urllib.urlencode(kwargs["data"]))
        del kwargs["data"]
        kwargs["bodyProducer"] = body
        headers.update({'Content-Type': ['application/x-www-form-urlencoded']})
        kwargs["headers"] = headers
    if "headers" in kwargs:
        kwargs["headers"] = Headers(kwargs["headers"])
    if "username" in kwargs:
        username = kwargs["username"]
        del kwargs["username"]
    else:
        username = None
    if "password" in kwargs:
        password = kwargs["password"]
        del kwargs["password"]
    else:
        password = None
    response = yield AGENT.request(*args, **kwargs)
    finished = Deferred()
    response.deliverBody(BodyDeliverProtocol(finished))
    response.body = yield finished
    if response.code == 401 and username and password:
        headers.update(produce_credential_headers(
            username, 
            password))
        kwargs["headers"] = Headers(headers)
        response = yield AGENT.request(*args, **kwargs)
        finished = Deferred()
        response.deliverBody(BodyDeliverProtocol(finished))
        response.body = yield finished
    response.cookies = AGENT.cookieJar
    returnValue(response)
