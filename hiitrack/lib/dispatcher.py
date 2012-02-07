#!/usr/bin/env python
# -*- coding: utf-8 -*-

import routes
from twisted.web.resource import Resource
from twisted.internet.defer import maybeDeferred
from twisted.web.server import NOT_DONE_YET
import gzip
from cStringIO import StringIO
import ujson
from twisted.web import http
from traceback import format_exc


def update_if_json(request):
    if request.getHeader("content-type") == "application/json":
        data = ujson.loads(request.content.read())
        request.args.update(dict([(k, [v]) for k, v in data.items()]))


class Dispatcher(Resource):
    '''
    Based on txroutes

    Helpful background information:
    - Python routes: http://routes.groovie.org/
    - Using twisted.web.resources:
    http://twistedmatrix.com/documents/current/web/howto/web-in-60/dynamic-
    dispatch.html
    '''

    def __init__(self):
        Resource.__init__(self)
        self.__path = ['']
        self.__controllers = {}
        self.__mapper = routes.Mapper()

    def connect(self, name, route, controller, **kwargs):
        self.__controllers[name] = controller
        self.__mapper.connect(name, route, controller=name, **kwargs)

    def getChild(self, name, request):
        self.__path.append(name)
        return self

    def render(self, request):
        if request.path[-1:] == "/":    
            request.setHeader("location", request.path[:-1])
            request.setResponseCode(301)
            return ""
        return Resource.render(self, request)

    def render_HEAD(self, request):
        return self.__render('HEAD', request)

    def render_GET(self, request):
        return self.__render('GET', request)

    def render_POST(self, request):
        update_if_json(request)
        return self.__render('POST', request)

    def render_PUT(self, request):
        update_if_json(request)
        content_type = request.getHeader("content-type")
        if content_type == "application/x-www-form-urlencoded":
            request.args.update(http.parse_qs(request.content.read(), 1))
        return self.__render('PUT', request)

    def render_DELETE(self, request):
        return self.__render('DELETE', request)

    def __render(self, method, request):
        request.setHeader("content-type", "application/json")
        try:
            wsgi_environ = {}
            wsgi_environ['REQUEST_METHOD'] = method
            wsgi_environ['PATH_INFO'] = '/'.join(self.__path)
            result = self.__mapper.match(environ=wsgi_environ)
            handler = None
            if result is not None:
                controller = result.get('controller', None)
                controller = self.__controllers.get(controller)
                if controller is not None:
                    del result['controller']
                    action = result.get('action', None)
                    if action is not None:
                        del result['action']
                        handler = getattr(controller, action, None)
        finally:
            self.__path = ['']
        if handler:
            result = dict([(x[0], x[1].encode("utf8")) \
                for x in result.items()])
            d = maybeDeferred(handler, request, **result)
            d.addCallback(self._success_response)
            d.addErrback(self._error_response, request)
            if "callback" in request.args:
                d.addCallback(self._add_jsonp_callback, request)
            d.addCallback(self._gzip_response, request)
            return NOT_DONE_YET
        else:
            request.setResponseCode(404)
            return ujson.dumps({"error": "Not found"})

    def _success_response(self, data):
        return ujson.dumps(data)

    def _error_response(self, error, request):
        try:
            error.raiseException()
        except:
            exc = format_exc()
        if request.code == 401:
            return ujson.dumps({"error": "Authorization required."})
        if request.code < 400:
            request.setResponseCode(500)
            print error.getTraceback()
        return ujson.dumps({"error": str(error.value), "exc": exc})

    def _add_jsonp_callback(self, data, request):
        return "%s(%s);" % (request.args["callback"][0], data)

    def _gzip_response(self, data, request):
        encoding = request.getHeader("accept-encoding")
        if encoding and "gzip" in encoding:
            zbuf = StringIO()
            zfile = gzip.GzipFile(None, 'wb', 9, zbuf)
            if isinstance(data, unicode):
                zfile.write(unicode(data).encode("utf-8"))
            elif isinstance(data, str):
                zfile.write(unicode(data, 'utf-8').encode("utf-8"))
            else:
                zfile.write(unicode(data).encode("utf-8"))
            zfile.close()
            request.setHeader("Content-encoding", "gzip")
            request.write(zbuf.getvalue())
        else:
            request.write(data)
        request.finish()
