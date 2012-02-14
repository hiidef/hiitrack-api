#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Properties are key/value pairs linked to a visitor and stored in buckets.
"""

from base64 import b64decode
from twisted.internet.defer import inlineCallbacks, returnValue
import ujson
from ..models import bucket_check, user_authorize, bucket_create
from ..models import PropertyValueModel, VisitorModel, PropertyModel
from ..lib.authentication import authenticate
from ..lib.b64encode import b64encode_keys, uri_b64encode, b64encode_nested_keys, \
    uri_b64decode
from ..lib.parameters import require
from ..lib.profiler import profile


class Property(object):
    """
    Property controller.
    """

    def __init__(self, dispatcher):
        dispatcher.connect(
            name='property',
            route=('/{user_name}/{bucket_name}/property/{property_name}'),
            controller=self,
            action='post',
            conditions={"method": "POST"})
        dispatcher.connect(
            name='property',
            route=('/{user_name}/{bucket_name}/property/{property_name}'),
            controller=self,
            action='get',
            conditions={"method": "GET"})
        dispatcher.connect(
            name='property',
            route=('/{user_name}/{bucket_name}/property_id/{property_id}'),
            controller=self,
            action='get_by_id',
            conditions={"method": "GET"})


    @authenticate
    @user_authorize
    @bucket_check
    @profile
    def get(self,
            request,
            user_name,
            bucket_name,
            property_name):
        """
        Information about the property.
        """
        _property = PropertyModel(user_name, bucket_name, property_name)
        return self._get(_property, property_name)

    @authenticate
    @user_authorize
    @bucket_check
    @profile
    @inlineCallbacks
    def get_by_id(self,
            request,
            user_name,
            bucket_name,
            property_id):
        """
        Information about the property.
        """
        property_id = uri_b64decode(property_id)
        _property = PropertyModel(user_name, bucket_name, property_id=property_id)
        property_name = yield _property.get_name()
        data = yield self._get(_property, property_name)
        returnValue(data)

    @inlineCallbacks
    def _get(self, _property, property_name):
        """
        Information about the property.
        """
        values = yield _property.get_values()
        totals = yield _property.get_totals()
        values = dict([(
            uri_b64encode(x), 
            {"value":values[x], "total":b64encode_keys(totals[x])}) 
            for x in values])
        returnValue({
            "id":uri_b64encode(_property.id),
            "name":property_name,
            "values":values})

    @require("visitor_id", "value")
    @bucket_create
    @profile
    @inlineCallbacks
    def post(self,
            request,
            user_name,
            bucket_name,
            property_name):
        """
        Record property for visitor.
        """
        property_value = PropertyValueModel(
            user_name,
            bucket_name,
            property_name,
            ujson.loads(b64decode(request.args["value"][0])))
        visitor_id = request.args["visitor_id"][0]
        visitor = VisitorModel(
            user_name,
            bucket_name,
            visitor_id)
        yield property_value.add(visitor)

