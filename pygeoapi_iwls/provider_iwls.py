# Standard library imports
import requests
import json
import datetime
import os
# Packages imports
import pandas as pd
import requests_cache
import dateutil.parser
from pygeoapi.provider.base import BaseProvider

from provider_iwls.iwls import IwlsApiConnector

#temp
from zipfile import ZipFile
import uuid

class ProviderIwls(BaseProvider):
    """
    Provider class for iwls data
    """
    def __init__(self, provider_def):
        """Inherit from parent class"""
        super().__init__(provider_def)



    def get(self, identifier, **kwargs):
        """
        Get Feature by id
        :param identifier: feature id
        :returns: feature collection
        """
        result = None

        # Establish connection to IWLS API
        api = IwlsApiConnector()
        # Only latest 24h of data available throught get method
        now = datetime.datetime.now()
        tomorrow = now + datetime.timedelta(days=1)
        yesterday = now - datetime.timedelta(days=1)
        end_time = tomorrow.strftime('%Y-%m-%dT%H:%M:%SZ')
        start_time = yesterday.strftime('%Y-%m-%dT%H:%M:%SZ')
        # Pass query to IWLS API
        result = api.get_station_data(identifier,start_time,end_time)

        return result

    def query(self, startindex=0, limit=10, resulttype='results',
              bbox=[], datetime_=None, properties=[], sortby=[],
              select_properties=[], skip_geometry=False, q=None, **kwargs):
        
        """
        Query IWLS
        :param startindex: starting record to return (default 0)
        :param limit: number of records to return (default 10)
        :param resulttype: return results or hit limit (default results)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param properties: list of tuples (name, value)
        :param sortby: list of dicts (property, order)
        :param select_properties: list of property names
        :param skip_geometry: bool of whether to skip geometry (default False)
        :param q: full-text search term(s)

        :returns: dict of 0..n GeoJSON features
        """
        result  = None

        if not bbox:
           bbox = [-180,-90,180,90]

        if not datetime_:
            now = datetime.datetime.now()
            yesterday = now - datetime.timedelta(days=1)
            tomorrow = now + datetime.timedelta(days=1)
            end_time = tomorrow.strftime('%Y-%m-%dT%H:%M:%SZ')
            start_time = yesterday.strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            start_time = datetime_.split('/')[0]
            end_time = datetime_.split('/')[1]
        # Establish connection to IWLS API
        api = IwlsApiConnector()
        # Pass query to IWLS API
        result = api.get_timeseries_by_boundary(start_time,end_time,bbox,limit,startindex)

        return result

