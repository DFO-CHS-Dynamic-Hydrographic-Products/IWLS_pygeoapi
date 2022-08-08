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
    Provider abstract base class for iwls data
    Used as parent by ProviderIwlsWaterLevels and ProviderIwlsCurrents
    """
    def __init__(self, provider_def):
        """Inherit from parent class"""
        super().__init__(provider_def)

    def _provider_get_station_data(self):
        # Method needs to be implemented by child class
        raise NotImplementedError("Must override _provider_get_station_data")

    def _provider_get_timeseries_by_boundary(self):
        # Method needs to be implemented by child class
        raise NotImplementedError("Must override _provider_get_timeseries_by_boundary")

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
        result = self._provider_get_station_data(identifier,start_time,end_time,api)

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
        result = self._provider_get_timeseries_by_boundary(start_time,end_time,bbox,limit,startindex,api)

        return result

class ProviderIwlsWaterLevels(ProviderIwls):
    """
    Provider class for iwls Water Level data
    """
    def __init__(self, provider_def):
        """Inherit from parent class"""
        super().__init__(provider_def)

    def _provider_get_station_data(self,identifier,start_time,end_time,api):
        """        
        Used by Get Method
        :param identifier: station ID
        :param start_time: Start time (ISO 8601)
        :param end_time: End time (ISO 8601)
        :param api: api connection to IWLS
        
        :returns: GeoJSON feature
        """
        station_data = api.get_station_data(identifier,start_time,end_time)
        return station_data

    def _provider_get_timeseries_by_boundary(self,start_time,end_time,bbox,limit,startindex,api):
        """        
        Used by Query Method
        :param start_time: Start time (ISO 8601)
        :param end_time: End time (ISO 8601)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param limit: number of records to return (default 10)
        :param startindex: starting record to return (default 0)
        :param api: api connection to IWLS

        :returns: dict of 0..n GeoJSON features
        """
        timeseries = api.get_timeseries_by_boundary(start_time,end_time,bbox,limit,startindex)
        return timeseries


class ProviderIwlsCurrents(ProviderIwls):
    """
    Provider class for iwls Water Level Currents
    """
    def __init__(self, provider_def):
        """Inherit from parent class"""
        super().__init__(provider_def)

    def _provider_get_station_data(self,identifier,start_time,end_time,api):
        """
        Used by Get Method
        :param identifier: station ID
        :param start_time: Start time (ISO 8601)
        :param end_time: End time (ISO 8601)
        :param api: api connection to IWLS

        :returns: GeoJSON feature
        """

        station_data = api.get_station_data(identifier,start_time,end_time, dtype='wcs')
        return station_data

    def _provider_get_timeseries_by_boundary(self,start_time,end_time,bbox,limit,startindex,api):
        """
        Used by Query Method
        :start_time: Start time (ISO 8601)
        :end_time: End time (ISO 8601)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param limit: number of records to return (default 10)
        :param startindex: starting record to return (default 0)
        :param api: api connection to IWLS

        :returns: dict of 0..n GeoJSON features
        """
        timeseries = api.get_timeseries_by_boundary(start_time,end_time,bbox,limit,startindex, dtype='wcs')
        return timeseries