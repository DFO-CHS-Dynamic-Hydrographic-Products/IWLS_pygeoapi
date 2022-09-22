# Standard library imports
import requests, json, datetime, os, uuid

# Packages imports
from zipfile import ZipFile
import pandas as pd
import requests_cache
import dateutil.parser

# Local imports
from provider_iwls.iwls_api_connector import IwlsApiConnector

class IwlsApiConnectorWaterLevels(IwlsApiConnector):
    """
    Provider class used to retrieve iwls SurfaceCurrents data.
    """
    def __init__(self, provider_def):
        super().__init__(provider_def)

    def _get_station_data(self, identifier: int, start_time: str, end_time: str):
        """
        Get the station data.

        :param identifier: station ID (int)
        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param  end_time: End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)

        :returns: station GeoJSON feature
        """

        time_ranges_strings, metadata, url = super()._get_station_data(identifier, start_time, end_time)

        # Get Observations, Predictions, Forecasts and SPINE
        wlo = self._get_timeseries(url,time_ranges_strings,'wlo')
        wlp = self._get_timeseries(url,time_ranges_strings,'wlp')
        wlf = self._get_timeseries(url,time_ranges_strings,'wlf')
        spine = self._get_timeseries(url,time_ranges_strings,'wlf-spine')

        # Build Geojson feature for station
        station_geojson = {'type': 'Feature', 'id': metadata['code']}

        station_geojson["geometry"] = {
            "type": "Point",
            "coordinates":[metadata['longitude'],metadata['latitude']],
            }
        station_geojson['properties'] = {
            'metadata':metadata,
            'wlo':json.loads(wlo)['value'],
            'wlp':json.loads(wlp)['value'],
            'wlf':json.loads(wlf)['value'],
            'spine':json.loads(spine)['value'],
            }

        return station_geojson

    def _get_timeseries_by_boundary(self, start_time: str, end_time: str, bbox: list,
                                             limit=10, start_index=0):
        """
        Used by Query Method
        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param bbox: bounding box [minx,miny,maxx,maxy] (list)
        :param limit: number of records to return (default 10) (int)
        :param start_index: starting record to return (default 0) (int)
        :param api: api connection to IWLS (IwlsApiConnector)

        :returns: dict of 0..n GeoJSON features
        """
        within_lat, within_lon, stations_list, end_index, timeseries_data = super()._get_timeseries_by_boundary(start_time, end_time,bbox, limit, start_index)

        for stn in stations_list.code.iteritems():
            feature = self.get_station_data(stn[1],start_time,end_time)
            features.append(feature)

        timeseries_data['features'] = features

        with open('test.json', 'w', encoding='utf-8') as f:
            json.dump(timeseries_data, f, ensure_ascii=False, indent=4)

        return timeseries_data

    def query(self, startindex=0, limit=10, resulttype='results',
              bbox=[], datetime_=None, properties=[], sortby=[],
              select_properties=[], skip_geometry=False, q=None, **kwargs):

        """
        Query IWLS
        :param startindex: starting record to return (default 0) (int)
        :param limit: number of records to return (default 10) (int)
        :param resulttype: return results or hit limit (default results)
        :param bbox: bounding box [minx,miny,maxx,maxy] (list)
        :param datetime_: temporal (datestamp or extent) (string)
        :param properties: list of tuples (name, value) (list)
        :param sortby: list of dicts (property, order) (list)
        :param select_properties: list of property names (list)
        :param skip_geometry: bool of whether to skip geometry (default False) (bool)
        :param q: full-text search term(s) (string)
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
        # Pass query to IWLS API
        result = self._get_timeseries_by_boundary(start_time,end_time,bbox,limit,startindex)

        return result

    def get(self, identifier, **kwargs):
        """
        Get Feature by id
        :param identifier: feature id (int)

        :returns: feature collection
        """
        result = None

        # Only latest 24h of data available throught get method
        now = datetime.datetime.now()
        tomorrow = now + datetime.timedelta(days=1)
        yesterday = now - datetime.timedelta(days=1)
        end_time = tomorrow.strftime('%Y-%m-%dT%H:%M:%SZ')
        start_time = yesterday.strftime('%Y-%m-%dT%H:%M:%SZ')
        # Pass query to IWLS API
        result = self._get_station_data(identifier,start_time,end_time)

        return result
