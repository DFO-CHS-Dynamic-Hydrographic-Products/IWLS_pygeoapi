# Standard library imports
import requests, json, datetime, os, uuid

# Packages imports
from zipfile import ZipFile
import pandas as pd
import requests_cache
import dateutil.parser

# Local imports
from provider_iwls.iwls_api_connector import IwlsApiConnector

class IwlsApiConnectorCurrents(IwlsApiConnector):
    """
    Provider class used to retrieve iwls SurfaceCurrents data.
    """
    def __init__(self, provider_def):
        super().__init__(provider_def)

    def _get_station_data(self, station_code: int, start_time: str, end_time: str):
        """
        Sends a request to retreive the station data.

        :param identifier: station ID (int)
        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param  end_time: End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param api: api connection to IWLS (IwlsApiConnector)

        :returns: GeoJSON feature (json)
        """
        time_ranges_strings, metadata, url = super()._get_station_data(station_code, start_time, end_time)

        #Get Surface Currents observations (speed and direction)
        wcs = self._get_timeseries(url,time_ranges_strings,'wcs1')
        wcd = self._get_timeseries(url,time_ranges_strings,'wcd1')

        # Build Geojson feature for station
        station_geojson = {}
        station_geojson['type'] = 'Feature'
        station_geojson['id'] = metadata['code']
        station_geojson["geometry"] = {
            "type": "Point",
            "coordinates":[metadata['longitude'],metadata['latitude']],
            }
        station_geojson['properties'] = {
            'metadata':metadata,
            'wcs':json.loads(wcs)['value'],
            'wcd':json.loads(wcd)['value'],
            }

        return station_geojson

    def _get_timeseries_by_boundary(self, start_time: str, end_time: str, bbox: list,
                                             limit: int, start_index: int):
        """
        Sends a request to retreive timeseries data in a specified bounding box.

        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param  end_time: End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param bbox: bounding box [minx,miny,maxx,maxy] (list)
        :param limit: number of records to return (default 10) (int)
        :param start_index: starting record to return (default 0) (int)
        :param api: api connection to IWLS (IwlsApiConnector)

        :returns: dict of 0..n GeoJSON features (json)
        """
        within_lat, within_lon, stations_list, end_index, timeseries_data = super()._get_timeseries_by_boundary(start_time, end_time,bbox, limit, start_index)
        stations_list = stations_list[stations_list['timeSeries'].astype(str).str.contains('wcs1')]

        for stn in stations_list.code.iteritems():
            feature = self.get_station_data(stn[1],start_time,end_time,'wcs')
            features.append(feature)
        timeseries_data['features'] = features

        # with open('test.json', 'w', encoding='utf-8') as f:
        #     json.dump(timeseries_data, f, ensure_ascii=False, indent=4)

        return timeseries_data
