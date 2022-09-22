# Standard library imports
import requests, json, datetime, os, uuid, logging

# Packages imports
from zipfile import ZipFile
import pandas as pd
import requests_cache
import dateutil.parser

from pygeoapi.provider.base import BaseProvider

class IwlsApiConnector(BaseProvider):
    """
    Provider abstract base class for iwls data
    Used as parent by ProviderIwlsWaterLevels and ProviderIwlsCurrents
    """
    def __init__(self, provider_def):
        self.info = self.get_summary_info()

    def get_summary_info(self) -> pd.DataFrame:
        """
        Get summary information for all stations.Runs on class instantiation.
        Output is used to match station codes and names to unique IWLS database id.

        :returns: Pandas dataframe containing summary information for all stations
        """

        # Set up requests_cache session
        s_summary_info = requests_cache.CachedSession('http_cache', backend='filesystem',expire_after=86400)

        # Request data from cache or trought get request
        url = 'https://api-iwls.dfo-mpo.gc.ca/api/v1/stations/'
        r = s_summary_info.get(url=url, params={})

        logging.info(f'From cache: {r.from_cache}')
        logging.info(f'Created: {r.created_at}')
        logging.info(f'Expires: {r.expires}')

        r.raise_for_status()
        data_json = r.json()

        return pd.DataFrame.from_dict(data_json)

    def id_from_station_code(self, station_code: str) -> int:
        """
        Return unique id for a station from five digits identifier
        :param station_code: five digits station identifier (string)
        :returns: unique IWLS database id (int)
        """
        iwls_id = self.info.loc[self.info.code==station_code].id.values[0]
        return iwls_id

    def id_from_station_name(self, station_name: str) -> int:
        """
        Return unique id for a station from exact official name
        :param sation_name: Station exact official name (string)
        :returns: unique IWLS database id (int)
        """
        iwls_id = self.info.loc[self.info.officialName==station_name].id.values[0]
        return iwls_id

    def station_name_from_id(self, iwls_id: str) -> str:
        """
        Return  station  exact official name from unique id
        :param iwls_id:unique IWLS database id (string)
        :returns: Station Name (string)
        """
        station_name = self.info.loc[self.info.id==iwls_id].officialName.values[0]
        return station_name

    def get_station_metadata(self, station_code: str, cache_result=True):
        """
        Return a json response from the IWLS API containing full metadata for a single station.

        :param station_code: = five digit station identifier (string)
        :cache_result: boolean, if true use requests_cache to cache results (bool)
        :returns: Station Metadata (JSON)
        """
        station_id = self.id_from_station_code(station_code)
        url = 'https://api-iwls.dfo-mpo.gc.ca/api/v1/stations/' + station_id + '/metadata'
        params = {}

        if cache_result == True:
            s_stn_metadata = requests_cache.CachedSession('http_cache', backend='filesystem',expire_after=2629746)
            r = s_stn_metadata.get(url=url, params=params)
        else:
            r = requests.get(url=url, params=params)
        r.raise_for_status()
        metadata_json = r.json()
        return metadata_json

    def _get_station_data(self, station_id: int, start_time: str, end_time: str) -> dict:
        """
        Get the station data.

        :param identifier: station ID (int)
        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param  end_time: End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)

        :returns: GeoJSON feature
        """

        # Can only get 7 days of data per request, split data in multiple requests if needed
        #(ToDo: had check to block large requests in ProviderIwls class)
        start_time_dt = dateutil.parser.parse(start_time)
        end_time_dt = dateutil.parser.parse(end_time)
        date_list = [start_time_dt]

        if (end_time_dt - start_time_dt) > datetime.timedelta(days=5):
            date_list.append(start_time_dt + datetime.timedelta(days=5))
            while (date_list[-1]  + datetime.timedelta(days=5)) < end_time_dt:
                date_list.append(date_list[-1] + datetime.timedelta(days=5))
        date_list.append(end_time_dt)

        # Create list of start time and end time pairs
        time_ranges = []

        for idx, i in enumerate(date_list[:-1]):
            time_ranges.append([i,date_list[idx+1] - datetime.timedelta(seconds=1)])
        time_ranges[-1][-1] = time_ranges[-1][-1] + datetime.timedelta(seconds=1)

        #  Convert datetime object back to ISO 8601 format strings
        time_ranges_strings = [[datetime.datetime.strftime(i,'%Y-%m-%dT%H:%M:%SZ',) for i in x] for x in time_ranges]

        # Execute all get requests

        # Get metadata
        metadata = self.get_station_metadata(station_id)

        station_id = self.id_from_station_code(station_id)
        url = f'https://api-iwls.dfo-mpo.gc.ca/api/v1/stations/{station_id}/data'

        return time_ranges_strings, metadata, url

    def _get_timeseries_by_boundary(self, start_time: str, end_time: str, bbox: list,
                                             limit=10, start_index=0):
        """
        Contains all logic common to WaterLevels/SurfaceCurrents IWLSConnector class
        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param bbox: bounding box [minx,miny,maxx,maxy] (list)
        :param limit: number of records to return (default 10) (int)
        :param start_index: starting record to return (default 0) (int)

        :returns: lat/lon values in bounding box, list of stations, end index and unpopulated geojson
        """
        # use summary metadata info to find stations within request
        within_lat = self.info['latitude'].between(bbox[1], bbox[3])
        within_lon = self.info['longitude'].between(bbox[0], bbox[2])

        stations_list_data = self.info[within_lat & within_lon]
        stations_list = stations_list_data[start_index:end_index]

        # Only Query stations up  from start index to limit
        end_index = start_index + limit

        timeseries_data = {"type": "featureCollection"}

        return within_lat, within_lon, stations_list, end_index, timeseries_data

    def get(self, station_id: int, **kwargs):
        """
        Get feature by id

        :param identifier: feature id (int)
        :returns: feature collection
        """

        # Only latest 24h of data available throught get method
        now = datetime.datetime.now()
        tomorrow = now + datetime.timedelta(days=1)
        yesterday = now - datetime.timedelta(days=1)
        end_time = tomorrow.strftime('%Y-%m-%dT%H:%M:%SZ')
        start_time = yesterday.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Pass query to IWLS API and return result
        return self._get_station_data(station_id, start_time, end_time)
