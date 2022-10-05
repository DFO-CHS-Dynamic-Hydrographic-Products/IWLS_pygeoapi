# Standard library imports
import requests
import json
import datetime
import os
import uuid
import logging
import requests_cache
import dateutil.parser

# Packages imports
from zipfile import ZipFile
import pandas as pd

class IwlsApiConnector():
    """
    Provider abstract base class for iwls data
    Used as parent by ProviderIwlsWaterLevels and ProviderIwlsCurrents
    """
    def __init__(self):
        """
        Init function that provides summary data (from cached sessions if available)
        """
        self.info = self._get_summary_info()

    def _get_summary_info(self) -> pd.core.frame.DataFrame:
        """
        Get summary information for all stations.Runs on class instantiation.
        Output is used to match station codes and names to unique IWLS database id.

        :returns: Pandas dataframe containing summary information for all stations (pd.DataFrame)
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

    def _id_from_station_code(self, station_code: str) -> int:
        """
        Return unique id for a station from five digits identifier

        :param station_code: five digits station identifier (string)
        :returns: unique IWLS database id (int)
        """
        return self.info.loc[self.info.code==station_code].id.values[0]

    def _id_from_station_name(self, station_name: str) -> int:
        """
        Return unique id for a station from exact official name

        :param sation_name: Station exact official name (string)
        :returns: unique IWLS database id (int)
        """
        return self.info.loc[self.info.officialName==station_name].id.values[0]

    def _station_name_from_id(self, iwls_id: str) -> str:
        """
        Return  station  exact official name from unique id

        :param iwls_id:unique IWLS database id (string)
        :returns: Station Nname (string)
        """
        return self.info.loc[self.info.id==iwls_id].officialName.values[0]

    def _get_station_metadata(self, station_code: str, cache_result=True):
        """
        Return a json response from the IWLS API containing full metadata for a single station.

        :param station_code: = five digit station identifier (string)
        :cache_result: boolean, if true use requests_cache to cache results (bool)
        :returns: Station Metadata (JSON)
        """
        station_id = self._id_from_station_code(station_code)
        url = 'https://api-iwls.dfo-mpo.gc.ca/api/v1/stations/' + station_id + '/metadata'
        params = {}

        if cache_result == True:
            s_stn_metadata = requests_cache.CachedSession('http_cache', backend='filesystem',expire_after=2629746)
            r = s_stn_metadata.get(url=url, params=params)
        else:
            r = requests.get(url=url, params=params)
        r.raise_for_status()

        return r.json()

    def _get_station_data(self, station_code: int, start_time: str, end_time: str) -> dict:
        """
        Get the station data.

        :param station_code: five digits station identifier (string)
        :param  time_series_code: Code of the timeseries (wlo,wlp, wlf, all); all return (wlo,wlp,wlf) tuple for every timestamps (string)
        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param  end_time: End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :returns: time_strings_range, metadata and url for query
        """

        # Can only get 7 days of data per request, psplit data in multiple requests if needed
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

        # Get metadata
        metadata = self._get_station_metadata(station_code)

        # Get the station id from the station code
        station_id = self._id_from_station_code(station_code)

        # Use the station id in the url for the query
        url = f'https://api-iwls.dfo-mpo.gc.ca/api/v1/stations/{station_id}/data'

        return time_ranges_strings, metadata, url

    def _get_timeseries(self,url: str, time_ranges_strings: pd.core.frame.DataFrame, series_code: str):
        """
        Send a series of queries to the IWLS API and return

        :param url: url used for queries (String)
        :param time_ranges_string: pairs of start times and end times used for queries (pd.Dataframe)
        :param series_code: three letter identifer for time series (String)
                            'wlo' = Observed Water Levels
                            'wlp' = Tidal Predictions
                            'wlf' = Quality Control Forecast
                            'wlf-spine' = SPINE Forcast
                            'wcs1' = Observed Surface Currents Speed
                            'wcd1' = Observed Surface Currents Direction
        returns: series of pairs of time stamps and water level values (json)
        """
        series_data = pd.DataFrame()
        for i in time_ranges_strings:
            params = {
                'time-series-code':series_code,
                'from':i[0],
                'to': i[1]
                }
            r = requests.get(url=url, params=params)
            r.raise_for_status()
            series_data = pd.concat([series_data, pd.DataFrame.from_dict(r.json())])

        if series_data.empty:
                return json.dumps({'value':{}})
        else:
                series_data['eventDate'] = pd.to_datetime(series_data['eventDate'])
                series_data = series_data.set_index('eventDate').sort_index()
                series_data = series_data[['value']]

                return series_data.to_json(date_format='iso')


    def _get_timeseries_by_boundary(self, start_time: str, end_time: str, bbox: list,
                                    limit: int, start_index: int):
        """
        Contains all logic common to WaterLevels/SurfaceCurrents IWLSConnector class to retrieve timeseries data inside of a bounding box.

        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param bbox: bounding box [minx,miny,maxx,maxy] (list)
        :param limit: number of records to return (default 10) (int)
        :param start_index: starting record to return (default 0) (int)
        :returns: lat/lon values in bounding box, list of stations, end index and unpopulated geojson (timeseries_data)
        """
        # use summary metadata info to find stations within request
        within_lat = self.info['latitude'].between(bbox[1], bbox[3])
        within_lon = self.info['longitude'].between(bbox[0], bbox[2])

        stations_list_data = self.info[within_lat & within_lon]

        # Only Query stations up  from start index to limit
        end_index = start_index + limit
        stations_list = stations_list_data[start_index:end_index]

        timeseries_data = {"type": "featureCollection"}

        return within_lat, within_lon, stations_list, end_index, timeseries_data
