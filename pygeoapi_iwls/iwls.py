# Standard library imports
import requests
import json
import datetime
import os
# Packages imports
import pandas as pd
import dateutil.parser
import requests_cache

class IwlsApiConnector:
    """
    Connector class to interact with the CHS
    Integrated Water Level System API
    (https://api-iwls.dfo-mpo.gc.ca)
    """
    def __init__(self):
        self.info = self.get_summary_info()


    def get_summary_info(self):
        """
        Get summary information for all stations.Runs on class instantiation.
        Output is used to match station codes and names to unique IWLS database id.

        :returns: Pandas dataframe containing summary information for all stations
        """
        
        # Set up requests_cache session
        s_summary_info = requests_cache.CachedSession('http_cache', backend='filesystem',expire_after=86400)

        # Request data from cache or trought get request
        url = 'https://api-iwls.dfo-mpo.gc.ca/api/v1/stations/'
        params = {}
        r = s_summary_info.get(url=url, params=params)

        print(f'From cache: {r.from_cache}')
        print(f'Created: {r.created_at}')
        print(f'Expires: {r.expires}')
 
        r.raise_for_status()
        data_json = r.json()
        df = pd.DataFrame.from_dict(data_json)

        return df


    def id_from_station_code(self, station_code):
        """
        Return unique id for a station from five digits identifier
        :param sation_code: five digits station identifier (string)
        :returns: unique IWLS database id (int)
        """
        iwls_id = self.info.loc[self.info.code==station_code].id.values[0]
        return iwls_id
    
    def id_from_station_name(self, station_name):
        """
        Return unique id for a station from exact official name
        :param sation_name: Station exact official name (string)
        :returns: unique IWLS database id (int)
        """
        iwls_id = self.info.loc[self.info.officialName==station_name].id.values[0]
        return iwls_id

    def station_name_from_id(self, iwls_id):
        """
        Return  station  exact official name from unique id
        :param iwls_id:unique IWLS database id (string)
        :returns: Station Name (string)
        """
        station_name = self.info.loc[self.info.id==iwls_id].officialName.values[0]
        return station_name

    def get_station_metadata(self, station_code, cache_result=True):
        """
        Return a json response from the IWLS API containing full metadata for a single station
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

    def _get_timeseries(self,url,time_ranges_strings,series_code):
        """
        Send a series of queries to the IWLS API and return 
        :param url: url used for queries (String)
        :param time_ranges_string: pairs of start times and end times used for queries (pandas Dataframe)
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
            series_data = series_data.append(pd.DataFrame.from_dict(r.json()))

        if series_data.empty:
                return json.dumps({'value':{}})
        else:
                series_data['eventDate'] = pd.to_datetime(series_data['eventDate'])
                series_data = series_data.set_index('eventDate').sort_index()
                series_data = series_data[['value']]

                return series_data.to_json(date_format='iso')

    def get_station_data(self,station_code,start_time,end_time, dtype='wl'):
        """
        Get water level timeseries (observations, predictions, forecasts) for a single station
        :param station_code: five digits station identifier (string)
        :param  time_series_code: Code of the timeseries (wlo,wlp, wlf, all); all return (wlo,wlp,wlf) tuple for every timestamps (string)
        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param  end_time: End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param dtype: Type of data to query, ('wl' for water levels, 'wcs' for surface currents) (string)
        :returns: GeoJSON containing requested station metadata and available water level time series for specified time range (Json)
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
        metadata = self.get_station_metadata(station_code)

        station_id = self.id_from_station_code(station_code)
        url = 'https://api-iwls.dfo-mpo.gc.ca/api/v1/stations/' + station_id + '/data'

        # Query Correct data type, raise error if invalid
        # Water Levels
        if dtype == 'wl':
            # Get Observations, Predictions, Forecasts and SPINE
            wlo = self._get_timeseries(url,time_ranges_strings,'wlo')
            wlp = self._get_timeseries(url,time_ranges_strings,'wlp')
            wlf = self._get_timeseries(url,time_ranges_strings,'wlf')
            spine = self._get_timeseries(url,time_ranges_strings,'wlf-spine')

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
                'wlo':json.loads(wlo)['value'],
                'wlp':json.loads(wlp)['value'],
                'wlf':json.loads(wlf)['value'],
                'spine':json.loads(spine)['value'],
                }
        # Surface Currents
        elif dtype == 'wcs':
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



        # Raise Error if  invalid data type
        else:
            raise ValueError('Invalid data type. Accepted values are "wl" for water levels and "wcs" for surface currents')       

        return station_geojson

    def get_timeseries_by_boundary(self,start_time,end_time,bbox,limit=10, startindex=0, dtype='wl'):
        """
        Do a series of queries to the IWLS API and return data
        within provided time frame and bounding box
        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param  end_time: End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param bbox: bounding box [minx,miny,maxx,maxy] (list)
        :param limit: Maximum number of station to query (int)
        :param time_limit: Maximum time that can be requested, in hours (float)
        :param startindex: starting index for query (int)
        :param dtype: Type of data to query, ('wl' for water levels, 'wcs' for surface currents) (string)
        :returns: GeoJson Feature Collection
        """
        # use summary metadata info to find stations within request
        within_lat = self.info['latitude'].between(bbox[1], bbox[3])
        within_lon = self.info['longitude'].between(bbox[0], bbox[2])
        stations_list = self.info[within_lat & within_lon]
        # Only Query stations up  from start index to limit
        end_index = startindex + limit
        stations_list = stations_list[startindex:end_index]
        
        # If surface currents, filter out stations with only water level observations
        if dtype =='wcs':
            stations_list = stations_list[stations_list['timeSeries'].astype(str).str.contains('wcs1')]

        # Query stations and populate Geojson feature collection
        geojson = {}
        geojson ['type'] = 'FeatureCollection'
        features = []

        # Query Correct data type, raise error if invalid

        if dtype == 'wl':
            for stn in stations_list.code.iteritems():
                feature = self.get_station_data(stn[1],start_time,end_time)
                features.append(feature)
            geojson ['features'] = features
        
        elif dtype == 'wcs':
            for stn in stations_list.code.iteritems():
                feature = self.get_station_data(stn[1],start_time,end_time,'wcs')
                features.append(feature)
            geojson ['features'] = features

        else:
            raise ValueError('Invalid data type. Accepted values are "wl" for water levels and "wcs" for surface currents ')
        
        
        with open('test.json', 'w', encoding='utf-8') as f:
            json.dump(geojson, f, ensure_ascii=False, indent=4)
        
        return geojson

api = IwlsApiConnector()
data = api.get_timeseries_by_boundary('2022-06-19T16:00:00Z','2022-06-19T22:00:00Z',[-123.28,49.07,-123.01,49.35],dtype='wcs')
with open('s111_test.json', 'w') as f:
    json.dump(data, f,indent=4)