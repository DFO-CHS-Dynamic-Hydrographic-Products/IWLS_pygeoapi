# Standard library imports
import json

# Local imports
from provider_iwls.api_connector.iwls_api_connector import IwlsApiConnector

class IwlsApiConnectorCurrents(IwlsApiConnector):
    """
    Provider class used to retrieve iwls SurfaceCurrents data.
    """
    def __init__(self):
        super().__init__()

    def _get_station_data(self, station_code: str, start_time: str, end_time: str):
        """
        Get water level timeseries (observations, predictions, forecasts) for a single station.

        :param station_code: five digits station identifier (string)
        :param  time_series_code: Code of the timeseries (wlo,wlp, wlf, all); all return (wlo,wlp,wlf) tuple for every timestamps (string)
        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param  end_time: End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :returns: GeoJSON containing requested station metadata and available water level time series for specified time range (Json)
        """
        time_ranges_strings, metadata, url = super()._get_station_data(station_code, start_time, end_time)

        #Get Surface Currents observations (speed and direction)
        wcs = self._get_timeseries(url,time_ranges_strings,'wcs1')
        wcd = self._get_timeseries(url,time_ranges_strings,'wcd1')

        # Build Geojson feature for station
        station_geojson = {'type': 'Feature',
                           'id': metadata['code'],
                           'geometry':  {
                               "type": "Point",
                               "coordinates":[metadata['longitude'],metadata['latitude']]
                           },
                           'properties': {
                               'metadata':metadata,
                               'wcs':json.loads(wcs)['value'],
                               'wcd':json.loads(wcd)['value'],
                           }
                           }


        return station_geojson

    def _get_timeseries_by_boundary(self, start_time: str, end_time: str,
                                    bbox: list, limit=10, start_index=0):
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
        features = []

        within_lat, within_lon, stations_list, end_index, timeseries_data = super()._get_timeseries_by_boundary(
            start_time, end_time, bbox, limit, start_index
        )
        stations_list = stations_list[stations_list['timeSeries'].astype(str).str.contains('wcs1')]

        for stn in stations_list.code.iteritems():
            feature = self._get_station_data(stn[1], start_time, end_time)
            features.append(feature)
        timeseries_data['features'] = features

        # with open('test.json', 'w', encoding='utf-8') as f:
        #     json.dump(timeseries_data, f, ensure_ascii=False, indent=4)

        return timeseries_data
