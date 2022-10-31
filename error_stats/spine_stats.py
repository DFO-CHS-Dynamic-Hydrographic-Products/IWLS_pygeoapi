
import sys
sys.path.append('../')

import pandas as pd
from pygeoapi_iwls import iwls



class SpineErrors():
    def __init__(self):
        self.spine_stations = pd.read_csv('spine_stn.csv',dtype = str)

    def _get_station_data(self,start_time, end_time):
        codes = self.spine_stations['code'].to_list()
        print(codes)
        api = iwls.IwlsApiConnector()
        stns_data = {}
        for i in codes:
            stn = api.get_station_data(i, start_time, end_time)
            print(stn['properties'])
            wlo = pd.DataFrame.from_dict(stn['properties']['wlo'],orient='index')
            wlo.index = pd.to_datetime(wlo.index)
            stn_data = wlo
            wlp = pd.DataFrame.from_dict(stn['properties']['wlp'],orient='index')
            wlp.index = pd.to_datetime(wlp.index)
            stn_data = pd.merge(stn_data, wlp,how='outer', left_index=True, right_index=True)
            wlf = pd.DataFrame.from_dict(stn['properties']['wlo'],orient='index')
            wlf.index = pd.to_datetime(wlf.index)
            stn_data = pd.merge(stn_data, wlf,how='outer', left_index=True, right_index=True)
            spine = pd.DataFrame.from_dict(stn['properties']['spine'],orient='index')
            spine.index = pd.to_datetime(spine.index)
            stn_data = pd.merge(stn_data, spine,how='outer', left_index=True, right_index=True)

            stn_data.columns = ['wlo','wlp','wlf','spine']
            stn_name = i + '_' + stn['properties']['metadata']['officialName']
            stns_data[stn_name] = stn_data
            print(stn_name)

        return stns_data

    def compute_errors(self,start_time, end_time):
        pass

SpineErrors()._get_station_data('2022-10-30T00:00:00Z','2022-10-30T00:30:00Z')