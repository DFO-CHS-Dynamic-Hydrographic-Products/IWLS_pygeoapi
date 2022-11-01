
import sys
from telnetlib import PRAGMA_HEARTBEAT
sys.path.append('../')

import pandas as pd
from pygeoapi_iwls import iwls



class SpineErrors():
    def __init__(self):
        self.spine_stations = pd.read_csv('spine_stn.csv',dtype = str)

    def _get_station_data(self,start_time, end_time):
        codes = self.spine_stations['code'].to_list()
        api = iwls.IwlsApiConnector()
        stns_data = {}
        for i in codes:
            stn = api.get_station_data(i, start_time, end_time)
            wlo = pd.DataFrame.from_dict(stn['properties']['wlo'],orient='index',columns=['wlo'])
            wlo.index = pd.to_datetime(wlo.index, utc=True)
            stn_data = wlo
            wlp = pd.DataFrame.from_dict(stn['properties']['wlp'],orient='index',columns=['wlp'])
            wlp.index = pd.to_datetime(wlp.index, utc=True)
            stn_data = pd.merge(stn_data, wlp,how='outer', left_index=True, right_index=True)
            wlf = pd.DataFrame.from_dict(stn['properties']['wlo'],orient='index',columns=['wlf'])
            wlf.index = pd.to_datetime(wlf.index, utc=True)
            stn_data = pd.merge(stn_data, wlf,how='outer', left_index=True, right_index=True)
            spine = pd.DataFrame.from_dict(stn['properties']['spine'],orient='index',columns=['spine'])
            spine.index = pd.to_datetime(spine.index, utc=True)
            stn_data = pd.merge(stn_data, spine,how='outer', left_index=True, right_index=True)
            stn_name = i + '_' + stn['properties']['metadata']['officialName']
            stns_data[stn_name] = stn_data
            

        return stns_data

    def compute_errors(self,start_time, end_time):
        # Request data for station with SPINE time series
        stns_data = self._get_station_data(start_time, end_time)
        # Compute diff between wlo and SPINE
        errors_df = pd.DataFrame(columns=['station','start_time','end_time','median_of_diffs','MAE','RMSE','covariance'])
        for k,v in stns_data.items():
            v = v[['wlo','spine']]
            v['diff'] = v['wlo'] - v['spine']
            v = v.dropna()
            # station name
            station = k
            # start_time = start_time
            # end_time = end_time
            # median of differences
            median_of_diffs= v['diff'].median()
            # Mean Absolute Error
            mae = v['diff'].abs().mean()
            # Root Mean Square Error
            rmse = ((v['wlo'] - v['spine'])**2).mean()**0.5
            # covariance (Observed-SPINE)
            covariance = v['wlo'].cov(v['spine'])

            if not errors_df.empty:
                errors_df.loc[max(errors_df.index) + 1] = [station,start_time,end_time,median_of_diffs,mae,rmse,covariance]
            else:
                errors_df.loc[0] = [station,start_time,end_time,median_of_diffs,mae,rmse,covariance]

        return errors_df

errors = SpineErrors().compute_errors('2022-10-30T00:00:00Z','2022-10-30T00:30:00Z')
print(errors)