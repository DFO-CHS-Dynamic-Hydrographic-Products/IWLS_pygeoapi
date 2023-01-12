
from provider_iwls.iwls_api_connector_waterlevels  import IwlsApiConnectorWaterLevels
import pandas as pd
import numpy as np
import datetime
import dateutil
import sqlite3
import logging

class Error_db_spine():

    def __init__(self):
        self.db = 'error_statistics_spine.db'

    def add_record_daily_spine(self, error_df):
        conn = sqlite3.connect(self.db)
        cur = conn.cursor()
        for row in error_df.itertuples():
            sql = (f'INSERT INTO statistics'
                f'( {row["station"]} {row["start_time"]} {row["end_time"]} {row["median_of_diffs"]}'
                f'{row["MAE"]} {row["RMSE"]} {row["covariance"]} )'
                f'VALUES')
                
            try:
                cur.execute(sql)

            except sqlite3.Error as error:
                logging.error(f'{error}')

            finally:
                if conn:
                    conn.close


class SpineErrors():
    def __init__(self):
        self.spine_stations = pd.read_csv('spine_stn.csv', dtype=str)

    def _get_station_data(self, start_time, end_time):
        """
        Request data from IWLS public API for stations with SPINE timeseries for a user defined time frame
        :param start_time:  Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param end_time:  End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :returns: dict of pandas.DataFrame
        """
        print('Sending Query to IWLS API')
        codes = self.spine_stations['code'].to_list()
        api = IwlsApiConnectorWaterLevels()
        stns_data = {}
        for i in codes:
            stn = api._get_station_data(i, start_time, end_time)
            wlo = pd.DataFrame.from_dict(
                stn['properties']['wlo'], orient='index', columns=['wlo'])
            wlo.index = pd.to_datetime(wlo.index, utc=True)
            stn_data = wlo
            wlp = pd.DataFrame.from_dict(
                stn['properties']['wlp'], orient='index', columns=['wlp'])
            wlp.index = pd.to_datetime(wlp.index, utc=True)
            stn_data = pd.merge(stn_data, wlp, how='outer',
                                left_index=True, right_index=True)
            wlf = pd.DataFrame.from_dict(
                stn['properties']['wlo'], orient='index', columns=['wlf'])
            wlf.index = pd.to_datetime(wlf.index, utc=True)
            stn_data = pd.merge(stn_data, wlf, how='outer',
                                left_index=True, right_index=True)
            spine = pd.DataFrame.from_dict(
                stn['properties']['spine'], orient='index', columns=['spine'])
            spine.index = pd.to_datetime(spine.index, utc=True)
            stn_data = pd.merge(stn_data, spine, how='outer',
                                left_index=True, right_index=True)
            stn_name = i + '_' + stn['properties']['metadata']['officialName']
            stns_data[stn_name] = stn_data

        return stns_data

    def compute_errors(self, start_time, end_time, output_format='df'):
        """
        Compute  errors between SPINE and WLO for a user
        defined time frame
        :param start_time:  Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param end_time:  End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param output: output format ('dataframe', 'csv' or 'sqlite')
        :returns: output
        """
        
        # Check if start and end time are valid
        try:
            dateutil.parser.parse(start_time)
        except:
            raise ValueError('invalid date format for start_time')

        try:
            dateutil.parser.parse(end_time)
        except:
            raise ValueError('invalid date format for end_time')

        # Request data for station with SPINE time series
        stns_data = self._get_station_data(start_time, end_time)
        # Compute diff between wlo and SPINE
        print('Computing Statistics')
        errors_df = pd.DataFrame(columns=[
                                 'station', 'start_time', 'end_time', 'median_of_diffs', 'MAE', 'RMSE', 'covariance'])
        for k, v in stns_data.items():
            v = v[['wlo', 'spine']].copy()
            v.loc[:,'diff'] = v['wlo'] - v['spine']
            v = v.dropna()
            # station name
            station = k
            # start_time = start_time
            # end_time = end_time
            # median of differences
            median_of_diffs = np.median(v['diff'])
            # Mean Absolute Error
            mae = np.mean(v['diff'].abs())
            # Root Mean Square Error
            rmse = np.mean((v['wlo'] - v['spine'])**2)**0.5
            # covariance (Observed-SPINE)
            covariance = v['wlo'].cov(v['spine'])

            if not errors_df.empty:
                errors_df.loc[max(errors_df.index) + 1] = [station, start_time,
                                                           end_time, median_of_diffs, mae, rmse, covariance]
            else:
                errors_df.loc[0] = [station, start_time, end_time,
                                    median_of_diffs, mae, rmse, covariance]                               
        print(errors_df)
        output = errors_df
        # Format output
        if output_format=='df':
           output = errors_df

        elif output_format=='csv':
            errors_df.to_csv(f'{start_time}.csv')

        elif output_format=='sqlite':
            Error_db_spine().add_record_daily_spine(errors_df)
        else:
            raise ValueError('output for compute_error must be df, csv or sqlite')

        return output

    def compute_daily_errors(self):
        """
        Compute daily errors between SPINE and WLO
        :returns: csv file
        """
        start_time = datetime.date.today() - datetime.timedelta(days=1)
        end_time = start_time + datetime.timedelta(minutes=1439)

        start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        errors = self.compute_errors(start_time, end_time)
        errors.to_csv(f'{start_time}.csv')


errors = SpineErrors().compute_errors(
    '2023-01-08T00:00:00Z', '2023-01-08T23:59:00Z')


