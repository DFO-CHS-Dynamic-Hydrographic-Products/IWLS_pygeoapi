import sqlite3
import logging
import math

from iwls import IwlsApiConnector


class Error_db():

    def __init__(self):
        self.db = 'error_statistics_test.db'

    def add_record_water_level(self, record):
        print(record)
        conn = sqlite3.connect(self.db)
        cur = conn.cursor()
        sql = (f'INSERT INTO water_level'
               f'({record["station_code"]} {record["comp_time"]} '
               f'{record["start_time"]} {record["end_time"]} {record["model"]} '
               f'{record["mean"]} {record["median"]} {record["variance"]} '
               f'{record["mean_lt"]} {record["median_lt"]} {record["variance_lt"]} )'
               f'VALUES')
        try:
            cur.execute(sql)

        except sqlite3.Error as error:
            logging.error(f'{error}')

        finally:
            if conn:
                conn.close

    def add_record_surace_current(self, record):
        conn = sqlite3.connect(self.db)
        cur = conn.cursor()
        sql = (f'INSERT INTO surace_current'
               f'({record["station_code"]} {record["comp_time"]} '
               f'{record["start_time"]} {record["end_time"]} {record["model"]} '
               f'{record["u_mean"]} {record["v_mean"]} {record["u_median"]} '
               f'{record["v_median"]} {record["u_variance"]} {record["v_variance"]} )'
               f'{record["u_mean_lt"]} {record["v_mean_lt"]} {record["u_median_lt"]} '
               f'{record["v_median_lt"]} {record["u_variance_lt"]} {record["v_variance_lt"]} )'
               f'VALUES')

        try:
            cur.execute(sql)

        except sqlite3.Error as error:
            logging.error(f'{error}')

        finally:
            if conn:
                conn.close

    def query_latest_lt_stats(self):
        q_water_level = ('SELECT *'
                         'FROM water_level '
                         'ORDER BY ID DESC '
                         'LIMIT 1')

        q_surface_current = ('SELECT *'
                             'FROM surface_current '
                             'ORDER BY ID DESC '
                             'LIMIT 1')

        conn = sqlite3.connect(self.db)
        cur = conn.cursor()
        try:

            latest_water_level_q = cur.execute(q_water_level)
            latest_water_level = latest_water_level_q.fetchone()

            latest_surface_current_q = cur.execute(q_surface_current)
            latest_surface_current = latest_surface_current_q.fetchone()

            latest_lt = {'wl_mean': latest_water_level[9],
                         'wl_median': latest_water_level[10],
                         'wl_variance': latest_water_level[11],
                         'u_mean': latest_surface_current[12],
                         'v_mean': latest_surface_current[13],
                         'u_median': latest_surface_current[14],
                         'v_median': latest_surface_current[15],
                         'u_variance': latest_surface_current[16],
                         'v_variance': latest_surface_current[17]
                         }
            return latest_lt

        except sqlite3.Error as error:
            print(error)

        finally:
            if conn:
                conn.close


class Compute_stats():

    def __init__(self):
        pass

    def _read_model(self):
        """
        Read and format model data (S-104 or S-111 files)
        Must be implemented by child class
        """
        raise NotImplementedError('_read_model')

    def _read_station_data(self):
        """
        Fetch and format observed data from IWLS
        Must be implemented by child class
        """
        raise NotImplementedError('_read_station_data')

    def _compute_mean(self):
        pass

    def _compute_median(self):
        pass

    def _compute_variance(self):
        pass

    def compute_error_stats(self):
        """
        Compute mean, median and variance
        Must be implemented by child class
        """
        raise NotImplementedError('compute_error_stats')

    def update_lt_stats(self):
        """
        Update long term mean, median and variance
        Must be implemented by child class
        """
        raise NotImplementedError('update_lt_stats')

    def update_db(self):
        """
        Add entry to database
        Must be implemented by child class
        """
        raise NotImplementedError('update_db')


class Compute_stats_wl(Compute_stats):

    def __init__(self):
        pass

    def _read_model(self):
        pass

    def _read_station_data(self, start_time, end_time, station_codes):
        """
        Fetch observed stations data
        :param station_codes: list of five digits station identifiers (list)
        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param  end_time: End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)

        :return: list of observed stations water level data
        """

        data = []

        for i in station_codes:
            api = IwlsApiConnector()
            station = api.get_station_data(
                station, start_time, end_time, dtype='wl')
            data.append(station)
        return data

    def compute_error_stats(self):
        pass

    def update_lt_stats(self):
        pass

    def update_db(self,
                  station_code,
                  comp_time,
                  start_time,
                  end_time,
                  model,
                  mean,
                  median,
                  variance,
                  mean_lt,
                  median_lt,
                  variance_lt
                  ):

        record = {
            'station_code': station_code,
            'comp_time': comp_time,
            'start_time': start_time,
            'end_time': end_time,
            'model': model,
            'mean': mean,
            'median': median,
            'variance': variance,
            'mean_lt': mean_lt,
            'median_lt': median_lt,
            'variance_lt': variance_lt
        }

        Error_db().add_record_water_level


class Compute_stats_surface_current(Compute_stats):

    def __init__(self):
        pass

    def _uv_from_speed_direction(speed, direction):
        """
        Compute velocity vectors (u,v) from speed and direction

        :param speed: ater current speed(float)
        :param direction: angle in degrees from north (float)

        :return: Velocity vectors (u,v) (tuple of floats)

        """

        u = speed * math.cos(direction)
        v = speed * math.sin(direction)

        return (u, v)

    def _read_model(self):
        pass

    def _read_station_data(self, start_time, end_time, station_codes):
        """
        Fetch observed stations data
        :param station_codes: list of five digits station identifiers (list)
        :param  start_time: Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)
        :param  end_time: End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z) (string)

        :return: list of observed stations surface current data
        """

        data = []
        for i in station_codes:
            api = IwlsApiConnector()
            station = api.get_station_data(
                station, start_time, end_time, dtype='wcs')
            data.append(station)
        return data

    def compute_error_stats(self):
        pass

    def update_lt_stats(self):
        pass

    def update_db(self,
                  station_code,
                  comp_time,
                  start_time,
                  end_time,
                  model,
                  mean_u,
                  mean_v,
                  median_u,
                  median_v,
                  variance_u,
                  variance_v,
                  mean_lt_u,
                  mean_lt_v,
                  median_lt_u,
                  median_lt_v,
                  variance_lt_u,
                  variance_lt_v,
                  ):

        record = {
            'station_code': station_code,
            'comp_time': comp_time,
            'start_time': start_time,
            'end_time': end_time,
            'model': model,
            'mean_u': mean_u,
            'mean_u': mean_v,
            'median_u': median_u,
            'median_v': median_v,
            'variance_u': variance_u,
            'variance_u': variance_v,
            'mean_lt_u': mean_lt_u,
            'mean_lt_v': mean_lt_v,
            'median_lt_u': median_lt_u,
            'median_lt_v': median_lt_v,
            'variance_lt_u': variance_lt_u,
            'variance_lt_v': variance_lt_v
        }

        Error_db().add_record_surace_current


sample_record_wl = {
    'station_code': 7122,
    'comp_time': '2019-11-13T19:18:00Z',
    'start_time': '2019-11-13T19:18:00Z',
    'end_time': '2019-11-13T19:18:00Z',
    'model': 'test',
    'mean': 3.4,
    'median': 4.5,
    'variance': 3.3,
    'mean_lt': 31.4,
    'median_lt': 2.5,
    'variance_lt': 3.1}

test_db = Error_db()
# test_db.add_record_water_level(sample_record_wl)
