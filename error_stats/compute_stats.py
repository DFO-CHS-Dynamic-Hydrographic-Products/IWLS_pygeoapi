import sqlite3


class Error_db():

    def __init__(self):
        self.db = 'error_statistics_test.db'

    def add_record_water_level(self):
        pass

    def add_record_surface_current(self):
        pass

    def query_latest_lt_stats(self):
        q_water_level = ('SELECT *'
                         'FROM water_level '
                         'ORDER BY ID DESC '
                         'LIMIT 1')
        print(q_water_level)
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

            latest_lt = {'wl_mean' : latest_water_level[9],
                         'wl_median' : latest_water_level[10],
                         'wl_variance' : latest_water_level[11],
                         'u_mean' : latest_surface_current[12],
                         'v_mean' : latest_surface_current[13],
                         'u_median' : latest_surface_current[14],
                         'v_median' : latest_surface_current[15],
                         'u_variance' : latest_surface_current[16],
                         'v_variance' : latest_surface_current[17]
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

    def _read_station_data(self):
        pass

    def compute_error_stats(self):
        pass

    def update_lt_stats(self):
        pass

    def update_db(self):
        pass

class Compute_stats_surface_current(Compute_stats):
    
    def __init__(self):
        pass

    def _uv_from_speed_direction(speed,direction):
        pass

    def _read_model(self):
        pass

    def _read_station_data(self):
        pass

    def compute_error_stats(self):
        pass

    def update_lt_stats(self):
        pass

    def update_db(self):
        pass

test_db = Error_db()
test_db.query_latest_lt_stats()