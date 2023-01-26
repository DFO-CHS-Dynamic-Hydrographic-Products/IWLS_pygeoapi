import pandas as pd
import csv
import h5py
import datetime

def s100_to_csv(filepath):
    """
    Export timeseries data from S-104 or S-111 DFC8 file
    to csv file(s)
    :params filepath: path to S-104 or S-111 DFC8 file
    :returns: One csv file per station
    """

    with h5py.File("file.hdf5", "r") as hdf_file:
        # Read file type from metadata
        if '/WaterLevel/' in hdf_file:
            stype = 's104'
        elif '/SurfaceCurrent/' in hdf_file:
            stype = 's111'
        else:
            raise ValueError('Input must be S-104 or S-111 DFC8 file')

        data = {}
        for i in hdf_file['/WaterLevel/']:
            data_df = pd.DataFrame()
            data_type = i.attrs['typeOfWaterLevelData']
            for group in i.keys():
                stn_name = group.attrs['stationName']
                stn_data = group['values'].values()

                # Generate Timestamps for index
                start_time = group.attrs['startDateTime']
                end_time = group.attrs['endDateTime']
                interval = group.attrs['timeRecordInterval']
                number_of_times = group.attrs['numberOFTimes']
                timestamps = [(datetime.datetime(start_time) + datetime.timedelta(second=interval)).time() for x in range(number_of_times)]
                stn_index = pd.to_datetime(timestamps)

                group_df = pd.Series(stn_data,index=stn_index, name=stn_name)
                pd.merge(data_df, group_df, how="outer")


        

