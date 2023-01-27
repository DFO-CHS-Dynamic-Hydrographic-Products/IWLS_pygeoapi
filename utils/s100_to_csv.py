import pandas as pd
import csv
import h5py
import datetime
import dateutil
import sys

def s100_to_csv(filepath):
    """
    Export timeseries data from S-104 or S-111 DFC8 file
    to csv file(s)
    :params filepath: path to S-104 or S-111 DFC8 file
    :returns: One csv file per station
    """

    with h5py.File(filepath, "r") as hdf_file:
        # Read file type from metadata
        if '/WaterLevel/' in hdf_file:
            group_path = '/WaterLevel/'
        elif '/SurfaceCurrent/' in hdf_file:
            group_path = '/SurfaceCurrent/'
        else:
            raise ValueError('Input must be S-104 or S-111 DFC8 file')
        data_df = pd.DataFrame()

        ### S104 Conversion ###
        if group_path == '/WaterLevel/':
            for i in hdf_file['/WaterLevel/']:
                if i != 'axisName':
                    data_type = hdf_file[f'/WaterLevel/{i}/'].attrs['typeOfWaterLevelData']
                    #Format data type
                    if data_type == 2:
                        data_type = 'wlp'
                    elif data_type == 1:
                        data_type = 'wlo'
                    else:
                        data_type = 'wlf'

                    #Format data into dataframe with datetime index
                    for group in hdf_file[f'/WaterLevel/{i}/']:
                        if group != 'Positioning':
                            # Get Station label
                            stn_name = hdf_file[f'/WaterLevel/{i}/{group}/'].attrs['stationName']
                            stn_id = hdf_file[f'/WaterLevel/{i}/{group}/'].attrs['stationIdentification']
                            stn_label = f'{stn_id}_{stn_name}_{data_type}'
                            # Get Station data
                            stn_data = hdf_file[f'/WaterLevel/{i}/{group}/values'][:]
                            # Return only water Level from (water levels,trends) tuples
                            stn_data = [i[0] for i in stn_data]
                            # Generate Timestamps for index
                            number_of_times = int(hdf_file[f'/WaterLevel/{i}/{group}/'].attrs['numberOfTimes'])
                            interval = int(hdf_file[f'/WaterLevel/{i}/{group}/'].attrs['timeRecordInterval'])
                            start_time = hdf_file[f'/WaterLevel/{i}/{group}/'].attrs['startDateTime']
                            start_time = dateutil.parser.parse(start_time)
                            end_time = start_time + datetime.timedelta(seconds=((number_of_times*interval)-interval))
                            timestamps = pd.date_range(start_time,end_time, freq = f'{interval}S')
                            
                            group_df = pd.DataFrame(stn_data,index=timestamps, columns=[stn_label])
                            if data_df.empty:
                                data_df = group_df
                            else:
                                data_df = pd.concat([data_df, group_df], axis=1)

        ### S111 Conversion ###                      
        else:
            for group in hdf_file['/SurfaceCurrent/SurfaceCurrent.01/']:
                if group != 'Positioning':
                    # Get Station label
                    stn_name = hdf_file[f'/SurfaceCurrent/SurfaceCurrent.01/{group}/'].attrs['stationName']
                    stn_id = hdf_file[f'/SurfaceCurrent/SurfaceCurrent.01/{group}/'].attrs['stationIdentification']
                    stn_label = f'{stn_id}_{stn_name}'
                    # Get Station data
                    stn_data = hdf_file[f'/SurfaceCurrent/SurfaceCurrent.01/{group}/values'][:]
                    stn_speed = [i[0] for i in stn_data]
                    stn_direction = [i[1] for i in stn_data]
                    # Generate Timestamps for index
                    number_of_times = int(hdf_file[f'/SurfaceCurrent/SurfaceCurrent.01/{group}/'].attrs['numberOfTimes'])
                    interval = int(hdf_file[f'/SurfaceCurrent/SurfaceCurrent.01/{group}/'].attrs['timeRecordInterval'])
                    start_time = hdf_file[f'/SurfaceCurrent/SurfaceCurrent.01/{group}/'].attrs['startDateTime']
                    start_time = dateutil.parser.parse(start_time)
                    end_time = start_time + datetime.timedelta(seconds=((number_of_times*interval)-interval))
                    timestamps = pd.date_range(start_time,end_time, freq = f'{interval}S')

                    group_df = pd.DataFrame(stn_speed,index=timestamps, columns=[f'{stn_label}_wcs'])
                    dir_df = pd.DataFrame(stn_direction,index=timestamps, columns=[f'{stn_label}_wcd'])
                    group_df = pd.concat([group_df, dir_df], axis=1)
                    if data_df.empty:
                        data_df = group_df
                    else:
                        data_df = pd.concat([data_df, group_df], axis=1)




        # Export stations to csv files
        station_list = data_df.columns.values.tolist()
        station_list = list(set([i[0:-4] for i in station_list]))
        for i in station_list:
            station_df = data_df.loc[:, data_df.columns.str.contains(f'{i}')]
            station_df = station_df.rename(columns = lambda x : str(x)[-3:])
            station_df.index.name = 'datetime'
            station_df.to_csv(f'{i}.csv')

                

filepath = str(sys.argv[1])

s100_to_csv(filepath)