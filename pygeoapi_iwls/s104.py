# Standard library imports
import json
# Packages imports
import provider_iwls.s100
import h5py
import numpy as np
import pandas as pd
from scipy.stats import linregress

class S104GeneratorDCF8(provider_iwls.s100.S100GeneratorDCF8):
    """
    class for generating S-111 Data Coding Format 8 (Stationwise arrays)
    files. Inherit from S100GeneratorDCF8
    """
    def __init__(self, json_path, folder_path,template_path):
        super().__init__(json_path, folder_path,template_path)
        # overide file type from base class
        self.file_type = '104'

    def _get_flags(self,x):
        """
        Transform slope value to trend flag:
        "STEADY" : 0, "DECREASING" : 1, "INCREASING" : 2, "UNKNOWN" : 3
        param x: slope value calculated with a 1 hour rolling window (float)
        return: trend flag (int)
        """
        trend_treshold = 0.2
        if np.isnan(x):
            return 3
        elif x > trend_treshold:
            return 2
        elif x < (trend_treshold * -1):
            return 1
        else:
            return 0

    def _gen_S104_trends(self,df_wl):
        """
        Generate water level trend flags from water level values
        :param df_wl: pandas dataframe containing water level values (pandas.core.DataFrame)
        :return: pandas Dataframe containing trend Flags for respective water level values (pandas.core.DataFrame)
        """
        if not df_wl.empty:
            df_wl_trend = df_wl
            interval = (df_wl_trend.index[1] - df_wl_trend.index[0]).total_seconds()
            timestamps_per_hour = int(3600 // interval)
            #Interpolate gaps for trend calculation if NaNs are in dataset
            if df_wl_trend.isna().values.any():
                # Create mask for NaN values
                nan_mask = df_wl_trend.isna()
                # Interpolate gaps
                df_wl_trend = df_wl_trend.interpolate(method ='linear', limit_direction ='forward')
                # Calulate slope
                slope_values = df_wl_trend.rolling(timestamps_per_hour, center = True).apply(lambda x: linregress(range(timestamps_per_hour),x)[0])
                # Restore NaN
                slope_values[nan_mask] = np.nan
            else:
                slope_values = df_wl_trend.rolling(timestamps_per_hour, center = True).apply(lambda x: linregress(range(timestamps_per_hour),x)[0])
            # Get Trend Flags
            df_trend = slope_values.apply(np.vectorize(self._get_flags))

            return df_trend
        else:
            return pd.DataFrame()

    def _format_data_arrays(self,data):
        """
        product specific pre formating to convert API response to valid
        data arrays.
        :param data: raw water level data received from IWLS API call (json)
        :return: processed water level data (dict)

        """
        # Convert JSON data to Pandas tables
        df_wlp = self._gen_data_table(data,'wlp')
        df_wlo = self._gen_data_table(data,'wlo')
        df_wlf = self._gen_data_table(data,'wlf')
        df_spine = self._gen_data_table(data,'spine')

         # Create Trend Flags tables
        df_wlf_trend = self._gen_S104_trends(df_wlf)
        df_wlo_trend = self._gen_S104_trends(df_wlo)
        df_wlp_trend = self._gen_S104_trends(df_wlp)
        df_spine_trend = self._gen_S104_trends(df_spine)

        trend = {'wlp':df_wlp_trend,'wlo':df_wlo_trend,'wlf':df_wlf_trend,'spine':df_spine_trend}


        # Calculate min and max values for file
        dataset_max = max([df_wlp.max().max(),df_wlf.max().max(),df_wlo.max().max(),df_spine.max().max()])
        dataset_min = min([df_wlp.min().min(),df_wlf.min().min(),df_wlo.min().min(),df_spine.min().min()])

        # Replace NaN with fill value (-9999)
        df_wlp = df_wlp.fillna(-9999)
        df_wlo = df_wlo.fillna(-9999)
        df_wlf = df_wlf.fillna(-9999)
        df_spine = df_spine.fillna(-9999)

        wl = {'wlp':df_wlp,'wlo':df_wlo,'wlf':df_wlf, 'spine':df_spine}


        # Create Positions Dict
        df_wlp_position = self._gen_positions(df_wlp)
        df_wlo_position = self._gen_positions(df_wlo)
        df_wlf_position = self._gen_positions(df_wlf)
        df_spine_position = self._gen_positions(df_spine)
        position = {'wlp':df_wlp_position,'wlo':df_wlo_position,'wlf':df_wlf_position,'spine':df_spine_position}

         # List available data sets
        dataset_types = []
        if not df_wlo.empty:
            dataset_types.append('wlo')
        if not df_wlf.empty:
            dataset_types.append('wlf')
        if not df_wlp.empty:
            dataset_types.append('wlp')
        if not df_spine.empty:
            dataset_types.append('spine')

        data_arrays = {'wl':wl,'trend':trend,'position':position,'max':dataset_max,'min':dataset_min,'dataset_types':dataset_types}

        return  data_arrays


    def _update_product_specific_general_metadata(self,h5_file):
        """
        Update product specific (S-104) general metadata.
        :param h5_file: h5 file to update (hdf5)
        """
        # No Changes from Template
        pass

    def _update_feature_metadata(self,h5_file,data):
        """
        Update feature level metadata (WaterLevel)

        :param h5_file: h5 file to update
        :param data: formatted data arrays generated from _format_data_arrays (dict)
        """
        # commonPointRule = no changes from template
        # dataCodingFormat = no changes from template
        # dimension = no changes from template
        # horizontalPositionUncertainty = no changes from template (for now, -1.0 unassessed)
        # maxDatasetHeight
        h5_file['WaterLevel'].attrs.modify('maxDatasetHeight',data['max'])
        # methodWaterLevelProduct = no changes from template
        # minDatasetHeight
        h5_file['WaterLevel'].attrs.modify('minDatasetHeight',data['min'])
        # numInstance
        h5_file['WaterLevel'].attrs.modify('numInstances',len(data['dataset_types']))
        # pickPriorityType = no changes from template
        # timeUncertainty = no changes from template (for now, -1.0 unassessed)
        # verticalUncertainty = no changes from template (for now, -1.0 unassessed)

    def _create_groups(self,h5_file,data):
        """
        Create data groups for each station
        :param h5_file: h5 file to update (hdf5)
        :param data: formatted data arrays generated from _format_data_arrays
        """
        no_of_instances = len(data['dataset_types'])
        instance_group_counter = 1

        # Create all potential WaterLevel Instances
        for x in range(no_of_instances):
            ### Create Instance Group ###
            data_type = data['dataset_types'][x]
            instance_wl = data['wl'][data_type]
            instance_trend = data['trend'][data_type]
            instance_position = data['position'][data_type]
            instance_group_path = 'WaterLevel/WaterLevel.0' + str(instance_group_counter)
            instance_group_counter += 1
            instance_group = h5_file.create_group(instance_group_path)

            ### Create Instance Metadata ###
            # N/A 1 to 4 bounding box same as feature
            # 5 Number times of records
            num_times = len(instance_wl)
            instance_group.attrs.create('numberOfTimes', num_times)
            # 6 Time Interval
            time_record_interval = int((instance_wl.index[1] - instance_wl.index[0]).total_seconds())
            instance_group.attrs.create('timeRecordInterval', time_record_interval)
            # 7 Start Time
            start_date_time = instance_wl.index[0].strftime("%Y%m%dT%H%M%SZ").encode('UTF-8')
            instance_group.attrs.create('dateTimeOfFirstRecord', start_date_time)
            # 8 End times
            end_time = instance_wl.index[-1].strftime("%Y%m%dT%H%M%SZ").encode('UTF-8')
            instance_group.attrs.create('dateTimeOfLastRecord', end_time)
            # 9 num groups
            num_groups = instance_wl.shape[1]
            instance_group.attrs.create('numGRP', num_groups)
            # 11 typeOfWaterLevelData
            dt_wl_type = h5py.enum_dtype({
                'Observation': 1,
                'Astronomical prediction': 2,
                'Analysis or hybrid method': 3,
                'Hydrodynamic model hindcast': 4,
                'Hydrodynamic model forecast':5,
                'Observed minus predicted': 6,
                'Observed minus hindcast': 7,
                'Observed minus forecast': 9,
                'Forecast minus predicted': 10},basetype='i')
            if data_type == 'wlo':
                wl_type = 1
            elif data_type =='wlp':
                wl_type = 2
            else:
                wl_type = 5
            instance_group.attrs.create('typeOfWaterLevelData', wl_type, dtype=dt_wl_type)
            # 12 no of stations
            instance_group.attrs.create('numberOfStations', num_groups)

            ### Create Group for each stations ###
            group_counter = 1
            for i in range(num_groups):
                # Create Group
                group_path = instance_group_path + '/Group_' + str(group_counter).zfill(3)
                group = h5_file.create_group(group_path)
                group_counter += 1
                ### Create Group Metadata ##
                group.attrs.create('endDateTime', end_time)
                group.attrs.create('numberOfTimes', instance_wl.shape[0])
                group.attrs.create('startDateTime', start_date_time)
                stn_id = instance_wl.columns[i].split("$")[0]
                group.attrs.create('stationIdentification', stn_id)
                stn_name = instance_wl.columns[i].split("$")[1]
                group.attrs.create('stationName', stn_name)
                group.attrs.create('timeIntervalIndex', 1)
                group.attrs.create('timeRecordInterval', time_record_interval)

                 ### Populate Group ###
                values = list(zip(instance_wl.iloc[:, i].to_list(), instance_trend.iloc[:, i].to_list()))
                values_type = np.dtype([('waterLevelHeight',np.float64),('waterLevelTrend',np.int8)])
                values_array = group.create_dataset('values',data=values,dtype=values_type)

            ### Create Positioning Group ###
            positioning_path = instance_group_path + '/Positioning'
            positioning = h5_file.create_group(positioning_path)
            lat = instance_position['lat']
            lon = instance_position['lon']
            lat_lon = list(zip(lat, lon))
            geometry_values_type = np.dtype([('latitude',np.float64), ('longitude',np.float64)])
            geometry_values = positioning.create_dataset('geometryValues',data=lat_lon,dtype=geometry_values_type)
