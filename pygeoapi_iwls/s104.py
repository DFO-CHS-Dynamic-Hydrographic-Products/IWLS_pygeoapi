# Packages imports
import numpy as np
import pandas as pd
import h5py
from scipy.stats import linregress

# Import local files
from provider_iwls.s100 import S100GeneratorDCF8

class S104GeneratorDCF8(S100GeneratorDCF8):
    """
    class for generating S-111 Data Coding Format 8 (Stationwise arrays)
    files. Inherit from S100GeneratorDCF8
    """

    def __init__(
            self,
            json_path: str,
            folder_path: str,
            template_path: str
    ):
        # Call s100 base class with preconfigured S104 data
        super().__init__(json_path=json_path,
                         folder_path=folder_path,
                         template_path=template_path,
                         dataset_names= ('waterLevelHeight', 'waterLevelTrend'),
                         dataset_types= (np.float64, np.int8),
                         product_id= 'WaterLevel',
                         file_type= '104')


    def _get_flags(
            self,
            x: float,
            trend_treshold = 0.2
    ):
        """
        Transform slope value to trend flag:
        "STEADY" : 0, "DECREASING" : 1, "INCREASING" : 2, "UNKNOWN" : 3
        param x: slope value calculated with a 1 hour rolling window
        return: trend flag (int)
        """
        if np.isnan(x):
            return 3
        elif x > trend_treshold:
            return 2
        elif x < (trend_treshold * -1):
            return 1
        else:
            return 0

    def _gen_S104_trends(
            self,
            df_wl: pd.core.frame.DataFrame
    ):
        """
        Generate water level trend flags from water level values
        :param df_wl: pandas dataframe containing water level values
        :return: pandas Dataframe containing trend Flags for respective water level values
        """
        if not df_wl.empty:
            df_wl_trend = df_wl
            interval = (df_wl_trend.index[1] - df_wl_trend.index[0]).total_seconds()
            timestamps_per_hour = int(3600 // interval)
            slope_values = df_wl_trend.rolling(timestamps_per_hour, center = True).apply(lambda x: linregress(range(0,timestamps_per_hour),x)[0])
            df_trend = slope_values.apply(np.vectorize(self._get_flags))
            return df_trend
        else:
            return pd.DataFrame()

    def _format_data_arrays(
            self,
            data: list
    ):
        """
        product specific pre formating to convert API response to valid
        data arrays.
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

    def _update_product_specific_general_metadata(
            self,
            h5_file: h5py._hl.files.File
    ):
        """
        Update product specific (S-104) general metadata.

        :param h5_file: h5 file to update (h5py._hl.files.File)
        """
        # No Changes from Template
        pass

    def _update_feature_metadata(
            self,
            h5_file: h5py._hl.files.File,
            data: dict,
            metadata_attrs = None
    ):
        """
        Update feature level metadata (WaterLevel)
        :param h5_file: h5 file to update (h5py._hl.files.File)
        :param data: formatted data arrays generated from _format_data_arrays (dict)
        """
        # methodWaterLevelProduct = no changes from template
        # commonPointRule = no changes from template
        # dataCodingFormat = no changes from template
        # dimension = no changes from template
        # horizontalPositionUncertainty = no changes from template (for now, -1.0 unassessed)
        # maxDatasetHeight
        # pickPriorityType = no changes from template
        # timeUncertainty = no changes from template (for now, -1.0 unassessed)
        # verticalUncertainty = no changes from template (for now, -1.0 unassessed)

        metadata_attrs = {'minDatasetHeight': data['min'], 'maxDatasetHeight': data['max'], 'numInstances': len(data['dataset_types'])}
        super()._update_feature_metadata(h5_file, data, metadata_attrs)

    def _create_groups(
            self,
            h5_file: h5py._hl.files.File,
            data: dict
    ):
        """
        Create data groups for each station
        :param h5_file: h5 file to update (h5py._hl.files.File)
        :param data: formatted data arrays generated from _format_data_arrays (dict)
        """
        no_of_instances = len(data['dataset_types'])

        # Create all potential WaterLevel Instances
        for i in range(no_of_instances):

            ### Create Instance Group ###
            data_type = data['dataset_types'][i]
            instance_position = data['position'][data_type]

            # Create group to assign attribute metadata and datasets for each station
            instance_group_path = f'{self.product_id}/{self.product_id}.0{i+1}'

            # Ensure that the group exists
            assert not h5_file[self.product_id].__contains__(instance_group_path), f"Group: {instance_group_path} exists already, cannot recreate a group that already exists"

            # Configure root group to attach datasets for each station
            instance_wl_group = h5_file.create_group(instance_group_path)

            # configure typeOfWaterLevelData enum
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

            # typeOfWaterLevelData
            instance_wl_group.attrs.create('typeOfWaterLevelData', wl_type, dtype=dt_wl_type)
            # numberOfStations
            instance_wl_group.attrs.create('numberOfStations', data['wl'][data_type].shape[1]
)
            # Parse datasets into a tuple
            datasets = (data['wl'][data_type],  data['trend'][data_type])

            ### Create atttributes
            self._create_attributes(h5_file, instance_wl_group, datasets, group_counter=i+1)

            ### Create Positioning Group ###
            self._create_positioning_group(
                h5_file, instance_group_path, instance_position['lat'], instance_position['lon']
            )
