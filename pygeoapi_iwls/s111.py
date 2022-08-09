# Standard library imports
import json
# Packages imports
import provider_iwls.s100
import numpy as np

class S111GeneratorDCF8(provider_iwls.s100.S100GeneratorDCF8):
    """
    class for generating S-111 Data Coding Format 8 (Stationwise arrays)
    files. Inherit from S100GeneratorDCF8
    """
    def __init__(self, json_path, folder_path,template_path):
        super().__init__(json_path, folder_path,template_path)
        # overide file type from base class
        self.file_type = '111'


    def _format_data_arrays(self,data):
        """
        product specific pre formating to convert API response to valid
        data arrays. Must be implemented by child class
        :param data: raw current level data received from IWLS API call (json)
        :return: processed current level data (dict)
        """
        # Convert JSON data to Pandas tables
        wcs = self._gen_data_table(data,'wcs')
        wcd = self._gen_data_table(data,'wcd')

        # Find dataset min and max values
        dataset_max = wcs.max().max()
        dataset_min = wcs.min().min()

        # Replace NaN values
        wcs = wcs.fillna(-1)
        wcd = wcd.fillna(-1)
        
        position = self._gen_positions(wcs)
        data_arrays = {'wcs':wcs,'wcd':wcd,'position':position,'max':dataset_max,'min':dataset_min}

        return  data_arrays




    def _update_product_specific_general_metadata(self,h5_file):
        """
        Update product specific (S-111) general metadata.
        :param h5_file: h5 file to update (hdf5)
        """
        # Surface Current Depth, No change from template
        # ToDo: surfaceCurrentDepth is proposed for S111 1.1.1,
        # create here for now and move to template when 1.1.1 is finalized
        h5_file.attrs.create('surfaceCurrentDepth',1.0)

    def _update_feature_metadata(self,h5_file,data):
        """
        Update feature level metadata (SurfaceCurrent)

        :param h5_file: h5 file to update (hdf5)
        :param data: formatted data arrays generated from _format_data_arrays (dict)
        """
        # commonPointRule, No change from template
        # dataCodingFormat, No change from template
        # dimension, No change from template
        # horizontalPositionUncertainty, currently unassessed
        # maxDatasetCurrentSpeed
        h5_file['SurfaceCurrent'].attrs.modify('maxDatasetCurrentSpeed',data['max'])
        # minDatasetCurrentSpeed
        h5_file['SurfaceCurrent'].attrs.modify('minDatasetCurrentSpeed',data['min'])
        # typeOfCurrentData, No change from template
        # verticalPositionUncertainty, currently unassessed
        # Number of Feature Instances
        # ToDo: numInstances is proposed for S111 1.1.1,
        # create here for now and move to template when 1.1.1 is finalized
        h5_file['SurfaceCurrent'].attrs.create('numInstances',data['wcs'].shape[1])
        

    def _create_groups(self,h5_file,data):
        """
        Create data groups for each station

        :param h5_file: h5 file to update (hdf5)
        :param data: formatted data arrays generated from _format_data_arrays (dict)
        """
        ### Update Instance Instance Group Metadata ###
        # N/A 1 to 4 bounding box same as feature
        # 5 Number times of records
        num_times = len(data['wcs'])
        h5_file['SurfaceCurrent/SurfaceCurrent.01'].attrs.modify('numberOfTimes', num_times)
        # 6 Time Interval
        time_record_interval = np.int32((data['wcs'].index[1] - data['wcs'].index[0]).total_seconds())
        h5_file['SurfaceCurrent/SurfaceCurrent.01'].attrs.modify('timeRecordInterval', time_record_interval)
        # 7 Start Time
        start_date_time = data['wcs'].index[0].strftime("%Y%m%dT%H%M%SZ").encode('UTF-8')
        h5_file['SurfaceCurrent/SurfaceCurrent.01'].attrs.modify('dateTimeOfFirstRecord', start_date_time)
        # 8 End times
        end_time = data['wcs'].index[-1].strftime("%Y%m%dT%H%M%SZ").encode('UTF-8')
        h5_file['SurfaceCurrent/SurfaceCurrent.01'].attrs.modify('dateTimeOfLastRecord', end_time)
        # 9 num groups
        num_groups = data['wcs'].shape[1]
        h5_file['SurfaceCurrent/SurfaceCurrent.01'].attrs.modify('numGRP', num_groups)

        ### Create and populate data groups ###
        group_counter = 1
        for i in range(num_groups):
            # Create Group
            group_path = 'SurfaceCurrent/SurfaceCurrent.01' + '/Group_' + str(group_counter).zfill(3)
            group = h5_file.create_group(group_path)
            group_counter += 1
            ### Create Group Metadata ##
            group.attrs.create('endDateTime', end_time)
            group.attrs.create('numberOfTimes', data['wcs'].shape[0])
            group.attrs.create('startDateTime', start_date_time)
            stn_id = data['wcs'].columns[i].split("$")[0]
            group.attrs.create('stationIdentification', stn_id)
            stn_name = data['wcs'].columns[i].split("$")[1]
            group.attrs.create('stationName', stn_name)
            group.attrs.create('timeIntervalIndex', 1)
            group.attrs.create('timeRecordInterval', time_record_interval)
            ### Populate Group ###
            values = list(zip(data['wcs'].iloc[:, i].to_list(), data['wcd'].iloc[:, i].to_list()))
            values_type = np.dtype([('surfaceCurrentSpeed',np.float64),('surfaceCurrentDirection',np.float64)])
            values_array = group.create_dataset('values',data=values,dtype=values_type)
        ### Create Positioning Group ###
        positioning_path = 'SurfaceCurrent/SurfaceCurrent.01' + '/Positioning'
        positioning = h5_file.create_group(positioning_path)
        lat = data['position']['lat']
        lon = data['position']['lon']
        lat_lon = list(zip(lat, lon))
        geometry_values_type = np.dtype([('latitude',np.float64), ('longitude',np.float64)])
        geometry_values = positioning.create_dataset('geometryValues',data=lat_lon,dtype=geometry_values_type)

