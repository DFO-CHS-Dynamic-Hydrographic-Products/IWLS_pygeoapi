# Standard library imports
import json
import os
import shutil
import datetime

# Packages imports
import pandas as pd
import numpy as np
import h5py

# Import utility script
import s100_util

class S100GeneratorDCF8():
    """
    abstract base class for generating S-100 Data Coding Format 8 (Stationwise arrays)
    files. Used as parent by S104GeneratorDCF8 and S111GeneratorDCF8
    """

    def __init__(self, json_path, folder_path, template_path):
        """
        S100GeneratorDCF8 init method
        :param json_path: path to geojson to process
        :param folder_path: path to processing folder
        :param template_path: path to S-100 h5 file production template
        """
        self.folder_path = folder_path
        self.json_path = json_path
        self.template_path = template_path
        self.dataset_names = None
        # Overide with correct layer in child class
        self.file_type = '100'
        # Raise error if incorect layer type is passed
        if self.file_type == '111' or self.file_type == '104':
            raise ValueError('Invalid file_type,must be 104 or 111')


    def create_s100_tiles_from_template(self,grid_path):
        """
        Create S-100 tiles from production template
        :param grid_path: path to geojson tile grid
        """
        # Load Json and convert to python dict
        with open(self.json_path) as data_file:
            data = json.loads(data_file.read())
        # convert to list of stations dicts
        data = data['features']

        # Convert geojson grid to list of grid cells
        with open(grid_path) as grid_file:
            grid_list = json.load(grid_file)['features']

        # Loop over every cells and generate S-100 if data exists in request
        for i in grid_list:
            polygon = i['geometry']['coordinates'][0]
            cell_max_lat = max([x[1] for x in polygon])
            cell_min_lat = min([x[1] for x in polygon])
            cell_max_lon = max([x[0] for x in polygon])
            cell_min_lon = min([x[0] for x in polygon])
            cell_data_list = []
            # Search for stations within cell boundaries

            for item in data:
                if (cell_min_lat < item['properties']['metadata']['latitude'] < cell_max_lat) and (cell_min_lon < item['properties']['metadata']['longitude'] < cell_max_lon):
                   cell_data_list.append(item)

            # Generate S100 file if data exist within cell
            if len(cell_data_list) != 0:
                name = i['properties']['cell']
                filename = self.file_type + name[0:2] + '00' + name[2] + name[4:] + '.h5'
                bbox = [cell_max_lat,cell_min_lat,cell_max_lon,cell_min_lon]
                self._create_s100_dcf8(cell_data_list,filename,bbox)

    def _create_s100_dcf8(self, s100_data, filename, bbox):
        """
        Create single S-100  file from production template
        :param s100_data: Data to include in file
        :param filename: name of S-100 file
        :param bbox: file limit
        """
        # Create file from template in working folder
        s100_path = os.path.join(self.folder_path, filename)
        shutil.copy(self.template_path,s100_path)
        #format JSON data
        data_arrays = self._format_data_arrays(s100_data)
        # Open and update file
        with h5py.File(s100_path, 'r+') as h5_file:
            ### Update General Metadata (File Level) ###
            self._update_general_metadata(h5_file,filename,bbox)
            ### Update Feature Metadata (WaterLevel) ###
            self._update_feature_metadata(h5_file,data_arrays)
            ### Create and populate group arrays ###
            self._create_groups(h5_file,data_arrays)

    def _format_data_arrays(self):
        """
        product specific pre formating to convert API response to valid
        data arrays. Must be implemented by child class
        """
        raise NotImplementedError('Must override _format_data_arrays')

    def _update_general_metadata(self,h5_file,filename,bbox):
        """
        Update general metadata (file level)
        """

        # epoch = no changes from template
        # horizontalCRS = no changes from template
        # horizontalDatumReference = no changes from template
        # product Specification = no changes from template

        # geographicIdentifier
        geo_identifier = 'CND S' +  self.file_type + ' tile ' + filename[:-3]
        s100_util.create_modify_attribute(h5_file, 'geographicIdentifier', geo_identifier)

        # issueDate
        date_issue = datetime.datetime.now().utcnow().strftime("%Y%m%d").encode('UTF-8')
        s100_util.create_modify_attribute(h5_file, 'issueDate', date_issue)

        # issueTime
        time_issue = datetime.datetime.now().utcnow().strftime("%H%M%SZ").encode('UTF-8')
        s100_util.create_modify_attribute(h5_file, 'issueTime', time_issue)

        # metadata
        md_name = 'MD_' + filename[:-3] + '.XML'
        s100_util.create_modify_attribute(h5_file, 'metadata', md_name)

        # eastBoundLongitude
        s100_util.create_modify_attribute(h5_file, 'eastBoundLongitude', bbox[2])

        # northBoundLatitude
        s100_util.create_modify_attribute(h5_file, 'northBoundLatitude', bbox[0])

        # southBoundLatitude
        s100_util.create_modify_attribute(h5_file, 'southBoundLatitude', bbox[1])

        # westBoundLongitude
        s100_util.create_modify_attribute(h5_file, 'westBoundLongitude', bbox[3])

        self._update_product_specific_general_metadata(h5_file)

    def _gen_data_table(self,s100_data,code):
        """
        Generate dataframe of water level information needed to produce S-100 files
        :param s100_data: iwls json timeseries
        :param code: data type code
        :return df: dataframe of water level information needed to produce S-100 files
        """
        data_list = []
        for i in  s100_data:
            if i['properties'][code]:
                # [stn_id, stn_name, lat, long]
                name = i['properties']['metadata']['code'] + '$' + i['properties']['metadata']['officialName'] + '$' + str(i['properties']['metadata']['latitude']) + '$' + str(i['properties']['metadata']['longitude'])
                stn_data = pd.Series(i['properties'][code], name=name)
                stn_data.index = pd.to_datetime(stn_data.index)
                data_list.append(stn_data)
        if data_list:
            return pd.concat(data_list, axis=1)
        else:
            return pd.DataFrame()

        return df

    def _gen_positions(self,df):
        """
        Generate position for stations
        :param df: pandas data frame of water level or current information information
        :return position: Dictionnary of latitudes and longitudes
        """
        lat = [float(i.split("$")[2]) for i in df.columns]
        lon = [float(i.split("$")[3]) for i in df.columns]
        position = {'lat':lat, 'lon':lon}
        return position

    def _update_product_specific_general_metadata(self,h5_file):
        """
        Update product specific general metadata.
        Must be implemented by child class
        """
        raise NotImplementedError('Must override _update_product_specific_general_metadata')

    def _update_feature_metadata(self,h5_file,data):
        """
        Update feature level metadata
        Must be implemented by child class
        """
        raise NotImplementedError('Must override _update_feature_metadata')

    def _create_groups(self,h5_file,data):
        """
        Update feature level metadata
        Must be implemented by child class
        """
        raise NotImplementedError('Must override _create_groups')

    def _create_attributes(self, h5_file, group, datasets):
        """
        Create data attributes for each station

        """
        assert self.dataset_names is not None, \
            "Must invoke S104 or S111 class to get the dataset names"

        ### Create Instance Metadata ###

        # numberOfTimes
        s100_util.create_modify_attribute(group, 'numberOfTimes', len(datasets[0]))
        time_record_interval = int((datasets[0].index[1] - datasets[0].index[0]).total_seconds())

        # timeRecordInterval
        s100_util.create_modify_attribute(group, 'timeRecordInterval', time_record_interval)

        # dateTimeofFirstRecord
        start_datetime = datasets[0].index[0].strftime("%Y%m%dT%H%M%SZ").encode('UTF-8')
        s100_util.create_modify_attribute(group, 'dateTimeOfFirstRecord', start_datetime)

        # dateTimeOfLastRecord
        end_datetime = datasets[0].index[-1].strftime("%Y%m%dT%H%M%SZ").encode('UTF-8')
        s100_util.create_modify_attribute(group, 'dateTimeOfLastRecord', end_datetime)

        # numGroups
        num_groups = datasets[0].shape[1]
        s100_util.create_modify_attribute(group, 'numGRP', datasets[0].shape[1])

        for i in range(1, num_groups):
            # Create Group
            group_path = '{product_id}/{product_id}.01/Group_{group_no}'.format(
                product_id=self.product_id, group_no= str(i).zfill(3)
            )

            group = h5_file.create_group(group_path)

            ### Create Group Metadata ##
            group.attrs.create('endDateTime', end_datetime)
            group.attrs.create('numberOfTimes', datasets[0].shape[0])
            group.attrs.create('startDateTime', start_datetime)
            stn_id = datasets[0].columns[i].split("$")[0]
            group.attrs.create('stationIdentification', stn_id)
            stn_name = datasets[0].columns[i].split("$")[1]
            group.attrs.create('stationName', stn_name)
            group.attrs.create('timeIntervalIndex', 1)
            group.attrs.create('timeRecordInterval', time_record_interval)
            values = list(zip(datasets[0].iloc[:, i].to_list(), datasets[1].iloc[:, i].to_list()))

            values_type = np.dtype([(self.dataset_names[0],np.float64),(self.dataset_names[1] ,np.float64)])
            group.create_dataset('values',data=values,dtype=values_type)

        def create_positioning_path(self, h5_file, instance_group_path, lat, lon):

            positioning_path = instance_group_path + '/Positioning'
            positioning = h5_file.create_group(positioning_path)
            geometry_values_type = np.dtype([('latitude',np.float64), ('longitude',np.float64)])
            positioning.create_dataset('geometryValues',data=list(zip(lat, lon)),dtype=geometry_values_type)
