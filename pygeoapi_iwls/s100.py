# Standard library imports
import json
import os
import shutil
import datetime
import logging

# Packages imports
from pathlib import Path
import pandas as pd
import numpy as np
import h5py

# Import utility script
import provider_iwls.s100_util as s100_util

class S100GeneratorDCF8():
    """
    abstract base class for generating S-100 Data Coding Format 8 (Stationwise arrays)
    files. Used as parent by S104GeneratorDCF8 and S111GeneratorDCF8
    """

    def __init__(self,
                 json_path: str,
                 folder_path: str,
                 template_path: str,
                 dataset_names: tuple,
                 dataset_types: tuple,
                 product_id: str,
                 file_type: str
    ):
        """

        S100GeneratorDCF8 init method
        :param json_path: path to geojson to process
        :param folder_path: path to processing folder
        :param template_path: path to S-100 h5 file production template
        """
        self.folder_path = Path(folder_path)
        self.json_path = Path(json_path)
        self.template_path = Path(template_path)
        self.dataset_names = dataset_names
        self.dataset_types = dataset_types
        self.product_id = product_id
        self.file_type = file_type


    def create_s100_tiles_from_template(self,
                                        grid_path: str
    ):
        """
        Create S-100 tiles from production template
        :param grid_path: path to geojson tile grid
        """

        assert self.json_path.exists(), "Json path does not exist: {json_path}".format(json_path=self.json_path)

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

    def _create_s100_dcf8(self,
                          s100_data,
                          filename: str,
                          bbox: list
    ):
        """
        Create single S-100  file from production template
        :param s100_data Data to include in file
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

    def _format_data_arrays(
            self,
            data: list
    ):
        """
        product specific pre formating to convert API response to valid
        data arrays. Must be implemented by child class
        """
        raise NotImplementedError('Must override _format_data_arrays')

    def _update_general_metadata(self,
                                 h5_file: h5py._hl.files.File,
                                 filename: str,
                                 bbox: list
    ):
        """
        Update general metadata (file level)
        :param h5_file: h5 file to update
        :param filename: h5 file name (string)
        :param bbox: bounding box [minx,miny,maxx,maxy]
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

    def _gen_data_table(self,
                        s100_data: list,
                        code: str
    ):
        """
        Generate dataframe of water level information needed to produce S-100 files
        :param list s100_data: iwls json timeseries
        :param str code: data type code
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

        return pd.DataFrame()

    def _gen_positions(
            self,
            df: pd.core.frame.DataFrame
    ):
        """
        Generate position for stations
        :param df: pandas data frame of water level or current information information
        :return position: Dictionnary of latitudes and longitudes
        """
        lat = [float(i.split("$")[2]) for i in df.columns]
        lon = [float(i.split("$")[3]) for i in df.columns]

        position = {'lat':lat, 'lon':lon}

        return position

    def _update_product_specific_general_metadata(
            self,
            h5_file: h5py._hl.files.File
    ):
        """
        Update product specific general metadata.
        Must be implemented by child class
        """
        raise NotImplementedError('Must override _update_product_specific_general_metadata')

    def _update_feature_metadata(
            self,
            h5_file: h5py._hl.files.File,
            data: dict,
            attr_data: dict
    ):
        """
        Update feature level metadata
        Must be implemented by child class
        """
        # Create pointer to group that holds attributes
        group = h5_file[self.product_id]

        # Iterate through dict to update attribute items
        for key, value in attr_data.items():
            s100_util.create_modify_attribute(group, key, value)

    def _create_groups(self,
                       h5_file: h5py._hl.files.File,
                       data: dict
    ):
        """
        Update feature level metadata
        Must be implemented by child class
        """
        raise NotImplementedError('Must override _create_groups')

    def _create_attributes(self,
                           h5_file: h5py._hl.files.File,
                           group: h5py._hl.group.Group,
                           datasets: tuple,
                           group_counter=1

    ):
        """
        Create data attributes for each station
        :param h5py._hl.group.Group group: Root group to assign values
        :param pd.core.frame.DataFrame dataset1: Water level or surface current dataset
        :param pd.core.frame.DataFrame dataset2: Water level trend or surface current direction dataset
        :param int group_counter: Group count for each station

        """

        dataset1, dataset2 = datasets

        assert self.dataset_names is not None, \
            "Must invoke S104 or S111 class to get the dataset names"

        ### Create Instance Metadata ###

        # numberOfTimes
        num_times = len(dataset1)
        s100_util.create_modify_attribute(group, 'numberOfTimes', num_times)

        # timeRecordInterval
        time_record_interval = int((dataset1.index[1] - dataset1.index[0]).total_seconds())
        s100_util.create_modify_attribute(group, 'timeRecordInterval', time_record_interval)
        # dateTimeofFirstRecord
        start_datetime = dataset1.index[0].strftime("%Y%m%dT%H%M%SZ").encode('UTF-8')

        s100_util.create_modify_attribute(group, 'dateTimeOfFirstRecord', start_datetime)
        # dateTimeOfLastRecord
        end_datetime = dataset1.index[-1].strftime("%Y%m%dT%H%M%SZ").encode('UTF-8')
        s100_util.create_modify_attribute(group, 'dateTimeOfLastRecord', end_datetime)

        # numGroups
        num_groups = dataset1.shape[1]
        s100_util.create_modify_attribute(group, 'numGRP', num_groups)

        # Instansiate dataclass to store Attribute Data
        attr_data = s100_util.AttributeData(num_groups, start_datetime, end_datetime, time_record_interval, dataset1.shape[0])

        # Populate metadata for each group
        self._populate_group_metadata(h5_file, datasets, group_counter, attr_data)

    def _create_positioning_group(self,
                                 h5_file: h5py._hl.files.File,
                                 instance_group_path: str,
                                 lat: float,
                                 lon: float
    ):
        '''
        Creates the positioning group and associated geometry values dataset of lat/lon values

        :param h5py._hl.files.File h5_file: The output h5 file
        :param str instance_group_path: Path to root group for positioning path
        :param float lat: Lon dataset values
        :param float lon: Lat dataset values
        '''

        # Create path to positioning group
        positioning_path = instance_group_path + '/Positioning'

        # Create positioning group
        positioning = h5_file.create_group(positioning_path)

        # Create the type for geometry values dataset
        geometry_values_type = np.dtype([('latitude',np.float64), ('longitude',np.float64)])

        # Create geometry values dataset with lat/lon values
        positioning.create_dataset(
            'geometryValues',data=list(zip(lat, lon)),dtype=geometry_values_type
        )

    def _populate_group_metadata(self,
                                 h5_file,
                                 datasets,
                                 group_counter,
                                 attr_data
        ):
        '''
        Creates the dataset groups and attaches associated attributes and datasets

        :param h5py._hl.files.File h5_file: The output h5 file
        :param tuple datasets: Tuple containing the two datasets
        :param int group_counter: Group counter for each station
        :param object attr_data: Attribute class that stores the metadata
        '''
        dataset1, dataset2 = datasets

        for i in range(attr_data.num_groups):
            # Create Group
            group_path = f'{self.product_id}/{self.product_id}.0{group_counter}/Group_{str(i+1).zfill(3)}'

            group = h5_file.create_group(group_path)

            ### Create Group Metadata ##

            # endDateTime
            group.attrs.create('endDateTime', attr_data.end_datetime)

            # numberOfTimes
            group.attrs.create('numberOfTimes', attr_data.num_times)

            # startDateTime
            group.attrs.create('startDateTime', attr_data.start_datetime)

            # stationIdentification
            stn_id = dataset1.columns[i].split("$")[0]
            group.attrs.create('stationIdentification', stn_id)

            # stationName
            stn_name = dataset1.columns[i].split("$")[1]
            group.attrs.create('stationName', stn_name)

            # timeIntervalIndex
            group.attrs.create('timeIntervalIndex', 1)

            # timeRecordInterval
            group.attrs.create('timeRecordInterval', attr_data.time_record_interval)

            # Create dataset containing waterlevel or surface current data

            values = list(zip(dataset1.iloc[:, i].to_list(), dataset2.iloc[:, i].to_list()))

            values_type = np.dtype(
                [(self.dataset_names[0], self.dataset_types[0]),(self.dataset_names[1] , self.dataset_types[1])]
            )

            group.create_dataset('values',data=values,dtype=values_type)
