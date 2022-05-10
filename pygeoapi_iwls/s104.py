import logging
import json
from zipfile import ZipFile
import uuid
import os
import io
import shutil
import h5py
import dateutil.parser
from scipy.stats import linregress

# Packages imports
import pandas as pd
import numpy as np
import datetime
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from provider_iwls.iwls import IwlsApiConnector

LOGGER = logging.getLogger(__name__)

# Tests
from timeit import default_timer as timer

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 's104',
    'title': 'S104',
    'description': 'Generate archive of S104 files from IWLS Geojson response',
    'keywords': ['IWLS', 'Water Level', 'S104'],
    'links': [{
        'type': 'text/html',
        'rel': 'canonical',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'start_time': {
            'title': 'Start Time',
            'description': 'Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z)',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use?
            'keywords': ['Datetime']
        },
        'end_time': {
            'title': 'End Time',
            'description': 'End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z)',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use?
            'keywords': ['Datetime']
        },
        'bbox': {
            'title': 'Bounding Box',
            'description': 'bounding box [minx,miny,maxx,maxy], Latitude and Longitude (WGS84)',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use?
            'keywords': ['Latitude', 'Longitude']
        },
    },
    'outputs': {
        'zip': {
            'title': 'S104 Archive',
            'description': 'Zip archive of S104 files',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/zip'
            }
        }
    },
    'example': {
        'inputs': {
            'start_time': '2021-12-06T00:00:00Z',
            'end_time': '2021-12-06T23:00:00Z',
            'bbox': '-123.28,49.07,-123.01,49.35',
        }
    }
}
        

class S104Processor(BaseProcessor):
    """S-104 plugin process for pygeoAPI"""

    def __init__(self, processor_def):
        """
        Initialize object
        :param processor_def: provider definition
        :returns: pygeoapi.process.S104Processor
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data, folder_cleanup=True):
        """
        Execution Method
        :param data: User Input, format defined in PROCESS_METADATA
        :PROCESS_METADATA: process description
        :returns: MimeType: 'application/zip', zip archive of S-104 files
        """

        print("Processsing request")
        t_start = timer()
        # Make folder to process request
        folder_name = str(uuid.uuid4())
        folder_path = os.path.join('./s104_process', folder_name)
        os.mkdir(folder_path)

        # Clean up request folders older than 24 hours to preserve space on the server
        if folder_cleanup:
            yesterday = (datetime.datetime.utcnow() - datetime.timedelta(days=1))
            for root,directories,files in os.walk('./s104_process'):
                for d in directories:
                    timestamp = datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(root,d)))
                    if timestamp < yesterday:
                        shutil.rmtree(os.path.join(root,d))

        # Get Request parameters from Inputs
        start_time = data['start_time']
        if start_time is None:
            raise ProcessorExecuteError('Cannot process without a valid Start Time')

        end_time = data['end_time']
        if end_time is None:
            raise ProcessorExecuteError('Cannot process without a valid End Time')

        bbox_string = data['bbox']
        if bbox_string is None:
            raise ProcessorExecuteError('Cannot process without a Bounding Box') 

        # Raise error if requesting more data than the time limit
        start_time_datetime = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
        end_time_datetime = datetime.datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ")
        time_delta = (start_time_datetime - end_time_datetime).total_seconds() / 3600
        if time_delta > 96:
            raise ProcessorExecuteError('Difference between start time and end time greater than 4 days')   

        bbox =  [float(x) for x in bbox_string.split(',')]

        t_end = timer()
        print(t_end - t_start)
        print("Sending Request to IWLS")
        t_start = timer()
        # Send Request to IWLS API and return geojson    
         # Establish connection to IWLS API
        api = IwlsApiConnector()
        # Pass query to IWLS API
        result = api.get_timeseries_by_boundary(start_time,end_time,bbox)

        # Write Json to Folder
        response_path = os.path.join(folder_path, 'output.json')
        with open(response_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=4)

        t_end = timer()
        print(t_end - t_start)
        print('Creating S-104 Files')
        t_start = timer()
        # Create S-104 Files from Geojson return
        s104_folder = os.path.join(folder_path, 's104')
        os.mkdir(s104_folder)
        S104Generator(response_path,s104_folder,'./templates/DCF8_009_104CA0024900N12400W_production.h5').create_s104_tiles_from_template('./templates/tiles_grid_level_2.json')

        t_end = timer()
        print(t_end - t_start)
        print('Creating Archive and Returning Result')
        t_start = timer()
        # Create Zip File containing S-104

        # Gen archive
        zip_folder = os.path.join(s104_folder)
        zip_file = os.path.join(folder_path,'104.zip')
        zip_dest = os.path.join(folder_path,'104')
        shutil.make_archive(zip_dest,'zip', zip_folder)
        with open(zip_file, 'rb') as f:
            value = f.read()

        outputs = {
            'id': 'zip',
            'value': value
            }

    
        t_end = timer()
        print(t_end - t_start)

        return 'application/zip', value

    def __repr__(self):
        return '<S104Processor> {}'.format(self.name)


class S104Generator():
    """ S-104 file Generator"""
    def __init__(self, json_path, folder_path,template_path):
        """
        S104Generator init method
        :param json_path: path to geojson to process
        :param folder_path: path to processing folder
        :param template_path: path to S-104 h5 file production template
        """
        self.folder_path = folder_path
        self.json_path = json_path
        self.template_path = template_path

    def _get_flags(self,x):
        """
        Transform slope value to trend flag:
        "STEADY" : 0, "DECREASING" : 1, "INCREASING" : 2, "UNKNOWN" : 3
        param x: slope value calculated with a 1 hour rolling window
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

    def _create_s104_waterlevel_groups(self,h5_file,dataset_types,wl,trend,position):
        """
        Create and append  Water Level group to S-104 file
        :param h5_file: S-104 file to modify
        :param dataset_types: Data set types (wlf, wlo, wlp) avaible
        :param wl: Dictionnary of water level Dataframes
        :param trend: Dictionnary of trend Dataframes
        :param position: Dictionnary of position Dataframes
        """
        
        no_of_instances = len(dataset_types)
        instance_group_counter = 1

        # Create all popential WaterLevel Instances
        for x in range(no_of_instances):
            ### Create Instance Group ###
            data_type = dataset_types[x]
            instance_wl = wl[data_type]
            instance_trend = trend[data_type]
            instance_position = position[data_type]
            instance_group_path = 'WaterLevel/WaterLevel.0' + str(instance_group_counter)
            instance_group_counter += 1
            instance_group = h5_file.create_group(instance_group_path)

            ### Create Instance Metadata ###
            # N/A 1 to 4 bounding box same as feature
            # 5 Number times of records
            num_times = len([])
            instance_group.attrs.create('numberOfTimes', num_times)
            # 6 Time Interval
            time_record_interval = int((instance_wl.index[1] - instance_wl.index[0]).total_seconds())
            instance_group.attrs.create('timeRecordInterval', time_record_interval)
            # 7 Start Time
            start_date_time = instance_wl.index[-0].strftime("%Y%m%dT%H%M%SZ").encode('UTF-8')
            instance_group.attrs.create('dateTimeOfFirstRecord', start_date_time)
            # 8 End times
            end_time = instance_wl.index[0].strftime("%Y%m%dT%H%M%SZ").encode('UTF-8')
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


    def _gen_data_table(self,s104_data,code):
        """
        Generate dataframe of water level information needed to produce S-104 files
        :param s104_data: water level data
        :param code: data type code
        : return df: dataframe of water level information needed to produce S-104 files
        """
        data_list = []
        for i in  s104_data:
            if i['properties'][code]:
                # [stn_id, stn_name, lat, long]
                name = i['properties']['metadata']['code'] + '$' + i['properties']['metadata']['officialName'] + '$' + str(i['properties']['metadata']['latitude']) + '$' + str(i['properties']['metadata']['longitude'])
                stn_data = pd.Series(i['properties'][code], name=name)
                stn_data.index = pd.to_datetime(stn_data.index)
                data_list.append(stn_data)
        if data_list:
            df = pd.concat(data_list, axis=1)
        else:
            df = pd.DataFrame()

        return df

    def _gen_positions(self,df_wl):
        """
        Generate position for stations
        :param df_wl: pandas data frame of water level information
        "return position: Dictionnary of latitudes and longitudes
        """
        lat = [float(i.split("$")[2]) for i in df_wl.columns]
        lon = [float(i.split("$")[3]) for i in df_wl.columns]
        position = {'lat':lat, 'lon':lon}
        return position 

    def _create_s104(self, s104_data,filename,bbox):
        """
        Create single S-104  file from production template
        :param s104_data: Data to include in file
        :param filename: name of S-104 file
        :param bbox: file limit
        """

        # Create file from template in working folder
        s104_path = os.path.join(self.folder_path, filename)
        shutil.copy(self.template_path,s104_path)

        # Convert Json data to Pandas tables
        df_wlp = self._gen_data_table(s104_data,'wlp')
        df_wlo = self._gen_data_table(s104_data,'wlo')
        df_wlf = self._gen_data_table(s104_data,'wlf')
        df_spine = self._gen_data_table(s104_data,'spine')

        # Create Trend Flags tables
        df_wlf_trend = self._gen_S104_trends(df_wlf)
        df_wlo_trend = self._gen_S104_trends(df_wlo)
        df_wlp_trend = self._gen_S104_trends(df_wlp)
        df_spine_trend = self._gen_S104_trends(df_spine)

        trend = {'wlp':df_wlp_trend,'wlo':df_wlo_trend,'wlf':df_wlf_trend,'spine':df_spine_trend}

        # Create Positions Dict 
        df_wlp_position = self._gen_positions(df_wlp)
        df_wlo_position = self._gen_positions(df_wlo)
        df_wlf_position = self._gen_positions(df_wlf)
        df_spine_position = self._gen_positions(df_spine)

        position = {'wlp':df_wlp_position,'wlo':df_wlo_position,'wlf':df_wlf_position,'spine':df_spine_position}
        
        # Calculate min and max values for file
        dataset_max = max([df_wlp.max().max(),df_wlf.max().max(),df_wlo.max().max(),df_spine.max().max()])
        dataset_min = min([df_wlp.min().min(),df_wlf.min().min(),df_wlo.min().min(),df_spine.min().min()])

        # Replace NaN with fill value (-9999)
        df_wlp = df_wlp.fillna(-9999)
        df_wlo = df_wlo.fillna(-9999)
        df_wlf = df_wlf.fillna(-9999)
        df_spine = df_spine.fillna(-9999)

        wl = {'wlp':df_wlp,'wlo':df_wlo,'wlf':df_wlf, 'spine':df_spine}

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

        # Open and update file
        with h5py.File(s104_path, 'r+') as h5_file:
            ### Update General Metadata (File Level) ###
            # eastBoundLongitude
            east_lon= bbox[2]
            h5_file.attrs.modify('eastBoundLongitude',east_lon)
            # epoch = no changes from template
            # geographicIdentifier
            geo_identifier = 'CND S104 tile ' + filename[:-3]
            h5_file.attrs.modify('geographicIdentifier',geo_identifier)
            # horizontalCRS = no changes from template
            # horizontalDatumReference = no changes from template
            # issueDate
            date_issue = datetime.datetime.now().utcnow().strftime("%Y%m%d").encode('UTF-8')
            h5_file.attrs.modify('issueDate',date_issue)
            # issueTime
            time_issue = datetime.datetime.now().utcnow().strftime("%H%M%SZ").encode('UTF-8')
            h5_file.attrs.modify('issueTime',time_issue)
            # metadata
            md_name = 'MD_' + filename[:-3] + '.XML'
            h5_file.attrs.modify('metadata',md_name)
            # northBoundLatitude
            north_lat = bbox[0]
            h5_file.attrs.modify('eastBoundLongitude',north_lat)
            # product Specification = no changes from template
            # southBoundLatitude
            south_lat = bbox[1]
            h5_file.attrs.modify('eastBoundLongitude',south_lat)
            # verticalCS = no changes from template
            # verticalCoordinateBase = no changes from template
            # verticalDatum = no changes from template
            # verticalDatumReference = no changes from template
            # waterLevelTrendThreshold = hard coded at 0.2 for now, might be dynamic in the future
            # westBoundLongitude
            west_lon = bbox[3]
            h5_file.attrs.modify('eastBoundLongitude',west_lon)

            ### Update Feature Metadata (WaterLevel) ###
            waterlevel = h5_file['WaterLevel']
            # commonPointRule = no changes from template
            # dataCodingFormat = no changes from template
            # dimension = no changes from template
            # horizontalPositionUncertainty = no changes from template (for now, -1.0 unassessed)
            # maxDatasetHeight
            waterlevel.attrs.modify('maxDatasetHeight',dataset_max)
            # methodWaterLevelProduct = no changes from template
            # minDatasetHeight
            waterlevel.attrs.modify('minDatasetHeight',dataset_min)
            # numInstance
            waterlevel.attrs.modify('numInstance',len(dataset_types))
            # pickPriorityType = no changes from template
            # timeUncertainty = no changes from template (for now, -1.0 unassessed)
            # verticalUncertainty = no changes from template (for now, -1.0 unassessed)

            ### Create and populate Water Level Groups ###
            self._create_s104_waterlevel_groups(h5_file, dataset_types,wl,trend,position)



    def create_s104_tiles_from_template(self,grid_path):
        """
        Create S-104 tiles from production template
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

        # Loop over every cells and generate S-104 if data exists in request
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

            # Generate S104 file if data exist within cell      
            if len(cell_data_list) != 0:
                name = i['properties']['cell']
                filename = '104' + name[0:2] + '00' + name[2] + name[4:] + '.h5'
                bbox = [cell_max_lat,cell_min_lat,cell_max_lon,cell_min_lon]
                self._create_s104(cell_data_list,filename,bbox)

# curl -X POST "http://localhost:5000/processes/s104/execution" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"inputs": {\"bbox"\: \"-123.28,49.07,-123.01,49.35"\,\"end_time"\: \"2021-12-06T23:00:00Z"\,\"start_time"\: \"2021-12-06T00:00:00Z"\}}"
# curl -X POST "http://localhost:5000/processes/s104/execution" -H "accept: application/json" -H "Content-Type: application/json" -d {"inputs": {"bbox":"-123.28,49.07,-123.01,49.35","end_time":"2021-12-06T23:00:00Z","start_time":"2021-12-06T00:00:00Z"}} > s104.zip
# curl -X POST "http://localhost:5000/processes/s104/execution" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"inputs\": {\"bbox\":\"-123.28,49.07,-123.01,49.35\",\"end_time\":\"2021-12-06T23:00:00Z\",\"start_time\":\"2021-12-06T00:00:00Z\"}}" > s104.zip