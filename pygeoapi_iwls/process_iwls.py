
# Standard library imports
import uuid, os, io, datetime, json, shutil, logging

# Package imports
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from zipfile import ZipFile
from timeit import default_timer as timer

# Local imports
from provider_iwls.iwls_api_connector_waterlevels import IwlsApiConnectorWaterLevels
from provider_iwls.iwls_api_connector_currents import IwlsApiConnectorCurrents
import provider_iwls.s104 as s104
import provider_iwls.s111 as s111
from provider_iwls.iwls_api_connector_currents import IwlsApiConnectorCurrents
from provider_iwls.iwls_api_connector_waterlevels import IwlsApiConnectorWaterLevels

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 's100',
    'title': 'S100',
    'description': 'Generate archive of S100 files from IWLS Geojson response',
    'keywords': ['IWLS', 'Water Level', 'S104', 'S111'],
    'links': [{
        'type': 'text/html',
        'rel': 'canonical',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
            'layer': {
            'title': 'S100 Layer',
            'description': 'Layer to query, valid input: S104 or S111',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use?
            'keywords': ['S100']
        },
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
            'title': 'S100 Archive',
            'description': 'Zip archive of S100 files',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/zip'
            }
        }
    },
    'example': {
        'inputs': {
            'layer': 's104',
            'start_time': '2021-12-06T00:00:00Z',
            'end_time': '2021-12-06T23:00:00Z',
            'bbox': '-123.28,49.07,-123.01,49.35',
        }
    }
}

class S100Processor(BaseProcessor):
    """S-100 plugin process for pygeoAPI"""

    def __init__(self, processor_def):
        """
        Initialize S100 object.

        :param processor_def: provider definition
        """
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data: dict, folder_cleanup=True):
        """
        Execute request to IWLS and create S111/S104 file.

        :param data: User Input, format defined in PROCESS_METADATA
        :returns:  zip archive of S-100 files, MimeType: 'application/zip',
        """
        try:

            logging.info("Processsing request")

            t_start = timer()
            # Make folder to process request
            folder_name = str(uuid.uuid4())
            folder_path = os.path.join('./s100_process', folder_name)
            os.mkdir(folder_path)

            # Clean up request folders older than 24 hours to preserve space on the server
            if folder_cleanup:
                yesterday = (datetime.datetime.utcnow() - datetime.timedelta(days=1))
                for root,directories,files in os.walk('./s100_process'):
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

            layer = data['layer']
            if layer != 'S104' and layer != 'S111':
                raise ProcessorExecuteError('Cannot process without a valid layer name (S104 or S111)')

            # Raise error if requesting more data than the time limit
            start_time_datetime = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
            end_time_datetime = datetime.datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ")
            time_delta = (start_time_datetime - end_time_datetime).total_seconds() / 3600
            if time_delta > 96:
                raise ProcessorExecuteError('Difference between start time and end time greater than 4 days')

            bbox =  [float(x) for x in bbox_string.split(',')]

            t_end = timer()
            logging.info(t_end - t_start)
            logging.info("Sending Request to IWLS")
            t_start = timer()
            # Send Request to IWLS API and return geojson

            # Pass query to IWLS API
            if layer == 'S104':
                # Establish connection to IWLS API
                api = IwlsApiConnectorWaterLevels()
            else:
                api = IwlsApiConnectorCurrents()

            result = api._get_timeseries_by_boundary(start_time, end_time, bbox)

            # Write Json to Folder
            response_path = os.path.join(folder_path, 'output.json')
            with open(response_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=4)

            t_end = timer()
            logging.info(t_end - t_start)
            t_start = timer()

            # Create S-100 Files from Geojson return
            s100_folder = os.path.join(folder_path, 's100')
            os.mkdir(s100_folder)
            if layer == 'S104':
                logging.info('Creating S-104 Files')
                s104.S104GeneratorDCF8(response_path,s100_folder,'./templates/DCF8_009_104CA0024900N12400W_production.h5').create_s100_tiles_from_template('./templates/tiles_grid_level_2.json')

            else:
                logging.info('Creating S-111 Files')
                s111.S111GeneratorDCF8(response_path,s100_folder,'./templates/DCF8_111_111CA0024900N12400W_production.h5').create_s100_tiles_from_template('./templates/tiles_grid_level_2.json')

            t_end = timer()
            logging.info(t_end - t_start)
            logging.info('Creating Archive and Returning Result')
            t_start = timer()
            # Create Zip File containing S-104

            # Gen archive
            zip_folder = os.path.join(s100_folder)
            zip_file = os.path.join(folder_path,'100.zip')
            zip_dest = os.path.join(folder_path,'100')
            shutil.make_archive(zip_dest,'zip', zip_folder)
            with open(zip_file, 'rb') as f:
                value = f.read()

            outputs = {
                'id': 'zip',
                'value': value
                }


            t_end = timer()
            logging.info(t_end - t_start)

            logging.info("Completed Process")

            return 'application/zip', value

        except Exception as e:
            logging.info(f'Error: {e}\nExiting process. See log file for more details.')
            logging.error(e, exc_info=True)

    def __repr__(self):
        return '<S100Processor> {}'.format(self.name)
