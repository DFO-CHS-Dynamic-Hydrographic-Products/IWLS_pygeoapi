# Standard library imports
import uuid
import os
import io
import datetime
import json
import shutil
import logging
import json

# Package imports
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from zipfile import ZipFile
from timeit import default_timer as timer

# Local imports
from provider_iwls.iwls_api_connector_waterlevels import IwlsApiConnectorWaterLevels
from provider_iwls.iwls_api_connector_currents import IwlsApiConnectorCurrents
import provider_iwls.s104 as s104
import provider_iwls.s111 as s111

#Process metadata and description
with open('./templates/process_metadata.json', 'r', encoding='utf-8') as f:
  PROCESS_METADATA = json.load(f)

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

        :param data: User Input, format defined in PROCESS_METADATA (json)
        :param folder_cleanup: Removes s100 process files if true (bool)
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
