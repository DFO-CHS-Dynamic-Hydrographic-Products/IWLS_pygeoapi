# Standard library imports
import os
import datetime
import json
import shutil
import logging
import json
import tempfile
import sys
import traceback

# Package imports
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from zipfile import ZipFile
from timeit import default_timer as timer
from pathlib import Path
from dataclasses import dataclass

# Local imports
from provider_iwls.api_connector.iwls_api_connector_waterlevels import IwlsApiConnectorWaterLevels
from provider_iwls.api_connector.iwls_api_connector_currents import IwlsApiConnectorCurrents
import provider_iwls.s100_processing.s104 as s104
import provider_iwls.s100_processing.s111 as s111

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

    def execute(self, data: dict):
        """
        Execute request to IWLS and create S111/S104 file.

        :param data: User Input, format defined in PROCESS_METADATA (json)
        :param folder_cleanup: Removes s100 process files if true (bool)
        :returns:  zip archive of S-100 files, MimeType: 'application/zip',
        """
        try:

            logging.info("Processsing request")
            t_start = timer()

            logging.info("Validating Inputs")
            bbox, layer = validate_inputs(data)

            logging.info("Sending Request to IWLS")
            # Send Request to IWLS API and return geojson
            # Pass query to IWLS API
            if layer == 'S104':
                # Establish connection to IWLS API
                api = IwlsApiConnectorWaterLevels()
            else:
                api = IwlsApiConnectorCurrents()

            result = api._get_timeseries_by_boundary(data['start_time'], data['end_time'], bbox)

            # Create a temp file to temporarily create and read in zip file
            with tempfile.TemporaryDirectory() as temp_folder_path:
              s100_folder_path = Path(temp_folder_path).joinpath('s100')
              os.mkdir(s100_folder)

              result_path = os.path.join(temp_folder_path, 'output.json')
              with open(result_path, 'w', encoding='utf-8') as output_file:
                  json.dump(result, output_file, ensure_ascii=False, indent=4)

              # Process s100 request to h5 file
              self.process_s100_request(result_path, s100_folder_path, layer)

              # Create Zip File containing S-104
              logging.info('Creating Archive and Returning Result')

              # Create response.json file for a status report for client
              success_dict_resp = format_http_response(200, "OK")
              with open(Path(s100_folder_path).joinpath('response.json'), 'w') as f:
                json.dump(success_dict_resp, f)

              # Make zip file
              zip_dest = os.path.join(temp_folder_path,'100')
              shutil.make_archive(zip_dest,'zip', s100_folder_path)

              # Read in zip file
              zip_file = os.path.join(temp_folder_path,'100.zip')
              with open(zip_file, 'rb') as f:
                  value = f.read()

              logging.info("Completed Process")
              t_end = round(timer() - t_start, 2)
              logging.info(f"Total time to process request {t_end}")

              # Return encoded zip file
              return 'application/zip', value

        except InputValidationError as e:

          # Log stored error from InputValidation data class
          logging.error(f"InputValidationError: {e.status_code}, {e.message}")

          # Return detailed error message to clients if input validation error
          error_dict_resp = format_http_response(e.status_code, e.message, success=False)

          return create_zip(error_dict_resp)

        except Exception as e:
          # Log error from stack
          logging.error(e, exc_info=True)

          # Return error message to clients if any other error
          error_dict_resp = format_http_response(500, "Internal Server Error", success=False)

          return create_zip(error_dict_resp)

    def process_s100_request(self, response_path: str, s100_folder_path: str, layer: str):
        # Create S-100 Files from Geojson return
        if layer == 'S104':
            logging.info('Creating S-104 Files')
            s104.S104GeneratorDCF8(
              response_path,s100_folder_path,'./templates/DCF8_009_104CA0024900N12400W_production.h5'
            ).create_s100_tiles_from_template('./templates/tiles_grid_level_2.json')

        else:
            logging.info('Creating S-111 Files')
            s111.S111GeneratorDCF8(
              response_path,s100_folder_path,'./templates/DCF8_111_111CA0024900N12400W_production.h5'
            ).create_s100_tiles_from_template('./templates/tiles_grid_level_2.json')


    def __repr__(self):
        return '<S100Processor> {}'.format(self.name)

@dataclass
class InputValidationError(BaseException):
    status_code: int
    message: str

def create_zip(error_dict_resp):
      response_filename = 'response.json'
      output_filename = 'response.zip'

      # Create temp file
      with tempfile.TemporaryDirectory() as folder_path:
        folder_path = Path(folder_path)
        output_path = folder_path.joinpath(output_filename)

        # Create response.json containing error message
        with open(folder_path.joinpath(response_filename), 'w') as f:
          json.dump(error_dict_resp, f)

        # Create zip file
        with ZipFile(output_path, 'w') as zipf:
          zipf.write(folder_path.joinpath(response_filename), arcname=response_filename)

        # Read in zip file
        with open(output_path, 'rb') as f:
            value = f.read()

        return 'application/zip', value

def format_http_response(status_code, message, success=True):
    # Foramt http response
    if success:
      return {"status": "success", "body": { 'code' : status_code, 'message': message}}
    else:
      return {"status": "error", "body": { 'code' : status_code, 'message': message}}

def parse_datetime_text(date_text, datetime_format="%Y-%m-%dT%H:%M:%SZ"):
    try:
        return datetime.datetime.strptime(date_text, datetime_format)
    except ValueError:
        raise InputValidationError(400, f"Incorrect data format, should be {datetime_format}")

def parse_bbox_text(bbox_text):
    # Test bounding box cases
    try:
      bbox = [float(x) for x in bbox_text.split(',')]
    except ValueError as value_error_:
      raise InputValidationError(400, "bad format") from value_error_
    if len(bbox) != 4:
      raise InputValidationError(400, "Invalid number of values in bounding box, \
      should be: bbox:<longitude>,<latitude>,<longitude>,<latitude>")
    if not (-90 <= bbox[1] <= 90) and (-90 <= bbox[3] <= 90):
      raise InputValidationError(400, "Bounding box latitudes must be between -90 and 90")
    if not (-180 <= bbox[0] <= 180) and (-180 <= bbox[2] <= 180):
      raise InputValidationError(400, "Bounding box longitudes must be between -180 and 180")
    return bbox

def validate_inputs(data):
    start_time_datetime = parse_datetime_text(data['start_time'])
    end_time_datetime = parse_datetime_text(data['end_time'])

    bbox = parse_bbox_text(data['bbox'])

    layer = data['layer']
    if layer != 'S104' and layer != 'S111':
        raise InputValidationError(400, 'Cannot process without a valid layer name (S104 or S111)')

    # Raise error if requesting more data than the time limit
    time_delta = (start_time_datetime - end_time_datetime).total_seconds() / 3600
    if time_delta > 96:
        raise InputValidationError(400, 'Difference between start time and end time greater than 4 days')
    return bbox, layer
