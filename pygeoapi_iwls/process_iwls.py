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

        :param processor_def: Provider definition
        """
        super().__init__(processor_def, PROCESS_METADATA)

        # Request input formats
        self.datetime_format="%Y-%m-%dT%H:%M:%SZ"
        self.valid_layer_names = 'S111', 'S104'
        self.bbox_format = "\'bbox:<longitude>,<latitude>,<longitude>,<latitude>\'"
        self.datetime_limit = 96

        # Output status json file name
        self.response_filename = 'response.json'

        # Temporary file for zip output
        self.output_dir_name = 'output_response'


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
            bbox, layer = self.validate_inputs(data)

            logging.info("Sending Request to IWLS")
            result = self.send_api_request(layer, bbox, data['start_time'], data['end_time'])

            # Create a temp file to temporarily create and read in zip file
            with tempfile.TemporaryDirectory() as temp_folder_path:

              logging.info("Processing S100 Request")
              s100_folder_path = self.process_s100_request(temp_folder_path, layer, result)

              logging.info('Creating Archive and Returning Result')
              value = self.create_success_zip(temp_folder_path, s100_folder_path)

              logging.info("Completed Process")
              logging.info(f"Total time to process request {round(timer() - t_start, 2)}")

              # Return encoded zip file
              return 'application/zip', value

        except InputValidationError as e:
          # Log stored error from InputValidation data class
          logging.error(f"InputValidationError: {e.status_code}, {e.message}")

          # Return detailed error message to clients if input validation error
          error_dict_resp = self.format_http_response(e.status_code, e.message, success=False)

          # Format response into a zip containing json
          return self.create_error_zip(error_dict_resp)

        except Exception as e:
          # Log error from stack
          logging.error(e, exc_info=True)

          # Return error message to clients if any other error
          error_dict_resp = self.format_http_response(500, "Internal Server Error", success=False)

          # Format response into a zip containing json
          return self.create_error_zip(error_dict_resp)


    def __repr__(self):
        return '<S100Processor> {}'.format(self.name)

    def send_api_request(self, layer: str, bbox: list, start_time: str, end_time: str):
        '''
        Query pygeoapi database and make request given user validated inputs.

        :param layer: Layer name, i.e. S104/S111 (str)
        :param bbox: List containing bounding box coordinates (list)
        :param start_time: Start time for request (str)
        :param end_time: End time request (str)
        :returns: Api request result (dict)
        '''
        # Send Request to IWLS API
        if layer == 'S104':
            # Establish connection to IWLS API
            api = IwlsApiConnectorWaterLevels()
        else:
            api = IwlsApiConnectorCurrents()

        # Pass query to IWLS API and return geojson
        return api._get_timeseries_by_boundary(start_time, end_time, bbox)

    def process_s100_request(self, temp_folder_path: str, layer: str, result: dict):
        '''
        Process a s100 request through the s104/s111 generator classes.

        :param response_path: Path to json containing all results (str)
        :param temp_folder_path: Temp directory path (str)
        :param layer: Specified layer to create S*** file (i.e. S104 or S111)
        :param result: Returned results from api request (dict)
        :returns: S100 folder path that containing output h5 file (str)
        '''
        # Create s100 folder to temporarily house the h5 file
        s100_folder_path = Path(temp_folder_path).joinpath('s100')
        os.mkdir(s100_folder_path)

        # Create a json containing the output results from the api
        result_path = os.path.join(temp_folder_path, 'output.json')
        with open(result_path, 'w', encoding='utf-8') as result_file:
            json.dump(result, result_file, ensure_ascii=False, indent=4)

        # Create S-100 Files from Geojson return
        if layer == 'S104':
            logging.info('Creating S-104 Files')
            s104.S104GeneratorDCF8(
              result_path, s100_folder_path,'./templates/DCF8_009_104CA0024900N12400W_production.h5'
            ).create_s100_tiles_from_template('./templates/tiles_grid_level_2.json')

        else:
            logging.info('Creating S-111 Files')
            s111.S111GeneratorDCF8(
              result_path, s100_folder_path,'./templates/DCF8_111_111CA0024900N12400W_production.h5'
            ).create_s100_tiles_from_template('./templates/tiles_grid_level_2.json')

        return s100_folder_path

    def create_success_zip(self, temp_folder_path: str, s100_folder_path: str):
        '''Create an zip fille containing the response json and the h5 file.

        :param temp_folder_path: Temporary working directory to save files for this session (str)
        :s100_folder_path: Path to directory (inside of temp dir) containing h5 file (str)
        :returns: Parsed zip file result (zip)
        '''
        # Create response dictionary with a status report for client
        success_dict_resp = self.format_http_response(200, "OK")

        # Create a json file containing the response
        with open(Path(s100_folder_path).joinpath(self.response_filename), 'w') as f:
          json.dump(success_dict_resp, f)

        # Make zip file with output h5 file and json
        zip_dest = Path(temp_folder_path).joinpath(self.output_dir_name)
        shutil.make_archive(zip_dest, 'zip', s100_folder_path)

        # Read in zip file
        with open(zip_dest.with_suffix('.zip'), 'rb') as f:
            return f.read()

    def create_error_zip(self, error_dict_resp):
        '''
        Create the zip file containing the response json

        :param status_code: Error or success code (http status code) (str)
        :param message: Message detailing outcome/comments (str)
        :param success: Determine to use success or error protocol (bool)
        '''
        # Create temp folder
        with tempfile.TemporaryDirectory() as temp_folder_path:
          temp_folder_path = Path(temp_folder_path)
          output_path = temp_folder_path.joinpath(Path(self.output_dir_name).with_suffix('.zip'))

          # Create response.json containing error message
          with open(temp_folder_path.joinpath(self.response_filename), 'w') as f:
            json.dump(error_dict_resp, f)

          # Create zip file
          with ZipFile(output_path, 'w') as zipf:
            zipf.write(temp_folder_path.joinpath(self.response_filename), arcname=self.response_filename)

          # Read in zip file
          with open(output_path, 'rb') as f:
              value = f.read()

          return 'application/zip', value

    def format_http_response(self, status_code, message, success=True):
        '''
        Creates status dict that formats status code and message (eventually gets turned into json).

        :param status_code: Error or success code (http status code) (str)
        :param message: Message detailing outcome/comments (str)
        :param success: Determine to use success or error protocol (bool)
        '''
        if success:
          return {"status": "success", "body": { 'code' : status_code, 'message': message}}
        else:
          return {"status": "error", "body": { 'code' : status_code, 'message': message}}

    def parse_datetime_text(self, date_text):
        '''
        Parse datetime text from user input.

        :param date_text: User specified dates (str)
        '''
        try:
            # Parse datetime and throw specified error if unable to parse
            return datetime.datetime.strptime(date_text, self.datetime_format)
        except ValueError:
            raise InputValidationError(400, f"Incorrect datetime format, should be {self.datetime_format} but got {date_text}")

    def parse_bbox_text(self, bbox_text):
        '''
        Parses bounding box coordinates from string

        :param bbox_text: User inputted bounding box (str)
        :returns: List of valid bounding boxes
        '''
        try:
          # Parse bounding box from string to list
          bbox = [float(x) for x in bbox_text.split(',')]

        except ValueError as value_error_:
          raise InputValidationError(400, f"Unable to parse bounding box, format should be: {self.bbox_format}.") from value_error_

        # Ensure that bounding box length contains correct number of coordinate points
        if len(bbox) != 4:
          raise InputValidationError(400, f"Invalid number of bounding box values, should be 4 but found {str(len(bbox))}. Suggested format in a request is: {self.bbox_format}.")

        # Validate lat/lon values
        if not (-90 <= bbox[1] <= 90) and (-90 <= bbox[3] <= 90):
          raise InputValidationError(400, "Bounding box latitudes must be between -90 and 90.")
        if not (-180 <= bbox[0] <= 180) and (-180 <= bbox[2] <= 180):
          raise InputValidationError(400, "Bounding box longitudes must be between -180 and 180.")

        return bbox

    def validate_inputs(self, data: dict):
        """
        Validates user input

        :param data: User Input, format defined in PROCESS_METADATA (dict)
        :returns: list of valid bounding boxes and layer name
        """
        # Parse input start/end time
        start_time_datetime = self.parse_datetime_text(data['start_time'])
        end_time_datetime = self.parse_datetime_text(data['end_time'])

        # Parse input bounding box from coordinates
        bbox = self.parse_bbox_text(data['bbox'])

        # Extract layer type and ensure it is valid i.e. S104/S111
        layer = data['layer'].capitalize()
        if layer != self.valid_layer_names[0] and layer != self.valid_layer_names[1]:
            raise InputValidationError(400, 'Cannot process without a valid layer name (S104 or S111)')

        # Raise error if requesting more data than the time limit
        time_delta = (start_time_datetime - end_time_datetime).total_seconds() / 3600
        if time_delta > self.datetime_limit:
            raise InputValidationError(400, f'Difference between start time and end time cannot exceed {round(time_delta/24, 2)} days or {time_delta} hours.')

        return bbox, layer

@dataclass
class InputValidationError(BaseException):
    status_code: int
    message: str
