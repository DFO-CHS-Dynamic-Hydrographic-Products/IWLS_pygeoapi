import pytest, os, h5py, io, requests
from pytest import fixture
from pathlib import Path
from zipfile import ZipFile

import test_data
from test_util import run_request

h5_filename = '104CA0024900N12400W.h5'

@fixture(scope='session')
def h5_file():
    response = run_request("S104")

    if response.status_code == 200:
        print("Success")
        zip = ZipFile(io.BytesIO(response.content), 'r')
        zip.extractall("./")
        assert Path('./104CA0024900N12400W.h5').exists(), "H5 file not available"

        with h5py.File(h5_filename, 'r') as h5_file:
            yield h5_file
    else:
        print(f'Error processing request: {response.status_code}')

def test_metadata(h5_file):
    assert h5_file.attrs['metadata'] == "MD_104CA0024900N12400W.XML"

def test_geographic_id(h5_file):
    assert h5_file.attrs['geographicIdentifier'] == "CND S104 tile 104CA0024900N12400W"

def test_sbl(h5_file):
    assert round(h5_file.attrs['southBoundLatitude'],2) == 49.0

def test_wbl(h5_file):
    assert round(h5_file.attrs['westBoundLongitude'],2) == -124.00

def test_nbl(h5_file):
    assert round(h5_file.attrs['northBoundLatitude'], 2) == 50.00

def test_ebl(h5_file):
    assert round(h5_file.attrs['eastBoundLongitude'], 2) == -123.00

def test_wl_trend_threshold(h5_file):
    assert h5_file.attrs["waterLevelTrendThreshold"] == 0.2

def test_product_spec(h5_file):
    assert h5_file.attrs["productSpecification"] == "INT.IHO.S-104.0.0"

def check_attrs(h5_file, path_attributes, attr_names, path):
    # Iterate through attribute names

    for attr_name in attr_names:
        # Ensure that the attribute exists in the h5 file
        assert attr_name in path_attributes, f"{attr_name} does not exist in {path}"

        # Check a specific case where dcf8 timeIntervalIndex is 0 or 1
        if attr_name == 'timeIntervalIndex':
            time_record_interval = h5_file[path].attrs['timeRecordInterval']
            check_time_record_interval_attr(time_record_interval, path_attributes)

def check_time_record_interval_attr(time_record_interval, path_attributes):
    # If timeRecordIndex attr is 1, timeRecordInterval attr should be present in h5 file
    if time_record_interval == 1:
        assert "timeRecordInterval" in path_attributes, \
        "timeRecordInterval attribute must exist if timeRecordInterval attribute is 1"
    # If timeRecordIndex attr is 0, waterLevelTime attr should be present in h5 file
    elif time_record_interval == 0:
        assert "waterLevelTime" in path_attributes, \
        "waterLevelTime attribute must exist if timeRecordInterval attribute is 0"
    else:
        print(f"Error in timeRecordInterval attribute, value must be 0 or 1 but is {time_record_interval}")

def test_dcf8_attrs_exist(h5_file):
    # Parse test data attribute names from test data script
    attr_names_dict = test_data.s104_attr_names

    for path, attr_names in attr_names_dict.items():
        # Append 001, 002 ... nnn to Group_ prefix depending on Number of Stations
        if path == 'WaterLevel/WaterLevel.01/Group_':
            num_stations = int(h5_file['WaterLevel/WaterLevel.01'].attrs['numberOfStations'])

            # Iterate through number of stations
            for station_no in range(1, num_stations+1):
                # Create group path with station number e.g. 'WaterLevel/WaterLevel.01/Group_001
                group_path = f'{path}{str(station_no).zfill(3)}'

                # Get the attribute names in the group path (from the h5_file)
                path_attributes = list(h5_file[group_path].attrs)

                # Ensure that the attributes are present in the h5 file
                check_attrs(h5_file, path_attributes, attr_names, group_path)
        else:
            # Get the names of the attributes in group (from the h5_file)
            path_attributes = list(h5_file[path].attrs)

            # Ensure that the attributes are present in the h5 file
            check_attrs(h5_file, path_attributes, attr_names, path)
