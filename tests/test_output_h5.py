import pytest, os, h5py, io, requests
from pytest import fixture
from pathlib import Path
from zipfile import ZipFile

import s104_test_data as test_data

h5_filename = '104CA0024900N12400W.h5'

def run_s104_request():

    headers = {
    "accept": "application/json",
    "Content-Type": "application/json"
    }

    data = '{"inputs":{"layer":"S104","bbox":"-123.28,49.07,-123.01,49.35","end_time":"2021-12-06T23:00:00Z","start_time":"2021-12-06T00:00:00Z"}}'

    return requests.post("http://localhost:5000/processes/s100/execution", headers=headers, data=data)

@fixture(scope='session')
def h5_file():
    response = run_s104_request()

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

# TODO refactor
def check_attrs(h5_file, path_attributes, attr_names, path):
    for attr_name in attr_names:
        assert attr_name in path_attributes, f"{attr_name} does not exist in {path}"
        if attr_name == 'timeIntervalIndex':
            time_record_index = h5_file[path].attrs['timeRecordInterval']
            if time_record_index == 1:
                assert "timeRecordInterval" in path_attributes, "timeRecordInterval attribute must exist if timeRecordInterval attribute is 1"
            elif time_record_index == 0:
                assert "waterLevelTime" in path_attributes, "waterLevelTime attribute must exist if timeRecordInterval attribute is 0"

# TODO refactor
def test_dcf8_attrs_exist(h5_file):
    attr_names_dict = test_data.attr_names

    for path, attr_names in attr_names_dict.items():
        if path == 'WaterLevel/WaterLevel.01/Group_':
            num_stations = int(h5_file['WaterLevel/WaterLevel.01'].attrs['numberOfStations'])
            for i in range(1, num_stations+1):
                group_path = f'{path}{str(i).zfill(3)}'
                path_attributes = list(h5_file[group_path].attrs)
                check_attrs(h5_file, path_attributes, attr_names, group_path)
        else:
            path_attributes = list(h5_file[path].attrs)
            check_attrs(h5_file, path_attributes, attr_names, path)
