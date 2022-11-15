import pytest, os, h5py, io, requests
from pytest import fixture
from pathlib import Path
from zipfile import ZipFile
from datetime import datetime
import numpy as np
import os

import test_data
import test_util

test_dir = Path('.')
h5_filename = '104CA0024900N12400W.h5'
product_attr_name = "waterLevel"
product_name = "WaterLevel"

@fixture(scope='session')
def h5_file():
    response = test_util.run_request("S104")

    if response.status_code == 200:
        print("Success")
        zip = ZipFile(io.BytesIO(response.content), 'r')
        zip.extractall(test_dir)
        assert test_dir.joinpath(h5_filename).exists(), "H5 file not available"

        with h5py.File(h5_filename, 'r') as h5_file:
            yield h5_file

        test_util.clean_up(test_dir)
    else:
        print(f'Error processing request: {response.status_code}')

def test_epoch(h5_file):
    assert h5_file.attrs['epoch'] == '2005.0'

def test_horizontal_datum_ref(h5_file):
    assert h5_file.attrs['horizontalDatumReference'] == 'EPSG'

def test_issue_date(h5_file):
    date_today = datetime.today().strftime('%Y%m%d')
    assert h5_file.attrs['issueDate'] == date_today

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

def test_meth_wl_product(h5_file):
    assert h5_file[product_name].attrs['methodWaterLevelProduct'] == 'Timeseries generated from official published water levels, predictions and forecast for Canadian waters requested using the IWLS API'

def test_pick_priority_type(h5_file):
    h5_file[product_name].attrs['pickPriorityType'] == '1,2,5'

def test_dcf8_attrs_exist(h5_file):
    # Parse test data attribute names from test data script
    attr_names_dict = test_data.s104_attr_names
    test_util.test_dcf8_attrs_exist(h5_file, product_name, attr_names_dict, product_attr_name, s104=True)

def test_common_point_rule(h5_file):
    h5_file['WaterLevel'].attrs['commonPointRule'] == 4

def test_datetime_first_last_record(h5_file):
    test_util.test_datetime_first_last_record(h5_file, product_name)

def test_min_max_dataset_values(h5_file):

    # Get min/max from h5_file to test
    max_height = h5_file[product_name].attrs['maxDatasetHeight']
    min_height = h5_file[product_name].attrs['minDatasetHeight']

    test_util.run_test_min_max(h5_file, max_height, min_height, product_name)

def test_num_stations(h5_file):
    test_util.test_num_stations(h5_file, product_name)

def test_positioning_group(h5_file):
    test_util.test_positioning_group(h5_file, product_name)

def test_feature_attribute(h5_file):
    test_util.test_feature_attribute(h5_file, product_name)

def test_group_f_data(h5_file):
    group_f_data = test_data.s104_group_f
    test_util.test_group_f_dataset(h5_file, group_f_data)

def test_dataset_types(h5_file):
    test_util.test_dataset_types(h5_file, product_name)

def test_axis_names(h5_file):
    assert 'axisNames' in h5_file[product_name], "axisNames does not exist in {product_name} group"
