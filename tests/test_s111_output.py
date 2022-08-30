import pytest, os, h5py, io, requests
from pytest import fixture
from pathlib import Path
from zipfile import ZipFile
from datetime import datetime

import test_data
import test_util

h5_filename = '111CA0024900N12400W.h5'
product_attr_name = "surfaceCurrent"
product_name = "SurfaceCurrent"

@fixture(scope='session')
def h5_file():
    response = test_util.run_request("S111")

    if response.status_code == 200:
        print("Success")
        zip = ZipFile(io.BytesIO(response.content), 'r')
        zip.extractall("./")
        assert Path('./111CA0024900N12400W.h5').exists(), "H5 file not available"

        with h5py.File(h5_filename, 'r') as h5_file:
            yield h5_file
    else:
        print(f'Error processing request: {response.status_code}')

def test_horizontal_datum_ref(h5_file):
    assert h5_file.attrs['horizontalDatumReference'] == 'EPSG'

def test_issue_date(h5_file):
    date_today = datetime.today().strftime('%Y%m%d')
    assert h5_file.attrs['issueDate'] == date_today

def test_surface_curr_depth(h5_file):
    assert h5_file.attrs['surfaceCurrentDepth'] == 1.0

def test_metadata(h5_file):
    assert h5_file.attrs['metadata'] == "MD_111CA0024900N12400W.XML"

def test_geographic_id(h5_file):
    assert h5_file.attrs['geographicIdentifier'] == "CND S111 tile 111CA0024900N12400W"

def test_sbl(h5_file):
    assert round(h5_file.attrs['southBoundLatitude'],2) == 49.0

def test_wbl(h5_file):
    assert round(h5_file.attrs['westBoundLongitude'],2) == -124.00

def test_nbl(h5_file):
    assert round(h5_file.attrs['northBoundLatitude'], 2) == 50.00

def test_ebl(h5_file):
    assert round(h5_file.attrs['eastBoundLongitude'], 2) == -123.00

def test_product_spec(h5_file):
    assert h5_file.attrs["productSpecification"] == "INT.IHO.S-111.1.0"

def test_dcf8_attrs_exist(h5_file):
    attr_names_dict = test_data.s111_attr_names

    test_util.test_dcf8_attrs_exist(h5_file, product_name, attr_names_dict, product_attr_name, s104=False)

def test_min_max_dataset_values(h5_file):
    max_speed = h5_file[product_name].attrs['maxDatasetCurrentSpeed']
    min_speed = h5_file[product_name].attrs['minDatasetCurrentSpeed']

    test_util.run_test_min_max(h5_file, max_speed, min_speed, product_name)

def test_positioning_group(h5_file):
    test_util.test_positioning_group(h5_file, product_name)

def test_datetime_first_last_record(h5_file):
    test_util.test_datetime_first_last_record(h5_file, product_name)

def test_group_f_data(h5_file):
    group_f_data = test_data.s111_group_f
    test_util.test_group_f_dataset(h5_file, group_f_data)

def test_dataset_types(h5_file):
    test_util.test_dataset_types(h5_file, product_name)

# bug fix
# def test_axis_names(h5_file):
#     assert 'axisNames' in h5_file[product_name], "axisNames does not exist in {product_name} group"
