import pytest, os, h5py, io, requests
from pytest import fixture
from pathlib import Path
from zipfile import ZipFile

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
    # Parse test data attribute names from test data script
    attr_names_dict = test_data.s111_attr_names
    test_util.test_dcf8_attrs_exist(h5_file, product_name, attr_names_dict, product_attr_name)