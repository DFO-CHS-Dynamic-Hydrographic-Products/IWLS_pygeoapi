import pytest, os, h5py, io, requests
from pytest import fixture
from pathlib import Path
from zipfile import ZipFile

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

def test_wbl(h5_file):
    assert h5_file.attrs['westBoundLongitude'] == -124
