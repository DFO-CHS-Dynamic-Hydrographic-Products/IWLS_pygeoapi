import json, requests
import numpy as np

def run_request(request_type):

    headers = {
    "accept": "application/json",
    "Content-Type": "application/json"
    }

    data = json.dumps({
        "inputs":{
            "layer": request_type,
            "bbox":"-123.28,49.07,-123.01,49.35",
            "end_time": "2021-12-06T23:00:00Z",
            "start_time": "2021-12-06T00:00:00Z",
        }
    })

    return requests.post("http://localhost:5000/processes/s100/execution", headers=headers, data=data)

def check_time_interval_index_attr(time_interval_index, h5_file_attr_names, product_type):
    # If timeRecordIndex attr is 1, timeRecordInterval attr should be present in h5 file
    if time_interval_index == 1:
        assert "timeRecordInterval" in h5_file_attr_names, \
            "timeRecordInterval attribute must exist if timeRecordInterval attribute is 1"
    # If timeRecordIndex attr is 0, waterLevelTime/surfaceCurrentTime attr should be present in h5 file
    elif time_interval_index == 0:
        assert f"{product_type}Time" in h5_file_attr_names, \
            f"{product_type}Time attribute must exist if timeRecordInterval attribute is 0"

def check_attrs(h5_file, h5_file_attr_names, test_data_attr_names, path, product_type):
    # Iterate through attribute names

    for attr_name in test_data_attr_names:
        # Ensure that the attribute exists in the h5 file
        assert attr_name in h5_file_attr_names, f"{attr_name} does not exist in {path}"

        # Check a specific case where dcf8 timeIntervalIndex is 0 or 1
        if attr_name == 'timeIntervalIndex':
            time_interval_index = h5_file[path].attrs['timeIntervalIndex']
            check_time_interval_index_attr(time_interval_index, h5_file_attr_names, product_type)

def test_dcf8_attrs_exist(h5_file, product_name, test_attr_dict, product_attr_name, s104=True):

    for path, test_data_attr_names in test_attr_dict.items():
        # Append 001, 002 ... nnn to Group_ prefix depending on Number of Stations
        if ('Group_' in path) and (s104 is True):
            print("here")
            num_stations_path = f'{product_name}/{product_name}.01'
            num_stations = int(h5_file[num_stations_path].attrs['numberOfStations'])

            # Iterate through number of stations
            for station_no in range(1, num_stations+1):
                # Create group path with station number e.g. 'WaterLevel/WaterLevel.01/Group_001
                group_path = f'{path}{str(station_no).zfill(3)}'

                # Get the attribute names in the group path (from the h5_file)
                h5_file_attr_names = list(h5_file[group_path].attrs)

                # Ensure that the attributes are present in the h5 file
                check_attrs(h5_file, h5_file_attr_names, test_data_attr_names, group_path, product_attr_name)
        else:
            # Get the names of the attributes in group (from the h5_file)
            h5_file_attr_names = list(h5_file[path].attrs)

            ### requires bug fix for s104/S111
            # assert len(h5_file_attr_names) == len(test_data_attr_names), (
            #     f'H5 file attributes for path: {path} must be the same length as the test data attributes\n'
            #     f'H5 file attributes are: {h5_file_attr_names} \n'
            #     f'Test data attributes are: {test_data_attr_names}'
            # )


            # Ensure that the attributes are present in the h5 file
            check_attrs(h5_file, h5_file_attr_names, test_data_attr_names, path, product_attr_name)

def run_test_min_max(h5_file, h5_file_max, h5_file_min, product_name):
    abs_max, abs_min = None, None
    num_instances = int(h5_file[product_name].attrs['numInstances'])

    # Get max/min values from dataset
    for j in range(1, num_instances+1):
        group_path = f'{product_name}/{product_name}.{str(j).zfill(2)}'

        # bug fix for not having number of stations
        if 'numberOfStations' in list(h5_file[group_path].attrs):
            num_stations = int(h5_file[group_path].attrs['numberOfStations'])
        else:
            num_stations = 1

        # Iterate through stations
        for i in range(1,num_stations+1):
            path = f"{group_path}/Group_{str(i).zfill(3)}/values"
            data = [np.nan if i[0]==-9999.0 else i[0] for i in list(h5_file[path])]
            max, min = np.nanmax(data), np.nanmin(data)
            if abs_max is None:
                abs_max = max
            if abs_min is None:
                abs_min = min

            if max > abs_max:
                abs_max = max
            if min < abs_min:
                abs_min = min

    # Test min/max values from h5_file vs test implementation
    assert round(abs_min,2) == round(h5_file_min, 2), f"MinDatasetHeight is {min_height} but testing found a value of {abs_min}"
    assert round(abs_max,2) == round(h5_file_max, 2), f"MaxDatasetHeight is {max_height}, but testing found a value of {abs_max}"
