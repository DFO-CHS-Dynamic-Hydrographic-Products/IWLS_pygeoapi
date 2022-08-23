import json, requests

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

def check_time_interval_index_attr(time_interval_index, path_attributes, product_type):
    # If timeRecordIndex attr is 1, timeRecordInterval attr should be present in h5 file
    if time_record_index == 1:
        assert "timeRecordInterval" in path_attributes, \
            "timeRecordInterval attribute must exist if timeRecordInterval attribute is 1"
    # If timeRecordIndex attr is 0, waterLevelTime/surfaceCurrentTime attr should be present in h5 file
    elif time_record_index == 0:
        assert f"{product_type}Time" in path_attributes, \
            f"{product_type}Time attribute must exist if timeRecordInterval attribute is 0"

def check_attrs(h5_file, path_attributes, attr_names, path, product_type):
    # Iterate through attribute names

    for attr_name in attr_names:
        # Ensure that the attribute exists in the h5 file
        assert attr_name in path_attributes, f"{attr_name} does not exist in {path}"

        # Check a specific case where dcf8 timeIntervalIndex is 0 or 1
        if attr_name == 'timeIntervalIndex':
            time_record_index = h5_file[path].attrs['timeRecordInterval']
            check_time_interval_index_attr(time_interval_index, path_attributes, product_type)

def test_dcf8_attrs_exist(h5_file, product_name, attr_names_dict, product_attr_name):

    for path, attr_names in attr_names_dict.items():
        # Append 001, 002 ... nnn to Group_ prefix depending on Number of Stations
        if 'Group_' in path:
            pass
            # num_stations_path = f'{product_name}/{product_name}.01'
            # num_stations = int(h5_file[num_stations_path].attrs['numberOfStations'])

            # # Iterate through number of stations
            # for station_no in range(1, num_stations+1):
            #     # Create group path with station number e.g. 'WaterLevel/WaterLevel.01/Group_001
            #     group_path = f'{path}{str(station_no).zfill(3)}'

            #     # Get the attribute names in the group path (from the h5_file)
            #     path_attributes = list(h5_file[group_path].attrs)

            #     # Ensure that the attributes are present in the h5 file
            #     check_attrs(h5_file, path_attributes, attr_names, group_path, product_attr_name)
        else:
            # Get the names of the attributes in group (from the h5_file)
            path_attributes = list(h5_file[path].attrs)

            # Ensure that the attributes are present in the h5 file
            check_attrs(h5_file, path_attributes, attr_names, path, product_attr_name)
