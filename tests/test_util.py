import json, requests
import numpy as np
import test_data

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
    for attr_name, attr_type in test_data_attr_names:
        # Ensure that the attribute exists in the h5 file
        assert attr_name in h5_file_attr_names, f"{attr_name} does not exist in {path}"
        print(attr_name)
        print(attr_type)

        # Check that the attribute type is correct
        assert issubclass(type(h5_file[path].attrs[attr_name]), attr_type), "Attribute name: {attr_name} and value is {h5_file[path].attrs[attr_name]} incorrect type. Type should be: {attr_type} but is {type(h5_file[path].attrs[attr_name])}"

        # Check a specific case where dcf8 timeIntervalIndex is 0 or 1
        if attr_name == 'timeIntervalIndex':
            time_interval_index = h5_file[path].attrs['timeIntervalIndex']
            check_time_interval_index_attr(time_interval_index, h5_file_attr_names, product_type)

def test_dcf8_attrs_exist(h5_file, product_name, test_attr_dict, product_attr_name, s104=True):

    # Iterate through test data hardcoded attributes in dict {path: [attributes...]}
    for path, test_data_attr_names in test_attr_dict.items():
        # Append 001, 002 ... nnn to Group_ prefix depending on Number of Stations
        if ('Group_' in path) and (s104 is True):
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

            assert len(h5_file_attr_names) == len(test_data_attr_names), (
                f'H5 file attributes for path: {path} must be the same length as the test data attributes\n'
                f'H5 file attributes are: {h5_file_attr_names} \n'
                f'Test data attributes are: {test_data_attr_names}'
            )

            # Ensure that the attributes are present in the h5 file
            check_attrs(h5_file, h5_file_attr_names, test_data_attr_names, path, product_attr_name)

def run_test_min_max(h5_file, h5_file_max, h5_file_min, product_name):
    abs_max, abs_min = None, None
    num_instances = int(h5_file[product_name].attrs['numInstances'])

    # Iterate through number of instances
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
            # set min max values in first iteration
            if abs_max is None:
                abs_max = max
            if abs_min is None:
                abs_min = min

            # check if current max is greater or less than previous absolute max
            if max > abs_max:
                abs_max = max
            if min < abs_min:
                abs_min = min

    # Test min/max values from h5_file vs test implementation
    assert round(abs_min,2) == round(h5_file_min, 2), f"MinDatasetHeight is {min_height} but testing found a value of {abs_min}"
    assert round(abs_max,2) == round(h5_file_max, 2), f"MaxDatasetHeight is {max_height}, but testing found a value of {abs_max}"

def test_datetime_first_last_record(h5_file, product_name):
    # get first/last record attribute
    first_record = h5_file[f'{product_name}/{product_name}.01'].attrs['dateTimeOfFirstRecord']
    last_record = h5_file[f'{product_name}/{product_name}.01'].attrs['dateTimeOfLastRecord']

    # last/first record of dataset
    dataset_last_record = h5_file[f'{product_name}/{product_name}.01/Group_001'].attrs['endDateTime']
    dataset_first_record = h5_file[f'{product_name}/{product_name}.01/Group_001'].attrs['startDateTime']

    # ensure that first record matches dataset first record and vice versa for last record
    assert first_record == dataset_first_record, "first datetime record must be equal to first datetime in datasets"
    assert last_record == dataset_last_record, "last datetime record must be equal to last datetime in datasets"

def test_num_stations(h5_file, product_name):
    num_instances = int(h5_file[product_name].attrs['numInstances'])

    # Iterate through num instances
    for j in range(1, num_instances+1):
        group_path = f'{product_name}/{product_name}.{str(j).zfill(2)}'
        num_stations = int(h5_file[group_path].attrs['numberOfStations'])

        # Iterate through stations
        for i in range(1,num_stations+1):
            station_no = f"{group_path}/Group_{str(i).zfill(3)}"

            # assert that all stations exist
            assert station_no in h5_file, f"{station_no} does not exist in h5_file"

def test_positioning_group(h5_file, product_name):
    num_instances = int(h5_file[product_name].attrs['numInstances'])

    # Iterte through instances to find Positioning group
    for j in range(1, num_instances+1):
        group_path = f'{product_name}/{product_name}.{str(j).zfill(2)}'
        assert f"{group_path}/Positioning" in h5_file, f"Positioning group not present in {group_path}"

def test_group_f_dataset(h5_file, group_f_data):
    # iterate through group_f product name items
    for path, test_group_f_data in group_f_data.items():
        for i, tuple_fields in enumerate(h5_file[path]):
            check_decoded_fields(tuple_fields, test_group_f_data[i])

def check_decoded_fields(h5_tuple_fields, test_data_fields):
    # bug fix: decode - can remove decode
    decoded_fields = [field.decode("utf-8").strip() for field in h5_tuple_fields]

    assert decoded_fields == test_data_fields, f"Fields for {path} in h5_file must be the same as test data"

def test_dataset_types(h5_file, product_name):
    dataset_types_dict = test_data.dataset_types
    field1_metadata, field2_metadata = get_dataset_types(h5_file, product_name)

    field1_name, field1_type = field1_metadata
    field2_name, field2_type = field2_metadata

    # Iterate through groups
    for j in range(1, h5_file[product_name].attrs['numInstances']+1):
        group_path = f'{product_name}/{product_name}.{str(j).zfill(2)}'

        # bug fix for not having number of stations
        if 'numberOfStations' in list(h5_file[group_path].attrs):
            num_stations = int(h5_file[group_path].attrs['numberOfStations'])
        else:
            num_stations = 1

        # Iterate through stations
        for i in range(1,num_stations+1):

            dataset_path = f"{group_path}/Group_{str(i).zfill(3)}/values"
            # get dataset types in h5 file
            dataset = h5_file[dataset_path]

            # check dataset type is correct
            assert issubclass(dataset.dtype[field1_name].type, field1_type), f"Specified dataset type is not the same as group F attribute, dataset type is {dataset.dtype[field1_name]} and group f type is {field1_type}"
            assert issubclass(dataset.dtype[field2_name].type, field2_type), f"Specified dataset type is not the same as group F attribute, dataset type is {dataset.dtype[field2_name]} and group f type is {field2_type}"

            # check dataset values are of correct type
            assert dataset[0][0].dtype == dataset.dtype[field1_name].type, f"Dataset values must of same type as specified in group_f attributes. Group_F attribute type is {field1_type} and the current type is {dataset[0][0].dtype}"
            assert dataset[0][1].dtype == dataset.dtype[field2_name].type, f"Dataset values must of same type as specified in group_f attributes. Group_F attribute type is {field2_type} and the current type is {dataset[0][1].dtype}"

def get_dataset_types(h5_file, product_name):
    # Extract dataset names and types from group f
    dataset_types_dict = test_data.dataset_types
    group_f_data = h5_file[f'Group_F/{product_name}']

    # extract dataset name and type from group_f/{waterlevel or surfacecurrent} list and decode from utf-8
    # bug fix - to remove decode
    field1_metadata = group_f_data[0][0].decode('utf-8'), dataset_types_dict[group_f_data[0][4].decode('utf-8')]
    field2_metadata = group_f_data[1][0].decode('utf-8'), dataset_types_dict[group_f_data[1][4].decode('utf-8')]

    return field1_metadata, field2_metadata
