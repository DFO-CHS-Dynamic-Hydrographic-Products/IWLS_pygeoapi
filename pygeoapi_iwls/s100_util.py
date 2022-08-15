from dataclasses import dataclass
import h5py

@dataclass
class AttributeData:
    """Class for keeping track of attribute data."""
    num_groups: int
    start_datetime: str
    end_datetime: str
    time_record_interval: int
    num_times: int

def create_modify_attribute(
        group: h5py._hl.group.Group,
        attribute_name: str,
        attribute_value: str
    ):

    if group.attrs.__contains__(attribute_name):
        group.attrs.modify(attribute_name, attribute_value)
    else:
        group.attrs.create(attribute_name, attribute_value)
