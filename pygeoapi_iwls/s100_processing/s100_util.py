# Standard library imports
import h5py

# Packages imports
from dataclasses import dataclass

@dataclass
class AttributeData:
    """
    Class to store attribute data.
    """
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
    """
    Creates an attribute if it doesn't exist.

    :param group: Base group or dataset where attribute (h5py._hl.group.Group)
    :param attribute_name: Name of the attribute (str)
    :param attribute_value: Assigned attribute value (str)
    """
    if group.attrs.__contains__(attribute_name):
        group.attrs.modify(attribute_name, attribute_value)
    else:
        group.attrs.create(attribute_name, attribute_value)
