from dataclasses import dataclass

@dataclass
class AttributeData:
    """Class for keeping track of attribute data."""
    num_groups: int
    start_datetime: str
    end_datetime: str
    time_record_interval: int
    num_times: int

def create_modify_attribute(group, attribute_name, attribute_value):

    if group.attrs.__contains__(attribute_name):
        group.attrs.modify(attribute_name, attribute_value)
    else:
        group.attrs.create(attribute_name, attribute_value)
