

def create_modify_attribute(group, attribute_name, attribute_value):

    if group.attrs.__contains__(attribute_name):
        group.attrs.modify(attribute_name, attribute_value)
    else:
        group.attrs.create(attribute_name, attribute_value)
