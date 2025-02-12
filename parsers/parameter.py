import xml.etree.ElementTree as ET
import json


def create_parameters(parent, node_name, parameters, index):
    """
    Creates <parameter> elements (and their child <derivedValueDef>) from the node parameters.

    Args:
        parent (Element): The parent XML element.
        node_name (str): The name of the node.
        parameters (dict): The parameters of the node.
        index (int): The index of the node.
    """
    for param_key, param_value in parameters.items():
        # If the value is complex (list or dict), use json.dumps, otherwise str()
        param_value_str = json.dumps(param_value) if isinstance(param_value, (list, dict)) else str(param_value)
        param_elem = ET.SubElement(parent, "parameter", {
            "xsi:type": "Workflow:DerivedValue",
            "name": f"{node_name}_param_{param_key}",
            "value": param_value_str
        })
        ET.SubElement(param_elem, "derivedValueDef", {
            "href": f"library_validation.xmi#//@dataprocessingdefinition.{index}/@parameterdefinition.0"
        })
