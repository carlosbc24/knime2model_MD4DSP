import xml.etree.ElementTree as elementTree
from parsers.dataDictionary import create_input_port, create_output_port
import json

from parsers.parameter import create_parameters, get_node_parameters
from utils.logger import print_and_log


def get_library_transformation_id(json_file_path, node_name):
    """
    Reads the JSON file and finds the hashing function whose identifier matches the value of the node_name variable.
    Extracts the value of the library_transformation_id attribute from the first matching element.

    Args:
        json_file_path (str): Path to the JSON file.
        node_name (str): Name of the node to search for.

    Returns:
        int: Value of the library_transformation_id attribute of the first matching element.
    """
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    functions_hashing = data.get("functions_hashing", [])

    for function in functions_hashing:
        if node_name in function:
            return function[node_name].get("library_transformation_id")

    return None


def create_data_processing(node, index):
    """
    Processes a "normal" node (not Reader/Writer) from the JSON and returns:
      - node_id: identifier of the node (or its index)
      - dp: the generated XML 'dataprocessing' element
      - node_name: the name of the node (for reference in links)

    Args:
        node (dict): Node data from the JSON.
        index (int): Index of the node.

    Returns:
        tuple: (node_id, dp, node_name)
    """
    node_id = node.get("id", index)
    node_name = node.get("node_name", f"Node_{index}")

    # Create dataprocessing element
    dp = elementTree.Element("dataprocessing", {
        "xsi:type": "Workflow:DataProcessing",
        "name": node_name
    })

    # Determine base_name and fields from the node name
    if "(" in node_name and ")" in node_name:
        base_name = node_name.split("(")[0].strip()
        inner = node_name[node_name.find("(") + 1: node_name.find(")")]
        fields = [f.strip() for f in inner.split(",") if f.strip()]
    else:
        base_name = node_name
        fields = [node_name]

    # Create inputPort and outputPort
    create_input_port(dp, base_name, node_name, index, fields)
    create_output_port(dp, base_name, node_name, index, fields)

    # Set 'in' and 'out' attributes from the references of each datafield
    input_refs = [f"//@dataprocessing.{index}/@inputPort.0"]
    output_refs = [f"//@dataprocessing.{index}/@outputPort.0"]
    dp.set("in", " ".join(input_refs))
    dp.set("out", " ".join(output_refs))

    # Get library transformation ID
    library_transformation_id = get_library_transformation_id('library_function_hashing.json', node_name)

    if library_transformation_id is None:
        print_and_log(f"WARNING: No library transformation ID found for node {node_name}")
    else:
        print_and_log(f'Library Transformation ID: {library_transformation_id} for node {node_name}')
        # Add transformation definition
        elementTree.SubElement(dp, "dataProcessingDefinition", {
            "xsi:type": "Library:Transformation",
            "href": f"library_validation.xmi#//@dataprocessingdefinition.{library_transformation_id}"
        })

        # Create parameters
        library_parameters = get_node_parameters('library_function_hashing.json', node_name)
        create_parameters(dp, node_name, library_parameters, library_transformation_id)

    return node_id, dp, node_name
