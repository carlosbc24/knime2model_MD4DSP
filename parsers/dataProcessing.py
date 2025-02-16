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


def get_library_transformation_name(json_file_path, node_name):
    """
    Reads the JSON file and finds the hashing function whose identifier matches the value of the node_name variable.
    Extracts the value of the library_transformation_id attribute from the first matching element.

    Args:
        json_file_path (str): Path to the JSON file.
        node_name (str): Name of the node to search for.

    Returns:
        string: Value of the library_transformation_id attribute of the first matching node
    """
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    functions_hashing = data.get("functions_hashing", [])

    for function in functions_hashing:
        if node_name in function:
            return function[node_name].get("library_transformation_name")

    return None


def get_dest_node_from_connections(data: dict, node_id: int) -> str|None:
    """
    Get the destination node name from the connections in the JSON data.

    Args:
        data (dict): JSON data.
        node_id: ID of the source node.

    Returns:
        string: Name of the destination node.

    """
    links = data.get("connections", [])
    for conn in links:
        source_id = conn.get("sourceID")
        dest_id = conn.get("destID")
        if source_id == node_id:
            print_and_log(f"Source: {source_id}, Dest: {dest_id}")
            nodes = data.get("nodes", [])
            for node in nodes:
                if node.get("id") == dest_id:
                    return node.get("node_name")

    return None


def create_data_processing(data, node: dict, index: int, input_file_path: str):
    """
    Processes a "normal" node (not Reader/Writer) from the JSON and returns:
      - node_id: identifier of the node (or its index)
      - dp: the generated XML 'dataprocessing' element
      - node_name: the name of the node (for reference in links)

    Args:
        node (dict): Node data from the JSON.
        index (int): Index of the node.
        input_file_path (str): Path to the input file (if any).

    Returns:
        tuple: (node_id, dp, node_name)
    """
    node_id = node.get("id", index)
    node_name = node.get("node_name", f"Node_{index}")

    # Get library transformation ID
    library_transformation_name = get_library_transformation_name('library_function_hashing.json', node_name)

    # Create dataprocessing element
    if library_transformation_name is None:
        dp = elementTree.Element("dataprocessing", {
            "xsi:type": "Workflow:DataProcessing",
            "name": node_name
        })
    else:
        dp = elementTree.Element("dataprocessing", {
            "xsi:type": "Workflow:DataProcessing",
            "name": f"{library_transformation_name}(from KNIME node: {node_name})"
        })

    # Determine base_name and fields from the node name
    if "(" in node_name and ")" in node_name:
        base_name = node_name.split("(")[0].strip()
        inner = node_name[node_name.find("(") + 1: node_name.find(")")]
        fields = [f.strip() for f in inner.split(",") if f.strip()]
    else:
        base_name = node_name
        fields = [node_name]

    # Get destination node name
    dest_node_name = get_dest_node_from_connections(data, node_id)
    if dest_node_name is not None and "(" in dest_node_name and ")" in dest_node_name:
        dest_node_name = dest_node_name.split("(")[0].strip()
    else:
        dest_node_name = dest_node_name

    # Create inputPort and outputPort
    create_input_port(dp, base_name, node_name, index, fields, input_file_path)
    create_output_port(dp, base_name, node_name, index, fields, dest_node_name)

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


def modify_last_data_processing(node: dict, index: int, output_data_filename: str):
    """
    Modifies the last <dataprocessing> element in the XML tree by updating the outputPort filename.

    Args:
        node: dict: Node data from the JSON.
        index: int: Index of the previous node
        output_data_filename:

    Returns:

    """
    node_name = node.get("node_name", f"Node_{index}")
    base_name = node_name.split("(")[0].strip()
    inner = node_name[node_name.find("(") + 1: node_name.find(")")]
    fields = [f.strip() for f in inner.split(",") if f.strip()]

    # Get the last <dataprocessing> element
    dp = elementTree.Element("dataprocessing", {
        "xsi:type": "Workflow:DataProcessing",
        "name": node_name
    })

    # Create outputPort
    create_output_port(dp, base_name, node_name, index, fields)

    # Set 'in' and 'out' attributes from the references of each datafield
    output_refs = [f"//@dataprocessing.{index}/@outputPort.0"]
    dp.set("out", " ".join(output_refs))
