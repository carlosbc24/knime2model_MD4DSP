
import xml.etree.ElementTree as elementTree
from parsers_deprecated.parameter import create_parameters, get_node_parameters
from utils.library_functions import get_library_transformation_name, get_library_transformation_id
from parsers_deprecated.dataDictionary import create_input_port, create_output_port
from utils.logger import print_and_log


def get_dest_node_from_connections(data: dict, node_id: int) -> str | None:
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


def get_node_columns(node: dict) -> list:
    """
    Get the columns from the node parameters.
    Args:
        node:

    Returns:

    """
    node_columns = []

    # Add node columns if they exist
    if "columns" in node["parameters"]:
        node_columns = node["parameters"]["columns"]

    # Add included columns if they exist (from column filter)
    if "included_columns" in node["parameters"]:
        if node_columns == []:
            node_columns = node["parameters"]["included_columns"]
        else:
            node_columns.append(node["parameters"]["included_columns"])

    return node_columns


def create_data_processing(data: dict, node: dict, index: int, input_file_path: str, include_contracts: bool) -> tuple:
    """
    Processes a "normal" node (not Reader/Writer) from the JSON and returns:
      - node_id: identifier of the node (or its index)
      - dp: the generated XML 'dataprocessing' element
      - node_name: the name of the node (for reference in links)

    Args:
        data (dict): JSON data containing the workflow information.
        node (dict): Node data from the JSON.
        index (int): Index of the node.
        input_file_path (str): Path to the input file (if any).
        include_contracts (bool): Whether to include contracts in the XMI file.

    Returns:
        tuple: (node_id, dp, node_name, library_transformation_id)
    """
    node_id = node.get("id", index)
    node_name = node.get("node_name", f"Node_{index}")

    # Get library transformation name
    library_transformation_name = get_library_transformation_name('library_hashing/library_function_hashing.json',
                                                                  node, index)

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
    else:
        base_name = node_name

    # Get destination node name
    dest_node_name = get_dest_node_from_connections(data, node_id)
    if dest_node_name is not None and "(" in dest_node_name and ")" in dest_node_name:
        dest_node_name = dest_node_name.split("(")[0].strip()
    else:
        dest_node_name = dest_node_name

    # Get columns from the node
    dp_columns = get_node_columns(node)

    # Create inputPort and outputPort
    create_input_port(dp, base_name, node_name, index, dp_columns, input_file_path)
    create_output_port(dp, base_name, node_name, index, dp_columns, dest_node_name)

    # Get library transformation ID
    library_transformation_id = get_library_transformation_id('library_hashing/library_function_hashing.json',
                                                              node_name)

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
        library_parameters = get_node_parameters('library_hashing/library_function_hashing.json', node_name)
        create_parameters(dp, node_name, library_parameters, library_transformation_id)

    return node_id, dp, node_name, library_transformation_id
