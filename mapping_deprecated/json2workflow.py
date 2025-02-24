import os
import json
from xml.dom import minidom
import xml.etree.ElementTree as elementTree

from parsers_deprecated.dataLink import create_link
from parsers_deprecated.dataProcessing import create_data_processing
from utils.logger import print_and_log


def process_nodes(data: dict, root: elementTree.Element, include_contracts: bool = None) -> tuple[dict, int, int]:
    """
    Processes the nodes from the JSON data and appends the corresponding XML elements to the root element.

    Args:
        data (dict): The JSON data containing the workflow information.
        root (Element): The root XML element to which the nodes will be appended.
        include_contracts (bool): Whether to include contracts in the XMI file.

    Returns:
        node_mapping (dict): Mapping of node IDs to their XML elements and metadata.
        nodes_cont (int): Number of nodes in the workflow.
        mapped_nodes (int): Number of nodes that were mapped to a library transformation.
    """
    node_mapping = {}

    nodes = data.get("nodes", [])
    input_file_path = ""
    index = 0
    mapped_nodes = 0

    for node in nodes:
        node_id = node.get("id", index)
        node_name = node.get("node_name", f"Node_{index}")

        # Detect if the node includes the substrings "Reader" or "Connector"
        if any(substring in node_name for substring in ["Reader"]):
            input_file_path = node.get("parameters", {}).get("file_path", "")
            print_and_log(f"Input data for workflow node {node_id}: {input_file_path}")

        node_id = node.get("id", index)
        n_id, dp_element, n_name, library_transformation_id = create_data_processing(data, node, index,
                                                                                     input_file_path,
                                                                                     include_contracts)
        input_file_path = ""

        if library_transformation_id is not None:
            mapped_nodes += 1

        root.append(dp_element)
        node_mapping[node_id] = {"element": dp_element, "index": index, "name": n_name}

        index += 1

    nodes_cont = len(nodes)

    return node_mapping, nodes_cont, mapped_nodes


def process_links(data: dict, root: elementTree.Element, node_mapping: dict):
    """
    Processes the links between nodes from the JSON data and appends the corresponding XML elements to the root element.

    Args:
        data (dict): The JSON data containing the workflow information.
        root (Element): The root XML element to which the links will be appended.
        node_mapping (dict): Mapping of node IDs to their XML elements and metadata.
    """

    links = data.get("connections", [])
    link_index = 0
    for conn in links:
        source_id = conn.get("sourceID")
        dest_id = conn.get("destID")

        # Connection between two "normal" nodes
        if source_id in node_mapping and dest_id in node_mapping:
            link_element = create_link(link_index, conn, node_mapping)
            if link_element is not None:
                root.append(link_element)
            link_index += 1


def json_to_xmi_workflow(json_input_folder: str, workflow_filename: str, xmi_output_folder: str,
                         include_contracts: bool, node_mapping_desired_ratio: float = None) -> tuple[int, int]:
    """
    Converts a JSON structure of a KNIME workflow to a well-formatted XMI file.
    Processes nodes whose names end in "Reader" or "Writer" are not transformed
    into a <dataprocessing> element but their file_path is injected into
    the <inputPort> or <outputPort> of the connected node
    (subsequent or previous, respectively).

    Args:
        json_input_folder (str): Path to the folder containing the JSON files.
        workflow_filename (str): Name of the JSON file (without extension).
        xmi_output_folder (str): Path to the folder where the XMI file will be saved.
        include_contracts (bool): Whether to include contracts in the XMI file.
        node_mapping_desired_ratio (float): Desired ratio of nodes mapped to a library transformation.

    Returns:
        int: Number of nodes that were mapped to a library transformation.
        int: Number of nodes in the workflow.
    """
    # Load JSON data
    with (open(os.path.join(json_input_folder,
                            workflow_filename, workflow_filename + ".json"),
               "r", encoding="utf-8")
          as f):
        data = json.load(f)

    # Register namespaces
    ns = {
        "xmi": "http://www.omg.org/XMI",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "Library": "http://www.example.org/Library",
        "Workflow": "https://www.example.org/workflow"
    }
    for prefix, uri in ns.items():
        elementTree.register_namespace(prefix, uri)

    # Create root element (Workflow)
    root = elementTree.Element("{https://www.example.org/workflow}Workflow", {
        "{http://www.omg.org/XMI}version": "2.0",
        "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation":
            "http://www.example.org/Library ../metamodel/Library.ecore https://www.example.org/workflow ../metamodel/Workflow.ecore",
        "name": workflow_filename,
        "xmlns:Library": "http://www.example.org/Library",
    })

    # Process nodes and links
    node_mapping, nodes_cont, mapped_nodes = process_nodes(data, root, include_contracts)
    process_links(data, root, node_mapping)

    # Convert XML to string and format it
    raw_xml = elementTree.tostring(root, encoding='utf-8')
    parsed = minidom.parseString(raw_xml)
    formatted_xml = "\n".join(line for line in parsed.toprettyxml(indent="    ").split("\n") if line.strip())

    # Save formatted XML to file
    if node_mapping_desired_ratio is not None:
        if (mapped_nodes/nodes_cont) < node_mapping_desired_ratio:
            print_and_log(f"WARNING: Only {mapped_nodes}/{nodes_cont} nodes were mapped to a library transformation (less than {node_mapping_desired_ratio*100}%)")
            output_xmi_filepath = os.path.join(xmi_output_folder, f"less_than_{node_mapping_desired_ratio*100}%_nodes_mapped", workflow_filename + ".xmi")
        else:
            print_and_log(f"{mapped_nodes}/{nodes_cont} nodes mapped successfully to their model transformation ({node_mapping_desired_ratio*100}% or more)")
            output_xmi_filepath = os.path.join(xmi_output_folder, f"{node_mapping_desired_ratio*100}%_or_more_nodes_mapped",
                                               workflow_filename + ".xmi")
    else:
        output_xmi_filepath = os.path.join(xmi_output_folder, workflow_filename + ".xmi")
    os.makedirs(os.path.dirname(output_xmi_filepath), exist_ok=True)
    with open(output_xmi_filepath, "w", encoding="utf-8") as file:
        file.write(formatted_xml)

    print_and_log(f"Workflow XMI saved to: {output_xmi_filepath}")

    return mapped_nodes, nodes_cont
