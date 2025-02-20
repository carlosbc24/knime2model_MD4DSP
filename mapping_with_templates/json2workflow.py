import os
import json
from xml.dom import minidom
import xml.etree.ElementTree as elementTree
from string import Template
from parsers.dataLink import create_link
from parsers.dataProcessing import create_data_processing, get_library_transformation_name
from utils.logger import print_and_log

def process_nodes(nodes: list) -> tuple[dict, str]:
    """
    Processes the nodes from the JSON data and appends the corresponding XML elements to the root element.

    Args:
        nodes: (list) The list of nodes from the JSON data.

    Returns:
        node_mapping: (dict) The mapping of node IDs to their XML elements and metadata.
        dataProcessing_filled_content: (str) The filled content of the data processing nodes.
    """
    node_mapping = {}
    dataProcessing_filled_content = ""
    for index, node in enumerate(nodes):
        node_id = node.get("id", index)
        node_name = node.get("node_name", f"Node_{index}")

        # Detect if the node includes the substrings "Reader" or "Connector"
        if any(substring in node_name for substring in ["Reader"]):
            input_file_path = node.get("parameters", {}).get("file_path", "")
            print_and_log(f"Input data for workflow node {node_id}: {input_file_path}")

        # Get library transformation name
        library_transformation_name = get_library_transformation_name('library_function_hashing.json', node_name)

        if library_transformation_name is not None:
            # Read the workflow template file
            with open(f"templates/{library_transformation_name}DataProcessing.xmi", "r") as file:
                dataProcessing_template = Template(file.read())

            dataProcessing_values = {
                "transformation_name": library_transformation_name,
                "incoming": "",
                "outgoing": "",
                "in": "",
                "out": "",
                "input_filepath": f"{library_transformation_name}_dataDictionary.csv",
                "output_filepath": f"{library_transformation_name}_dataDictionary.csv",
                "dataField": "",
            }

        else:
            # Read the workflow template file
            with open(f"templates/unknownDataProcessing.xmi", "r") as file:
                dataProcessing_template = Template(file.read())

            dataProcessing_values = {
                "transformation_name": "",
                "incoming": "",
                "outgoing": "",
                "in": "",
                "out": "",
                "input_filepath": input_file_path if input_file_path else "",
                "output_filepath": "",
                "dataField": "",
            }

        # Fill the template
        dataProcessing_filled_content += dataProcessing_template.safe_substitute(
            dataProcessing_values
        ) + "\n"
        node_mapping[node_id] = {"index": index, "name": node_name}

    return node_mapping, dataProcessing_filled_content


def process_links(data: dict, node_mapping: dict) -> str:
    """
    Processes the links between nodes from the JSON data and appends the corresponding XML elements to the root element.

    Args:
        data (dict): The JSON data containing the workflow information.
        node_mapping (dict): Mapping of node IDs to their XML elements and metadata.

    Returns:
        links_filled_content: (str) The filled content of the links.
    """

    links = data.get("connections", [])
    links_filled_content = ""
    link_index = 0
    for conn in links:
        source_id = conn.get("sourceID")
        dest_id = conn.get("destID")

        # Connection between two "normal" nodes
        if source_id in node_mapping and dest_id in node_mapping:
            # Read the workflow template file
            with open("templates/link.xmi", "r") as file:
                link_template = Template(file.read())

            link_values = {
                "source": source_id,
                "target": dest_id,
            }

            # Fill the template
            links_filled_content += link_template.safe_substitute(
                link_values
            ) + "\n"
            link_index += 1

    return links_filled_content


def preprocess_nodes_connections(nodes, connections):
    """
    Convert nodes id and its associated links with its sourceID and destID to numbers between 0 and n-1.

    Args:
        nodes (list): List of nodes from the JSON data.
        connections (list): List of connections from the JSON data.

    Returns:
        tuple: Updated nodes and connections with IDs mapped to a new range.
    """
    id_mapping = {node['id']: index for index, node in enumerate(nodes)}

    for node in nodes:
        node['id'] = id_mapping[node['id']]

    for connection in connections:
        connection['sourceID'] = id_mapping[connection['sourceID']]
        connection['destID'] = id_mapping[connection['destID']]

    return nodes, connections


def json_to_xmi_workflow_with_templates(json_input_folder: str, workflow_filename: str, xmi_output_folder: str):
    """
    Converts a JSON workflow file to an XMI file using templates for the data processing nodes.

    Args:
        json_input_folder (str): Path to the folder containing the JSON files.
        workflow_filename (str): Name of the JSON file (without extension).
        xmi_output_folder (str): Path to the folder where the XMI file will be saved.

    Returns:

    """
    # Load JSON data
    with (open(os.path.join(json_input_folder, workflow_filename, workflow_filename + ".json"),
               "r", encoding="utf-8") as f):
        data = json.load(f)

        # Read the workflow template file
        with open("templates/workflow_template.xmi", "r") as file:
            workflow_template_content = Template(file.read())

        nodes = data.get("nodes", [])
        connections = data.get("connections", [])

        nodes, connections = preprocess_nodes_connections(nodes, connections)

        data_processing_filled_content = ""

        # Process nodes
        node_mapping, data_processing_filled_content = process_nodes(nodes)

        # Process links
        links_filled_content = process_links(data, node_mapping)

        # Define the replacement values
        workflow_values = {
            "workflow_name": workflow_filename,
            "data_processing_list": data_processing_filled_content,
            "link_list": links_filled_content
        }

        # Fill the template
        filled_content = workflow_template_content.safe_substitute(workflow_values)

        output_xmi_filepath = os.path.join(xmi_output_folder, workflow_filename + ".xmi")
        os.makedirs(os.path.dirname(output_xmi_filepath), exist_ok=True)
        with open(output_xmi_filepath, "w", encoding="utf-8") as file:
            file.write(filled_content)
