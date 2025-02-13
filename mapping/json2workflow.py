import os
import json
from xml.dom import minidom
import xml.etree.ElementTree as elementTree

from parsers.dataLink import create_link
from parsers.dataProcessing import create_data_processing
from utils.logger import print_and_log


def process_nodes(data, root):
    """
    Processes the nodes from the JSON data and appends the corresponding XML elements to the root element.

    Args:
        data (dict): The JSON data containing the workflow information.
        root (Element): The root XML element to which the nodes will be appended.

    Returns:
        tuple: A tuple containing three dictionaries:
            - node_mapping (dict): Mapping of node IDs to their XML elements and metadata.
            - reader_mapping (dict): Mapping of Reader node IDs to their file paths.
            - writer_mapping (dict): Mapping of Writer node IDs to their file paths.
    """
    node_mapping = {}
    reader_mapping = {}
    writer_mapping = {}

    nodes = data.get("nodes", [])
    index = 0
    for node in nodes:
        node_id = node.get("id", index)
        node_name = node.get("node_name", f"Node_{index}")

        # Detect if the node includes the substrings "Reader",
        # "Writer", "Connector" or "Table"

        if any(substring in node_name for substring in ["Reader", "Connector", "Table"]):
            file_path = node.get("parameters", {}).get("file_path", "")
            reader_mapping[node_id] = file_path

        elif "Writer" in node_name:
            file_path = node.get("parameters", {}).get("file_path", "")
            writer_mapping[node_id] = file_path
        else:
            # "Normal" node: transform into <dataprocessing>
            n_id, dp_element, n_name = create_data_processing(node, index)
            root.append(dp_element)
            node_mapping[node_id] = {"element": dp_element, "index": index, "name": n_name}
            index += 1

    return node_mapping, reader_mapping, writer_mapping


def process_links(data: dict, root: elementTree.Element, node_mapping: dict):
    """
    Processes the links (connections) from the JSON data and appends the corresponding XML elements to the root element.

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
            print_and_log(f"Link_index:{link_index}, Source: {source_id}, Dest: {dest_id}")
            link_element = create_link(link_index, conn, node_mapping)
            if link_element is not None:
                root.append(link_element)
            link_index += 1


def json_to_xmi_workflow(json_input_folder: str, workflow_filename: str, xmi_output_folder: str):
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
    node_mapping, reader_mapping, writer_mapping = process_nodes(data, root)
    process_links(data, root, node_mapping)

    # Convert XML to string and format it
    raw_xml = elementTree.tostring(root, encoding='utf-8')
    parsed = minidom.parseString(raw_xml)
    formatted_xml = "\n".join(line for line in parsed.toprettyxml(indent="    ").split("\n") if line.strip())

    # Save formatted XML to file
    output_xmi_filepath = os.path.join(xmi_output_folder, workflow_filename + ".xmi")
    os.makedirs(os.path.dirname(output_xmi_filepath), exist_ok=True)
    with open(output_xmi_filepath, "w", encoding="utf-8") as file:
        file.write(formatted_xml)

    print_and_log(f"Workflow XMI saved to: {output_xmi_filepath}")
