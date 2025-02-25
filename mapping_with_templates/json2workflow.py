
import os
import json
from string import Template

from parsers.json_parser_functions import get_transformation_dp_values
from utils.library_functions import get_library_transformation_name, get_library_transformation_names
from jinja2 import Template as JinjaTemplate


def process_nodes(data: dict, nodes: list, include_contracts: bool) -> tuple[dict, str]:
    """
    Processes the nodes from the JSON data and appends the corresponding XML elements to the root element.

    Args:
        data: (dict) The JSON data containing the workflow information.
        nodes: (list) The list of nodes from the JSON data.
        include_contracts: (bool) Flag to include the contracts in the data processing nodes.

    Returns:
        node_mapping: (dict) The mapping of node IDs to their XML elements and metadata.
        dataProcessings_filled_content: (str) The filled content of the data processing nodes.
    """
    node_mapping = {}
    dataProcessings_filled_content = ""
    library_transformation_names = get_library_transformation_names('library_hashing/library_transformation_names.json')

    for index, node in enumerate(nodes):
        node_id = node.get("id", index)
        node_name = node.get("node_name", f"Node_{index}")

        # Get library transformation name
        library_transformation_name = get_library_transformation_name('library_hashing/library_function_hashing.json',
                                                                      node_name)

        # Get the data processing values for the node type
        dataprocessing_values = get_transformation_dp_values(node, node_id, node_name, include_contracts)

        template_filepath = f"templates/{library_transformation_name}_template.xmi"
        # Check if the library transformation name exists and the template file exists. If so, use the template file.
        # Otherwise, use the unknownDataProcessing template.
        if library_transformation_name in library_transformation_names and os.path.exists(template_filepath):

            # Read the workflow template file
            with open(template_filepath, "r") as file:
                data_processing_jinja_template = JinjaTemplate(file.read())
                dataprocessing_values["transformation"]["name"] = library_transformation_name
                node_name = library_transformation_name

        else:
            # Read the workflow template file
            with open(f"templates/unknownDataProcessing.xmi", "r") as file:
                data_processing_jinja_template = JinjaTemplate(file.read())

        # Fill the template with jinja2
        dataProcessings_filled_content += data_processing_jinja_template.render(dataprocessing=dataprocessing_values) + "\n"
        node_mapping[node_id] = {"index": index, "name": node_name}

    return node_mapping, dataProcessings_filled_content


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

            source_transformation_name = node_mapping[source_id]["name"]
            target_transformation_name = node_mapping[dest_id]["name"]

            source_node = data["nodes"][source_id]
            target_node = data["nodes"][dest_id]

            if "in_columns" in source_node["parameters"]:
                source_columns = source_node["parameters"]["in_columns"]
                source_columns_str = ", ".join([column["column_name"] for column in source_columns])
            else:
                source_columns_str = ""

            if "in_columns" in target_node["parameters"]:
                target_columns = target_node["parameters"]["in_columns"]
                target_columns_str = ", ".join([column["column_name"] for column in target_columns])
            else:
                target_columns_str = ""

            link_values = {
                "source": source_id,
                "target": dest_id,
                "transformation_name_source": source_transformation_name,
                "transformation_name_target": target_transformation_name,
                "source_columns": source_columns_str,
                "target_columns": target_columns_str
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


def json_to_xmi_workflow_with_templates(json_input_folder: str, workflow_filename: str, xmi_output_folder: str,
                                        include_contracts=True):
    """
    Converts a JSON workflow file to an XMI file using templates for the data processing nodes.

    Args:
        json_input_folder (str): Path to the folder containing the JSON files.
        workflow_filename (str): Name of the JSON file (without extension).
        xmi_output_folder (str): Path to the folder where the XMI file will be saved.
        include_contracts (bool): Flag to include the contracts in the data processing nodes.

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

        # Preprocess nodes and connections to map IDs to a new range (0 to n-1)
        nodes, connections = preprocess_nodes_connections(nodes, connections)

        # Process nodes and get the node mapping
        node_mapping, data_processing_filled_content = process_nodes(data, nodes, include_contracts)

        # Process links
        links_filled_content = process_links(data, node_mapping)

        # Define the replacement values to the workflow template
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
