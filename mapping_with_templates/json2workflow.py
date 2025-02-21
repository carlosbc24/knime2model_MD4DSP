import os
import json
from string import Template

from utils.library_functions import get_library_transformation_name, get_library_transformation_names
from utils.logger import print_and_log
from jinja2 import Template as JinjaTemplate


def get_input_columns(node: dict) -> list:
    """
    Get the input columns from the node parameters.
    Args:
        node: (dict) The node from the JSON data.

    Returns:
        in_columns: (list) The list of input columns.

    """
    in_columns = []

    # Add included columns if they exist (from column filter)
    if "in_columns" in node["parameters"]:
        in_columns = node["parameters"]["in_columns"]

    return in_columns


def get_output_columns(node: dict) -> list:
    """
    Get the output columns from the node parameters.
    Args:
        node: (dict) The node from the JSON data.

    Returns:
        out_columns: (list) The list of output columns.

    """
    out_columns = []

    # Add excluded columns if they exist (from column filter)
    if "out_columns" in node["parameters"]:
        out_columns = node["parameters"]["out_columns"]

    return out_columns


def generate_datafields_content(node_columns: list, library_transformation_name: str) -> tuple:
    """
    Generates the filled content for input and output datafields.

    Args:
        node_columns (list): List of node columns.
        library_transformation_name (str): The name of the library transformation.

    Returns:
        tuple: Filled content for input and output datafields.
    """
    input_datafields_filled_content = ""
    output_datafields_filled_content = ""

    for node_column in node_columns:
        column_name = node_column["column_name"]
        column_type = node_column["column_type"]

        with open(f"templates/datafield.xmi", "r") as file:
            datafield_template = Template(file.read())

            input_datafield_values = {
                "column_name": column_name,
                "transformation_name": library_transformation_name,
                "data_type": "String" if column_type == "xstring" else "Integer",
                "inOut": "input"
            }

            input_datafields_filled_content += datafield_template.safe_substitute(
                input_datafield_values) + "\n"

            output_datafield_values = {
                "column_name": column_name,
                "transformation_name": library_transformation_name,
                "data_type": "String" if column_type == "xstring" else "Integer",
                "inOut": "output"
            }

            output_datafields_filled_content += datafield_template.safe_substitute(
                output_datafield_values) + "\n"

    return input_datafields_filled_content, output_datafields_filled_content





def process_nodes(data: dict, nodes: list) -> tuple[dict, str]:
    """
    Processes the nodes from the JSON data and appends the corresponding XML elements to the root element.

    Args:
        data: (dict) The JSON data containing the workflow information.
        nodes: (list) The list of nodes from the JSON data.

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

        # Detect if the node includes the substrings "Reader" or "Connector"
        if any(substring in node_name for substring in ["Reader"]):
            input_file_path = node.get("parameters", {}).get("file_path", "")
            print_and_log(f"Input data for workflow node {node_id}: {input_file_path}")

        # Get library transformation name
        library_transformation_name = get_library_transformation_name('library_hashing/library_function_hashing.json',
                                                                      node_name)

        included_columns = get_input_columns(node)
        included_column_names = [included_column["column_name"] for included_column in included_columns]
        included_column_names_str = ", ".join(included_column_names)
        print_and_log(f"Included columns for node {node_id}: {included_column_names_str}")

        excluded_columns = get_output_columns(node)
        excluded_column_names = [excluded_column["column_name"] for excluded_column in excluded_columns]
        excluded_column_names_str = ", ".join(excluded_column_names)
        print_and_log(f"Excluded columns for node {node_id}: {excluded_column_names_str}")

        dataprocessing = {
            "transformation": {"name": "", "KNIME_name": node_name},
            "column_names": included_column_names_str,
            "in_columns": [
                {"name": column["column_name"], "type": "String" if column["column_type"] == "xstring" else "Integer"}
                for column in included_columns
            ],
            "out_columns": [
                {"name": column["column_name"], "type": "String" if column["column_type"] == "xstring" else "Integer"}
                for column in excluded_columns
            ]
        }

        if library_transformation_name is not None:

            if library_transformation_name in library_transformation_names:
                # Read the workflow template file
                with open(f"templates/columnFilterDataProcessing.xmi", "r") as file:
                    data_processing_jinja_template = JinjaTemplate(file.read())
                    dataprocessing["transformation"]["name"] = library_transformation_name

        else:
            # Read the workflow template file
            with open(f"templates/unknownDataProcessing.xmi", "r") as file:
                data_processing_jinja_template = JinjaTemplate(file.read())

        # Fill the template with jinja2
        dataProcessings_filled_content += data_processing_jinja_template.render(dataprocessing=dataprocessing) + "\n"
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

            if "included_columns" in source_node["parameters"]:
                source_columns = source_node["parameters"]["included_columns"]
                source_columns_str = ", ".join([column["column_name"] for column in source_columns])
            else:
                source_columns_str = ""

            if "included_columns" in target_node["parameters"]:
                target_columns = target_node["parameters"]["included_columns"]
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

        # Preprocess nodes and connections to map IDs to a new range (0 to n-1)
        nodes, connections = preprocess_nodes_connections(nodes, connections)

        # Process nodes and get the node mapping
        node_mapping, data_processing_filled_content = process_nodes(data, nodes)

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
