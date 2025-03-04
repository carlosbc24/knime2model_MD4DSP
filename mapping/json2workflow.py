
import os
import json
from string import Template
from utils.json_parser_functions import get_transformation_dp_values
from utils.library_functions import get_library_transformation_name, get_library_transformation_names
from jinja2 import Template as JinjaTemplate
from utils.logger import print_and_log


def process_nodes(nodes: list, include_contracts: bool, node_flow_mapping: dict) -> tuple[str, int, int]:
    """
    Processes the nodes from the JSON data and appends the corresponding XML elements to the root element.

    Args:
        nodes: (list) The list of nodes from the JSON data.
        include_contracts: (bool) Flag to include the contracts in the data processing nodes.
        node_flow_mapping: (dict) The mapping of the nodes and their connections.

    Returns:
        dataProcessings_filled_content: (str) The filled content of the data processing nodes.
        nodes_cont (int): Number of nodes in the workflow.
        mapped_nodes (int): Number of nodes that were mapped to a library transformation.
    """
    dataProcessings_filled_content = ""
    library_transformation_names = get_library_transformation_names('library_hashing/library_transformation_names.json')
    mapped_nodes = 0
    nodes_cont = 0

    for index, node in enumerate(nodes):
        node_id = node.get("id", index)
        node_name = node.get("node_name", f"Node_{index}")

        # Get library transformation name
        library_transformation_name = get_library_transformation_name('library_hashing/library_function_hashing.json',
                                                                      node, index)

        # Get the data processing values for the node type
        dataprocessing_values = get_transformation_dp_values(node, node_id, node_name, include_contracts,
                                                             library_transformation_name)

        # If the node is not a reader/writer/connector node, increment the nodes_cont counter
        if dataprocessing_values["input_filepath"] == "":
            nodes_cont += 1

        # Check if the library transformation name exists and the template file exists. If so, use the template file.
        # Otherwise, use the unknownDataProcessing template.
        dp_templates_path = "templates/data_processing"
        dp_template_filepath = f"{dp_templates_path}/{library_transformation_name}_template.xmi"
        if library_transformation_name in library_transformation_names and os.path.exists(dp_template_filepath):

            mapped_nodes += 1
            print_and_log(f"KNIME node: {node_name} -> mapped to library transformation: {library_transformation_name}")

            # Read the workflow template file
            with open(dp_template_filepath, "r") as file:
                data_processing_jinja_template = JinjaTemplate(file.read())

        else:
            # Read the workflow template file
            with open(f"{dp_templates_path}/unknownDataProcessing.xmi", "r") as file:
                data_processing_jinja_template = JinjaTemplate(file.read())

        # Fill the template with jinja2
        dataProcessings_filled_content += data_processing_jinja_template.render(dataprocessing=dataprocessing_values) + "\n"

    return dataProcessings_filled_content, nodes_cont, mapped_nodes


def process_links(data: dict, nodes: list) -> tuple[str, dict]:
    """
    Processes the links between nodes from the JSON data and appends the corresponding XML elements to the root element.
    It also return a dict in which we have the node_name, the previous node_id and the next node_id. The dict is ordered
    so the connections between next_node_id and previous_node_id are consecutive.

    Args:
        data (dict): The JSON data containing the workflow information.
        nodes (list): The list of nodes from the JSON data.

    Returns:
        links_filled_content: (str) The filled content of the links.
        node_flow_mapping: (dict) an ordered dict with the node_name, the previous node_id and the next node_id.
    """

    node_mapping = {}
    # The node_flow_mapping is a list of dictionaries with the node_name, the previous node_id and the next node_id.
    node_flow_mapping = {}

    for index, node in enumerate(nodes):
        node_id = node.get("id", index)
        node_name = node.get("node_name", f"Node_{index}")
        node_mapping[node_id] = {"index": index, "name": node_name}
        node_flow_mapping[node_id] = {"node_name": node_name, "previous_node_id": None, "next_node_id": None}

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

            link_values = {
                "source": source_id,
                "target": dest_id,
                "transformation_name_source": source_transformation_name,
                "transformation_name_target": target_transformation_name
            }

            # Update the node flow link mapping
            node_flow_mapping[source_id]["next_node_id"] = dest_id
            node_flow_mapping[dest_id]["previous_node_id"] = source_id

            # Fill the template
            links_filled_content += link_template.safe_substitute(
                link_values
            ) + "\n"
            link_index += 1

    # Order the node_flow_mapping so the conncetions between next_node_id and previous_node_id are consecutive.
    ordered_mapping = {}
    current_node_id = next(
        node_id for node_id, details in node_flow_mapping.items() if details['previous_node_id'] is None)

    while current_node_id is not None:
        ordered_mapping[current_node_id] = node_flow_mapping[current_node_id]
        next_node_id = node_flow_mapping[current_node_id]['next_node_id']
        current_node_id = next_node_id if next_node_id in node_flow_mapping else None

    return links_filled_content, node_flow_mapping


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
        # Si el connection['sourceID'] o el connection['destID'] no están en el id_mapping, seelimina la conexión
        if connection['sourceID'] not in id_mapping or connection['destID'] not in id_mapping:
            connections.remove(connection)
            continue
        connection['sourceID'] = id_mapping[connection['sourceID']]
        connection['destID'] = id_mapping[connection['destID']]

    return nodes, connections


def json_to_xmi_workflow_with_templates(json_input_folder: str, workflow_filename: str, xmi_output_folder: str,
                                        include_contracts=True) -> tuple[int, int]:
    """
    Converts a JSON workflow file to an XMI file using templates for the data processing nodes.

    Args:
        json_input_folder (str): Path to the folder containing the JSON files.
        workflow_filename (str): Name of the JSON file (without extension).
        xmi_output_folder (str): Path to the folder where the XMI file will be saved.
        include_contracts (bool): Flag to include the contracts in the data processing nodes.

    Returns:
        tuple: Number of nodes mapped successfully and total number of nodes.

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

        # Process links
        links_filled_content, node_flow_mapping = process_links(data, nodes)

        # Process nodes and get the node mapping
        data_processing_filled_content, nodes_cont, mapped_nodes = process_nodes(nodes,
                                                                                 include_contracts,node_flow_mapping)

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

    return mapped_nodes, nodes_cont
