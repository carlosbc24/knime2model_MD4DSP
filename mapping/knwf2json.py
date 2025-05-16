import os
import json
import zipfile
import xml.etree.ElementTree as elementTree
from utils.knime_nodes_data_extraction import extract_input_output_node_settings, extract_imputation_node_settings, \
    extract_row_filter_node_settings, extract_mapping_node_settings, extract_binner_node_settings, \
    extract_math_formula_node_settings, extract_columns_data, extract_column_expressions_node_settings
from utils.logger import print_and_log, print_and_log_dict


def extract_data_knime2json(knwf_filename: str, input_folder: str, output_folder: str, output_json_filename: str):
    """
    Extracts the nodes and connections from a KNIME workflow file and saves them in a JSON file.

    :param knwf_filename: string with the name of the KNIME workflow file to parse
    :param input_folder: string with the path to the folder containing the KNIME workflow files
    :param output_folder: string with the path to the folder where the JSON files will be saved
    :param output_json_filename: string with the name of the JSON file to save the extracted data
    :return:
    """
    # Path to the KNIME workflow file
    knwf_file_path = f"{input_folder}/{knwf_filename}"
    knwf_filename_without_extension = knwf_file_path.split("/")[-1].split(".")[0]

    # Extract the KNIME workflow files
    extract_path = f"{input_folder}/extracted_data/{knwf_filename_without_extension}"
    if not os.path.exists(extract_path):
        os.makedirs(extract_path)
    print(f"Extracting {knwf_filename_without_extension} workflow...")
    with zipfile.ZipFile(knwf_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

    # Path to the workflow.knime file
    workflow_knime_path = os.path.join(extract_path, knwf_filename_without_extension, "workflow.knime")

    # Define the namespace for the XML file
    namespace = {"knime": "http://www.knime.org/2008/09/XMLConfig"}

    # Parse the XML file
    tree = elementTree.parse(workflow_knime_path)
    root = tree.getroot()

    nodes = []
    connections = []

    # Extract nodes
    for node in root.findall(".//knime:config[@key='nodes']/knime:config", namespace):
        node_id = node.find(".//knime:entry[@key='id']", namespace)
        node_settings_file = node.find(".//knime:entry[@key='node_settings_file']", namespace)

        if node_id is not None and node_settings_file is not None:
            nodes.append({
                "id": int(node_id.attrib["value"]),
                "node_settings_file": node_settings_file.attrib["value"]
            })

    # Extract connections
    for connection in root.findall(".//knime:config[@key='connections']/knime:config", namespace):
        source_id = connection.find(".//knime:entry[@key='sourceID']", namespace)
        dest_id = connection.find(".//knime:entry[@key='destID']", namespace)

        if source_id is not None and dest_id is not None:
            connections.append({
                "sourceID": int(source_id.attrib["value"]),
                "destID": int(dest_id.attrib["value"])
            })

    # Extract node settings
    new_nodes = []
    new_connections = []
    max_node_id = max(node["id"] for node in nodes)  # Get the current maximum node ID

    for node in nodes:
        settings_path = os.path.join(extract_path, knwf_filename_without_extension, node["node_settings_file"])
        if os.path.exists(settings_path):
            nodes_info = extract_node_settings(settings_path)
            if len(nodes_info) == 1:
                node.update(nodes_info[0])
            else:
                original_node_id = node["id"]
                previous_node_id = original_node_id
                dest_id_from_original_node = next(
                    (connection["destID"] for connection in connections if connection["sourceID"] == original_node_id),
                    None)
                for i, node_info in enumerate(nodes_info):
                    new_node = node.copy()
                    new_node.update(node_info)
                    if i == 0:
                        new_node["id"] = original_node_id
                    else:
                        max_node_id += 1
                        new_node["id"] = max_node_id
                    new_nodes.append(new_node)
                    if i > 0:
                        # If it is the last node, add a connection between the current node's id
                        # and the destID of the next node in the original list of links
                        if i == len(nodes_info) - 1 and dest_id_from_original_node is not None:
                            # Remove from connections the connection that has destID equal to
                            # dest_id_from_original_node if it exists
                            connections = [connection for connection
                                           in connections if connection["destID"] != dest_id_from_original_node]
                            new_connections.append({
                                "sourceID": new_node["id"],
                                "destID": dest_id_from_original_node
                            })
                        # If it is not the first node or the last node,
                        # add a connection between the previous node and the new node
                        new_connections.append({
                            "sourceID": previous_node_id,
                            "destID": new_node["id"]
                        })
                    previous_node_id = new_node["id"]

    nodes.extend(new_nodes)
    connections.extend(new_connections)

    # Filter nodes without node_name
    nodes = [node for node in nodes if "node_name" in node]

    # Remove node_settings_file key
    for node in nodes:
        if "node_settings_file" in node:
            del node["node_settings_file"]

    # Save the extracted data in a JSON file
    output_json_filepath = f"{output_folder}/{knwf_filename_without_extension}/{output_json_filename}"

    if not os.path.exists(os.path.dirname(output_json_filepath)):
        os.makedirs(os.path.dirname(output_json_filepath))
    # Check if the file already exists. If the file does exist, add the suffix "_1" to the filename. If the file
    # exists, add a suffix "_n" to the filename, where n is the lowest integer that makes the filename unique.
    if os.path.exists(output_json_filepath):
        i = 1
        while os.path.exists(output_json_filepath):
            output_json_filepath = (f"{output_folder}/{knwf_filename_without_extension}"
                                    f"/{output_json_filename.split('.')[0]}_{i}.json")
            i += 1

    # Save the extracted data in a JSON file
    result = {"nodes": nodes, "connections": connections}
    with open(output_json_filepath + ".json", "w", encoding="utf-8") as json_file:
        json.dump(result, json_file, indent=4)

    print_and_log(f"Data extracted from {knwf_filename_without_extension} workflow and saved in {output_json_filepath}")


def extract_node_settings(settings_path: str) -> list[dict]:
    """
    Extracts the node settings from the settings file of a KNIME node.

    Args:
        settings_path (str): Path to the settings file of a KNIME node.

    Returns:
        list[dict]: List of dictionaries with the node settings.

    """
    namespace = {"knime": "http://www.knime.org/2008/09/XMLConfig"}
    tree = elementTree.parse(settings_path)
    root = tree.getroot()

    node_name_entry = root.find(".//knime:entry[@key='node-name']", namespace)
    node_type_entry = root.find(".//knime:entry[@key='factory']", namespace)

    nodes_info = []

    node_info = {
        "node_name": node_name_entry.attrib["value"] if node_name_entry is not None else "Unknown",
        "node_type": node_type_entry.attrib["value"] if node_type_entry is not None else "Unknown",
        "parameters": {}
    }

    # Extract parameters from the data_model
    model = root.find(".//knime:config[@key='model']", namespace)
    if model is not None:

        # Extract the input and output KNIME nodes
        if ("CSV Reader" in node_info["node_name"] or "Excel Reader" in node_info["node_name"] or "File Reader"
                in node_info["node_name"] or "Table Reader"
                in node_info["node_name"] or "CSV Writer" in node_info["node_name"]):
            node_info = extract_input_output_node_settings(node_info, root, model, namespace)

            print_and_log_dict(node_info)
            nodes_info.append(node_info)

        # Extract the imputation nodes info
        elif "Missing Value" in node_info["node_name"] or "Numeric Outliers" in node_info["node_name"]:
            nodes_info = extract_imputation_node_settings(node_info, model, root, namespace, nodes_info)

        # Extract the column filter node info
        elif "Column Filter" in node_info["node_name"]:
            column_filter = model.find(".//knime:config[@key='column-filter']", namespace)
            if column_filter is not None:
                included_names, excluded_names = extract_columns_data(column_filter, namespace)
                node_info["parameters"]["in_columns"] = included_names
                node_info["parameters"]["out_columns"] = excluded_names

            print_and_log_dict(node_info)
            nodes_info.append(node_info)

        # Extract the row filter node info
        elif "Row Filter" in node_info["node_name"]:
            node_info = extract_row_filter_node_settings(node_info, model, namespace)

            print_and_log_dict(node_info)
            nodes_info.append(node_info)

        # Extract the mapping node info
        elif ("String Replacer" in node_info["node_name"] or "Rule Engine"
              in node_info["node_name"] or "String Manipulation"
              in node_info["node_name"] or "String Manipulation (Multi Column)"
              in node_info["node_name"]):
            node_info = extract_mapping_node_settings(node_info, model, namespace)

            print_and_log_dict(node_info)
            nodes_info.append(node_info)

        # Extract the binner node info
        elif "Numeric Binner" in node_info["node_name"] or "Auto-Binner" in node_info["node_name"]:
            nodes_info = extract_binner_node_settings(node_info, model, namespace, nodes_info)

        # Extract the categorical to numerical node info
        elif "String to Number" in node_info["node_name"]:
            # Extract the column to convert
            decimal_separator_entry = model.find(".//knime:entry[@key='decimal_separator']", namespace)
            if decimal_separator_entry is not None:
                node_info["parameters"]["decimal_separator"] = decimal_separator_entry.attrib["value"]
            # Extract the included and excluded columns
            included_names, excluded_names = extract_columns_data(model, namespace)
            node_info["parameters"]["in_columns"] = included_names + excluded_names
            node_info["parameters"]["out_columns"] = included_names

            print_and_log_dict(node_info)
            nodes_info.append(node_info)

        # Extract the math formula node info
        elif "Math Formula" in node_info["node_name"]:
            nodes_info = extract_math_formula_node_settings(node_info, model, namespace, nodes_info)

        elif "Column Renamer" in node_info["node_name"]:
            renamings = model.findall(".//knime:config[@key='renamings']/knime:config", namespace)
            node_info["parameters"]["renamings"] = [{
                "old": r.find("knime:entry[@key='oldName']", namespace).attrib["value"],
                "new": r.find("knime:entry[@key='newName']", namespace).attrib["value"]
            } for r in renamings if r.find("knime:entry[@key='oldName']", namespace) is not None]

            print_and_log_dict(node_info)
            nodes_info.append(node_info)

        elif "Number To String" in node_info["node_name"]:
            # Extract the included and excluded columns
            included_names, excluded_names = extract_columns_data(model, namespace)
            node_info["parameters"]["in_columns"] = included_names + excluded_names
            node_info["parameters"]["out_columns"] = included_names

            print_and_log_dict(node_info)
            nodes_info.append(node_info)

        elif "String To Date&Time" in node_info["node_name"]:
            # Extract the included and excluded columns
            included_names, excluded_names = extract_columns_data(model, namespace)
            node_info["parameters"]["in_columns"] = included_names + excluded_names
            node_info["parameters"]["out_columns"] = included_names

            print_and_log_dict(node_info)
            nodes_info.append(node_info)

        elif "Column Expressions" in node_info["node_name"]:
            # Extract the settings for the column expressions
            nodes_info = extract_column_expressions_node_settings(node_info, model, namespace, nodes_info)

        elif "Missing Value Column Filter" in node_info["node_name"]:
            # Extract the missing value threshold
            missing_value_threshold_entry = model.find(".//knime:entry[@key='missing_value_percentage']", namespace)
            # Extract the included and excluded columns
            included_names, excluded_names = extract_columns_data(model, namespace)
            node_info["parameters"]["missing_value_threshold"] = (
                float(missing_value_threshold_entry.attrib["value"])
                if missing_value_threshold_entry is not None else None
            )
            node_info["parameters"]["in_columns"] = included_names + excluded_names
            node_info["parameters"]["out_columns"] = included_names

            print_and_log_dict(node_info)
            nodes_info.append(node_info)

        elif "Constant Value Column Filter" in node_info["node_name"]:
            # Extract the included and excluded columns
            included_names, excluded_names = extract_columns_data(model, namespace)
            node_info["parameters"]["in_columns"] = included_names + excluded_names
            node_info["parameters"]["out_columns"] = included_names

            print_and_log_dict(node_info)
            nodes_info.append(node_info)

        elif "Column Expressions" in node_info["node_name"]:
            # Extract the included and excluded columns
            included_names, excluded_names = extract_columns_data(model, namespace)
            node_info["parameters"]["in_columns"] = included_names + excluded_names
            node_info["parameters"]["out_columns"] = included_names

            print_and_log_dict(node_info)
            nodes_info.append(node_info)

        elif "String Cleaner" in node_info["node_name"]:
            # Extract the included and excluded columns
            included_names, excluded_names = extract_columns_data(model, namespace)
            node_info["parameters"]["in_columns"] = included_names + excluded_names
            node_info["parameters"]["out_columns"] = included_names

            print_and_log_dict(node_info)
            nodes_info.append(node_info)

        elif "Sorter" in node_info["node_name"]:
            # Extract the included and excluded columns
            included_names, excluded_names = extract_columns_data(model, namespace)
            node_info["parameters"]["in_columns"] = included_names + excluded_names
            node_info["parameters"]["out_columns"] = included_names

            print_and_log_dict(node_info)
            nodes_info.append(node_info)

        elif "Category to Number (Apply)" in node_info["node_name"]:
            # Extract the included and excluded columns
            included_names, excluded_names = extract_columns_data(model, namespace)
            node_info["parameters"]["in_columns"] = included_names + excluded_names
            node_info["parameters"]["out_columns"] = included_names

            print_and_log_dict(node_info)
            nodes_info.append(node_info)

        else:
            print_and_log(f"Node type not recognized: {node_info['node_name']}")

    return nodes_info
