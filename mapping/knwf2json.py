
import os
import json
import zipfile
import xml.etree.ElementTree as elementTree
import copy
from utils.logger import print_and_log


def extract_columns_data(model: elementTree.Element, namespace: dict) -> (list, list):
    """
    Extracts the columns data from the data model XML element.
    Args:
        model (elementTree.Element): The data model XML element.
        namespace (dict): The namespace for the XML file.

    Returns:
        tuple: Tuple containing the included and excluded columns data.
    """
    # Extract the included columns
    included_names = model.findall(".//knime:config[@key='included_names']/knime:entry", namespace)
    if included_names is not None:
        included_names = [
            {"column_name": col.attrib["value"], "column_type": col.attrib["type"]}
            for col in included_names if col.attrib["key"] != "array-size"
        ]
    # Extract the excluded columns
    excluded_names = model.findall(".//knime:config[@key='excluded_names']/knime:entry", namespace)
    if excluded_names is not None:
        excluded_names = [
            {"column_name": col.attrib["value"], "column_type": col.attrib["type"]}
            for col in excluded_names if col.attrib["key"] != "array-size"
        ]
    return included_names, excluded_names


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
                destID_from_original_node = next(
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
                        # Si se trata del último nodo, añadir una conexión entre el id del nodo actual
                        # y el destID del nodo siguiente al original presente en la lista de links
                        if i == len(nodes_info) - 1 and destID_from_original_node is not None:
                            # Remove from connections the connection that has destID equal to destID_from_original_node if it exists
                            connections = [connection for connection in connections if connection["destID"] != destID_from_original_node]
                            new_connections.append({
                                "sourceID": new_node["id"],
                                "destID": destID_from_original_node
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

    # Filtrar nodos que tienen 'node_name'
    nodes = [node for node in nodes if "node_name" in node]

    # Remove node_settings_file key
    for node in nodes:
        if "node_settings_file" in node:
            del node["node_settings_file"]

    # Save the extracted data in a JSON file
    ouput_json_filepath = (f"{output_folder}/{knwf_filename_without_extension}"
                           f"/{output_json_filename}")

    if not os.path.exists(os.path.dirname(ouput_json_filepath)):
        os.makedirs(os.path.dirname(ouput_json_filepath))
    # Check if the file already exists. If the file does exist, add the suffix "_1" to the filename. If the file
    # exists, add a suffix "_n" to the filename, where n is the lowest integer that makes the filename unique.
    if os.path.exists(ouput_json_filepath):
        i = 1
        while os.path.exists(ouput_json_filepath):
            ouput_json_filepath = (f"{output_folder}/{knwf_filename_without_extension}"
                                   f"/{output_json_filename.split('.')[0]}_{i}.json")
            i += 1

    # Save the extracted data in a JSON file
    result = {"nodes": nodes, "connections": connections}
    with open(ouput_json_filepath+".json", "w", encoding="utf-8") as json_file:
        json.dump(result, json_file, indent=4)

    print_and_log(f"Data extracted from {knwf_filename_without_extension} workflow and saved in {ouput_json_filepath}")


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
        if "CSV Reader" in node_info["node_name"]:
            csv_reader = model.find(".//knime:config[@key='settings']", namespace)
            if csv_reader is not None:
                # Extract CSV path
                path_entry = csv_reader.find(
                    ".//knime:config[@key='file_selection']/knime:config[@key='path']/knime:entry[@key='path']",
                    namespace)
                if path_entry is not None:
                    node_info["parameters"]["file_path"] = path_entry.attrib["value"]
                # Extract column delimiter
                column_delimiter_entry = csv_reader.find(".//knime:entry[@key='column_delimiter']", namespace)
                if column_delimiter_entry is not None:
                    node_info["parameters"]["column_delimiter"] = column_delimiter_entry.attrib["value"]

            nodes_info.append(node_info)

        elif "Excel Reader" in node_info["node_name"] or "File Reader" in node_info["node_name"]:
            file_path_entry = model.find(".//knime:config[@key='path']/knime:entry[@key='path']", namespace)
            if file_path_entry is not None:
                node_info["parameters"]["file_path"] = file_path_entry.attrib["value"]

            nodes_info.append(node_info)

        elif "Column Filter" in node_info["node_name"]:
            column_filter = model.find(".//knime:config[@key='column-filter']", namespace)
            if column_filter is not None:
                included_names, excluded_names = extract_columns_data(column_filter, namespace)
                node_info["parameters"]["in_columns"] = included_names + excluded_names
                node_info["parameters"]["out_columns"] = included_names

            nodes_info.append(node_info)

        elif "Row Filter (deprecated)" in node_info["node_name"]:
            row_filter = model.find(".//knime:config[@key='rowFilter']", namespace)
            if row_filter is not None:
                # Extract the row range
                start_entry = row_filter.find("knime:entry[@key='RowRangeStart']", namespace)
                end_entry = row_filter.find("knime:entry[@key='RowRangeEnd']", namespace)
                if start_entry is not None and end_entry is not None:
                    node_info["parameters"]["row_range"] = {
                        "start": start_entry.attrib["value"],
                        "end": end_entry.attrib["value"]
                    }
                # Extract the filter type (EQUAL, CONTAINS, etc.)
                filter_type_entry = row_filter.find("knime:entry[@key='RowFilter_TypeID']", namespace)
                if filter_type_entry is not None and filter_type_entry.attrib["value"] == "RangeVal_RowFilter":
                    node_info["parameters"]["filter_type"] = filter_type_entry.attrib["value"]
                    # Extract the columns to filter
                    columns = row_filter.findall("knime:entry[@key='ColumnName']", namespace)
                    node_info["parameters"]["in_columns"] = [
                        {"column_name": col.attrib["value"], "column_type": col.attrib["type"]}
                        for col in columns
                    ]
                    node_info["parameters"]["out_columns"] = [
                        {"column_name": col.attrib["value"], "column_type": col.attrib["type"]}
                        for col in columns
                    ]
                    # Extract lower value and upper value
                    lower_bound = row_filter.find(
                        "knime:config[@key='lowerBound']/knime:config/knime:entry[@key='IntCell']", namespace)
                    upper_bound = row_filter.find("knime:config[@key='upperBound']/knime:entry[@key='datacell']",
                                                  namespace)
                    lower_bound_value = lower_bound.attrib[
                        "value"] if lower_bound is not None else None
                    upper_bound_value = upper_bound.attrib[
                        "value"] if upper_bound is not None else None
                    if lower_bound_value is not None and lower_bound_value != "":
                        node_info["parameters"]["lower_bound"] = lower_bound_value
                    else:
                        node_info["parameters"]["lower_bound"] = 0
                    if upper_bound_value is not None and upper_bound_value != "":
                        node_info["parameters"]["upper_bound"] = upper_bound_value
                    else:
                        node_info["parameters"]["upper_bound"] = 0
                    # Flags to indicate if the upeer and lower bounds are not None
                    node_info["parameters"]["has_lower_bound"] = lower_bound is not None and lower_bound.attrib.get(
                        "value") != ""
                    node_info["parameters"]["has_upper_bound"] = upper_bound is not None and upper_bound.attrib.get(
                        "value") != ""
            nodes_info.append(node_info)

        elif "Column Renamer" in node_info["node_name"]:
            renamings = model.findall(".//knime:config[@key='renamings']/knime:config", namespace)
            node_info["parameters"]["renamings"] = [{
                "old": r.find("knime:entry[@key='oldName']", namespace).attrib["value"],
                "new": r.find("knime:entry[@key='newName']", namespace).attrib["value"]
            } for r in renamings if r.find("knime:entry[@key='oldName']", namespace) is not None]

            nodes_info.append(node_info)

        elif "Missing Value" in node_info["node_name"]:

            column_settings = root.findall(".//knime:config[@key='columnSettings']/knime:config", namespace)
            factory_dict = {}

            for column in column_settings:
                col_name_entry = column.find(".//knime:config[@key='colNames']/knime:entry[@key='0']", namespace)
                factory_id_entry = column.find(".//knime:config[@key='settings']/knime:entry[@key='factoryID']",
                                               namespace)
                fix_string_value_entry = column.find(
                    ".//knime:config[@key='settings']/knime:entry[@key='fixStringValue']", namespace)

                if col_name_entry is not None and factory_id_entry is not None:
                    factory_id = factory_id_entry.attrib["value"]
                    column_name = col_name_entry.attrib["value"]
                    fix_string_value = fix_string_value_entry.attrib[
                        "value"] if fix_string_value_entry is not None else None

                    if factory_id not in factory_dict:
                        factory_dict[factory_id] = {
                            "in_columns": [],
                            "out_columns": [],
                            "fixStringValues": []
                        }

                    factory_dict[factory_id]["in_columns"].append(
                        {"column_name": column_name, "column_type": "xstring"})
                    factory_dict[factory_id]["out_columns"].append(
                        {"column_name": column_name, "column_type": "xstring"})
                    factory_dict[factory_id]["fixStringValues"].append(fix_string_value)

            for factory_id, data in factory_dict.items():
                node_info = {
                    "node_name": "Missing Value",
                    "parameters": {
                        "in_columns": data["in_columns"],
                        "out_columns": data["out_columns"],
                        "imputationType": "Interpolation" if "Interpolation" in factory_id else "Mean" if
                        "Mean" in factory_id else "MostFrequent" if "MostFrequent" in factory_id else "Fixed Value" if
                        "Fixed" in factory_id else "Previous" if "Previous" in factory_id else "Next" if
                        "Next" in factory_id else "Median" if "Median" in factory_id else "Closest" if "Closest" in
                        factory_id else "None",
                        "fixStringValues": data["fixStringValues"]
                    }
                }
                nodes_info.append(node_info)

        elif "String Replacer" in node_info["node_name"]:
            column_entry = model.find("knime:entry[@key='colName']", namespace)
            pattern_entry = model.find("knime:entry[@key='pattern']", namespace)
            replacement_entry = model.find("knime:entry[@key='replacement']", namespace)
            if column_entry is not None and pattern_entry is not None and replacement_entry is not None:
                node_info["parameters"]["replacement"] = {
                    "column": column_entry.attrib["value"],
                    "pattern": pattern_entry.attrib["value"],
                    "replacement": replacement_entry.attrib["value"]
                }

            nodes_info.append(node_info)

        elif "String to Number" in node_info["node_name"]:
            # Extract the column to convert
            decimal_separator_entry = model.find(".//knime:entry[@key='decimal_separator']", namespace)
            if decimal_separator_entry is not None:
                node_info["parameters"]["decimal_separator"] = decimal_separator_entry.attrib["value"]
            # Extract the included and excluded columns
            included_names, excluded_names = extract_columns_data(model, namespace)
            node_info["parameters"]["in_columns"] = included_names + excluded_names
            node_info["parameters"]["out_columns"] = included_names

            nodes_info.append(node_info)

        elif "Rule Engine" in node_info["node_name"]:
            rules_entries = model.findall(".//knime:config[@key='rules']/knime:entry", namespace)
            rules = [entry.attrib["value"] for entry in rules_entries if entry.attrib["key"].isdigit()]
            new_column = model.find(".//knime:entry[@key='new-column-name']", namespace)
            replace_column = model.find(".//knime:entry[@key='replace-column-name']", namespace)
            append_column = model.find(".//knime:entry[@key='append-column']", namespace)
            node_info["parameters"]["rules"] = rules
            node_info["parameters"]["new_column_name"] = new_column.attrib["value"] if new_column is not None else None
            node_info["parameters"]["replace_column_name"] = replace_column.attrib["value"] if replace_column is not None else None
            node_info["parameters"]["append_column"] = append_column.attrib["value"] == "true" if append_column is not None else False
            replace_column = model.findall(".//knime:entry[@key='replace-column-name']", namespace)
            in_columns = []
            if replace_column is not None:
                in_columns = [
                    {"column_name": col.attrib["value"], "column_type": col.attrib["type"]}
                    for col in replace_column if col.attrib["key"] != "array-size"
                ]
            node_info["parameters"]["in_columns"] = in_columns

            nodes_info.append(node_info)

        elif "Numeric Outliers" in node_info["node_name"]:
            included_names = root.findall(
                ".//knime:config[@key='outlier-list']/knime:config[@key='included_names']/knime:entry", namespace)

            node_info["parameters"]["in_columns"] = [{"column_name": col.attrib["value"], "column_type": "xstring"} for
                                                    col in included_names if col.attrib["key"] != "array-size"]

            node_info["parameters"]["out_columns"] = [{"column_name": col.attrib["value"], "column_type": "xstring"} for
                                                     col in included_names if col.attrib["key"] != "array-size"]

            # Extract estimation-type
            estimation_type = model.find(".//knime:entry[@key='estimation-type']", namespace)
            if estimation_type is not None:
                node_info["parameters"]["estimation_type"] = estimation_type.attrib["value"]

            # Extract iqr-scalar
            iqr_scalar = model.find(".//knime:entry[@key='iqr-scalar']", namespace)
            if iqr_scalar is not None:
                node_info["parameters"]["iqr_scalar"] = float(iqr_scalar.attrib["value"])

            # Extract replacement-strategy
            replacement_strategy = model.find(".//knime:entry[@key='replacement-strategy']", namespace)
            if replacement_strategy is not None:
                node_info["parameters"]["replacement_strategy"] = replacement_strategy.attrib["value"]

            # Extract outlier-treatment
            outlier_treatment = model.find(".//knime:entry[@key='outlier-treatment']", namespace)
            if outlier_treatment is not None:
                node_info["parameters"]["outlier_treatment"] = outlier_treatment.attrib["value"]

            # Extract detection-option
            detection_option = model.find(".//knime:entry[@key='detection-option']", namespace)
            if detection_option is not None:
                node_info["parameters"]["detection_option"] = detection_option.attrib["value"]

            nodes_info.append(node_info)

        elif "Numeric Binner" in node_info["node_name"]:

            binned_columns = model.findall(".//knime:config[@key='binned_columns']/knime:entry", namespace)
            for column_entry in binned_columns:
                column_name = column_entry.attrib["value"]
                new_column_name_entry = model.find(f".//knime:entry[@key='{column_name}_is_appended']", namespace)
                new_column_name = new_column_name_entry.attrib["value"] if new_column_name_entry is not None else None
                bins = model.findall(f".//knime:config[@key='{column_name}']/knime:config", namespace)
                bin_info = []
                for bin in bins:
                    bin_name = bin.find("knime:entry[@key='bin_name']", namespace).attrib["value"]
                    left_open = bin.find("knime:entry[@key='left_open']", namespace).attrib["value"] == "true"
                    right_open = bin.find("knime:entry[@key='right_open']", namespace).attrib["value"] == "true"
                    left_value = bin.find("knime:entry[@key='left_value']", namespace).attrib["value"]
                    right_value = bin.find("knime:entry[@key='right_value']", namespace).attrib["value"]
                    closureType = "openOpen" if left_open and right_open else "openClosed" if left_open and not right_open else "closedOpen" if not left_open and right_open else "closedClosed"
                    bin_info.append({
                        "binName": bin_name,
                        "closureType": closureType,
                        "leftMargin": left_value,
                        "rightMargin": right_value
                    })
                if new_column_name is not None and bin_info:
                    new_node_info = copy.deepcopy(node_info)  # Usar deepcopy para evitar referencias compartidas
                    new_node_info["parameters"] = {
                        "bins": bin_info,
                        "in_columns": [{"column_name": column_name, "column_type": "xstring"}],
                        "out_columns": [{"column_name": new_column_name, "column_type": "xstring"}]
                    }
                    nodes_info.append(new_node_info)

        elif "Auto-Binner" in node_info["node_name"]:
            # Extract the included and excluded columns
            included_names, excluded_names = extract_columns_data(model, namespace)
            # Extract binning method
            binning_method_entry = model.find(".//knime:entry[@key='method']", namespace)
            binning_method = binning_method_entry.attrib["value"] if binning_method_entry is not None else None
            # Extract bin count
            bin_count_entry = model.find(".//knime:entry[@key='binCount']", namespace)
            bin_count = int(bin_count_entry.attrib["value"]) if bin_count_entry is not None else None
            # Extract equality method
            equality_method_entry = model.find(".//knime:entry[@key='equalityMethod']", namespace)
            equality_method = equality_method_entry.attrib["value"] if equality_method_entry is not None else None
            # Extract integer bounds
            integer_bounds_entry = model.find(".//knime:entry[@key='integerBounds']", namespace)
            integer_bounds = integer_bounds_entry.attrib[
                                 "value"] == "true" if integer_bounds_entry is not None else False
            # Extract sample quantiles
            sample_quantiles = [
                float(q.attrib["value"])
                for q in model.findall(".//knime:config[@key='sampleQuantiles']/knime:entry", namespace)
            ]
            # Extract bin naming
            bin_naming_entry = model.find(".//knime:entry[@key='binNaming']", namespace)
            bin_naming = bin_naming_entry.attrib["value"] if bin_naming_entry is not None else None
            # Extract replace column
            replace_column_entry = model.find(".//knime:entry[@key='replaceColumn']", namespace)
            replace_column = replace_column_entry.attrib[
                                 "value"] == "true" if replace_column_entry is not None else False
            # Extract precision and output format
            precision_entry = model.find(".//knime:entry[@key='precision']", namespace)
            precision = int(precision_entry.attrib["value"]) if precision_entry is not None else None
            output_format_entry = model.find(".//knime:entry[@key='outputFormat']", namespace)
            output_format = output_format_entry.attrib["value"] if output_format_entry is not None else None
            # Save the parameters in the node_info dictionary
            node_info["parameters"] = {
                "in_columns": included_names + excluded_names,
                "out_columns": included_names,
                "binning_method": binning_method,
                "bin_count": bin_count,
                "equality_method": equality_method,
                "integer_bounds": integer_bounds,
                "sample_quantiles": sample_quantiles,
                "bin_naming": bin_naming,
                "replace_column": replace_column,
                "precision": precision,
                "output_format": output_format
            }

            nodes_info.append(node_info)

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

            nodes_info.append(node_info)

        elif "Duplicate Row Filter" in node_info["node_name"]:
            remove_duplicates_entry = model.find(".//knime:entry[@key='remove_duplicates']", namespace)
            retain_order_entry = model.find(".//knime:entry[@key='retain_order']", namespace)
            row_selection_entry = model.find(".//knime:entry[@key='row_selection']", namespace)
            add_row_duplicate_flag_entry = model.find(".//knime:entry[@key='add_row_duplicate_flag']", namespace)
            in_memory_entry = model.find(".//knime:entry[@key='in_memory']", namespace)
            included_names, excluded_names = extract_columns_data(model, namespace)

            node_info["parameters"]["remove_duplicates"] = remove_duplicates_entry.attrib["value"] == "true" if remove_duplicates_entry is not None else False
            node_info["parameters"]["retain_order"] = retain_order_entry.attrib["value"] == "true" if retain_order_entry is not None else False
            node_info["parameters"]["row_selection"] = row_selection_entry.attrib["value"] if row_selection_entry is not None else None
            node_info["parameters"]["add_row_duplicate_flag"] = add_row_duplicate_flag_entry.attrib["value"] == "true" if add_row_duplicate_flag_entry is not None else False
            node_info["parameters"]["in_memory"] = in_memory_entry.attrib["value"] == "true" if in_memory_entry is not None else False
            node_info["parameters"]["in_columns"] = included_names + excluded_names
            node_info["parameters"]["out_columns"] = included_names

            nodes_info.append(node_info)

        elif "Joiner" in node_info["node_name"]:
            # Extractar configuraciones clave de Joiner
            include_matches_entry = model.find(".//knime:entry[@key='includeMatchesInOutput']", namespace)
            include_left_unmatched_entry = model.find(".//knime:entry[@key='includeLeftUnmatchedInOutput']", namespace)
            include_right_unmatched_entry = model.find(".//knime:entry[@key='includeRightUnmatchedInOutput']",
                                                       namespace)
            composition_mode_entry = model.find(".//knime:entry[@key='compositionMode']", namespace)
            duplicate_handling_entry = model.find(".//knime:entry[@key='duplicateHandling']", namespace)
            if include_matches_entry is not None:
                node_info["parameters"]["include_matches"] = include_matches_entry.attrib["value"] == "true"
            if include_left_unmatched_entry is not None:
                node_info["parameters"]["include_left_unmatched"] = include_left_unmatched_entry.attrib[
                                                                        "value"] == "true"
            if include_right_unmatched_entry is not None:
                node_info["parameters"]["include_right_unmatched"] = include_right_unmatched_entry.attrib[
                                                                         "value"] == "true"
            if composition_mode_entry is not None:
                node_info["parameters"]["composition_mode"] = composition_mode_entry.attrib["value"]
            if duplicate_handling_entry is not None:
                node_info["parameters"]["duplicate_handling"] = duplicate_handling_entry.attrib["value"]

            nodes_info.append(node_info)

        elif "Partitioning" in node_info["node_name"]:
            # Extract the method used for partitioning
            method_entry = model.find(".//knime:entry[@key='method']", namespace)
            sampling_method_entry = model.find(".//knime:entry[@key='samplingMethod']", namespace)
            fraction_entry = model.find(".//knime:entry[@key='fraction']", namespace)
            count_entry = model.find(".//knime:entry[@key='count']", namespace)
            class_column_entry = model.find(".//knime:entry[@key='class_column']", namespace)
            if method_entry is not None:
                node_info["parameters"]["method"] = method_entry.attrib["value"]
            if sampling_method_entry is not None:
                node_info["parameters"]["sampling_method"] = sampling_method_entry.attrib["value"]
            if fraction_entry is not None:
                node_info["parameters"]["fraction"] = float(fraction_entry.attrib["value"])
            if count_entry is not None:
                node_info["parameters"]["count"] = int(count_entry.attrib["value"])
            if class_column_entry is not None:
                node_info["parameters"]["class_column"] = class_column_entry.attrib["value"]

            nodes_info.append(node_info)

        elif "String Manipulation" in node_info["node_name"]:
            # Extract the expression to apply
            expression_entry = model.find(".//knime:entry[@key='expression']", namespace)
            if expression_entry is not None:
                node_info["parameters"]["expression"] = expression_entry.attrib["value"]
            # Extract the column to apply the expression
            replaced_column_entry = model.find(".//knime:entry[@key='replaced_column']", namespace)
            if replaced_column_entry is not None:
                node_info["parameters"]["output_column"] = replaced_column_entry.attrib["value"]
            # Extract the column to append the result
            append_column_entry = model.find(".//knime:entry[@key='append_column']", namespace)
            if append_column_entry is not None:
                node_info["parameters"]["append_column"] = append_column_entry.attrib["value"] == "true"

            nodes_info.append(node_info)

        elif "String Manipulation (Multi Column)" in node_info["node_name"]:
            # Extract the expression to apply
            expression_entry = model.find(".//knime:entry[@key='EXPRESSION']", namespace)
            if expression_entry is not None:
                node_info["parameters"]["expression"] = expression_entry.attrib["value"]
            included_names, excluded_names = extract_columns_data(model, namespace)
            node_info["parameters"]["in_columns"] = included_names + excluded_names
            node_info["parameters"]["out_columns"] = included_names
            # Extract the columns to append the result
            append_or_replace_entry = model.find(".//knime:entry[@key='APPEND_OR_REPLACE']", namespace)
            if append_or_replace_entry is not None:
                node_info["parameters"]["append_or_replace"] = append_or_replace_entry.attrib["value"]
            # Extract the column to append the result
            append_column_suffix_entry = model.find(".//knime:entry[@key='APPEND_COLUMN_SUFFIX']", namespace)
            if append_column_suffix_entry is not None:
                node_info["parameters"]["append_column_suffix"] = append_column_suffix_entry.attrib["value"]

            nodes_info.append(node_info)

        elif "CSV Writer" in node_info["node_name"]:
            file_chooser = model.find(".//knime:config[@key='file_chooser_settings']/knime:config[@key='path']", namespace)
            column_delimiter = model.find(".//knime:entry[@key='column_delimiter']", namespace)
            if file_chooser is not None:
                path_entry = file_chooser.find("knime:entry[@key='path']", namespace)
                if path_entry is not None:
                    node_info["parameters"]["file_path"] = path_entry.attrib["value"]
            if column_delimiter is not None:
                node_info["parameters"]["column_delimiter"] = column_delimiter.attrib["value"]

            nodes_info.append(node_info)

        else:
            print_and_log(f"Node type not recognized: {node_info['node_name']}")

    return nodes_info
