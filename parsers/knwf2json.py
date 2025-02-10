
import os
import json
import zipfile
import xml.etree.ElementTree as ET


def extract_data_knime2json(knwf_filename, input_folder, output_folder, output_json_filename):
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
    tree = ET.parse(workflow_knime_path)
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
    for node in nodes:
        settings_path = os.path.join(extract_path, knwf_filename_without_extension, node["node_settings_file"])
        if os.path.exists(settings_path):
            node.update(extract_node_settings(settings_path))

    # Remove node_settings_file key
    for node in nodes:
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
    with open(ouput_json_filepath, "w", encoding="utf-8") as json_file:
        json.dump(result, json_file, indent=4)

    print(f"Data extracted from {knwf_filename_without_extension} workflow and saved in {ouput_json_filepath}")


def extract_node_settings(settings_path):
    namespace = {"knime": "http://www.knime.org/2008/09/XMLConfig"}
    tree = ET.parse(settings_path)
    root = tree.getroot()

    node_name_entry = root.find(".//knime:entry[@key='node-name']", namespace)
    node_type_entry = root.find(".//knime:entry[@key='factory']", namespace)

    node_info = {
        "node_name": node_name_entry.attrib["value"] if node_name_entry is not None else "Unknown",
        "node_type": node_type_entry.attrib["value"] if node_type_entry is not None else "Unknown",
        "parameters": {}
    }

    # Extract parameters from the model
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

        elif "Excel Reader" in node_info["node_name"]:
            file_path_entry = model.find(".//knime:config[@key='path']/knime:entry[@key='path']", namespace)
            if file_path_entry is not None:
                node_info["parameters"]["file_path"] = file_path_entry.attrib["value"]

        elif "Column Filter" in node_info["node_name"]:
            column_filter = model.find(".//knime:config[@key='column-filter']", namespace)
            if column_filter is not None:
                included_names = column_filter.find(".//knime:config[@key='included_names']", namespace)
                excluded_names = column_filter.find(".//knime:config[@key='excluded_names']", namespace)
                if included_names is not None:
                    node_info["parameters"]["included_columns"] = [
                        entry.attrib["value"] for entry in included_names.findall("knime:entry", namespace)
                        if entry.attrib["key"] != "array-size"
                    ]
                if excluded_names is not None:
                    node_info["parameters"]["excluded_columns"] = [
                        entry.attrib["value"] for entry in excluded_names.findall("knime:entry", namespace)
                        if entry.attrib["key"] != "array-size"
                    ]

        elif "Row Filter" in node_info["node_name"]:
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
                if filter_type_entry is not None:
                    node_info["parameters"]["filter_type"] = filter_type_entry.attrib["value"]
                # Extract the column name to filter
                column_name_entry = row_filter.find("knime:entry[@key='ColumnName']", namespace)
                if column_name_entry is not None:
                    node_info["parameters"]["column_name"] = column_name_entry.attrib["value"]
                # Determine if the filter is inclusive or exclusive
                include_entry = row_filter.find("knime:entry[@key='include']", namespace)
                if include_entry is not None:
                    node_info["parameters"]["include"] = include_entry.attrib["value"] == "true"
                # Extract the case sensitivity of the filter
                case_sensitive_entry = row_filter.find("knime:entry[@key='CaseSensitive']", namespace)
                if case_sensitive_entry is not None:
                    node_info["parameters"]["case_sensitive"] = case_sensitive_entry.attrib["value"] == "true"
                # Extract the pattern to match
                pattern_entry = row_filter.find("knime:entry[@key='Pattern']", namespace)
                if pattern_entry is not None:
                    node_info["parameters"]["pattern"] = pattern_entry.attrib["value"]
                # Determine if the pattern is a regular expression
                wildcards_entry = row_filter.find("knime:entry[@key='hasWildCards']", namespace)
                regex_entry = row_filter.find("knime:entry[@key='isRegExpr']", namespace)
                if wildcards_entry is not None:
                    node_info["parameters"]["has_wildcards"] = wildcards_entry.attrib["value"] == "true"
                if regex_entry is not None:
                    node_info["parameters"]["is_regex"] = regex_entry.attrib["value"] == "true"

        elif "Column Renamer" in node_info["node_name"]:
            renamings = model.findall(".//knime:config[@key='renamings']/knime:config", namespace)
            node_info["parameters"]["renamings"] = [{
                "old": r.find("knime:entry[@key='oldName']", namespace).attrib["value"],
                "new": r.find("knime:entry[@key='newName']", namespace).attrib["value"]
            } for r in renamings if r.find("knime:entry[@key='oldName']", namespace) is not None]

        elif "Missing Value" in node_info["node_name"]:
            data_types = model.findall(".//knime:config[@key='dataTypeSettings']/knime:config", namespace)
            node_info["parameters"]["missing_value_strategy"] = {}
            for dt in data_types:
                key = dt.attrib["key"]
                factory_id_entry = dt.find("knime:entry[@key='factoryID']", namespace)
                value_entry = dt.find("knime:entry[@key='fixDoubleValue']", namespace)
                if factory_id_entry is not None:
                    strategy = factory_id_entry.attrib["value"]
                    value = value_entry.attrib["value"] if value_entry is not None else None
                    node_info["parameters"]["missing_value_strategy"][key] = {"strategy": strategy, "value": value}

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

        elif "String to Number" in node_info["node_name"]:
            # Extract the column to convert
            decimal_separator_entry = model.find(".//knime:entry[@key='decimal_separator']", namespace)
            if decimal_separator_entry is not None:
                node_info["parameters"]["decimal_separator"] = decimal_separator_entry.attrib["value"]
            # Extract the column to convert
            included_columns = model.findall(".//knime:config[@key='included_names']/knime:entry", namespace)
            node_info["parameters"]["included_columns"] = [
                col.attrib["value"] for col in included_columns if col.attrib["key"] != "array-size"
            ]
            # Extract the excluded columns
            excluded_columns = model.findall(".//knime:config[@key='excluded_names']/knime:entry", namespace)
            node_info["parameters"]["excluded_columns"] = [
                col.attrib["value"] for col in excluded_columns if col.attrib["key"] != "array-size"
            ]

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

        elif "Numeric Outliers" in node_info["node_name"]:
            # Extract estimation-type
            estimation_type = model.find(".//knime:entry[@key='estimation-type']", namespace)
            if estimation_type is not None:
                node_info["parameters"]["estimation_type"] = estimation_type.attrib["value"]
            # Extract groups-list
            groups_list = model.find(".//knime:config[@key='groups-list']", namespace)
            if groups_list is not None:
                included_names = groups_list.find(".//knime:config[@key='included_names']", namespace)
                excluded_names = groups_list.find(".//knime:config[@key='excluded_names']", namespace)
                if included_names is not None:
                    node_info["parameters"]["included_names"] = [entry.attrib["value"] for entry in included_names.findall("knime:entry", namespace)]
                if excluded_names is not None:
                    node_info["parameters"]["excluded_names"] = [entry.attrib["value"] for entry in excluded_names.findall("knime:entry", namespace)]

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

        elif "Auto-Binner" in node_info["node_name"]:
            # Extract columnas incluidas y excluidas
            included_columns = [
                col.attrib["value"]
                for col in model.findall(".//knime:config[@key='included_names']/knime:entry", namespace)
            ]
            excluded_columns = [
                col.attrib["value"]
                for col in model.findall(".//knime:config[@key='excluded_names']/knime:entry", namespace)
            ]
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
                "included_columns": included_columns,
                "excluded_columns": excluded_columns,
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

        elif "Missing Value Column Filter" in node_info["node_name"]:
            missing_value_threshold_entry = model.find(".//knime:entry[@key='missing_value_percentage']", namespace)
            included_columns = [
                col.attrib["value"]
                for col in model.findall(".//knime:config[@key='included_names']/knime:entry", namespace)
                if col is not None
            ]
            excluded_columns = [
                col.attrib["value"]
                for col in model.findall(".//knime:config[@key='excluded_names']/knime:entry", namespace)
                if col is not None
            ]
            node_info["parameters"]["missing_value_threshold"] = (
                float(missing_value_threshold_entry.attrib["value"])
                if missing_value_threshold_entry is not None else None
            )
            node_info["parameters"]["included_columns"] = included_columns
            node_info["parameters"]["excluded_columns"] = excluded_columns

        elif "Duplicate Row Filter" in node_info["node_name"]:
            remove_duplicates_entry = model.find(".//knime:entry[@key='remove_duplicates']", namespace)
            retain_order_entry = model.find(".//knime:entry[@key='retain_order']", namespace)
            row_selection_entry = model.find(".//knime:entry[@key='row_selection']", namespace)
            add_row_duplicate_flag_entry = model.find(".//knime:entry[@key='add_row_duplicate_flag']", namespace)
            in_memory_entry = model.find(".//knime:entry[@key='in_memory']", namespace)
            included_columns = [
                col.attrib["value"]
                for col in model.findall(".//knime:config[@key='included_names']/knime:entry", namespace)
            ]
            excluded_columns = [
                col.attrib["value"]
                for col in model.findall(".//knime:config[@key='excluded_names']/knime:entry", namespace)
            ]
            node_info["parameters"]["remove_duplicates"] = remove_duplicates_entry.attrib["value"] == "true" if remove_duplicates_entry is not None else False
            node_info["parameters"]["retain_order"] = retain_order_entry.attrib["value"] == "true" if retain_order_entry is not None else False
            node_info["parameters"]["row_selection"] = row_selection_entry.attrib["value"] if row_selection_entry is not None else None
            node_info["parameters"]["add_row_duplicate_flag"] = add_row_duplicate_flag_entry.attrib["value"] == "true" if add_row_duplicate_flag_entry is not None else False
            node_info["parameters"]["in_memory"] = in_memory_entry.attrib["value"] == "true" if in_memory_entry is not None else False
            node_info["parameters"]["included_columns"] = included_columns
            node_info["parameters"]["excluded_columns"] = excluded_columns


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

        elif "String Manipulation (Multi Column)" in node_info["node_name"]:
            # Extract the expression to apply
            expression_entry = model.find(".//knime:entry[@key='EXPRESSION']", namespace)
            if expression_entry is not None:
                node_info["parameters"]["expression"] = expression_entry.attrib["value"]
            # Extract the columns to apply the expression
            included_columns = model.findall(".//knime:config[@key='included_names']/knime:entry", namespace)
            node_info["parameters"]["included_columns"] = [
                col.attrib["value"] for col in included_columns if col.attrib["key"] != "array-size"
            ]
            # Extract the columns to append the result
            append_or_replace_entry = model.find(".//knime:entry[@key='APPEND_OR_REPLACE']", namespace)
            if append_or_replace_entry is not None:
                node_info["parameters"]["append_or_replace"] = append_or_replace_entry.attrib["value"]
            # Extract the column to append the result
            append_column_suffix_entry = model.find(".//knime:entry[@key='APPEND_COLUMN_SUFFIX']", namespace)
            if append_column_suffix_entry is not None:
                node_info["parameters"]["append_column_suffix"] = append_column_suffix_entry.attrib["value"]

        elif "CSV Writer" in node_info["node_name"]:
            file_chooser = model.find(".//knime:config[@key='file_chooser_settings']/knime:config[@key='path']", namespace)
            column_delimiter = model.find(".//knime:entry[@key='column_delimiter']", namespace)
            if file_chooser is not None:
                path_entry = file_chooser.find("knime:entry[@key='path']", namespace)
                if path_entry is not None:
                    node_info["parameters"]["file_path"] = path_entry.attrib["value"]
            if column_delimiter is not None:
                node_info["parameters"]["column_delimiter"] = column_delimiter.attrib["value"]

        else:
            print(f"Node type not recognized: {node_info['node_name']}")

    return node_info
