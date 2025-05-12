import copy
import re
import xml.etree.ElementTree as elementTree
from utils.logger import print_and_log, print_and_log_dict

INFINITE_VALUE = 1000000000

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


def get_column_mapping_and_parameters(node: dict) -> dict:
    """
    Get the column mapping and parameters from the node parameters.
    Args:
        node: (dict) The node from the JSON data.

    Returns:
        dict: A dictionary containing the replace_column name and mapping parameters.
    """
    replace_column_name = ""
    if "replace_column_name" in node["parameters"]:
        replace_column_name = node["parameters"]["replace_column_name"]

    mapping_parameters = dict()
    map_operation = ""
    unique_replacement_one_column = False

    # Define the regular expressions for different types of functions
    patterns = {
        "replacement": r'(replace|replaceChars|substr)\(\$(.*?)\$\s*,\s*["\']?(.*?)["\']?\s*,\s*["\']?(.*?)["\']?\s*\)',
        "nested_replacement": r'replace\(\s*replace\(\s*string\(\s*\$\$(.*?)\$\$\s*\)\s*,\s*["\'](.*?)["\']\s*,\s*["\'](.*?)["\']\s*\)\s*,\s*["\'](.*?)["\']\s*,\s*["\'](.*?)["\']\s*\)'
    }

    if "rules" in node["parameters"]:

        expression = node["parameters"]["rules"]

        if any(func in expression for func in ["replace(", "replaceChars(", "substr("]):
            match = re.search(patterns["replacement"], expression)
            if match:
                str_manipulation_function = match.group(1)
                if str_manipulation_function == "replace":
                    map_operation = "VALUE_MAPPING"
                else:
                    map_operation = "SUBSTRING"
                replace_column_name = match.group(2)
                mapping_parameters[match.group(3)] = match.group(4)
                print_and_log("String manipulation function: " + str_manipulation_function)
                print_and_log("Replace column name: " + replace_column_name)
                print_and_log("Mapping parameters: " + str(mapping_parameters))
                if ((expression.count("replace(") == 1 or expression.count("replaceChars(") == 1 or expression.count(
                        "substr(") == 1) and expression.count("$") == 2):
                    unique_replacement_one_column = True
        else:
            parameter_list = [rule for rule in expression if rule.startswith('$')]
            map_operation = "VALUE_MAPPING"
            for value in parameter_list:
                match = re.search(r'\*(\w)\*.*"(\d)"', value)
                if match:
                    mapping_parameters[match.group(1)] = match.group(2)

    elif "rules_multiColumn" in node["parameters"]:

        expression = node["parameters"]["rules_multiColumn"]

        if any(func in expression for func in ["replace(", "replaceChars("]):
            match = re.search(patterns["nested_replacement"], expression)
            if match:
                map_operation = "SUBSTRING"
                replace_column_name = match.group(1)
                mapping_parameters[match.group(2)] = match.group(3)
        else:
            parameter_list = [rule for rule in expression if rule.startswith('$')]
            map_operation = "VALUE_MAPPING"
            for value in parameter_list:
                match = re.search(r'\*(\w)\*.*"(\d)"', value)
                if match:
                    mapping_parameters[match.group(1)] = match.group(2)

    elif "replacement" in node["parameters"]:
        map_operation = "SUBSTRING"
        string_replacement = node["parameters"]["replacement"]
        replace_column_name = string_replacement["column"]
        mapping_parameters[string_replacement["pattern"]] = string_replacement["replacement"]
        unique_replacement_one_column = True

    return {
        "replace_column_name": replace_column_name,
        "mapping_parameters": [{"key": key, "value": value} for key, value in mapping_parameters.items()],
        "map_operation": map_operation,
        "unique_replacement_one_column": unique_replacement_one_column
    }


def extract_input_output_node_settings(node_info: dict, root: elementTree.Element,
                                       model: elementTree.Element, namespace: dict) -> dict:
    """
    Extracts the input and output columns from the data model XML element.

    Args:
        node_info (dict): Dictionary with the node information.
        root (elementTree.Element): The root XML element.
        model (elementTree.Element): The data model XML element.
        namespace (dict): The namespace for the XML file.

    Returns:
        dict: Dictionary with the node information and the input and output columns.

    """
    if "CSV Reader" in node_info["node_name"]:
        settings_reader = model.find(".//knime:config[@key='settings']", namespace)
        if settings_reader is not None:
            # Extract CSV path
            path_entry = settings_reader.find(
                ".//knime:config[@key='file_selection']/knime:config[@key='path']/knime:entry[@key='path']",
                namespace)
            if path_entry is not None:
                node_info["parameters"]["file_path"] = path_entry.attrib["value"]
            # Extract column delimiter
            column_delimiter_entry = settings_reader.find(".//knime:entry[@key='column_delimiter']", namespace)
            if column_delimiter_entry is not None:
                node_info["parameters"]["column_delimiter"] = column_delimiter_entry.attrib["value"]

    elif "Excel Reader" in node_info["node_name"]:
        # Find the file path under the correct structure
        file_path_entry = root.find(
            ".//knime:config[@key='file_selection']/knime:config[@key='path']/knime:entry[@key='path']", namespace)
        # Extract and store the file path if found
        if file_path_entry is not None:
            node_info["parameters"]["file_path"] = file_path_entry.attrib["value"]

    elif "File Reader" in node_info["node_name"]:
        # Find the file path under the correct structure
        file_path_entry = root.find(
            ".//knime:config[@key='file_selection']/knime:config[@key='path']/knime:entry[@key='path']", namespace)
        # Extract and store the file path if found
        if file_path_entry is not None:
            node_info["parameters"]["file_path"] = file_path_entry.attrib["value"]

    elif "Table Reader" in node_info["node_name"]:
        settings_reader = model.find(".//knime:config[@key='settings']", namespace)
        file_path_entry = settings_reader.find(".//knime:config[@key='path']/knime:entry[@key='path']", namespace)
        if file_path_entry is not None:
            node_info["parameters"]["file_path"] = file_path_entry.attrib["value"]

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
        print_and_log(f"Node {node_info['node_name']} not found in the extract_input_output_node_settings function")

    return node_info


def extract_imputation_node_settings(node_info: dict, model: elementTree.Element, root: elementTree.Element,
                                     namespace: dict, nodes_info: list) -> list:
    """
    Extracts the imputation settings from the data model XML element.

    Args:
        node_info (dict): Dictionary with the node information.
        model (elementTree.Element): The data model XML element.
        root (elementTree.Element): The root XML element.
        namespace (dict): The namespace for the XML file.
        nodes_info (list): List of dictionaries with the node information

    Returns:
        list: List of dictionaries with the node information.

    """
    if "Missing Value" in node_info["node_name"]:

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
            print_and_log_dict(node_info)
            nodes_info.append(node_info)

        # If there werent factorials, add a node with default values
        if not factory_dict:
            node_info = {
                "node_name": "Missing Value",
                "parameters": {
                    "in_columns": [{"column_name": "column_name", "column_type": "xstring"}],
                    "out_columns": [{"column_name": "column_name", "column_type": "xstring"}],
                    "imputationType": "Mean",
                    "fixStringValues": None
                }
            }
            print_and_log_dict(node_info)
            nodes_info.append(node_info)

    elif "Numeric Outliers" in node_info["node_name"]:
        out_columns = root.findall(
            ".//knime:config[@key='outlier-list']/knime:config[@key='included_names']/knime:entry", namespace)

        node_info["parameters"]["in_columns"] = [{"column_name": col.attrib["value"], "column_type": "xstring"} for
                                                 col in out_columns if col.attrib["key"] != "array-size"]

        node_info["parameters"]["out_columns"] = [{"column_name": col.attrib["value"], "column_type": "xstring"} for
                                                  col in out_columns if col.attrib["key"] != "array-size"]

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

        print_and_log_dict(node_info)
        nodes_info.append(node_info)

    return nodes_info


def extract_row_filter_node_settings(node_info: dict, model: elementTree.Element, namespace: dict) -> dict:
    """
    Extracts the row filter settings from the data model XML element.

    Args:
        node_info (dict): Dictionary with the node information.
        model (elementTree.Element): The data model XML element.
        namespace (dict): The namespace for the XML file.

    Returns:
        dict: Dictionary with the node information.

    """
    if "Row Filter (deprecated)" in node_info["node_name"] or "Row Filter" in node_info["node_name"]:
        row_filter = model.find(".//knime:config[@key='rowFilter']", namespace)
        if row_filter is not None:
            # Extract the filter type (EQUAL, CONTAINS, etc.)
            filter_type_entry = row_filter.find("knime:entry[@key='RowFilter_TypeID']", namespace)

            if filter_type_entry is not None:
                # Filter type value
                node_info["parameters"]["filter_type"] = filter_type_entry.attrib["value"]
            else:
                # Default value or error logging if filter type entry not found
                node_info["parameters"]["filter_type"] = "UNKNOWN"
                print_and_log(f"Filter type entry not found in row filter for node {node_info['node_name']}")

            # If the filter type is RangeVal_RowFilter, extract the columns to filter, lower and upper bounds, and
            if filter_type_entry is not None and node_info["parameters"]["filter_type"] == "RangeVal_RowFilter":

                # Extract the row range
                start_entry = row_filter.find("knime:entry[@key='RowRangeStart']", namespace)
                end_entry = row_filter.find("knime:entry[@key='RowRangeEnd']", namespace)
                if start_entry is not None and end_entry is not None:
                    node_info["parameters"]["row_range"] = {
                        "start": start_entry.attrib["value"],
                        "end": end_entry.attrib["value"]
                    }
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
                    if isinstance(lower_bound_value, str) and lower_bound_value.startswith("org.knime.core.data.def"):
                        cell_type = lower_bound_value.split('.')[-1]
                        lower_bound_config = row_filter.find(
                            f"knime:config[@key='lowerBound']/knime:config[@key='org.knime.core.data.def.{cell_type}']/knime:entry[@key='{cell_type}']", namespace)
                        if lower_bound_config is not None:
                            lower_bound_value = lower_bound_config.attrib["value"]
                            if lower_bound_value is not None and lower_bound_value != "":
                                if isinstance(lower_bound_value, str):
                                    lower_bound_value_numeric = float(lower_bound_value)
                                    node_info["parameters"]["lower_bound"] = lower_bound_value_numeric
                            else:
                                node_info["parameters"]["lower_bound"] = 0
                        else:
                            node_info["parameters"]["lower_bound"] = 0
                    else:
                        node_info["parameters"]["lower_bound"] = lower_bound_value
                else:
                    node_info["parameters"]["lower_bound"] = 0

                if upper_bound_value is not None and upper_bound_value != "":
                    if isinstance(upper_bound_value, str) and upper_bound_value.startswith("org.knime.core.data.def"):
                        cell_type = upper_bound_value.split('.')[-1]
                        upper_bound_config = row_filter.find(
                            f"knime:config[@key='upperBound']/knime:config[@key='org.knime.core.data.def.{cell_type}']/knime:entry[@key='{cell_type}']", namespace)
                        if upper_bound_config is not None:
                            upper_bound_value = upper_bound_config.attrib["value"]
                            if upper_bound_value is not None and upper_bound_value != "":
                                if isinstance(upper_bound_value, str):
                                    upper_bound_value_numeric = float(upper_bound_value)
                                    node_info["parameters"]["upper_bound"] = upper_bound_value_numeric
                            else:
                                node_info["parameters"]["upper_bound"] = 0
                        else:
                            node_info["parameters"]["upper_bound"] = 0
                    else:
                        node_info["parameters"]["upper_bound"] = upper_bound_value
                else:
                    node_info["parameters"]["upper_bound"] = 0
                # Flags to indicate if the upeer and lower bounds are not None
                node_info["parameters"]["has_lower_bound"] = (not lower_bound_value is not None and lower_bound_value
                                                              != "")
                node_info["parameters"]["has_upper_bound"] = (not upper_bound_value is not None and upper_bound_value
                                                              != "")

            elif filter_type_entry is not None and node_info["parameters"]["filter_type"] == "StringComp_RowFilter":
                # Extract the pattern
                pattern_entry = row_filter.find("knime:entry[@key='Pattern']", namespace)
                node_info["parameters"]["pattern"] = pattern_entry.attrib[
                    "value"] if pattern_entry is not None else None

            # Extract the include or exclude parameter
            include_entry = row_filter.find("knime:entry[@key='include']", namespace)
            exclude_entry = row_filter.find("knime:entry[@key='exclude']", namespace)
            if (include_entry is not None and include_entry.attrib["value"] == "true") or (exclude_entry is not None and
                                                                                           exclude_entry.attrib["value"]
                                                                                           == "false"):
                node_info["parameters"]["filter_type_inclusion"] = "INCLUDE"
            elif (exclude_entry is not None and exclude_entry.attrib["value"] == "true") or (include_entry is not None and
                                                                                             include_entry.attrib["value"]
                                                                                             == "false"):
                node_info["parameters"]["filter_type_inclusion"] = "EXCLUDE"

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

    elif "Duplicate Row Filter" in node_info["node_name"]:
        remove_duplicates_entry = model.find(".//knime:entry[@key='remove_duplicates']", namespace)
        retain_order_entry = model.find(".//knime:entry[@key='retain_order']", namespace)
        row_selection_entry = model.find(".//knime:entry[@key='row_selection']", namespace)
        add_row_duplicate_flag_entry = model.find(".//knime:entry[@key='add_row_duplicate_flag']", namespace)
        in_memory_entry = model.find(".//knime:entry[@key='in_memory']", namespace)
        out_columns, in_columns = extract_columns_data(model, namespace)

        node_info["parameters"]["remove_duplicates"] = remove_duplicates_entry.attrib[
                                                           "value"] == "true" if remove_duplicates_entry is not None else False
        node_info["parameters"]["retain_order"] = retain_order_entry.attrib[
                                                      "value"] == "true" if retain_order_entry is not None else False
        node_info["parameters"]["row_selection"] = row_selection_entry.attrib[
            "value"] if row_selection_entry is not None else None
        node_info["parameters"]["add_row_duplicate_flag"] = add_row_duplicate_flag_entry.attrib[
                                                                "value"] == "true" if add_row_duplicate_flag_entry is not None else False
        node_info["parameters"]["in_memory"] = in_memory_entry.attrib[
                                                   "value"] == "true" if in_memory_entry is not None else False
        node_info["parameters"]["in_columns"] = out_columns + in_columns
        node_info["parameters"]["out_columns"] = out_columns

    return node_info


def extract_function_types(rules: list) -> list:
    """
    Extracts the function types from the rules.

    Args:
        rules (list): List of rules.

    Returns:
        list: List with the function types.
    """
    function_types = []
    patterns = {
        "LIKE": r'\$.*?\$ LIKE ".*?"',
        "<=": r'\$.*?\$\s*<=\s*.*?',
        ">=": r'\$.*?\$\s*>=\s*.*?',
        "<": r'\$.*?\$\s*<\s*.*?',
        "=": r'\$.*?\$\s*=\s*.*?',
        ">": r'\$.*?\$\s*>\s*.*?',
        "MATCHES": r'\$.*?\$ MATCHES ".*?"',
    }

    for rule in rules:
        if rule.strip().startswith('//'):
            continue
        for func_type, pattern in patterns.items():
            if re.search(pattern, rule):
                function_types.append(func_type)
                break

    return function_types


def extract_binner_node_settings_from_rule_engine(node_info: dict, binner_operator_types: list) -> dict:
    """
    Extracts the binner settings from the Rule Engine binner configuration.

    Args:
        node_info: (dict) The node information.
        binner_operator_types: (list) The list of binner operator types.

    Returns:
        node_info: (dict) The node information with the binner settings.

    """
    node_info["parameters"]["bins"] = []

    for rule in node_info["parameters"]["rules"]:
        if any(func in node_info["parameters"]["function_types"] for func in binner_operator_types):
            # Extract the numeric operator
            numeric_operator = ""
            first_operand = "0"
            second_operand = "0"
            match = re.search(r'(<=|>=|=>|<|>)', rule)
            if match:
                numeric_operator = match.group(1)

            # Extract the bin name
            match = re.findall(r'"([^"]*)"', rule)
            bin_name = match[-1] if match else ""

            if numeric_operator != "=>":
                # Extract the number, which is located just after the numeric operator
                parts = rule.split()
                operator_part = parts[1]
                column_part = parts[0]
                if bool(re.match(r'([<>=]{1,2})(\d+)', operator_part)):
                    match = re.search(r'([<>=]{1,2})(\d+)', operator_part)
                    if match:
                        numeric_operator = match.group(1)
                        second_operand = match.group(2)
                else:
                    for i, part in enumerate(parts):
                        if part == numeric_operator:
                            second_operand = parts[i + 1]
                            if second_operand.startswith('"') and second_operand.endswith('"'):
                                second_operand = second_operand[1:-1]
                            break

                # Extraer columna entre $
                column_part = column_part.split("$")
                if len(column_part) > 1:
                    column_part = column_part[1]
                    # Update the node_info with the column name
                    node_info["parameters"]["in_columns"] = [
                        {"column_name": column_part, "column_type": "xstring"}
                    ]

            else:
                second_operand = INFINITE_VALUE

            if numeric_operator == "<":
                closure_type = "openOpen"
                first_operand = -float(INFINITE_VALUE)

            elif numeric_operator == "<=":
                closure_type = "openClosed"
                first_operand = -float(INFINITE_VALUE)

            elif numeric_operator == ">":
                closure_type = "openOpen"
                first_operand = second_operand
                second_operand = float(INFINITE_VALUE)

            elif numeric_operator == ">=":
                closure_type = "closedOpen"
                first_operand = second_operand
                second_operand = float(INFINITE_VALUE)

            elif numeric_operator == "=>":
                closure_type = "openOpen"
                first_operand = None
                second_operand = None
            else:
                closure_type = ""

            # Add the bin to the list of bins
            node_info["parameters"]["bins"].append(
                {
                    "binName": bin_name,
                    "closureType": closure_type,
                    "leftMargin": first_operand,
                    "rightMargin": second_operand
                }
            )

            # Insert
            if node_info["parameters"]["bins"]:
                node_info["parameters"]["bins"].insert(0, node_info["parameters"]["bins"].pop())

    if any(bin["leftMargin"] is None and bin["rightMargin"] is None for bin in node_info["parameters"]["bins"]):
        # Retrieve binName from the None margins bin
        bin_name = next(
            (bin["binName"] for bin in node_info["parameters"]["bins"]
             if bin["leftMargin"] is None and bin["rightMargin"] is None), None)
        # Remove the bin with None margins
        node_info["parameters"]["bins"] = [
            bin for bin in node_info["parameters"]["bins"]
            if not (bin["leftMargin"] is None and bin["rightMargin"] is None)
        ]
        # Añadir tantos bins como haga falta para cubrir los valores restantes que no cubren los intervalos del resto de bins
        # Se trata del bin que binariza el resto de los valores no contemplados por los bins 'normales'
        # Complete the bin with None margins
        # Convert the margins to numeric values
        bins_numeric = []
        for bin in node_info["parameters"]["bins"]:
            try:
                left_margin = float(bin["leftMargin"])
            except (TypeError, ValueError):
                left_margin = -float(INFINITE_VALUE)
            try:
                right_margin = float(bin["rightMargin"])
            except (TypeError, ValueError):
                right_margin = float(INFINITE_VALUE)
            bins_numeric.append({
                "binName": bin["binName"],
                "closureType": bin["closureType"],
                "leftMargin": left_margin,
                "rightMargin": right_margin,
            })
        # Order the bins by left margin
        bins_sorted = sorted(bins_numeric, key=lambda x: x["leftMargin"])
        additional_bins = []
        # Bin for values less than the first bin if the first bin's left margin is not negative infinity
        if bins_sorted[0]["leftMargin"] > -float(INFINITE_VALUE):
            additional_bins.append({
                "binName": bin_name,
                "closureType": "openOpen",
                "leftMargin": -float(INFINITE_VALUE),
                "rightMargin": bins_sorted[0]["leftMargin"]
            })
        # Bins for gaps between bins
        for i in range(len(bins_sorted) - 1):
            current = bins_sorted[i]
            next_bin = bins_sorted[i + 1]
            if current["rightMargin"] < next_bin["leftMargin"]:
                additional_bins.append({
                    "binName": bin_name,
                    "closureType": "openOpen",
                    "leftMargin": current["rightMargin"],
                    "rightMargin": next_bin["leftMargin"]
                })
        # Bin for values greater than the last bin if the last bin's right margin is not infinity
        if bins_sorted[-1]["rightMargin"] < INFINITE_VALUE:
            additional_bins.append({
                "binName": bin_name,
                "closureType": "openOpen",
                "leftMargin": bins_sorted[-1]["rightMargin"],
                "rightMargin": INFINITE_VALUE
            })
        # Add the additional bins to the node_info
        node_info["parameters"]["bins"].extend(additional_bins)


    return node_info


def get_join_parameters(rules: str) -> list:
    """
    Get the join operands from the rules string.
    Join operands are defined in the rules string as "join($Name$, " - ", $City$)

    Join description:
    Joins two or more strings into a single string. Examples:
    join("a", "b", "c")
    join(null, "", "a")

    Args:
        rules: (str) The rules string.

    Returns:
        dict: A list containing the join operands.
    """
    # Extract the join operands
    match = re.search(r'join\((.*?)\)', rules)
    if match:
        join_operands = match.group(1)
        # Split the operands by comma
        operands_list = [param.strip() for param in join_operands.split(",")]
        # Preprocess the operands to parse them as fix value or column name if $ is present
        operands_list = [
            {"type": "column", "value": param[1:-1]} if param.startswith("$") else
            {"type": "fix_value", "value": param.strip('"')} for param in operands_list
        ]

        return operands_list
    else:
        # If no join operands are found, return an empty list
        return {}


def extract_mapping_node_settings(node_info: dict, model: elementTree.Element, namespace: dict) -> dict:
    """
    Extracts the mapping settings from the data model XML element.

    Args:
        node_info (dict): Dictionary with the node information.
        model (elementTree.Element): The data model XML element.
        namespace (dict): The namespace for the XML file.

    Returns:
        dict: Dictionary with the node information.

    """
    if "String Replacer" in node_info["node_name"]:
        column_entry = model.find("knime:entry[@key='colName']", namespace)
        pattern_entry = model.find("knime:entry[@key='pattern']", namespace)
        replacement_entry = model.find("knime:entry[@key='replacement']", namespace)
        if column_entry is not None and pattern_entry is not None and replacement_entry is not None:
            node_info["parameters"]["replacement"] = {
                "column": column_entry.attrib["value"],
                "pattern": pattern_entry.attrib["value"],
                "replacement": replacement_entry.attrib["value"],
            }

        node_info["parameters"]["mapping"] = get_column_mapping_and_parameters(node_info)

    elif "Rule Engine" in node_info["node_name"]:
        rules_entries = model.findall(".//knime:config[@key='rules']/knime:entry", namespace)
        rules = [entry.attrib["value"] for entry in rules_entries if entry.attrib["key"].isdigit()]
        new_column = model.find(".//knime:entry[@key='new-column-name']", namespace)
        replace_column = model.find(".//knime:entry[@key='replace-column-name']", namespace)
        append_column = model.find(".//knime:entry[@key='append-column']", namespace)
        # Filter those columns that are commented (starting with //)
        filtered_rules = [rule for rule in rules if not rule.strip().startswith('//')]
        rules = filtered_rules
        node_info["parameters"]["rules"] = rules
        node_info["parameters"]["function_types"] = extract_function_types(node_info["parameters"]["rules"])
        node_info["parameters"]["new_column_name"] = new_column.attrib["value"] if new_column is not None else None
        node_info["parameters"]["replace_column_name"] = replace_column.attrib[
            "value"] if replace_column is not None else None
        node_info["parameters"]["append_column"] = append_column.attrib[
                                                       "value"] == "true" if append_column is not None else False
        replace_column = model.findall(".//knime:entry[@key='replace-column-name']", namespace)
        out_columns = []
        if replace_column is not None:
            out_columns = [
                {"column_name": col.attrib["value"], "column_type": col.attrib["type"]}
                for col in replace_column if col.attrib["key"] != "array-size"
            ]
        node_info["parameters"]["in_columns"] = out_columns
        node_info["parameters"]["out_columns"] = [{"column_name": node_info["parameters"]["new_column_name"],
                                                   "column_type": "xstring"}] if node_info["parameters"][
            "new_column_name"] is not None else out_columns

        # Specify the binning parameters
        binner_operator_types = ["<=", ">=", "<", ">"]
        # Check if the function types contain any of the binner operator types and extract the binner settings if so
        if any(func in node_info["parameters"]["function_types"] for func in binner_operator_types):
            extract_binner_node_settings_from_rule_engine(node_info, binner_operator_types)
        # If not, extract the mapping parameters
        else:
            node_info["parameters"]["mapping"] = get_column_mapping_and_parameters(node_info)

    elif node_info["node_name"] == "String Manipulation":
        # Extract the expression and replaced column values
        expression_entry = model.find(".//knime:entry[@key='expression']", namespace)
        replaced_column_entry = model.find(".//knime:entry[@key='replaced_column']", namespace)

        if expression_entry is not None:
            node_info["parameters"]["rules"] = expression_entry.attrib["value"]

        if replaced_column_entry is not None:
            node_info["parameters"]["replace_column_name"] = replaced_column_entry.attrib["value"]

        out_columns = []
        if replaced_column_entry is not None:
            out_columns = [
                {"column_name": replaced_column_entry.attrib["value"], "column_type": "xstring"}
            ]
        node_info["parameters"]["out_columns"] = out_columns

        if "join" in node_info["parameters"]["rules"]:
            node_info["parameters"]["join"] = get_join_parameters(node_info["parameters"]["rules"])

            node_info["parameters"]["in_columns"] = []
            for join_param in node_info["parameters"]["join"]:
                if join_param["type"] == "column":
                    node_info["parameters"]["in_columns"].append(
                        {"column_name": join_param["value"], "column_type": "xstring"}
                    )

        else:
            node_info["parameters"]["mapping"] = get_column_mapping_and_parameters(node_info)

            node_info["parameters"]["in_columns"] = out_columns

    elif node_info["node_name"] == "String Manipulation (Multi Column)":  # String Manipulation (Multi Column)
        # Extract the expression and replaced column values
        expression_entry = model.find(".//knime:entry[@key='EXPRESSION']", namespace)
        if expression_entry is not None:
            node_info["parameters"]["rules_multiColumn"] = expression_entry.attrib["value"]

        node_info["parameters"]["mapping"] = get_column_mapping_and_parameters(node_info)

    return node_info


def extract_column_expressions_node_settings(node_info: dict, model: elementTree.Element, namespace: dict,
                                             nodes_info: list) -> list:
    # Iterate over each expression element in the expressions config
    expressions = model.findall(".//knime:config[@key='expressions']/knime:config", namespace)
    for expr_config in expressions:
        expression_entry = expr_config.find("knime:entry[@key='expression']", namespace)
        output_entry = expr_config.find("knime:entry[@key='outputName']", namespace)
        if expression_entry is None or output_entry is None:
            continue

        # Create a copy of node_info for each expression
        new_node = copy.deepcopy(node_info)
        new_node["parameters"] = {}
        out_name = output_entry.attrib["value"]
        # Replace column("...") with $...$
        original_expr = expression_entry.attrib["value"]
        new_node["parameters"]["replace_column_name"] = out_name

        new_node["parameters"]["rules"] = re.sub(
            r'column\(["\'](.*?)["\']\)',
            lambda m: f'${m.group(1)}$',
            original_expr
        )

        # If the expression contains any string function
        if re.search(r'replace\(|replaceChars\(|substr\(', new_node["parameters"]["rules"]):
            new_node["node_name"] = "String Manipulation"
            new_node["parameters"]["mapping"] = get_column_mapping_and_parameters(new_node)
            nodes_info.append(new_node)

        # If the expression contains any math formula
        elif re.search(r'(?:\$\w+\$|\d+(?:\.\d+)?)\s*([\+\-\*/])\s*(?:\$\w+\$|\d+(?:\.\d+)?)', new_node["parameters"]["rules"]):
            new_node["node_name"] = "Math Formula"
            nodes_info = extract_math_formula_node_settings(new_node, model, namespace, nodes_info,
                                                            new_node["parameters"]["rules"])

        # Set in_columns and out_columns based on outputName
        out_columns = [{"column_name": out_name, "column_type": "xstring"}]
        new_node["parameters"]["in_columns"] = out_columns
        new_node["parameters"]["out_columns"] = out_columns

    return nodes_info


def extract_binner_node_settings(node_info: dict, model: elementTree.Element, namespace: dict,
                                 nodes_info: list) -> list:
    """
    Extracts the binner settings from the data model XML element.

    Args:
        node_info (dict): Dictionary with the node information.
        model (elementTree.Element): The data model XML element.
        namespace (dict): The namespace for the XML file.
        nodes_info (list): List of dictionaries with the node information.

    Returns:
        dict: Dictionary with the node information.

    """
    if "Numeric Binner" in node_info["node_name"]:

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
                closure_type = "openOpen" if left_open and right_open else "openClosed" if left_open and not right_open else "closedOpen" if not left_open and right_open else "closedClosed"
                bin_info.append({
                    "binName": bin_name,
                    "closureType": closure_type,
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
                print_and_log_dict(new_node_info)
                nodes_info.append(new_node_info)

    elif "Auto-Binner" in node_info["node_name"]:
        # Extract the included and excluded columns
        out_columns, in_columns = extract_columns_data(model, namespace)
        for column in out_columns:
            column["column_name"] = column["column_name"] + "_binned"
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
        bins = []
        if binning_method == "fixedNumber":
            if bin_naming == "numbered":
                for column in out_columns:
                    for bin_index in range(bin_count):
                        bins.append({
                            "binName": f'{column["column_name"]}_{bin_index + 1}',
                            "closureType": "openOpen",
                            "leftMargin": 0,
                            "rightMargin": 100
                        })
        elif binning_method == "Sample quantiles":
            print("Sample quantiles")
        elif binning_method == "Equal width":
            print("Equal width")
        else:
            print("Equal frequency")

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
            "in_columns": in_columns,
            "out_columns": out_columns,
            "binning_method": binning_method,
            "bin_count": bin_count,
            "equality_method": equality_method,
            "integer_bounds": integer_bounds,
            "sample_quantiles": sample_quantiles,
            "bin_naming": bin_naming,
            "replace_column": replace_column,
            "precision": precision,
            "output_format": output_format,
            "bins": bins
        }
        print_and_log_dict(node_info)
        nodes_info.append(node_info)

    return nodes_info


def extract_math_formula_node_settings(node_info: dict, model: elementTree.Element, namespace: dict,
                                       nodes_info: list, rule: str = None) -> list:
    """
    Extracts the node settings from the settings file of a KNIME Math Formula node.

    Args:
        node_info (dict): Dictionary with the node settings.
        model (elementTree.Element): The data model XML element.
        namespace (dict): The namespace for the XML file.
        nodes_info (list): List of dictionaries with the node settings.
        rule (str): The rule to extract the settings from.

    Returns:
        list: List of dictionaries with the node settings.

    """
    if "Math Formula" in node_info["node_name"]:
        expression_entry = model.find(".//knime:entry[@key='expression']", namespace)
        replaced_column_entry = model.find(".//knime:entry[@key='replaced_column']", namespace)
        append_column_entry = model.find(".//knime:entry[@key='append_column']", namespace)

        # Use the rule passed as argument if it is not None
        if rule is not None:
            expression = rule

        if expression_entry is not None:
            if rule is None:
                expression = expression_entry.attrib["value"]

            # Extract operands and operator from the expression
            match = re.search(r'(\$[^$]+\$|[^$]+)\s*([\+\-\*/])\s*(\$[^$]+\$|[^$]+)', expression)
            if match:
                first_operand = match.group(1).strip()
                operator = match.group(2).strip()
                second_operand = match.group(3).strip()

                # Determine if operands are fixed values or columns
                first_operand_type = "column" if first_operand.startswith('$') and first_operand.endswith(
                    '$') else "fixed_value"
                second_operand_type = "column" if second_operand.startswith('$') and second_operand.endswith(
                    '$') else "fixed_value"

                # Determine the fix value
                fix_value = None
                if first_operand_type == "fixed_value":
                    fix_value = first_operand
                elif second_operand_type == "fixed_value":
                    fix_value = second_operand
                node_info["parameters"]["fix_value"] = fix_value

                if operator == "-":
                    node_info["parameters"]["operator"] = "SUBSTRACT"
                elif operator == "+":
                    node_info["parameters"]["operator"] = "SUM"
                elif operator == "*":
                    node_info["parameters"]["operator"] = "MULTIPLY"
                elif operator == "/":
                    node_info["parameters"]["operator"] = "DIVIDE"

                node_info["parameters"]["operands"] = []

                node_info["parameters"]["operands"].append({
                    "type": first_operand_type,
                    "value": first_operand.strip('$') if first_operand_type == "column" else first_operand
                })
                node_info["parameters"]["operands"].append({
                    "type": second_operand_type,
                    "value": second_operand.strip('$') if second_operand_type == "column" else second_operand
                })

                out_columns = []
                if first_operand_type == "column":
                    out_columns.append({"column_name": first_operand.strip('$'), "column_type": "xstring"})
                if second_operand_type == "column":
                    out_columns.append({"column_name": second_operand.strip('$'), "column_type": "xstring"})
                node_info["parameters"]["in_columns"] = out_columns

        # Determine the output column
        out_column = None
        if replaced_column_entry is not None and replaced_column_entry.attrib["value"].lower() not in ["false",
                                                                                                       "true"]:
            out_column = replaced_column_entry.attrib["value"]
        elif append_column_entry is not None and append_column_entry.attrib["value"].lower() not in ["false",
                                                                                                     "true"]:
            out_column = append_column_entry.attrib["value"]
        elif "replace_column_name" in node_info["parameters"]:
            out_column = node_info["parameters"]["replace_column_name"]

        if out_column is not None:
            node_info["parameters"]["out_column"] = out_column

        node_info["parameters"]["in_columns"] = [{"column_name": out_column, "column_type": "xstring"}]

        print_and_log_dict(node_info)
        nodes_info.append(node_info)

    return nodes_info
