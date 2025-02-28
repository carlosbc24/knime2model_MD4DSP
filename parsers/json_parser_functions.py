
import re


def get_transformation_dp_values(node: dict, node_id: int, node_name: str, include_contracts: bool, library_transformation_name: str) -> dict:
    """
    Get the values for the data processing node from the node parameters.

    Args:
        node (dict): The node from the JSON data.
        node_id (int): The ID of the node.
        node_name (str): The name of the node.
        include_contracts (bool): Flag to include the contracts in the data processing nodes.
        library_transformation_name (str): The name of the library transformation.

    Returns:
        dict: The values for the data processing node.
    """
    input_file_path = ""
    # Detect if the node includes the substrings "Reader" or "Connector"
    if any(substring in node_name for substring in ["Reader", "Table", "Connector", "Writer"]):
        input_file_path = node.get("parameters", {}).get("file_path", "")

    # Get input and output columns
    in_columns = get_input_columns(node)
    in_column_names = [included_column["column_name"] for included_column in in_columns]
    in_column_names_str = ", ".join(in_column_names)

    # Output columns
    out_columns = get_output_columns(node)
    out_column_names = [excluded_column["column_name"] for excluded_column in out_columns]
    out_column_names_str = ", ".join(out_column_names)

    # Initialize the dictionaries
    column_filter_dict = {}
    mapping_dict = {}
    binner_dict = {}
    row_dict = {}
    imputation_dict = {}

    if library_transformation_name == "columnFilter":
        # Filter columns that are not in the output
        filtered_columns = [
            {"name": column["column_name"], "type": "String" if column["column_type"] == "xstring" else "Integer"}
            for column in in_columns if column["column_name"] not in out_column_names]
        filtered_column_names = [column["name"] for column in filtered_columns]
        filtered_column_names_str = ", ".join(filtered_column_names)
        column_filter_dict = {"filtered_columns": filtered_columns, "filtered_column_names": filtered_column_names_str}

    if library_transformation_name == "rowFilterRange":
        row_dict = {"lower_bound": node["parameters"]["lower_bound"] if "lower_bound" in node["parameters"] else "", "upper_bound": node["parameters"]["upper_bound"] if "upper_bound" in node["parameters"] else "",
                    "has_lower_bound": node["parameters"]["has_lower_bound"] if "has_lower_bound" in node["parameters"] else "",
                    "has_upper_bound": node["parameters"]["has_upper_bound"] if "has_upper_bound" in node["parameters"] else ""}

    elif library_transformation_name == "mapping":
        # Get the column mapping and parameters
        column_mapping_and_parameters = get_column_mapping_and_parameters(node)
        replace_column_name = column_mapping_and_parameters["replace_column_name"]
        mapping_parameters = column_mapping_and_parameters["mapping_parameters"]
        mapping_parameters = [{"key": key, "value": value} for key, value in mapping_parameters.items()]
        mapping_dict = {"mapping_parameters": mapping_parameters, "replace_column_name": replace_column_name}

    elif library_transformation_name == "binner":
        binner_dict = {"bins": node["parameters"]["bins"]}

    elif library_transformation_name in ["imputeByDerivedValue", "imputeByFixValue", "imputeByNumericOp"]:
        imputation_dict = {"imputationType": node["parameters"]["imputationType"], "fixStringValues": node["parameters"]["fixStringValues"]}

    dataprocessing_values = {
        "transformation": {"name": library_transformation_name, "KNIME_name": node_name},
        "input_filepath": input_file_path,
        "output_filepath": "",
        "in_column_names": in_column_names_str,
        "out_column_names": out_column_names_str,
        "in_columns": [
            {"name": column["column_name"], "type": "String" if column["column_type"] == "xstring" else "Integer"}
            for column in in_columns
        ],
        "out_columns": [
            {"name": column["column_name"], "type": "String" if column["column_type"] == "xstring" else "Integer"}
            for column in out_columns
        ],
        "mapping": mapping_dict,
        "column_filter": column_filter_dict,
        "row_filter": row_dict,
        "binner": binner_dict,
        "imputation": imputation_dict,
        "include_contracts": include_contracts,
        "index": node_id
    }

    return dataprocessing_values


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
    if "parameters" in node and "in_columns" in node["parameters"]:
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
    if "parameters" in node and "out_columns" in node["parameters"]:
        out_columns = node["parameters"]["out_columns"]

    return out_columns


def get_column_mapping_and_parameters(node: dict) -> dict:
    """
    Get the column mapping and parameters from the node parameters.
    Args:
        node: (dict) The node from the JSON data.

    Returns:
        dict: A dictionary containing the replace column name and mapping parameters.
    """
    replace_column_name = ""
    if "replace_column_name" in node["parameters"]:
        replace_column_name = node["parameters"]["replace_column_name"]

    mapping_parameters = dict()
    parameter_list = []
    if "rules" in node["parameters"]:
        parameter_list = [rule for rule in node["parameters"]["rules"] if rule.startswith('$')]

    for value in parameter_list:
        match = re.search(r'\*(\w)\*.*"(\d)"', value)
        if match:
            mapping_parameters[match.group(1)] = match.group(2)

    return {
        "replace_column_name": replace_column_name,
        "mapping_parameters": mapping_parameters
    }
