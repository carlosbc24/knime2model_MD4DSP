import json

from utils.logger import print_and_log


def get_library_transformation_id(json_file_path: str, node_name: str) -> int | None:
    """
    Reads the JSON file and finds the hashing function whose identifier matches the value of the node_name variable.
    Extracts the value of the library_transformation_id attribute from the first matching element.

    Args:
        json_file_path (str): Path to the JSON file.
        node_name (str): Name of the node to search for.

    Returns:
        int: Value of the library_transformation_id attribute of the first matching element.
    """
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    functions_hashing = data.get("functions_hashing", [])

    for function in functions_hashing:
        if node_name in function:
            return function[node_name].get("library_transformation_id")

    return None


def get_library_transformation_names(json_file_path: str) -> list:
    """
    Reads the JSON file and returns a list with the library transformation names.

    Args:
        json_file_path (str): Path to the JSON file.

    Returns:
        library_transformations_names: List with the library transformation names.
    """
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Get the library transformation names
    library_transformations = data.get("library_transformation_names", [])
    library_transformations_names = [function.get("name") for function in library_transformations]

    return library_transformations_names


def get_library_transformation_name(node: dict, index: int) -> str | None:
    """
    Returns the library transformation name for the given node.

    Args:
        node (dict): Node to search for.
        index (int): Index of the node.

    Returns:
        string: Value of the library_transformation_id attribute of the first matching node
    """
    node_name = node.get("node_name", f"Node_{index}")

    # 100% mapped imputation KNIME node
    if node_name == "Missing Value":
        if node.get("parameters", {}).get("imputationType") in ["MostFrequent", "Previous", "Next"]:
            return "imputeByDerivedValue"
        elif node.get("parameters", {}).get("imputationType") == "Fixed Value":
            return "imputeByFixValue"
        elif node.get("parameters", {}).get("imputationType") in ["Mean", "Interpolation", "Median", "Closest"]:
            return "imputeByNumericOp"
        else:
            print_and_log(f"Unknown imputation type: {node.get('parameters', {}).get('imputationType')}")

    # 20% mapped row filter KNIME node
    elif node_name == "Row Filter (deprecated)" or node_name == "Row Filter":
        if node.get("parameters", {}).get("filter_type") == "RangeVal_RowFilter":
            return "rowFilterRange"
        elif node.get("parameters", {}).get("filter_type") == "StringComp_RowFilter":
            return "rowFilterPrimitive"
        elif node.get("parameters", {}).get("filter_type") == "MissingVal_RowFilter":
            return "rowFilterMissing"

    # 100% mapped KNIME nodes
    elif node_name == "Column Filter":
        return "columnFilter"

    elif node_name == "String to Number":
        return "categoricalToContinuous"

    elif node_name == "Numeric Outliers":
        return "imputeOutliersByClosest"

    elif node_name == "Numeric Binner":
        return "binner"

    # Just mapped sum, substract, multiply and divide operations from Math Formula KNIME node when the
    # operation has 2 operands
    elif node_name == "Math Formula" and node.get("parameters", {}).get("operator") in ["SUM", "SUBSTRACT",
                                                                                        "MULTIPLY", "DIVIDE"]:
        return "mathOperation"

    # Just mapped LIKE operation from Rule Engine KNIME node
    elif node_name == "Rule Engine" and all(
            func in ["<=", ">=", "<", ">"] for func in node.get("parameters", {}).get("function_types")):
        return "binner"
    elif node_name == "Rule Engine" and "LIKE" in node.get("parameters", {}).get("function_types"):
        return "mapping"

    # Just mapped String Manipulation KNIME node when the operation is
    # "replace or replaceChars" and there is just one column
    elif ((node_name == "String Manipulation" or node_name == "String Manipulation (Multi Column)" or
           node_name == "String Replacer") and node.get("parameters", {}).get("mapping") and
          node.get("parameters", {}).get("mapping").get("unique_replacement_one_column") is True):
        return "mapping"

    elif node_name == "String Manipulation" and node.get("parameters", {}).get("join"):
        return "join"

    else:
        print_and_log(f"Not mapped unknown KNIME node: {node_name}")
        return None
