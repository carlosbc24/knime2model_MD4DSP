import json


def get_library_transformation_id(json_file_path: str, node_name: str) -> int:
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
        librery_transformations_names: List with the library transformation names.
    """
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Get the library transformation names
    librery_transformations = data.get("library_transformation_names", [])
    librery_transformations_names = [function.get("name") for function in librery_transformations]

    return librery_transformations_names


def get_library_transformation_name(json_file_path: str, node: dict, index: int) -> str:
    """
    Reads the JSON file and finds the hashing function whose identifier matches the value of the node_name variable.
    Extracts the value of the library_transformation_id attribute from the first matching element.

    Args:
        json_file_path (str): Path to the JSON file.
        node (dict): Node to search for.

    Returns:
        string: Value of the library_transformation_id attribute of the first matching node
    """
    node_name = node.get("node_name", f"Node_{index}")

    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    functions_hashing = data.get("functions_hashing", [])

    for function in functions_hashing:
        if node_name in function:
            if node_name == "Row Filter (deprecated)":
                if node.get("parameters", {}).get("filter_type") == "RangeVal_RowFilter":
                    return function[node_name].get("library_transformation_name")
            if node_name == "Missing Value":
                if node.get("parameters", {}).get("imputationType") in ["MostFrequent", "Previous", "Next"]:
                    return "imputeByDerivedValue"
                elif node.get("parameters", {}).get("imputationType") == "Fixed Value":
                    return "imputeByFixValue"
                elif node.get("parameters", {}).get("imputationType") in ["Mean", "Interpolation", "Median", "Closest"]:
                    return "imputeByNumericOp"
                else:
                    raise ValueError(f"Unknown imputation type: {node.get('parameters', {}).get('imputationType')}")
            else:
                return function[node_name].get("library_transformation_name")

    return None
