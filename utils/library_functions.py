import json


def get_library_transformation_id(json_file_path, node_name):
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


def get_library_transformation_name(json_file_path, node_name):
    """
    Reads the JSON file and finds the hashing function whose identifier matches the value of the node_name variable.
    Extracts the value of the library_transformation_id attribute from the first matching element.

    Args:
        json_file_path (str): Path to the JSON file.
        node_name (str): Name of the node to search for.

    Returns:
        string: Value of the library_transformation_id attribute of the first matching node
    """
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    functions_hashing = data.get("functions_hashing", [])

    for function in functions_hashing:
        if node_name in function:
            return function[node_name].get("library_transformation_name")

    return None
