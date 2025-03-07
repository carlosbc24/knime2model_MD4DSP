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
        librery_transformations_names: List with the library transformation names.
    """
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Get the library transformation names
    librery_transformations = data.get("library_transformation_names", [])
    librery_transformations_names = [function.get("name") for function in librery_transformations]

    return librery_transformations_names


def get_library_transformation_name(json_file_path: str, node: dict, index: int) -> str | None:
    """
    Reads the JSON file and finds the hashing function whose identifier matches the value of the node_name variable.
    Extracts the value of the library_transformation_id attribute from the first matching element.

    Args:
        json_file_path (str): Path to the JSON file.
        node (dict): Node to search for.
        index (int): Index of the node.

    Returns:
        string: Value of the library_transformation_id attribute of the first matching node
    """
    node_name = node.get("node_name", f"Node_{index}")

    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    functions_hashing = data.get("functions_hashing", [])

    for function in functions_hashing:
        if node_name in function:
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
            elif node_name == "Column Filter" or node_name == "String to Number" or node_name == "Numeric Outliers" or node_name == "Numeric Binner" or node_name == "String Replacer":
                return function[node_name].get("library_transformation_name")

            # Just mapped sum and substract operations from Math Formula KNIME node when the operation has 2 operands
            elif node_name == "Math Formula" and node.get("parameters", {}).get("operator") in ["SUM", "SUBSTRACT"]:
                if node.get("parameters", {}).get("mathOpTransformation") == "mathOperationFieldFixValue":
                    return "mathOperationFieldFixValue"
                elif node.get("parameters", {}).get("mathOpTransformation") == "mathOperationFieldField":
                    return "mathOperationFieldField"
                else:
                    print_and_log(f"Unknown imputation type: {node.get('parameters', {}).get('imputationType')}")

            # Just mapped LIKE operation from Rule Engine KNIME node
            elif node_name == "Rule Engine" and "LIKE" in node.get("parameters", {}).get("function_types"):
                return function[node_name].get("library_transformation_name")

            else:
                print_and_log(f"Not mapped unknown KNIME node: {node_name}")
                return None
