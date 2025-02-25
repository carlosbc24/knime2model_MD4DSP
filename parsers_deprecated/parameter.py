import xml.etree.ElementTree as elementTree
import json


def get_node_parameters(json_file_path: str, node_name: str) -> list:
    """
    Reads the JSON function hashing file and returns the library parameters of the node.

    Args:
        json_file_path (str): Path to the JSON file.
        node_name (str): Name of the node to search for.

    Returns:
        list: list of dictionaries containing the library parameters of the node.
    """
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    functions_hashing = data.get("functions_hashing", [])

    for function in functions_hashing:
        if node_name in function:
            return function[node_name].get("library_parameters", {})

    return []


def create_parameters(dp: elementTree.Element, node_name: str,
                      library_parameters: list, library_transformation_id: int):
    """
    Creates parameter elements for the dataprocessing element based on the node name and library parameters.

    Args:
        dp (Element): The dataprocessing XML element.
        node_name (str): Name of the node.
        library_parameters (list): List of library parameters from the JSON.
        library_transformation_id (int): ID of the library transformation.
    """
    param_index = 0
    for param in library_parameters:
        param_element = elementTree.SubElement(dp, "parameter", {
            "xsi:type": f"Workflow:{param['parameter_name']}",
            "name": f"{node_name}_param_{param['parameter_name']}",
        })
        elementTree.SubElement(param_element, param['parameter_def_name'], {
            "href": f"library_validation.xmi#//@dataprocessingdefinition."
                    f"{library_transformation_id}/@parameterdefinition.{param_index}"
        })
        param_index += 1
