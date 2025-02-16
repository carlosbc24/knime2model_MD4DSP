import xml.etree.ElementTree as elementTree


def create_input_port(parent: elementTree.Element, base_name: str, node_name: str, index: int, fields: list,
                      input_file_path: str):
    """
    Creates the inputPort element and its children (dataDictionaryDefinition).

    Args:
        parent (Element): The parent XML element.
        base_name (str): The base name for the input port.
        node_name (str): The name of the node.
        index (int): The index of the node.
        fields (list): List of fields for the input port.
        input_file_path (str): The path to the input file.

    Returns:
        Element: The created inputPort element.
    """
    input_port = elementTree.SubElement(parent, "inputPort", {
        "fileName": f"{base_name.lower().replace(' ', '_')}_dataDictionary.csv" if input_file_path == "" else input_file_path,
        "name": f"{base_name.lower().replace(' ', '_')}_input_dataDictionary",
        "out": f"//@dataprocessing.{index}/@outputPort.0"
    })
    elementTree.SubElement(input_port, "dataDictionaryDefinition", {
        "href": f"library_validation.xmi#//@dataprocessingdefinition.{index}/@inputPort.0"
    })
    return input_port


def create_output_port(parent: elementTree.Element, base_name: str, node_name: str, index: int, fields: list, dest_node_name: str):
    """
    Creates the outputPort element and its children (dataDictionaryDefinition).

    Args:
        parent (Element): The parent XML element.
        base_name (str): The base name for the output port.
        node_name (str): The name of the node.
        index (int): The index of the node.
        fields (list): List of fields for the output port.
        dest_node_name (str): The name of the destination node

    Returns:
        Element: The created outputPort element.
    """
    output_port = elementTree.SubElement(parent, "outputPort", {
        "fileName": f"{dest_node_name.lower().replace(' ', '_')}_dataDictionary.csv" if dest_node_name is not None else f"{base_name.lower().replace(' ', '_')}_dataDictionary.csv",
        "name": f"{base_name.lower().replace(' ', '_')}_output_dataDictionary",
        "in": f"//@dataprocessing.{index}/@inputPort.0"
    })
    elementTree.SubElement(output_port, "dataDictionaryDefinition", {
        "href": f"library_validation.xmi#//@dataprocessingdefinition.{index}/@outputPort.0"
    })
    return output_port
