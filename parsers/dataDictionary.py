import xml.etree.ElementTree as elementTree


def create_input_port(parent: elementTree.Element, base_name: str, node_name: str, index: int, fields: list,
                      input_data_filename: str):
    """
    Creates the inputPort element and its children (datafield and dataDictionaryDefinition).

    Args:
        parent (Element): The parent XML element.
        base_name (str): The base name for the input port.
        node_name (str): The name of the node.
        index (int): The index of the node.
        fields (list): List of fields for the input port.
        input_data_filename (str): The name of the input data file.

    Returns:
        Element: The created inputPort element.
    """
    input_port = elementTree.SubElement(parent, "inputPort", {
        "fileName": input_data_filename,
        "name": f"{base_name}_input_dataDictionary",
        "out": f"//@dataprocessing.{index}/@outputPort.0"
    })
    elementTree.SubElement(input_port, "dataDictionaryDefinition", {
        "href": f"library_validation.xmi#//@dataprocessingdefinition.{index}/@inputPort.0"
    })
    return input_port


def create_output_port(parent: elementTree.Element, base_name: str, node_name: str, index: int, fields: list,
                       output_data_filename: str = None):
    """
    Creates the outputPort element and its children (datafield and dataDictionaryDefinition).

    Args:
        parent (Element): The parent XML element.
        base_name (str): The base name for the output port.
        node_name (str): The name of the node.
        index (int): The index of the node.
        fields (list): List of fields for the output port.
        output_data_filename (str): The name of the output

    Returns:
        Element: The created outputPort element.
    """
    output_port = elementTree.SubElement(parent, "outputPort", {
        "fileName": output_data_filename,
        "name": f"{base_name}_output_dataDictionary" if output_data_filename is None else output_data_filename,
        "in": f"//@dataprocessing.{index}/@inputPort.0"
    })
    elementTree.SubElement(output_port, "dataDictionaryDefinition", {
        "href": f"library_validation.xmi#//@dataprocessingdefinition.{index}/@outputPort.0"
    })
    return output_port
