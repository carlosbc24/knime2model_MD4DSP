import xml.etree.ElementTree as elementTree


def create_datafield(port: elementTree.Element, columns: list, node_name: str, index: int):
    """
    Creates the datafield elements for the input/output port.

    Args:
        port: input/output port XML element.
        columns: list of columns for the input/output port.
        node_name: name of the node.
        index: index of the node.

    Returns:

    """
    # Create datafield elements
    for i, column in enumerate(columns):
        datafield = elementTree.SubElement(port, "datafield", {
            "xsi:type": "Workflow:Categorical" if column["column_type"] == "xstring" else "Workflow:Continuous",
            "name": f"{node_name}_input_dataField_{column['column_name']}",
            "displayName": column["column_name"],
            "out": f"//@dataprocessing.{index}/@outputPort.0/@datafield.{i}"
        })
        if column["column_type"] == "xstring":
            elementTree.SubElement(datafield, "categoricalDef", {
                "href": f"library_validation.xmi#//@dataprocessingdefinition."
                        f"{index}/@inputPort.0/@datafielddefinition.0"
            })
        else:
            elementTree.SubElement(datafield, "continuousDef", {
                "href": f"library_validation.xmi#//@dataprocessingdefinition."
                        f"{index}/@inputPort.0/@datafielddefinition.0"
            })

    return port


def create_input_port(parent: elementTree.Element, base_name: str, node_name: str, index: int, columns: list,
                      input_file_path: str):
    """
    Creates the inputPort element and its children (dataDictionaryDefinition).

    Args:
        parent (Element): The parent XML element.
        base_name (str): The base name for the input port.
        node_name (str): The name of the node.
        index (int): The index of the node.
        columns (list): List of columns for the input port.
        input_file_path (str): The path to the input file.

    Returns:
        Element: The created inputPort element.
    """
    print("Columns: ", columns)
    input_port = elementTree.SubElement(parent, "inputPort", {
        "fileName": f"{base_name.lower().replace(' ', '_')}_dataDictionary.csv" if input_file_path == "" else input_file_path,
        "name": f"{base_name.lower().replace(' ', '_')}_input_dataDictionary",
        "out": f"//@dataprocessing.{index}/@outputPort.0"
    })
    elementTree.SubElement(input_port, "dataDictionaryDefinition", {
        "href": f"library_validation.xmi#//@dataprocessingdefinition.{index}/@inputPort.0"
    })

    # Set 'in' attributes from the references of each dataDictionary
    input_refs = [f"//@dataprocessing.{index}/@inputPort.0"]
    parent.set("in", " ".join(input_refs))

    input_port = create_datafield(input_port, columns, node_name, index)

    return input_port


def create_output_port(parent: elementTree.Element, base_name: str, node_name: str, index: int, columns: list, dest_node_name: str):
    """
    Creates the outputPort element and its children (dataDictionaryDefinition).

    Args:
        parent (Element): The parent XML element.
        base_name (str): The base name for the output port.
        node_name (str): The name of the node.
        index (int): The index of the node.
        columns (list): List of columns for the output port.
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

    # Set 'out' attributes from the references of each dataDictionary
    output_refs = [f"//@dataprocessing.{index}/@outputPort.0"]
    parent.set("out", " ".join(output_refs))

    output_port = create_datafield(output_port, columns, node_name, index)

    return output_port
