import xml.etree.ElementTree as elementTree
from parsers.dataDictionary import create_input_port, create_output_port


def create_data_processing(node, index):
    """
    Processes a "normal" node (not Reader/Writer) from the JSON and returns:
      - node_id: identifier of the node (or its index)
      - dp: the generated XML 'dataprocessing' element
      - node_name: the name of the node (for reference in links)

    Args:
        node (dict): Node data from the JSON.
        index (int): Index of the node.

    Returns:
        tuple: (node_id, dp, node_name)
    """
    node_id = node.get("id", index)
    node_name = node.get("node_name", f"Node_{index}")

    # Create dataprocessing element
    dp = elementTree.Element("dataprocessing", {
        "xsi:type": "Workflow:DataProcessing",
        "name": node_name
    })

    # Determine base_name and fields from the node name
    if "(" in node_name and ")" in node_name:
        base_name = node_name.split("(")[0].strip()
        inner = node_name[node_name.find("(") + 1: node_name.find(")")]
        fields = [f.strip() for f in inner.split(",") if f.strip()]
    else:
        base_name = node_name
        fields = [node_name]

    # Create inputPort and outputPort
    create_input_port(dp, base_name, node_name, index, fields)
    create_output_port(dp, base_name, node_name, index, fields)

    # Set 'in' and 'out' attributes from the references of each datafield
    input_refs = [f"//@dataprocessing.{index}/@inputPort.0"]
    output_refs = [f"//@dataprocessing.{index}/@outputPort.0"]
    dp.set("in", " ".join(input_refs))
    dp.set("out", " ".join(output_refs))

    # Add transformation definition
    elementTree.SubElement(dp, "dataProcessingDefinition", {
        "xsi:type": "Library:Transformation",
        "href": f"library_validation.xmi#//@dataprocessingdefinition.{index}"
    })

    return node_id, dp, node_name
