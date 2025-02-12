import os
import json
from xml.dom import minidom
import xml.etree.ElementTree as ET

from auxiliar_parsers.dataLink_parser import build_link
from auxiliar_parsers.dataProcessing_parser import build_node


def json_to_xmi_workflow(json_input_filepath, xmi_output_path, xmi_output_filename, workflow_name="Model data set with metanode (KNIME)"):
    """
    Converts a JSON structure of a KNIME workflow to a well-formatted XMI file.
    Processes nodes modularly; nodes whose names end in "Reader" or "Writer" are not transformed into a <dataprocessing> element but their file_path is injected into the <inputPort> or <outputPort> of the connected node (subsequent or previous, respectively).

    Args:
        json_input_filepath (str): Path to the input JSON file.
        xmi_output_path (str): Path to the output directory for the XMI file.
        xmi_output_filename (str): Name of the output XMI file.
        workflow_name (str): Name of the workflow.
    """
    # Load JSON data
    with open(json_input_filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Register namespaces
    ns = {
        "xmi": "http://www.omg.org/XMI",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "Library": "http://www.example.org/Library",
        "Workflow": "https://www.example.org/workflow"
    }
    for prefix, uri in ns.items():
        ET.register_namespace(prefix, uri)

    # Create root element (Workflow)
    root = ET.Element("{https://www.example.org/workflow}Workflow", {
                      "{http://www.omg.org/XMI}version": "2.0",
                      "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation":
                      "http://www.example.org/Library ../metamodel/Library.ecore https://www.example.org/workflow ../metamodel/Workflow.ecore",
                      "name": workflow_name,
                      "xmlns:Library": "http://www.example.org/Library",
    })

    # Diccionarios para nodos "normales" y para nodos especiales (Reader/Writer)
    node_mapping = {}      # nodos que se transforman en <dataprocessing>
    reader_mapping = {}    # id -> file_path (para nodos Reader)
    writer_mapping = {}    # id -> file_path (para nodos Writer)



    reader_node_exists = False

    nodes = data.get("nodes", [])
    for index, node in enumerate(nodes):
        node_id = node.get("id", index)
        node_name = node.get("node_name", f"Node_{index}")

        # Detect if the node is a Reader or Writer (using endswith; logic can be adjusted)
        if node_name.strip().endswith("Reader"):
            file_path = node.get("parameters", {}).get("file_path", "")
            reader_mapping[node_id] = file_path

        elif node_name.strip().endswith("Writer"):
            file_path = node.get("parameters", {}).get("file_path", "")
            writer_mapping[node_id] = file_path
        else:
            # "Normal" node: transform into <dataprocessing>
            n_id, dp_element, n_name = build_node(node, index)
            root.append(dp_element)
            node_mapping[node_id] = {"element": dp_element, "index": index, "name": n_name}

    # Process connections
    links = data.get("connections", [])
    for link_index, conn in enumerate(links):
        source_id = conn.get("sourceID")
        dest_id = conn.get("destID")

        print("Link_index main:", link_index, "Source:", source_id, "Dest:", dest_id)
        # Case 1: connection from a Reader node to a "normal" node
        if source_id in reader_mapping and dest_id in node_mapping:
            target_node = node_mapping[dest_id]["element"]
            input_port = target_node.find("inputPort")
            reader_node_exists = True
            if input_port is not None:
                # Overwrite the fileName attribute with the file_path of the Reader
                input_port.set("fileName", reader_mapping[source_id])
            continue  # Do not create <link> element
        # Case 2: connection from a "normal" node to a Writer node
        if dest_id in writer_mapping and source_id in node_mapping:
            if reader_node_exists:
                link_index = link_index - 1
            source_node = node_mapping[source_id]["element"]
            output_port = source_node.find("outputPort")
            if output_port is not None:
                output_port.set("fileName", writer_mapping[dest_id])
            continue  # Do not create <link> element
        # Case 3: connection between two "normal" nodes
        if source_id in node_mapping and dest_id in node_mapping:
            if reader_node_exists:
                link_index = link_index - 1
            link_element = build_link(link_index, conn, node_mapping)
            if link_element is not None:
                root.append(link_element)

    # Convert XML to string and format it
    raw_xml = ET.tostring(root, encoding='utf-8')
    parsed = minidom.parseString(raw_xml)
    formatted_xml = "\n".join(line for line in parsed.toprettyxml(indent="    ").split("\n") if line.strip())

    # Save formatted XML to file
    output_xmi_filepath = os.path.join(xmi_output_path, xmi_output_filename)
    os.makedirs(os.path.dirname(output_xmi_filepath), exist_ok=True)
    with open(output_xmi_filepath, "w", encoding="utf-8") as file:
        file.write(formatted_xml)

    print(f"Workflow XMI saved to: {xmi_output_path}/{xmi_output_filename}")
