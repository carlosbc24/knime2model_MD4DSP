import xml.etree.ElementTree as ET


def create_link(link_index: int, conn: dict, node_mapping: dict) -> ET.Element:
    """
    Processes a connection (link) between "normal" nodes and updates the 'incoming' and 'outgoing' attributes of the
    source and target node elements. Returns the generated <link> element.

    Args:
        link_index (int): Index of the link.
        conn (dict): Connection data from the JSON.
        node_mapping (dict): Mapping of node IDs to their XML elements and metadata.

    Returns:
        Element: The generated <link> element.
    """
    source_id = conn.get("sourceID")
    dest_id = conn.get("destID")
    print("Link_index:", link_index, "Source:", source_id, "Dest:", dest_id)
    # Only create links if both nodes are "normal"
    if source_id in node_mapping and dest_id in node_mapping:
        source_info = node_mapping[source_id]
        dest_info = node_mapping[dest_id]
        link_element = ET.Element("link", {
            "source": f"//@dataprocessing.{source_info['index']}",
            "target": f"//@dataprocessing.{dest_info['index']}",
            "name": f"{source_info['name']}-{dest_info['name']}"
        })
        link_ref = f"//@link.{link_index}"
        # Update outgoing attribute of the source node
        current_out = source_info["element"].get("outgoing")
        if current_out:
            source_info["element"].set("outgoing", current_out + " " + link_ref)
        else:
            source_info["element"].set("outgoing", link_ref)
        # Update incoming attribute of the target node
        current_in = dest_info["element"].get("incoming")
        if current_in:
            dest_info["element"].set("incoming", current_in + " " + link_ref)
        else:
            dest_info["element"].set("incoming", link_ref)
        return link_element
