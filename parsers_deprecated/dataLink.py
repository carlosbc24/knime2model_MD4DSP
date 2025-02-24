import xml.etree.ElementTree as elementTree


def create_link(link_index: int, conn: dict, node_mapping: dict) -> elementTree.Element:
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
    # Only create links if both nodes are "normal"
    if source_id in node_mapping and dest_id in node_mapping:
        source_info = node_mapping[source_id]
        dest_info = node_mapping[dest_id]
        link_element = elementTree.Element("link", {
            "source": f"//@dataprocessing.{source_info['index']}",
            "target": f"//@dataprocessing.{dest_info['index']}",
            "name": f"{source_info['name']}-{dest_info['name']}"
        })
        link_ref = f"//@link.{link_index}"
        # TODO: Correct way to update node links if ingoing and outgoing references permitted multiple links
        # Update outgoing attribute of the source node
        # current_out = source_info["element"].get("outgoing")
        # if current_out:
        #     source_info["element"].set("outgoing", current_out + " " + link_ref)
        # else:
        #     source_info["element"].set("outgoing", link_ref)
        # # Update incoming attribute of the target node
        # current_in = dest_info["element"].get("incoming")
        # if current_in:
        #     dest_info["element"].set("incoming", current_in + " " + link_ref)
        # else:
        #     dest_info["element"].set("incoming", link_ref)
        # TODO: Temporary solution to update node links if ingoing and outgoing references permitted only one link
        # Update outgoing attribute of the source node
        source_info["element"].set("outgoing", link_ref)
        # Update incoming attribute of the target node
        dest_info["element"].set("incoming", link_ref)
        return link_element
