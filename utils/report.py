import os


def write_resumed_workflow_report(report_filepath: str, workflow_name: str, mapped_nodes: int, nodes_count: int):
    """
    Write the mapping information to the report file.

    Args:
        report_filepath (str): Path to the report file.
        workflow_name (str): Name of the workflow.
        mapped_nodes (int): Number of mapped nodes.
        nodes_count (int): Total number of nodes.
    """
    # Remove commas from workflow name and format the report line
    workflow_name = workflow_name.replace(",", "")
    mapping_pct = str(round((mapped_nodes / nodes_count), 4) * 100) + "%"
    report_line = (f"{workflow_name},{mapping_pct}, ({mapped_nodes}/{nodes_count}) nodes mapped successfully to its "
                   f"model transformation\n")

    # Write the report line
    with open(report_filepath, "a") as report_file:
        report_file.write(report_line)


def write_nodes_mapping_report(global_mapped_nodes_info: dict, export_mapped_nodes_report: bool):
    """
    Write the mapping information for each node type to the report file.

    Args:
        global_mapped_nodes_info (dict): Global mapped nodes information.
        export_mapped_nodes_report (bool): Flag to export the mapped nodes report.
    """
    if export_mapped_nodes_report:
        individual_nodes_mapping_report_filepath = os.path.join("reports", "individual_nodes_mapping_report.csv")
        with open(individual_nodes_mapping_report_filepath, "w") as individual_nodes_report_file:
            individual_nodes_report_file.write("Node type,Nº of mapped nodes,Nº of not mapped nodes\n")
            for node_type, counts in global_mapped_nodes_info.items():
                mapped_count = counts.get("mapped_count", 0)
                not_mapped_count = counts.get("not_mapped_count", 0)
                report_line = f"{node_type},{mapped_count},{not_mapped_count}\n"
                individual_nodes_report_file.write(report_line)


def write_detailed_workflow_report(report_filepath: str, workflow_name: str, mapped_nodes: int, nodes_count: int,
                                   mapped_nodes_info: dict):
    """
    Write the mapping information to the report file.

    Args:
        report_filepath (str): Path to the report file.
        workflow_name (str): Name of the workflow.
        mapped_nodes (int): Number of mapped nodes.
        nodes_count (int): Total number of nodes.
        mapped_nodes_info (dict): Dictionary with the mapping information.
    """
    # Remove commas from workflow name and format the report line
    workflow_name = workflow_name.replace(",", "")
    mapping_pct = str(round((mapped_nodes / nodes_count), 4) * 100) + "%"
    workflow_report_line = \
        (f"{workflow_name},{mapping_pct}, ({mapped_nodes}/{nodes_count}) nodes mapped successfully to its "
         f"model transformation\n")

    # Write the workflow report line and the nodes mapping information
    with open(report_filepath, "a") as report_file:
        report_file.write(workflow_report_line)
        for key, value in mapped_nodes_info.items():
            node_type_report_line = (f"\t{key.ljust(50)}"
                                     f"{value['mapped_count']}/{value['mapped_count'] + value['not_mapped_count']} "
                                     f"nodes mapped successfully\n")
            report_file.write(node_type_report_line)
        report_file.write("\n--------------------------------------------------\n")


def update_global_mapped_nodes_info(global_mapped_nodes_info: dict, mapped_nodes_info: dict) -> dict:
    """
    Update the global mapped nodes information with the information of the current workflow.

    Args:
        global_mapped_nodes_info (dict): Global mapped nodes information.
        mapped_nodes_info (dict): Mapped nodes information of the current workflow.

    Returns:
        global_mapped_nodes_info (dict): Updated global mapped nodes information.
    """
    for key, value in mapped_nodes_info.items():
        if key not in global_mapped_nodes_info:
            global_mapped_nodes_info[key] = {"mapped_count": 0, "not_mapped_count": 0}
        global_mapped_nodes_info[key]["mapped_count"] += value["mapped_count"]
        global_mapped_nodes_info[key]["not_mapped_count"] += value["not_mapped_count"]

    return global_mapped_nodes_info
