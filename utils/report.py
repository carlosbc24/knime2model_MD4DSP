
def write_resumed_report(report_filepath: str, workflow_name: str, mapped_nodes: int, nodes_count: int):
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


def write_detailed_report(report_filepath: str, workflow_name: str, mapped_nodes: int, nodes_count: int, mapped_nodes_info: dict):
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
            node_type_report_line = f"\t{key.ljust(50)}{value['mapped_count']}/{value['mapped_count'] + value['not_mapped_count']} nodes mapped successfully\n"
            report_file.write(node_type_report_line)
        report_file.write("\n--------------------------------------------------\n")
