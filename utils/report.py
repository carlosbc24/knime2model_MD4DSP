

def write_report(report_filepath: str, workflow_name: str, mapped_nodes: int, nodes_count: int):
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
    with open(report_filepath, "a") as report_file:
        report_file.write(report_line)
