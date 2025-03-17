import os
import matplotlib.pyplot as plt


def generate_nodes_mapping_chart(global_mapped_nodes_info: dict):
    """
    Generate a horizontal bar chart showing the number of mapped and not mapped nodes per node type.

    This function dynamically adjusts the figure width to ensure all bars fit properly.

    Args:
        global_mapped_nodes_info (dict): Dictionary with node types as keys and mapping counts as values.
    """
    # Extract node types and counts
    node_types = list(global_mapped_nodes_info.keys())
    mapped_counts = [global_mapped_nodes_info[node]["mapped_count"] for node in node_types]
    not_mapped_counts = [global_mapped_nodes_info[node]["not_mapped_count"] for node in node_types]

    # Compute total nodes per type to determine the maximum value
    total_counts = [mapped + not_mapped for mapped, not_mapped in zip(mapped_counts, not_mapped_counts)]
    max_total_nodes = max(total_counts)  # Maximum total number of nodes for scaling

    # Adjust figure width dynamically based on the max total nodes with extra margin
    fig_width = max(12, max_total_nodes / 40)  # Increased minimum width and scaling factor
    fig, ax = plt.subplots(figsize=(fig_width, 8))  # Increased height slightly for clarity

    # Create stacked horizontal bar chart
    ax.barh(node_types, mapped_counts, color="blue", label="Mapped Nodes")
    ax.barh(node_types, not_mapped_counts, left=mapped_counts, color="orange", label="Not Mapped Nodes")

    # Labels and title
    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Node Type")
    ax.set_title("Nodes Mapping Report")
    ax.legend()

    # Save the chart in the reports folder
    report_dir = "reports"
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)

    chart_path = os.path.join(report_dir, "nodes_mapping_chart.png")
    plt.savefig(chart_path, bbox_inches="tight")
    plt.close()


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
