import csv
import os
import matplotlib.pyplot as plt
from matplotlib import ticker


def generate_workflow_nodes_mapping_table_report(workflows_summary: list):
    """
    Generate a CSV report where each row corresponds to a workflow, and columns represent
    the different node types mapped across all workflows. For each workflow, the table
    shows how many nodes of each type were mapped, plus a final column showing the total
    number of not mapped nodes across all node types for that workflow.

    Args:
        workflows_summary (list): A list of dictionaries, each containing:
            - "workflow_name" (str): Name of the workflow
            - "mapped_nodes_info" (dict): Node mapping information per node type.
              Example:
              {
                "CSV Reader": {"mapped_count": x, "not_mapped_count": y},
                "Excel Reader": {"mapped_count": x, "not_mapped_count": y},
                ...
              }
    """
    # Collect all node types that appear across all workflows
    all_node_types = set()
    for wf in workflows_summary:
        for node_type in wf["mapped_nodes_info"].keys():
            all_node_types.add(node_type)
    # Sort node types for consistent column ordering
    all_node_types = sorted(list(all_node_types))

    # Prepare column headers: one for workflow name, one per node type, plus "Not mapped nodes"
    column_labels = ["Workflow Name"] + all_node_types + ["Not mapped nodes"]

    # Prepare the table data
    data = []
    for wf in workflows_summary:
        workflow_name = wf["workflow_name"]
        mapped_nodes_info = wf["mapped_nodes_info"]

        # Build the row for this workflow
        row = [workflow_name]
        # Add mapped counts for each node type in the sorted list
        for node_type in all_node_types:
            mapped_count = mapped_nodes_info.get(node_type, {}).get("mapped_count", 0)
            row.append(mapped_count)

        # Compute the total number of not mapped nodes
        total_not_mapped = sum(info.get("not_mapped_count", 0) for info in mapped_nodes_info.values())
        row.append(total_not_mapped)

        data.append(row)

    # Ensure the 'reports' directory exists
    report_dir = "reports"
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)

    # Define the CSV file path
    csv_path = os.path.join(report_dir, "workflows_summary_report.csv")

    # Write the data to the CSV file
    with open(csv_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(column_labels)  # Write header
        writer.writerows(data)  # Write data rows


def generate_nodes_mapping_chart(global_mapped_nodes_info: dict):
    """
    Generate a horizontal bar chart showing the number of mapped and not mapped nodes per node type.

    This function dynamically adjusts the figure width to ensure all bars fit properly
    and sets the x-axis limit to avoid cutting off any bars.

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
    fig_width = max(12, max_total_nodes / 40)
    fig, ax = plt.subplots(figsize=(fig_width, 8))

    # Create stacked horizontal bar chart
    ax.barh(node_types, mapped_counts, color="blue", label="Mapped Nodes")
    ax.barh(node_types, not_mapped_counts, left=mapped_counts, color="orange", label="Not Mapped Nodes")

    # Set x-axis limit to ensure all bars are fully visible
    ax.set_xlim(0, max_total_nodes * 1.1)

    # Set x-axis to use integer ticks
    ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    # Labels and title
    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Node Name")
    ax.set_title("KNIME nodes to MD4DSP models Mapping Report")
    ax.legend()

    # Save the chart in the reports folder
    report_dir = "reports"
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)

    chart_path = os.path.join(report_dir, "nodes_mapping_chart.png")
    plt.savefig(chart_path, bbox_inches="tight")
    plt.close()


def generate_resumed_workflow_report(report_filepath: str, workflow_name: str, mapped_nodes: int, nodes_count: int):
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


def generate_nodes_mapping_report(global_mapped_nodes_info: dict, export_mapped_nodes_report: bool):
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


def generate_detailed_workflow_report(report_filepath: str, workflow_name: str, mapped_nodes: int, nodes_count: int,
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
