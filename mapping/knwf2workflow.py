import os
import yaml
from mapping.knwf2json import extract_data_knime2json
from mapping.json2workflow import json_to_xmi_workflow_with_templates
from utils.logger import set_logger
from utils.report import write_report


def process_workflow(file_name: str, input_knwf_folder: str, output_json_folder: str, output_xmi_folder: str,
                     include_contracts: bool) -> tuple[str, int, int]:
    """
    Process a workflow file to extract the mapping information and generate the corresponding XMI file.

    Args:
        file_name (str): Name of the workflow file.
        input_knwf_folder (str): Path to the input KNIME workflows folder.
        output_json_folder (str): Path to the output JSON folder.
        output_xmi_folder (str): Path to the output XMI folder.
        include_contracts (bool): Flag to include the contracts in the XMI file.

    Returns:
        tuple: Workflow name, number of mapped nodes and total number of nodes
    """
    # Generate workflow name and extract mapping info
    workflow_name = file_name.split(".")[0]
    extract_data_knime2json(file_name, input_knwf_folder, output_json_folder, workflow_name)
    mapped_nodes, nodes_count = json_to_xmi_workflow_with_templates(
        output_json_folder, workflow_name, output_xmi_folder, include_contracts
    )
    wf_mapping_info = (
        f"{workflow_name.ljust(70)} {round((mapped_nodes / nodes_count), 4) * 100}% "
        f"({mapped_nodes}/{nodes_count}) nodes mapped successfully to its model transformation"
    )
    print(wf_mapping_info)
    return workflow_name, mapped_nodes, nodes_count


def main():
    """
    Main function to process the workflows and generate the mapping reports.
    """
    set_logger(logger_name="mapping")

    # Load configuration
    with open("parser_config.yaml", "r") as file:
        config = yaml.safe_load(file)
        input_knwf_folder = config["input_knwf_folder"]
        output_json_folder = config["output_json_folder"]
        output_xmi_folder = config["output_xmi_folder"]
        workflow_filename = config["workflow_filename"]
        include_contracts = config["include_contracts"]
        export_mapped_nodes_report = config["export_mapped_nodes_report"]

    report_filepath = None
    # Setup report file if needed
    if export_mapped_nodes_report:
        report_filepath = os.path.join("reports", "node_mapping_report.csv")
        if not os.path.exists("reports"):
            os.makedirs("reports")
        with open(report_filepath, "w") as report_file:
            report_file.write("Workflow name,Mapping percentage,Nodes mapped\n")

    # Process a specific workflow or all workflows in the folder
    if workflow_filename and workflow_filename.endswith(".knwf"):
        workflow_name, mapped_nodes, nodes_count = process_workflow(
            workflow_filename, input_knwf_folder, output_json_folder, output_xmi_folder, include_contracts
        )
        if export_mapped_nodes_report:
            write_report(report_filepath, workflow_name, mapped_nodes, nodes_count)
    else:
        for file in os.listdir(input_knwf_folder):
            if file.endswith(".knwf"):
                workflow_name, mapped_nodes, nodes_count = process_workflow(
                    file, input_knwf_folder, output_json_folder, output_xmi_folder, include_contracts
                )
                if export_mapped_nodes_report:
                    write_report(report_filepath, workflow_name, mapped_nodes, nodes_count)

    print("\n--------------------------------------------------\n")
    print("Input workflows in: input_KNIME_workflows")
    print("Intermediate workflows in: parsed_json_workflows")
    print("Output workflows (Mapping results) in: parsed_xmi_workflows")


if __name__ == "__main__":
    main()
