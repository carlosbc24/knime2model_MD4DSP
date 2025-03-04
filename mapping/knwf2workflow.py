import os
import yaml
from mapping.knwf2json import extract_data_knime2json
from mapping.json2workflow import json_to_xmi_workflow_with_templates
from utils.logger import set_logger

# Set logger
set_logger(logger_name="mapping")

# Set report filepath and create reports folder if it does not exist
report_filepath = "reports/node_mapping_report.csv"
if not os.path.exists("reports"):
    os.makedirs("reports")

# Read yaml file configuration variables
with open("parser_config.yaml", "r") as file:
    config = yaml.safe_load(file)
    input_knwf_folder = config["input_knwf_folder"]
    output_json_folder = config["output_json_folder"]
    output_xmi_folder = config["output_xmi_folder"]
    workflow_filename = config["workflow_filename"]
    include_contracts = config["include_contracts"]
    export_mapped_nodes_report = config["export_mapped_nodes_report"]

# Extract data from a specific .knwf file
if workflow_filename is not None and workflow_filename != "":
    if workflow_filename.endswith(".knwf"):
        workflow_name = workflow_filename.split(".")[0]
        extract_data_knime2json(workflow_filename, input_knwf_folder, output_json_folder, workflow_name)
        mapped_nodes, nodes_count = json_to_xmi_workflow_with_templates(output_json_folder, workflow_name, output_xmi_folder, include_contracts)
        formated_wf_mapping_info = f"{workflow_name.ljust(70)} {round((mapped_nodes / nodes_count), 4) * 100}% ({mapped_nodes}/{nodes_count}) nodes mapped successfully to it's model transformation"
        print(formated_wf_mapping_info)

        if export_mapped_nodes_report:
            with open(report_filepath, "w") as file:
                file.write("Workflow name,Mapping percentage,Nodes mapped\n")
            with open(report_filepath, "a") as file:
                # In one row, write the workflow name in a cell
                # workflow name without commas
                workflow_name = workflow_name.replace(",", "")
                file.write(workflow_name + ",")
                # In the next cell, write the mapping percentage
                file.write(str(round((mapped_nodes / nodes_count), 4) * 100) + "%,")
                # In the next cell, write the proportion of nodes mapped
                file.write(f" ({mapped_nodes}/{nodes_count}) nodes mapped successfully to it's model transformation\n")

# Extract data from all .knwf files in the input folder
else:
    # Create a new file to store the mapping information
    with open(report_filepath, "w") as file:
        file.write("Workflow name,Mapping percentage,Nodes mapped\n")
    for file in os.listdir(input_knwf_folder):
        if file.endswith(".knwf"):
            workflow_name = file.split(".")[0]
            extract_data_knime2json(file, input_knwf_folder, output_json_folder, workflow_name)
            mapped_nodes, nodes_count = json_to_xmi_workflow_with_templates(output_json_folder, workflow_name, output_xmi_folder, include_contracts)
            formated_wf_mapping_info = f"{workflow_name.ljust(70)} {round((mapped_nodes / nodes_count), 4) * 100}% ({mapped_nodes}/{nodes_count}) nodes mapped successfully to it's model transformation"
            print(formated_wf_mapping_info)

            if export_mapped_nodes_report:
                with open(report_filepath, "a") as file:
                    # In one row, write the workflow name in a cell
                    # workflow name without commas
                    workflow_name = workflow_name.replace(",", "")
                    file.write(workflow_name + ",")
                    # In the next cell, write the mapping percentage
                    file.write(str(round((mapped_nodes / nodes_count), 4) * 100) + "%,")
                    # In the next cell, write the proportion of nodes mapped
                    file.write(f" ({mapped_nodes}/{nodes_count}) nodes mapped successfully to it's model transformation\n")

print("\n--------------------------------------------------\n")
print("Input workflows in: input_KNIME_workflows")
print("Intermediate workflows in: parsed_json_workflows")
print("Output workflows (Mapping results) in: parsed_xmi_workflows")
