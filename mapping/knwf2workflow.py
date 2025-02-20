import os
import yaml
from tabulate import tabulate

from mapping.json2workflow import json_to_xmi_workflow
from mapping.knwf2json import extract_data_knime2json
from utils.logger import set_logger

# Read yaml file configuration variables
with open("parser_config.yaml", "r") as file:
    config = yaml.safe_load(file)
    input_knwf_folder = config["input_knwf_folder"]
    output_json_folder = config["output_json_folder"]
    output_xmi_folder = config["output_xmi_folder"]
    workflow_filename = config["workflow_filename"]
    include_contracts = config["include_contracts"]
    node_mapping_desired_ratio = config["node_mapping_desired_ratio"]
    if include_contracts is None:
        include_contracts = True

# Set logger
set_logger(logger_name="mapping")

# Extract data from a specific .knwf file
if workflow_filename is not None and workflow_filename != "":
    if workflow_filename.endswith(".knwf"):
        workflow_name = workflow_filename.split(".")[0]
        extract_data_knime2json(workflow_filename, input_knwf_folder, output_json_folder, workflow_name)
        mapped_nodes, nodes_count, no_mapped_nodes = json_to_xmi_workflow(output_json_folder, workflow_name, output_xmi_folder,
                                                                          include_contracts, node_mapping_desired_ratio)
        print(f"{workflow_name.ljust(70)} {mapped_nodes}/{nodes_count} nodes mapped successfully to it's model transformation")

# Extract data from all .knwf files in the input folder
else:
    no_mapped_nodes_global = {}
    for file in os.listdir(input_knwf_folder):
        if file.endswith(".knwf"):
            workflow_name = file.split(".")[0]
            extract_data_knime2json(file, input_knwf_folder, output_json_folder, workflow_name)
            mapped_nodes, nodes_count, no_mapped_nodes = json_to_xmi_workflow(output_json_folder, workflow_name, output_xmi_folder,
                                                                              include_contracts, node_mapping_desired_ratio)

            print(f"{workflow_name.ljust(10)} {mapped_nodes}/{nodes_count} nodes mapped successfully to it's model transformation")

            for node in no_mapped_nodes:
                if node not in no_mapped_nodes_global:
                    no_mapped_nodes_global[node] = no_mapped_nodes[node]
                else:
                    no_mapped_nodes_global[node] += no_mapped_nodes[node]

            # Sort the dictionary by value
            no_mapped_nodes_global = dict(sorted(no_mapped_nodes_global.items(), key=lambda item: item[1], reverse=True))

    # Convert the dictionary to a list of lists for tabulate
    table_data = [[name, count] for name, count in no_mapped_nodes_global.items()]

    # Print the table
    print(tabulate(table_data, headers=["Node Name No Mapped to xmi", "Count"], tablefmt="grid"))

print("\n--------------------------------------------------\n")
print("Input workflows in: input_KNIME_workflows")
print("Intermediate workflows in: parsed_json_workflows")
print("Output workflows (Mapping results) in: parsed_xmi_workflows")
