import os
import yaml
from mapping.knwf2json import extract_data_knime2json
from mapping.json2workflow import json_to_xmi_workflow_with_templates
from utils.logger import set_logger

# Set logger
set_logger(logger_name="mapping")

# Read yaml file configuration variables
with open("parser_config.yaml", "r") as file:
    config = yaml.safe_load(file)
    input_knwf_folder = config["input_knwf_folder"]
    output_json_folder = config["output_json_folder"]
    output_xmi_folder = config["output_xmi_folder"]
    workflow_filename = config["workflow_filename"]
    include_contracts = config["include_contracts"]

# Extract data from a specific .knwf file
if workflow_filename is not None and workflow_filename != "":
    if workflow_filename.endswith(".knwf"):
        workflow_name = workflow_filename.split(".")[0]
        extract_data_knime2json(workflow_filename, input_knwf_folder, output_json_folder, workflow_name)
        json_to_xmi_workflow_with_templates(output_json_folder, workflow_name, output_xmi_folder, include_contracts)
        print(f"{workflow_name} workflow mapped successfully to it's model transformation")

# Extract data from all .knwf files in the input folder
else:
    for file in os.listdir(input_knwf_folder):
        if file.endswith(".knwf"):
            workflow_name = file.split(".")[0]
            extract_data_knime2json(file, input_knwf_folder, output_json_folder, workflow_name)
            json_to_xmi_workflow_with_templates(output_json_folder, workflow_name, output_xmi_folder, include_contracts)
            print(f"{workflow_name} workflow mapped successfully to it's model transformation")

print("\n--------------------------------------------------\n")
print("Input workflows in: input_KNIME_workflows")
print("Intermediate workflows in: parsed_json_workflows")
print("Output workflows (Mapping results) in: parsed_xmi_workflows")
