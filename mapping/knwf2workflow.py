import os
import yaml

from mapping.json2workflow import json_to_xmi_workflow
from mapping.knwf2json import extract_data_knime2json

# Read yaml file configuration variables
with open("parser_config.yaml", "r") as file:
    config = yaml.safe_load(file)
    input_knwf_folder = config["input_knwf_folder"]
    output_json_folder = config["output_json_folder"]
    output_xmi_folder = config["output_xmi_folder"]

# Extract data from all .knwf files in the input folder
for file in os.listdir(input_knwf_folder):
    if file.endswith(".knwf"):
        workflow_filename = file.split(".")[0]
        extract_data_knime2json(file, input_knwf_folder, output_json_folder, workflow_filename)
        json_to_xmi_workflow(output_json_folder, workflow_filename, output_xmi_folder)
