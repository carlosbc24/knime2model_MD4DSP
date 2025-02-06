import zipfile
import xml.etree.ElementTree as ET

# Path to the uploaded KNIME workflow file
knwf_file_path = "/mnt/data/Sample3.knwf"

# Extract the KNIME workflow files
extracted_path = "/mnt/data/knime_workflow_extracted"
with zipfile.ZipFile(knwf_file_path, 'r') as zip_ref:
    zip_ref.extractall(extracted_path)

# Locate and parse the workflow.knime file (this contains the node information)
workflow_knime_path = f"{extracted_path}/workflow.knime"

# Parse XML to extract nodes and their types
nodes_info = []
if zipfile.os.path.exists(workflow_knime_path):
    tree = ET.parse(workflow_knime_path)
    root = tree.getroot()

    for node in root.findall(".//node"):
        node_id = node.get("id", "Unknown")
        node_type = node.get("factory", "Unknown")
        nodes_info.append((node_id, node_type))

# Convert the extracted data into a structured format
import pandas as pd

df_nodes = pd.DataFrame(nodes_info, columns=["Node ID", "Node Type"])

# Display the extracted nodes information
import ace_tools as tools

tools.display_dataframe_to_user(name="KNIME Workflow Nodes", dataframe=df_nodes)
