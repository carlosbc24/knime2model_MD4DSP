import os
from parsers.knwf2json import extract_knwf_data

INPUT_KNIME_WORKFLOW_TO_PARSE = "Data Cleaning Project.knwf"
OUPUT_JSON_FILENAME = "InputKnimeWorkflow"  # Nombre del archivo JSON de salida


wf_count = 0
# Extraer datos de todos los archivos KNIME en el directorio
for file in os.listdir("selected_KNIME_workflows"):
    if file.endswith(".knwf"):
        wf_count += 1
        extracted_output_filename = f"{OUPUT_JSON_FILENAME}_{wf_count}.json"
        extract_knwf_data(file, extracted_output_filename)
