# T2M transformations with Python Parsers for MD4DSP

This project aims to map a KNIME workflow (`.knwf`) to a MD4DSP workflow (`.xmi`) using Python scripts. The project is divided into three main scripts:

1. **`parsers/knwf2json.py`**: script that parses a KNIME workflow and exports the data to a json intermediate file.
2. **`parsers/json2workflow.py`**: script that parses the intermediate json file and exports the data to a MD4DSP workflow.
3. **`parsers/knwf2workflow.py`**: script that combines the previous two scripts to parse a KNIME workflow and export the data to a MD4DSP Workflow instance.
## Prerequisites

- Anaconda Environment
- Python 3.11 (as it is the used version in the project)
- Libraries specified in `requirements.txt`

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/carlosbc24/knime2model_MD4DSP.git
    ```

2. Navigate to the project directory:
    ```bash
    cd your-project-directory
    ```
   
3. Crate a new conda environment:
   ```bash
   conda create --name kn2wf_mapping python=3.11 --yes
   ```
   
4. Deactivate any previous environment and activate the new one:
    ```bash
    conda deactivate
    conda activate kn2wf_mapping
    ```

5. Clean conda and pip caches:
    ```shell
    conda clean --all --yes
    pip cache purge
    ```
   This step will prevent you from retrieving libraries from the conda or pip caches, which may be incompatible with
   the project's requirements. If you are sure that the libraries in the cache are compatible, you can skip this step.

6. Install the required libraries:
   ```bash
   pip install -r requirements.txt
   ```
   
7. Edit the configuration file `parser_config.json` with the following structure:
   ```yaml
   input_knwf_folder: "input_KNIME_workflows/paper_workflows" # Folder containing the selected KNIME workflows
   output_json_folder: "parsed_json_workflows" # Folder where the parsed KNIME workflows will be saved
   output_xmi_folder: "parsed_xmi_workflows" # Folder where the parsed KNIME workflows will be saved
   
   include_contracts: True # Include contracts in the xmi output
   workflow_filename: "Model data set with metanode.knwf"
   #workflow_filename: "01 Data Cleaning.knwf"
   
   export_mapped_nodes_report: True # Export a report with the mapped nodes percentage
   ```
   
    The `input_knwf_folder` parameter must contain the path to the folder containing the KNIME workflows to be parsed and mapped.

    The `output_json_folder` parameter must contain the path to the folder where the parsed KNIME workflows will be saved in json format.

    The `output_xmi_folder` parameter must contain the path to the folder where the mapped KNIME workflows will be saved in xmi format, which is the MD4DSP workflow xml instance format.

   The `include_contracts` parameter is a boolean that indicates whether the contracts should be included in the xmi output or not.

   The `workflow_filename` must contain the name of the KNIME workflow to be parsed. By default, this parameter is empty,
   which means that every KNIME workflow in the input folder will be parsed. If you want to parse a specific KNIME 
   workflow, you must specify its filename with the extension `.knwf`.

    The `export_mapped_nodes_report` parameter is a boolean that indicates whether a report about the mapped nodes respect to the susceptible nodes to be mapped to the library should be exported or not.
   
8. Run the Python script to parse and export data to a MD4DSP workflow from a KNIME workflow using templates:
    ```bash
    python3 -m mapping.knwf2workflow
    ```

9. (Optional) Remove the environment created previously:
   ```bash
   conda deactivate
   conda remove --name kn2wf_mapping --all --yes
   ```

## Project Structure

The project structure must follow the next structure:

```bash
MD4DSP-m2python/
│
├── input_KNIME_workflows/
│ ├── 01 Data Cleaning.knwf
│ ├── 01_ Exercises.knwf
│ ├── 01_Column_Row_Filtering.knwf
│ ├── 02 Data Cleaning.knwf
│ ├── 02 Data Cleaning and Transformation.knwf
│ ├── 03_Data_Cleaning_Solution.knwf
│ ├── DATA CLEANING.knwf
│ ├── Data Cleaning Project.knwf
│ ├── Decision Tree Modelling - Key Triathlon Discipline Analysis.knwf
│ ├── Interactive Data Cleaning.knwf
│ ├── KNIME INTEGRATION WITH POWER  BI (DATA CLEANING).knwf
│ ├── ...
│ ├── extracted_data/
│ │ ├── 01 Data Cleaning.png
│ │ ├── 01_ Exercises.png
│ │ ├── 01_Column_Row_Filtering.png
│ │ ├── 02 Data Cleaning.png
│ │ ├── 02 Data Cleaning and Transformation.png
│ │ ├── 03_Data_Cleaning_Solution.png
│ │ ├── DATA CLEANING.png
│ │ ├── Data Cleaning Project.png
│ │ ├── Decision Tree Modelling - Key Triathlon Discipline Analysis.png
│ │ ├── Interactive Data Cleaning.png
│ │ ├── KNIME INTEGRATION WITH POWER  BI (DATA CLEANING).png
│ │ └── ...
│ └── images/
│   ├── 01 Data Cleaning.png
│   ├── 01_ Exercises.png
│   ├── 01_Column_Row_Filtering.png
│   ├── 02 Data Cleaning.png
│   ├── 02 Data Cleaning and Transformation.png
│   ├── 03_Data_Cleaning_Solution.png
│   ├── DATA CLEANING.png
│   ├── Data Cleaning Project.png
│   ├── Decision Tree Modelling - Key Triathlon Discipline Analysis.png
│   ├── Interactive Data Cleaning.png
│   ├── KNIME INTEGRATION WITH POWER  BI (DATA CLEANING).png
│   └── ...
│
├── logs/
│ ├── mapping/
│   ├── mapping_log_1.log
│   ├── mapping_log_2.log
│   └── ...
│
├── mapping/
│ ├── json2workflow.py
│ ├── knwf2workflow.py
│ └── knwf2json.py
│
├── parsed_json_workflows/
│   └── ...
│
├── parsed_xmi_workflows/
│   └── ...
│
├── utils/
│ ├── json_parser_functions.py
│ ├── library_functions.py
│ └── logger.py
│
├── .gitignore
├── library_function_hashing.json
├── parser_config.yaml
├── README.md
└── requirements.txt
```


- **`input_KNIME_workflows/`**: contains the input KNIME workflows to be parsed and exported json via Python script.


- **`input_KNIME_workflows/extracted_data/`**: contains the extracted data from the input KNIME workflows.


- **`input_KNIME_workflows/images/`**: contains the images of the input KNIME workflows.


- **`logs/`**: contains the logs of the project.


- **`mapping/`**: contains the Python scripts to parse and export data from a KNIME workflow to a json 
  file 
  and from a json file to a MD4DSP workflow


- **`parsed_json_workflows/`**: contains the json data from the parsed input KNIME workflows.


- **`parsed_xmi_workflows/`**: contains the xmi data from the parsed json data.


- **`utils/`**: contains the utility scripts of the project (e.g., logger, library functions, json parser functions).


- **`.gitignore`**: file that contains the files and directories to be ignored by Git.


- **`parser_config.yaml`**: yaml file that contains the configuration of the parser.


- **`README.md`**: file that contains the documentation of the project.
  

- **`requirements.txt`**: file that contains the libraries needed to run the project.

## Authors
- Carlos Breuer Carrasco
- Carlos Cambero Rojas

## Questions
If you have any questions, please contact to any of the authors.
