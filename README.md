# T2M transformations with LLMs for MD4DSP

## Prerequisites

- Anaconda Environment
- Python 3.11 (as it is the used version in the project)
- Libraries specified in `requirements.txt`

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/carlosbc24/T2M_LLM_MD4DSP.git
    ```

2. Navigate to the project directory:
    ```bash
    cd your-project-directory
    ```
   
3. Crate a new conda environment:
   ```bash
   conda create --name t2m_llm_md4dsp python=3.11 --yes
   ```
   
4. Deactivate any previous environment and activate the new one:
    ```bash
    $ conda deactivate
    $ conda activate t2m_llm_md4dsp
    ```

5. Clean conda and pip caches:
    ```shell
    $ conda clean --all --yes
    $ pip cache purge
    ```
   This step will prevent you from retrieving libraries from the conda or pip caches, which may be incompatible with
   the project's requirements. If you are sure that the libraries in the cache are compatible, you can skip this step.

6. Install the required libraries:
   ```bash
   pip install -r requirements.txt
   ```
   
7. Run the Python script to parse and export data from a KNIME workflow:
    ```bash
    python knwf_data_parser.py
    ```

8. (Optional) Remove the environment created previously:
   ```bash
   conda deactivate
   conda remove --name t2m_llm_md4dsp --all --yes
   ```

## Project Structure

The project structure must follow the next structure:

```bash
MD4DSP-m2python/
│
├── selected_KNIME_workflows/
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
│ └── images
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
│ └── extracted_data
│   └── ...
│ └── parsed_data
│   └── ...
│
├── .gitignore
├── knwf_data_parser.py
├── README.md
└── requirements.txt
```

- **`selected_KNIME_workflows/`**: contains the selected KNIME workflows to be parsed and exported json via Python script.


- **`selected_KNIME_workflows/images/`**: contains the images of the selected KNIME workflows.


- **`selected_KNIME_workflows/extracted_data/`**: contains the extracted data from the selected KNIME workflows.


- **`selected_KNIME_workflows/parsed_data/`**: contains the parsed data from the selected KNIME workflows.


- **`README.md`**: file that contains the documentation of the project.
  

- **`requirements.txt`**: file that contains the libraries needed to run the project.


## External Documentation
The external documentation of the project is available in the following link: https://unexes.sharepoint.com/:f:/s/PDI_i3lab/EutekinB0hRGg6lyGESzkE4B544DedZpFTlwswpUUZh8ag?e=5RPUgt
  
## Authors
- Carlos Breuer Carrasco
- Carlos Cambero Rojas

## Questions
If you have any questions, please contact to any of the authors.
