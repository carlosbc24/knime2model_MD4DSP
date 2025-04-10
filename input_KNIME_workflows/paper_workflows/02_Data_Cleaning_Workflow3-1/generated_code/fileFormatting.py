import pandas as pd
import json
import h5py
import pyarrow
				
mapping_Country__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/mapping_input_dataDictionary.csv', sep = ',')
mapping_Country__input_dataDictionary.to_parquet('/wf_validation_python/data/output/mapping_input_dataDictionary.parquet')
				
mapping_Country__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/mapping_output_dataDictionary.csv', sep = ',')
mapping_Country__output_dataDictionary.to_parquet('/wf_validation_python/data/output/mapping_output_dataDictionary.parquet')
