import pandas as pd
import json
import h5py
import pyarrow
				
join_Name_with_City__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/join_input_dataDictionary.csv', sep = ',')
join_Name_with_City__input_dataDictionary.to_parquet('/wf_validation_python/data/output/join_input_dataDictionary.parquet')
				
join_Name_with_City__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/join_output_dataDictionary.csv', sep = ',')
join_Name_with_City__output_dataDictionary.to_parquet('/wf_validation_python/data/output/join_output_dataDictionary.parquet')
