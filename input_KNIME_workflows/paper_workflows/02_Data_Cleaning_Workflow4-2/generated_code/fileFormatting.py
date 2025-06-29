import pandas as pd
import json
import h5py
import pyarrow
				
binner_Longitude__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/binner_input_dataDictionary.csv', sep = ',', decimal = '.')
binner_Longitude__input_dataDictionary.to_parquet('/wf_validation_python/data/output/binner_input_dataDictionary.parquet')
				
binner_Longitude__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/binner_output_dataDictionary.csv', sep = ',', decimal = '.')
binner_Longitude__output_dataDictionary.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
