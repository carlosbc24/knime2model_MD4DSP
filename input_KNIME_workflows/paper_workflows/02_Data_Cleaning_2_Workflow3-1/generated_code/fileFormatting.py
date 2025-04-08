import pandas as pd
import json
import h5py
import pyarrow
				
mapping_Tz_database_time_zone__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/mapping1_input_dataDictionary.csv', sep = ',')
mapping_Tz_database_time_zone__input_dataDictionary.to_parquet('/wf_validation_python/data/output/mapping1_input_dataDictionary.parquet')
				
mapping_Tz_database_time_zone__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/mapping1_output_dataDictionary.csv', sep = ',')
mapping_Tz_database_time_zone__output_dataDictionary.to_parquet('/wf_validation_python/data/output/mapping1_output_dataDictionary.parquet')
				
mapping_Source__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/mapping1_output_dataDictionary.csv', sep = ',')
mapping_Source__input_dataDictionary.to_parquet('/wf_validation_python/data/output/mapping1_output_dataDictionary.parquet')
				
mapping_Source__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/mapping2_output_dataDictionary.csv', sep = ',')
mapping_Source__output_dataDictionary.to_parquet('/wf_validation_python/data/output/mapping2_output_dataDictionary.parquet')
