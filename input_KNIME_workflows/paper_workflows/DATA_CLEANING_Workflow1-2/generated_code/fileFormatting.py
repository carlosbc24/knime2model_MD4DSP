import pandas as pd
import json
import h5py
import pyarrow
				
binner_hours_per_week__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/binner_input_dataDictionary.csv', sep = ',', decimal = '.')
binner_hours_per_week__input_dataDictionary.to_parquet('/wf_validation_python/data/output/binner_input_dataDictionary.parquet')
				
binner_hours_per_week__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/binner_output_dataDictionary.csv', sep = ',', decimal = '.')
binner_hours_per_week__output_dataDictionary.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
				
mapping_native_country__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/binner_output_dataDictionary.csv', sep = ',', decimal = '.')
mapping_native_country__input_dataDictionary.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
				
mapping_native_country__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/columnExpressions_output_dataDictionary.csv', sep = ',', decimal = '.')
mapping_native_country__output_dataDictionary.to_parquet('/wf_validation_python/data/output/columnExpressions_output_dataDictionary.parquet')
				
mathOperation_year_of_birth__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/columnExpressions_output_dataDictionary.csv', sep = ',', decimal = '.')
mathOperation_year_of_birth__input_dataDictionary.to_parquet('/wf_validation_python/data/output/columnExpressions_output_dataDictionary.parquet')
				
mathOperation_year_of_birth__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/columnExpressions_output_dataDictionary.csv', sep = ',', decimal = '.')
mathOperation_year_of_birth__output_dataDictionary.to_parquet('/wf_validation_python/data/output/columnExpressions_output_dataDictionary.parquet')
