import pandas as pd
import json
import h5py
import pyarrow
				
mathOperation_Change__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/mathOperation1_input_dataDictionary.csv', sep = ',')
mathOperation_Change__input_dataDictionary.to_parquet('/wf_validation_python/data/output/mathOperation1_input_dataDictionary.parquet')
				
mathOperation_Change__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.csv', sep = ',')
mathOperation_Change__output_dataDictionary.to_parquet('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet')
				
mathOperation_Percentage__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.csv', sep = ',')
mathOperation_Percentage__input_dataDictionary.to_parquet('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet')
				
mathOperation_Percentage__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.csv', sep = ',')
mathOperation_Percentage__output_dataDictionary.to_parquet('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet')
				
binner_Percentage__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.csv', sep = ',')
binner_Percentage__input_dataDictionary.to_parquet('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet')
				
binner_Percentage__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/binner_output_dataDictionary.csv', sep = ',')
binner_Percentage__output_dataDictionary.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
