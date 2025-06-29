import pandas as pd
import json
import h5py
import pyarrow
				
mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/mathOperation_input_dataDictionary.csv', sep = ',', decimal = '.')
mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary.to_parquet('/wf_validation_python/data/output/mathOperation_input_dataDictionary.parquet')
				
mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/mathOperation_output_dataDictionary.csv', sep = ',', decimal = '.')
mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary.to_parquet('/wf_validation_python/data/output/mathOperation_output_dataDictionary.parquet')
