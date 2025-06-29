import pandas as pd
import json
import h5py
import pyarrow
				
columnFilter_Latitude_Longitude__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/columnFilter_input_dataDictionary.csv', sep = ',', decimal = '.')
columnFilter_Latitude_Longitude__input_dataDictionary.to_parquet('/wf_validation_python/data/output/columnFilter_input_dataDictionary.parquet')
				
columnFilter_Latitude_Longitude__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/columnFilter_output_dataDictionary.csv', sep = ',', decimal = '.')
columnFilter_Latitude_Longitude__output_dataDictionary.to_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
