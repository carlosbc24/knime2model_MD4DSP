import pandas as pd
import json
import h5py
import pyarrow
				
rowFilterRange_Altitude__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilterRange_input_dataDictionary.csv', sep = ',')
rowFilterRange_Altitude__input_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilterRange_input_dataDictionary.parquet')
				
rowFilterRange_Altitude__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.csv', sep = ',')
rowFilterRange_Altitude__output_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')
