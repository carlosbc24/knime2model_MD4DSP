import pandas as pd
import json
import h5py
import pyarrow
				
rowFilterMissing_Equipment__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.csv', sep = ',', decimal = '.')
rowFilterMissing_Equipment__input_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.parquet')
				
rowFilterMissing_Equipment__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.csv', sep = ',', decimal = '.')
rowFilterMissing_Equipment__output_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')
