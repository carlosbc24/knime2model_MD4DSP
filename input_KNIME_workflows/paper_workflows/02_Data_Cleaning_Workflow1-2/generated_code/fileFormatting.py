import pandas as pd
import json
import h5py
import pyarrow
				
rowFilterPrimitive_Name__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilterPrimitive_input_dataDictionary.csv', sep = ',')
rowFilterPrimitive_Name__input_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilterPrimitive_input_dataDictionary.parquet')
				
rowFilterPrimitive_Name__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.csv', sep = ',')
rowFilterPrimitive_Name__output_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
