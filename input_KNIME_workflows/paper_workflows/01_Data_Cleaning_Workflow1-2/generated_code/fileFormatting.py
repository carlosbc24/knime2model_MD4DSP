import pandas as pd
import json
import h5py
import pyarrow
				
rowFilterMissing_Life_expectancy__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.csv', sep = ',')
rowFilterMissing_Life_expectancy__input_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.parquet')
				
rowFilterMissing_Life_expectancy__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.csv', sep = ',')
rowFilterMissing_Life_expectancy__output_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')
				
rowFilterRange_Life_expectancy__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.csv', sep = ',')
rowFilterRange_Life_expectancy__input_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')
				
rowFilterRange_Life_expectancy__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.csv', sep = ',')
rowFilterRange_Life_expectancy__output_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')
				
rowFilterPrimitive_Year__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.csv', sep = ',')
rowFilterPrimitive_Year__input_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')
				
rowFilterPrimitive_Year__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.csv', sep = ',')
rowFilterPrimitive_Year__output_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
