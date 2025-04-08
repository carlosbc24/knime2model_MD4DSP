import pandas as pd
import json
import h5py
import pyarrow
				
binner_Life_expectancy__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/binner_input_dataDictionary.csv', sep = ',')
binner_Life_expectancy__input_dataDictionary.to_parquet('/wf_validation_python/data/output/binner_input_dataDictionary.parquet')
				
binner_Life_expectancy__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/binner_output_dataDictionary.csv', sep = ',')
binner_Life_expectancy__output_dataDictionary.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
				
rowFilterPrimitive_Region__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/binner_output_dataDictionary.csv', sep = ',')
rowFilterPrimitive_Region__input_dataDictionary.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
				
rowFilterPrimitive_Region__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.csv', sep = ',')
rowFilterPrimitive_Region__output_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
				
columnFilter_Country_Region_Life_expectancy_Life_Expectancy_High_Low_Avg__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.csv', sep = ',')
columnFilter_Country_Region_Life_expectancy_Life_Expectancy_High_Low_Avg__input_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
				
columnFilter_Country_Region_Life_expectancy_Life_Expectancy_High_Low_Avg__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/columnFilter_output_dataDictionary.csv', sep = ',')
columnFilter_Country_Region_Life_expectancy_Life_Expectancy_High_Low_Avg__output_dataDictionary.to_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
