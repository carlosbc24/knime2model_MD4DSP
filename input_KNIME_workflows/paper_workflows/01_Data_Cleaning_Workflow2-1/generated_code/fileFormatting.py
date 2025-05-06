import pandas as pd
import json
import h5py
import pyarrow
				
columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/columnFilter_input_dataDictionary.csv', sep = ',')
columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary.to_parquet('/wf_validation_python/data/output/columnFilter_input_dataDictionary.parquet')
				
columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/columnFilter_output_dataDictionary.csv', sep = ',')
columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__output_dataDictionary.to_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
