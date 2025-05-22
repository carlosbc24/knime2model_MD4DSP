import pandas as pd
import numpy as np
import functions.data_transformations as data_transformations
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	rowFilterRange_Altitude__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_input_dataDictionary.parquet')

	rowFilterRange_Altitude__input_dataDictionary_transformed=rowFilterRange_Altitude__input_dataDictionary_df.copy()
	columns_rowFilterRange_param_filter=['Altitude']
	
	filter_range_left_values_list_rowFilterRange_param_filter=[1000.0]
	filter_range_right_values_list_rowFilterRange_param_filter=[np.inf]
	closure_type_list_rowFilterRange_param_filter=[Closure(3)]
	
	rowFilterRange_Altitude__input_dataDictionary_transformed=data_transformations.transform_filter_rows_range(data_dictionary=rowFilterRange_Altitude__input_dataDictionary_transformed,
																											columns=columns_rowFilterRange_param_filter,
																											left_margin_list=filter_range_left_values_list_rowFilterRange_param_filter,
																											right_margin_list=filter_range_right_values_list_rowFilterRange_param_filter,
																											filter_type=FilterType(1),
																											closure_type_list=closure_type_list_rowFilterRange_param_filter)
	rowFilterRange_Altitude__output_dataDictionary_df=rowFilterRange_Altitude__input_dataDictionary_transformed
	rowFilterRange_Altitude__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')
	rowFilterRange_Altitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')
	

set_logger("transformations")
generateWorkflow()
