import pandas as pd
import numpy as np
import functions.data_transformations as data_transformations
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	rowFilterPrimitive_Name__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_input_dataDictionary.parquet')

	rowFilterPrimitive_Name__input_dataDictionary_transformed=rowFilterPrimitive_Name__input_dataDictionary_df.copy()
	columns_rowFilterPrimitive_param_filter=['Name']
	
	filter_fix_value_list_rowFilterPrimitive_param_filter=['a*']
	
	rowFilterPrimitive_Name__input_dataDictionary_transformed=data_transformations.transform_filter_rows_primitive(data_dictionary=rowFilterPrimitive_Name__input_dataDictionary_transformed,
																											columns=columns_rowFilterPrimitive_param_filter,
																		                                    filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_param_filter,
																											filter_type=FilterType(1))
	rowFilterPrimitive_Name__output_dataDictionary_df=rowFilterPrimitive_Name__input_dataDictionary_transformed
	rowFilterPrimitive_Name__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	rowFilterPrimitive_Name__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	

set_logger("transformations")
generateWorkflow()
