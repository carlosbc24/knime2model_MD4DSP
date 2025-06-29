import pandas as pd
import numpy as np
import functions.data_transformations as data_transformations
import functions.data_smells as data_smells
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	rowFilterPrimitive_Name__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_input_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=['a*']
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=rowFilterPrimitive_Name__input_dataDictionary_df, 
														missing_invalid_list=list_invalid, common_missing_invalid_list=common_invalid_list, field='Name')
	
	data_smells.check_integer_as_floating_point(data_dictionary=rowFilterPrimitive_Name__input_dataDictionary_df, field='Name')
	
	data_smells.check_types_as_string(data_dictionary=rowFilterPrimitive_Name__input_dataDictionary_df, field='Name', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=rowFilterPrimitive_Name__input_dataDictionary_df, field='Name')
	
	data_smells.check_suspect_precision(data_dictionary=rowFilterPrimitive_Name__input_dataDictionary_df, field='Name')
	
	data_smells.check_suspect_distribution(data_dictionary=rowFilterPrimitive_Name__input_dataDictionary_df, min_value=9.0, max_value=202.0, field='Name')
	
	data_smells.check_date_as_datetime(data_dictionary=rowFilterPrimitive_Name__input_dataDictionary_df, field='Name')
	
	data_smells.check_separating_consistency(data_dictionary=rowFilterPrimitive_Name__input_dataDictionary_df, decimal_sep='.',  field='Name')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=rowFilterPrimitive_Name__input_dataDictionary_df, field='Name')
	
	

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
