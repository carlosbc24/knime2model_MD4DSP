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
	rowFilterRange_Altitude__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_input_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=rowFilterRange_Altitude__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Altitude')
	
	data_smells.check_integer_as_floating_point(data_dictionary=rowFilterRange_Altitude__input_dataDictionary_df, field='Altitude')
	
	data_smells.check_types_as_string(data_dictionary=rowFilterRange_Altitude__input_dataDictionary_df, field='Altitude', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=rowFilterRange_Altitude__input_dataDictionary_df, field='Altitude')
	
	data_smells.check_suspect_precision(data_dictionary=rowFilterRange_Altitude__input_dataDictionary_df, field='Altitude')
	
	data_smells.check_suspect_distribution(data_dictionary=rowFilterRange_Altitude__input_dataDictionary_df, min_value=1000.0, max_value=1.0E9, field='Altitude')
	
	data_smells.check_date_as_datetime(data_dictionary=rowFilterRange_Altitude__input_dataDictionary_df, field='Altitude')
	
	data_smells.check_separating_consistency(data_dictionary=rowFilterRange_Altitude__input_dataDictionary_df, decimal_sep='.',  field='Altitude')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=rowFilterRange_Altitude__input_dataDictionary_df, field='Altitude')
	
	

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
