import os

import pandas as pd
import numpy as np
import functions.contract_invariants as contract_invariants
import functions.contract_pre_post as contract_pre_post
import functions.data_smells as data_smells
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	rowFilterRange_Latitude__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_input_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet'):
		rowFilterRange_Latitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=rowFilterRange_Latitude__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Latitude')
	
	data_smells.check_integer_as_floating_point(data_dictionary=rowFilterRange_Latitude__input_dataDictionary_df, field='Latitude')
	
	data_smells.check_types_as_string(data_dictionary=rowFilterRange_Latitude__input_dataDictionary_df, field='Latitude', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=rowFilterRange_Latitude__input_dataDictionary_df, field='Latitude')
	
	data_smells.check_suspect_precision(data_dictionary=rowFilterRange_Latitude__input_dataDictionary_df, field='Latitude')
	
	data_smells.check_suspect_distribution(data_dictionary=rowFilterRange_Latitude__input_dataDictionary_df, min_value=10.0, max_value=1.0E9, field='Latitude')
	
	data_smells.check_date_as_datetime(data_dictionary=rowFilterRange_Latitude__input_dataDictionary_df, field='Latitude')
	
	data_smells.check_separating_consistency(data_dictionary=rowFilterRange_Latitude__input_dataDictionary_df, decimal_sep='.',  field='Latitude')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=rowFilterRange_Latitude__input_dataDictionary_df, field='Latitude')
	
	
	if contract_pre_post.check_interval_range_float(left_margin=10.0, right_margin=1.0E9, data_dictionary=rowFilterRange_Latitude__input_dataDictionary_df,
	                                	closure_type=Closure(2), belong_op=Belong(0), field='Latitude', origin_function="Row Filter"):
		print('PRECONDITION Row Filter(Latitude) Interval:[10.0, 1.0E9) VALIDATED')
	else:
		print('PRECONDITION Row Filter(Latitude) Interval:[10.0, 1.0E9) NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=10.0, right_margin=1.0E9, data_dictionary=rowFilterRange_Latitude__output_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='Latitude', origin_function="Row Filter"):
		print('POSTCONDITION Row Filter(Latitude) Interval:[10.0, 1.0E9] VALIDATED')
	else:
		print('POSTCONDITION Row Filter(Latitude) Interval:[10.0, 1.0E9] NOT VALIDATED')
	
	
	
	columns_list_rowFilterRange_Latitude__INV_condition=['Latitude']
	left_margin_list_rowFilterRange_Latitude__INV_condition=[10.0]
	right_margin_list_rowFilterRange_Latitude__INV_condition=[1.0E9]
	closure_type_list_rowFilterRange_Latitude__INV_condition=[Closure.closedClosed]
	
	if contract_invariants.check_inv_filter_rows_range(data_dictionary_in=rowFilterRange_Latitude__input_dataDictionary_df,
											data_dictionary_out=rowFilterRange_Latitude__output_dataDictionary_df,
											columns=columns_list_rowFilterRange_Latitude__INV_condition,
											left_margin_list=left_margin_list_rowFilterRange_Latitude__INV_condition, right_margin_list=right_margin_list_rowFilterRange_Latitude__INV_condition,
											closure_type_list=closure_type_list_rowFilterRange_Latitude__INV_condition,
											filter_type=FilterType.INCLUDE, origin_function="Row Filter"):
		print('INVARIANT Row Filter(Latitude) FilterType:INCLUDE LeftMarginList:[10.0] RightMarginList:[1.0E9] ClosureTypeList:[Closure.closedClosed] VALIDATED')
	else:
		print('INVARIANT Row Filter(Latitude) FilterType:INCLUDE LeftMarginList:[10.0] RightMarginList:[1.0E9] ClosureTypeList:[Closure.closedClosed] NOT VALIDATED')
	
	
set_logger("contracts")
generateWorkflow()
