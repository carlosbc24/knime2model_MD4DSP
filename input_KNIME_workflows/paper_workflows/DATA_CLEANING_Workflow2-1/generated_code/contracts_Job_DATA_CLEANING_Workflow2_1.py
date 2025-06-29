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
	rowFilterMissing_marital_status__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.parquet')
	if os.path.exists('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet'):
		rowFilterMissing_marital_status__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=rowFilterMissing_marital_status__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='marital-status')
	
	data_smells.check_integer_as_floating_point(data_dictionary=rowFilterMissing_marital_status__input_dataDictionary_df, field='marital-status')
	
	data_smells.check_types_as_string(data_dictionary=rowFilterMissing_marital_status__input_dataDictionary_df, field='marital-status', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=rowFilterMissing_marital_status__input_dataDictionary_df, field='marital-status')
	
	data_smells.check_suspect_precision(data_dictionary=rowFilterMissing_marital_status__input_dataDictionary_df, field='marital-status')
	
	data_smells.check_suspect_distribution(data_dictionary=rowFilterMissing_marital_status__input_dataDictionary_df, min_value=0.0, max_value=1.0, field='marital-status')
	
	data_smells.check_date_as_datetime(data_dictionary=rowFilterMissing_marital_status__input_dataDictionary_df, field='marital-status')
	
	data_smells.check_separating_consistency(data_dictionary=rowFilterMissing_marital_status__input_dataDictionary_df, decimal_sep='.',  field='marital-status')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=rowFilterMissing_marital_status__input_dataDictionary_df, field='marital-status')
	
	
	missing_values_rowFilterMissing_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=rowFilterMissing_marital_status__input_dataDictionary_df, field='marital-status', 
									missing_values=missing_values_rowFilterMissing_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Row Filter (deprecated)"):
		print('PRECONDITION Row Filter (deprecated)(marital-status) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Row Filter (deprecated)(marital-status) MissingValues:[] NOT VALIDATED')
	
	missing_values_rowFilterMissing_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=rowFilterMissing_marital_status__output_dataDictionary_df, field='marital-status', 
									missing_values=missing_values_rowFilterMissing_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter (deprecated)"):
		print('POSTCONDITION Row Filter (deprecated)(marital-status) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Row Filter (deprecated)(marital-status) MissingValues:[] NOT VALIDATED')
	
	
	
	cols_special_type_values_rowFilterMissing_marital_status__INV_condition={'marital-status':{'missing': []}}
	
	if contract_invariants.check_inv_filter_rows_special_values(data_dictionary_in=rowFilterMissing_marital_status__input_dataDictionary_df,
											data_dictionary_out=rowFilterMissing_marital_status__output_dataDictionary_df,
											cols_special_type_values=cols_special_type_values_rowFilterMissing_marital_status__INV_condition,
											filter_type=FilterType.EXCLUDE, origin_function="Row Filter (deprecated)"):
		print('INVARIANT Row Filter (deprecated)(marital-status) FilterType:EXCLUDE SpecialValues: marital-statusMISSING:[] VALIDATED')
	else:
		print('INVARIANT Row Filter (deprecated)(marital-status) FilterType:EXCLUDE SpecialValues: marital-statusMISSING:[] NOT VALIDATED')
	
	
	#-----------------New DataProcessing-----------------
	rowFilterPrimitive_marital_status__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet'):
		rowFilterPrimitive_marital_status__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=['Divorced']
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=rowFilterPrimitive_marital_status__input_dataDictionary_df, 
														missing_invalid_list=list_invalid, common_missing_invalid_list=common_invalid_list, field='marital-status')
	
	data_smells.check_integer_as_floating_point(data_dictionary=rowFilterPrimitive_marital_status__input_dataDictionary_df, field='marital-status')
	
	data_smells.check_types_as_string(data_dictionary=rowFilterPrimitive_marital_status__input_dataDictionary_df, field='marital-status', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=rowFilterPrimitive_marital_status__input_dataDictionary_df, field='marital-status')
	
	data_smells.check_suspect_precision(data_dictionary=rowFilterPrimitive_marital_status__input_dataDictionary_df, field='marital-status')
	
	data_smells.check_suspect_distribution(data_dictionary=rowFilterPrimitive_marital_status__input_dataDictionary_df, min_value=9.0, max_value=202.0, field='marital-status')
	
	data_smells.check_date_as_datetime(data_dictionary=rowFilterPrimitive_marital_status__input_dataDictionary_df, field='marital-status')
	
	data_smells.check_separating_consistency(data_dictionary=rowFilterPrimitive_marital_status__input_dataDictionary_df, decimal_sep='.',  field='marital-status')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=rowFilterPrimitive_marital_status__input_dataDictionary_df, field='marital-status')
	
	
	if contract_pre_post.check_fix_value_range(value='Divorced', is_substring=False, data_dictionary=rowFilterPrimitive_marital_status__input_dataDictionary_df, belong_op=Belong(0), field='marital-status',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter (deprecated)"):
		print('PRECONDITION Row Filter (deprecated)(marital-status) FixValue:Divorced VALIDATED')
	else:
		print('PRECONDITION Row Filter (deprecated)(marital-status) FixValue:Divorced NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value='Divorced', is_substring=False, data_dictionary=rowFilterPrimitive_marital_status__output_dataDictionary_df, belong_op=Belong(0), field='marital-status',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter (deprecated)"):
		print('POSTCONDITION Row Filter (deprecated)(marital-status) FixValue:Divorced VALIDATED')
	else:
		print('POSTCONDITION Row Filter (deprecated)(marital-status) FixValue:Divorced NOT VALIDATED')
	
	
	
	columns_list_rowFilterPrimitive_marital_status__INV_condition=['marital-status']
	filter_fix_value_list_rowFilterPrimitive_marital_status__INV_condition=['Divorced']
	
	if contract_invariants.check_inv_filter_rows_primitive(data_dictionary_in=rowFilterPrimitive_marital_status__input_dataDictionary_df,
											data_dictionary_out=rowFilterPrimitive_marital_status__output_dataDictionary_df,
											columns=columns_list_rowFilterPrimitive_marital_status__INV_condition,
											filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_marital_status__INV_condition,
											filter_type=FilterType.INCLUDE, origin_function="Row Filter (deprecated)"):
		print('INVARIANT Row Filter (deprecated)(marital-status) FilterType:INCLUDE FixValueList:[Divorced] VALIDATED')
	else:
		print('INVARIANT Row Filter (deprecated)(marital-status) FilterType:INCLUDE FixValueList:[Divorced] NOT VALIDATED')
	
	


set_logger("contracts")
generateWorkflow()
