import os

import pandas as pd
import numpy as np
import functions.contract_invariants as contract_invariants
import functions.contract_pre_post as contract_pre_post
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	rowFilterMissing_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.parquet')
	if os.path.exists('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet'):
		rowFilterMissing_Life_expectancy__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')

	missing_values_rowFilterMissing_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy', 
									missing_values=missing_values_rowFilterMissing_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Row Filter"):
		print('PRECONDITION rowFilterMissing(Life_expectancy)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION rowFilterMissing(Life_expectancy)_PRE_valueRange NOT VALIDATED')
	
	missing_values_rowFilterMissing_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=rowFilterMissing_Life_expectancy__output_dataDictionary_df, field='Life_expectancy', 
									missing_values=missing_values_rowFilterMissing_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('POSTCONDITION rowFilterMissing(Life_expectancy)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION rowFilterMissing(Life_expectancy)_POST_valueRange NOT VALIDATED')
	
	
	
	cols_special_type_values_rowFilterMissing_Life_expectancy__INV_condition={'Life_expectancy':{'missing': []}}
	
	if contract_invariants.check_inv_filter_rows_special_values(data_dictionary_in=rowFilterMissing_Life_expectancy__input_dataDictionary_df,
											data_dictionary_out=rowFilterMissing_Life_expectancy__output_dataDictionary_df,
											cols_special_type_values=cols_special_type_values_rowFilterMissing_Life_expectancy__INV_condition,
											filter_type=FilterType.EXCLUDE):
		print('INVARIANT rowFilterMissing(Life_expectancy)_INV_condition VALIDATED')
	else:
		print('INVARIANT rowFilterMissing(Life_expectancy)_INV_condition NOT VALIDATED')
	
	
	#-----------------New DataProcessing-----------------
	rowFilterPrimitive_Year__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet'):
		rowFilterPrimitive_Year__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')

	if contract_pre_post.check_fix_value_range(value='2010', data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, belong_op=Belong(0), field='Year',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('PRECONDITION rowFilterPrimitive(Year)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION rowFilterPrimitive(Year)_PRE_valueRange NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value='2010', data_dictionary=rowFilterPrimitive_Year__output_dataDictionary_df, belong_op=Belong(0), field='Year',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('POSTCONDITION rowFilterPrimitive(Year)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION rowFilterPrimitive(Year)_POST_valueRange NOT VALIDATED')
	
	
	
	columns_list_rowFilterPrimitive_Year__INV_condition=['Year']
	filter_fix_value_list_rowFilterPrimitive_Year__INV_condition=['2010']
	
	if contract_invariants.check_inv_filter_rows_primitive(data_dictionary_in=rowFilterPrimitive_Year__input_dataDictionary_df,
											data_dictionary_out=rowFilterPrimitive_Year__output_dataDictionary_df,
											columns=columns_list_rowFilterPrimitive_Year__INV_condition,
											filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_Year__INV_condition,
											filter_type=FilterType.EXCLUDE):
		print('INVARIANT rowFilterPrimitive(Year)_INV_condition VALIDATED')
	else:
		print('INVARIANT rowFilterPrimitive(Year)_INV_condition NOT VALIDATED')
	
	
	#-----------------New DataProcessing-----------------
	rowFilterRange_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet'):
		rowFilterRange_Life_expectancy__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df,
	                                	closure_type=Closure(2), belong_op=Belong(0), field='Life_expectancy', origin_function="Row Filter"):
		print('PRECONDITION rowFilterRange(Life_expectancy)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION rowFilterRange(Life_expectancy)_PRE_valueRange NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value='-216', data_dictionary=rowFilterRange_Life_expectancy__output_dataDictionary_df, belong_op=Belong(0), field='Life_expectancy',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('POSTCONDITION rowFilterRange(Life_expectancy)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION rowFilterRange(Life_expectancy)_POST_valueRange NOT VALIDATED')
	
	
	
	columns_list_rowFilterRange_Life_expectancy__INV_condition=['Life_expectancy']
	left_margin_list_rowFilterRange_Life_expectancy__INV_condition=[0.0]
	right_margin_list_rowFilterRange_Life_expectancy__INV_condition=[0.0]
	closure_type_list_rowFilterRange_Life_expectancy__INV_condition=[Closure.openOpen]
	
	if contract_invariants.check_inv_filter_rows_range(data_dictionary_in=rowFilterRange_Life_expectancy__input_dataDictionary_df,
											data_dictionary_out=rowFilterRange_Life_expectancy__output_dataDictionary_df,
											columns=columns_list_rowFilterRange_Life_expectancy__INV_condition,
											left_margin_list=left_margin_list_rowFilterRange_Life_expectancy__INV_condition, right_margin_list=right_margin_list_rowFilterRange_Life_expectancy__INV_condition,
											closure_type_list=closure_type_list_rowFilterRange_Life_expectancy__INV_condition, filter_type=FilterType.EXCLUDE):
		print('INVARIANT rowFilterRange(Life_expectancy)_INV_condition VALIDATED')
	else:
		print('INVARIANT rowFilterRange(Life_expectancy)_INV_condition NOT VALIDATED')
	
	



set_logger("contracts")
generateWorkflow()
