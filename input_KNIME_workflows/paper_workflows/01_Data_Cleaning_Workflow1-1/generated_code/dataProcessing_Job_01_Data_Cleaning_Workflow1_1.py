import pandas as pd
import numpy as np
import functions.contract_invariants as contract_invariants
import functions.contract_pre_post as contract_pre_post
import functions.data_transformations as data_transformations
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():

	#-----------------New DataProcessing-----------------
	rowFilterMissing_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.parquet')
	rowFilterMissing_Life_expectancy__input_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.parquet')

	missing_values_rowFilterMissing_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy', 
									missing_values=missing_values_rowFilterMissing_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100):
		print('PRECONDITION rowFilterMissing(Life_expectancy)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION rowFilterMissing(Life_expectancy)_PRE_valueRange NOT VALIDATED')
	
	rowFilterMissing_Life_expectancy__input_dataDictionary_transformed=rowFilterMissing_Life_expectancy__input_dataDictionary_df.copy()
	columns_rowFilterMissing_param_filter=['Life_expectancy']
	
	dicc_rowFilterMissing_param_filter={'Life_expectancy':{'missing': []}}
	
	rowFilterMissing_Life_expectancy__input_dataDictionary_transformed=data_transformations.transform_filter_rows_special_values(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_transformed,
																											cols_special_type_values=dicc_rowFilterMissing_param_filter,
																											filter_type=FilterType(0))
	rowFilterMissing_Life_expectancy__output_dataDictionary_df=rowFilterMissing_Life_expectancy__input_dataDictionary_transformed
	rowFilterMissing_Life_expectancy__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')
	rowFilterMissing_Life_expectancy__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')
	
	missing_values_rowFilterMissing_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=rowFilterMissing_Life_expectancy__output_dataDictionary_df, field='Life_expectancy', 
									missing_values=missing_values_rowFilterMissing_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None):
		print('POSTCONDITION rowFilterMissing(Life_expectancy)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION rowFilterMissing(Life_expectancy)_POST_valueRange NOT VALIDATED')
	
	#-----------------New DataProcessing-----------------
	rowFilterRange_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df,
	                                	closure_type=Closure(2), belong_op=Belong(0), field='Life_expectancy'):
		print('PRECONDITION rowFilter(Life_expectancy)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION rowFilter(Life_expectancy)_PRE_valueRange NOT VALIDATED')
	
	rowFilterRange_Life_expectancy__input_dataDictionary_transformed=rowFilterRange_Life_expectancy__input_dataDictionary_df.copy()
	columns_rowFilterRange_param_filter=['Life_expectancy']
	
	filter_range_left_values_list_rowFilterRange_param_filter=[0.0]
	filter_range_right_values_list_rowFilterRange_param_filter=[0.0]
	closure_type_list_rowFilterRange_param_filter=[Closure(3)]
	
	rowFilterRange_Life_expectancy__input_dataDictionary_transformed=data_transformations.transform_filter_rows_range(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_transformed,
																											columns=columns_rowFilterRange_param_filter,
																											left_margin_list=filter_range_left_values_list_rowFilterRange_param_filter,
																											right_margin_list=filter_range_right_values_list_rowFilterRange_param_filter,
																											filter_type=FilterType(1),
																											closure_type_list=closure_type_list_rowFilterRange_param_filter)
	rowFilterRange_Life_expectancy__output_dataDictionary_df=rowFilterRange_Life_expectancy__input_dataDictionary_transformed
	rowFilterRange_Life_expectancy__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')
	rowFilterRange_Life_expectancy__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')
	
	if contract_pre_post.check_fix_value_range(value='-216', data_dictionary=rowFilterRange_Life_expectancy__output_dataDictionary_df, belong_op=Belong(0), field='Life_expectancy',
									quant_abs=None, quant_rel=None, quant_op=None):
		print('POSTCONDITION rowFilter(Life_expectancy)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION rowFilter(Life_expectancy)_POST_valueRange NOT VALIDATED')
	


set_logger("dataProcessing")
generateWorkflow()
