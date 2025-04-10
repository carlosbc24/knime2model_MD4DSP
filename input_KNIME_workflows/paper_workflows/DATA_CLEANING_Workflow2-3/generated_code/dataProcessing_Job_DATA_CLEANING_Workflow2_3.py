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
	rowFilterMissing_marital_status__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.parquet')
	rowFilterMissing_marital_status__input_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.parquet')

	missing_values_rowFilterMissing_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=rowFilterMissing_marital_status__input_dataDictionary_df, field='marital-status', 
									missing_values=missing_values_rowFilterMissing_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100):
		print('PRECONDITION rowFilterMissing(marital-status)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION rowFilterMissing(marital-status)_PRE_valueRange NOT VALIDATED')
	
	rowFilterMissing_marital_status__input_dataDictionary_transformed=rowFilterMissing_marital_status__input_dataDictionary_df.copy()
	columns_rowFilterMissing_param_filter=['marital-status']
	
	dicc_rowFilterMissing_param_filter={'marital-status':{'missing': []}}
	
	rowFilterMissing_marital_status__input_dataDictionary_transformed=data_transformations.transform_filter_rows_special_values(data_dictionary=rowFilterMissing_marital_status__input_dataDictionary_transformed,
																											cols_special_type_values=dicc_rowFilterMissing_param_filter,
																											filter_type=FilterType(0))
	rowFilterMissing_marital_status__output_dataDictionary_df=rowFilterMissing_marital_status__input_dataDictionary_transformed
	rowFilterMissing_marital_status__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')
	rowFilterMissing_marital_status__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')
	
	missing_values_rowFilterMissing_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=rowFilterMissing_marital_status__output_dataDictionary_df, field='marital-status', 
									missing_values=missing_values_rowFilterMissing_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None):
		print('POSTCONDITION rowFilterMissing(marital-status)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION rowFilterMissing(marital-status)_POST_valueRange NOT VALIDATED')
	
	#-----------------New DataProcessing-----------------
	rowFilterPrimitive_workclass__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')

	if contract_pre_post.check_fix_value_range(value='s*', data_dictionary=rowFilterPrimitive_workclass__input_dataDictionary_df, belong_op=Belong(0), field='workclass',
									quant_abs=None, quant_rel=None, quant_op=None):
		print('PRECONDITION rowFilterPrimitive(workclass)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION rowFilterPrimitive(workclass)_PRE_valueRange NOT VALIDATED')
	
	rowFilterPrimitive_workclass__input_dataDictionary_transformed=rowFilterPrimitive_workclass__input_dataDictionary_df.copy()
	columns_rowFilterPrimitive_param_filter=['workclass']
	
	filter_fix_value_list_rowFilterPrimitive_param_filter=['s*']
	
	rowFilterPrimitive_workclass__input_dataDictionary_transformed=data_transformations.transform_filter_rows_primitive(data_dictionary=rowFilterPrimitive_workclass__input_dataDictionary_transformed,
																											columns=columns_rowFilterPrimitive_param_filter,
																		                                    filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_param_filter,
																											filter_type=FilterType(1))
	rowFilterPrimitive_workclass__output_dataDictionary_df=rowFilterPrimitive_workclass__input_dataDictionary_transformed
	rowFilterPrimitive_workclass__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	rowFilterPrimitive_workclass__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	
	if contract_pre_post.check_fix_value_range(value='s*', data_dictionary=rowFilterPrimitive_workclass__output_dataDictionary_df, belong_op=Belong(0), field='workclass',
									quant_abs=None, quant_rel=None, quant_op=None):
		print('POSTCONDITION rowFilterPrimitive(workclass)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION rowFilterPrimitive(workclass)_POST_valueRange NOT VALIDATED')
	


set_logger("dataProcessing")
generateWorkflow()
