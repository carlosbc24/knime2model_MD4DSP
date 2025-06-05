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

	missing_values_rowFilterMissing_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=rowFilterMissing_marital_status__input_dataDictionary_df, field='marital-status', 
									missing_values=missing_values_rowFilterMissing_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Row Filter (deprecated)"):
		print('PRECONDITION Row Filter (deprecated)(marital-status) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Row Filter (deprecated)(marital-status) MissingValues:[] NOT VALIDATED')
	
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

	if contract_pre_post.check_fix_value_range(value='Never-married', is_substring=False, data_dictionary=rowFilterPrimitive_marital_status__input_dataDictionary_df, belong_op=Belong(0), field='marital-status',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter (deprecated)"):
		print('PRECONDITION Row Filter (deprecated)(marital-status) FixValue:Never-married VALIDATED')
	else:
		print('PRECONDITION Row Filter (deprecated)(marital-status) FixValue:Never-married NOT VALIDATED')
	
	rowFilterPrimitive_marital_status__input_dataDictionary_transformed=rowFilterPrimitive_marital_status__input_dataDictionary_df.copy()
	columns_rowFilterPrimitive_param_filter=['marital-status']
	
	filter_fix_value_list_rowFilterPrimitive_param_filter=['Never-married']
	
	rowFilterPrimitive_marital_status__input_dataDictionary_transformed=data_transformations.transform_filter_rows_primitive(data_dictionary=rowFilterPrimitive_marital_status__input_dataDictionary_transformed,
																											columns=columns_rowFilterPrimitive_param_filter,
																		                                    filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_param_filter,
																											filter_type=FilterType(1))
	rowFilterPrimitive_marital_status__output_dataDictionary_df=rowFilterPrimitive_marital_status__input_dataDictionary_transformed
	rowFilterPrimitive_marital_status__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	rowFilterPrimitive_marital_status__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	
	if contract_pre_post.check_fix_value_range(value='Never-married', is_substring=False, data_dictionary=rowFilterPrimitive_marital_status__output_dataDictionary_df, belong_op=Belong(0), field='marital-status',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter (deprecated)"):
		print('POSTCONDITION Row Filter (deprecated)(marital-status) FixValue:Never-married VALIDATED')
	else:
		print('POSTCONDITION Row Filter (deprecated)(marital-status) FixValue:Never-married NOT VALIDATED')
	
	
	
	columns_list_rowFilterPrimitive_marital_status__INV_condition=['marital-status']
	filter_fix_value_list_rowFilterPrimitive_marital_status__INV_condition=['Never-married']
	
	if contract_invariants.check_inv_filter_rows_primitive(data_dictionary_in=rowFilterPrimitive_marital_status__input_dataDictionary_df,
											data_dictionary_out=rowFilterPrimitive_marital_status__output_dataDictionary_df,
											columns=columns_list_rowFilterPrimitive_marital_status__INV_condition,
											filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_marital_status__INV_condition,
											filter_type=FilterType.INCLUDE, origin_function="Row Filter (deprecated)"):
		print('INVARIANT Row Filter (deprecated)(marital-status) FilterType:INCLUDE FixValueList:[Never-married] VALIDATED')
	else:
		print('INVARIANT Row Filter (deprecated)(marital-status) FilterType:INCLUDE FixValueList:[Never-married] NOT VALIDATED')
	
	
	#-----------------New DataProcessing-----------------
	rowFilterRange_age__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=20.0, right_margin=40.0, data_dictionary=rowFilterRange_age__input_dataDictionary_df,
	                                	closure_type=Closure(2), belong_op=Belong(0), field='age', origin_function="Row Filter (deprecated)"):
		print('PRECONDITION Row Filter (deprecated)(age) Interval:[20.0, 40.0) VALIDATED')
	else:
		print('PRECONDITION Row Filter (deprecated)(age) Interval:[20.0, 40.0) NOT VALIDATED')
	
	rowFilterRange_age__input_dataDictionary_transformed=rowFilterRange_age__input_dataDictionary_df.copy()
	columns_rowFilterRange_param_filter=['age']
	
	filter_range_left_values_list_rowFilterRange_param_filter=[20.0]
	filter_range_right_values_list_rowFilterRange_param_filter=[40.0]
	closure_type_list_rowFilterRange_param_filter=[Closure(3)]
	
	rowFilterRange_age__input_dataDictionary_transformed=data_transformations.transform_filter_rows_range(data_dictionary=rowFilterRange_age__input_dataDictionary_transformed,
																											columns=columns_rowFilterRange_param_filter,
																											left_margin_list=filter_range_left_values_list_rowFilterRange_param_filter,
																											right_margin_list=filter_range_right_values_list_rowFilterRange_param_filter,
																											filter_type=FilterType(1),
																											closure_type_list=closure_type_list_rowFilterRange_param_filter)
	rowFilterRange_age__output_dataDictionary_df=rowFilterRange_age__input_dataDictionary_transformed
	rowFilterRange_age__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')
	rowFilterRange_age__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')
	
	if contract_pre_post.check_interval_range_float(left_margin=20.0, right_margin=40.0, data_dictionary=rowFilterRange_age__output_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='age', origin_function="Row Filter (deprecated)"):
		print('POSTCONDITION Row Filter (deprecated)(age) Interval:[20.0, 40.0] VALIDATED')
	else:
		print('POSTCONDITION Row Filter (deprecated)(age) Interval:[20.0, 40.0] NOT VALIDATED')
	
	
	
	columns_list_rowFilterRange_age__INV_condition=['age']
	left_margin_list_rowFilterRange_age__INV_condition=[20.0]
	right_margin_list_rowFilterRange_age__INV_condition=[40.0]
	closure_type_list_rowFilterRange_age__INV_condition=[Closure.closedClosed]
	
	if contract_invariants.check_inv_filter_rows_range(data_dictionary_in=rowFilterRange_age__input_dataDictionary_df,
											data_dictionary_out=rowFilterRange_age__output_dataDictionary_df,
											columns=columns_list_rowFilterRange_age__INV_condition,
											left_margin_list=left_margin_list_rowFilterRange_age__INV_condition, right_margin_list=right_margin_list_rowFilterRange_age__INV_condition,
											closure_type_list=closure_type_list_rowFilterRange_age__INV_condition,
											filter_type=FilterType.INCLUDE, origin_function="Row Filter (deprecated)"):
		print('INVARIANT Row Filter (deprecated)(age) FilterType:INCLUDE LeftMarginList:[20.0] RightMarginList:[40.0] ClosureTypeList:[Closure.closedClosed] VALIDATED')
	else:
		print('INVARIANT Row Filter (deprecated)(age) FilterType:INCLUDE LeftMarginList:[20.0] RightMarginList:[40.0] ClosureTypeList:[Closure.closedClosed] NOT VALIDATED')
	
	



set_logger("dataProcessing")
generateWorkflow()
