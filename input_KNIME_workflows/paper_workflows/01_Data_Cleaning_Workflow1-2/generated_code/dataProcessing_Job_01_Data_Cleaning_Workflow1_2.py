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

	missing_values_rowFilterMissing_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy', 
									missing_values=missing_values_rowFilterMissing_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Row Filter"):
		print('PRECONDITION Row Filter(Life_expectancy) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Row Filter(Life_expectancy) MissingValues:[] NOT VALIDATED')
	
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
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=rowFilterMissing_Life_expectancy__output_dataDictionary_df, field='Life_expectancy', 
									missing_values=missing_values_rowFilterMissing_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('POSTCONDITION Row Filter(Life_expectancy) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Row Filter(Life_expectancy) MissingValues:[] NOT VALIDATED')
	
	
	
	cols_special_type_values_rowFilterMissing_Life_expectancy__INV_condition={'Life_expectancy':{'missing': []}}
	
	if contract_invariants.check_inv_filter_rows_special_values(data_dictionary_in=rowFilterMissing_Life_expectancy__input_dataDictionary_df,
											data_dictionary_out=rowFilterMissing_Life_expectancy__output_dataDictionary_df,
											cols_special_type_values=cols_special_type_values_rowFilterMissing_Life_expectancy__INV_condition,
											filter_type=FilterType.EXCLUDE, origin_function="Row Filter"):
		print('INVARIANT Row Filter(Life_expectancy) FilterType:EXCLUDE SpecialValues: Life_expectancyMISSING:[] VALIDATED')
	else:
		print('INVARIANT Row Filter(Life_expectancy) FilterType:EXCLUDE SpecialValues: Life_expectancyMISSING:[] NOT VALIDATED')
	
	
	#-----------------New DataProcessing-----------------
	rowFilterRange_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=-1.0E9, right_margin=50.0, data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df,
	                                	closure_type=Closure(2), belong_op=Belong(0), field='Life_expectancy', origin_function="Row Filter"):
		print('PRECONDITION Row Filter(Life_expectancy) Interval:[-1.0E9, 50.0) VALIDATED')
	else:
		print('PRECONDITION Row Filter(Life_expectancy) Interval:[-1.0E9, 50.0) NOT VALIDATED')
	
	rowFilterRange_Life_expectancy__input_dataDictionary_transformed=rowFilterRange_Life_expectancy__input_dataDictionary_df.copy()
	columns_rowFilterRange_param_filter=['Life_expectancy']
	
	filter_range_left_values_list_rowFilterRange_param_filter=[-np.inf]
	filter_range_right_values_list_rowFilterRange_param_filter=[50.0]
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
	
	if contract_pre_post.check_interval_range_float(left_margin=-1.0E9, right_margin=50.0, data_dictionary=rowFilterRange_Life_expectancy__output_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='Life_expectancy', origin_function="Row Filter"):
		print('POSTCONDITION Row Filter(Life_expectancy) Interval:[-1.0E9, 50.0] VALIDATED')
	else:
		print('POSTCONDITION Row Filter(Life_expectancy) Interval:[-1.0E9, 50.0] NOT VALIDATED')
	
	
	
	columns_list_rowFilterRange_Life_expectancy__INV_condition=['Life_expectancy']
	left_margin_list_rowFilterRange_Life_expectancy__INV_condition=[-1.0E9]
	right_margin_list_rowFilterRange_Life_expectancy__INV_condition=[50.0]
	closure_type_list_rowFilterRange_Life_expectancy__INV_condition=[Closure.closedClosed]
	
	if contract_invariants.check_inv_filter_rows_range(data_dictionary_in=rowFilterRange_Life_expectancy__input_dataDictionary_df,
											data_dictionary_out=rowFilterRange_Life_expectancy__output_dataDictionary_df,
											columns=columns_list_rowFilterRange_Life_expectancy__INV_condition,
											left_margin_list=left_margin_list_rowFilterRange_Life_expectancy__INV_condition, right_margin_list=right_margin_list_rowFilterRange_Life_expectancy__INV_condition,
											closure_type_list=closure_type_list_rowFilterRange_Life_expectancy__INV_condition,
											filter_type=FilterType.INCLUDE, origin_function="Row Filter"):
		print('INVARIANT Row Filter(Life_expectancy) FilterType:INCLUDE LeftMarginList:[-1.0E9] RightMarginList:[50.0] ClosureTypeList:[Closure.closedClosed] VALIDATED')
	else:
		print('INVARIANT Row Filter(Life_expectancy) FilterType:INCLUDE LeftMarginList:[-1.0E9] RightMarginList:[50.0] ClosureTypeList:[Closure.closedClosed] NOT VALIDATED')
	
	
	#-----------------New DataProcessing-----------------
	rowFilterPrimitive_Year__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')

	if contract_pre_post.check_fix_value_range(value='2014', data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, belong_op=Belong(0), field='Year',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('PRECONDITION Row Filter(Year) FixValue:2014 VALIDATED')
	else:
		print('PRECONDITION Row Filter(Year) FixValue:2014 NOT VALIDATED')
	
	rowFilterPrimitive_Year__input_dataDictionary_transformed=rowFilterPrimitive_Year__input_dataDictionary_df.copy()
	columns_rowFilterPrimitive_param_filter=['Year']
	
	filter_fix_value_list_rowFilterPrimitive_param_filter=['2014']
	
	rowFilterPrimitive_Year__input_dataDictionary_transformed=data_transformations.transform_filter_rows_primitive(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_transformed,
																											columns=columns_rowFilterPrimitive_param_filter,
																		                                    filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_param_filter,
																											filter_type=FilterType(1))
	rowFilterPrimitive_Year__output_dataDictionary_df=rowFilterPrimitive_Year__input_dataDictionary_transformed
	rowFilterPrimitive_Year__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	rowFilterPrimitive_Year__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	
	if contract_pre_post.check_fix_value_range(value='2014', data_dictionary=rowFilterPrimitive_Year__output_dataDictionary_df, belong_op=Belong(0), field='Year',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('POSTCONDITION Row Filter(Year) FixValue:2014 VALIDATED')
	else:
		print('POSTCONDITION Row Filter(Year) FixValue:2014 NOT VALIDATED')
	
	
	
	columns_list_rowFilterPrimitive_Year__INV_condition=['Year']
	filter_fix_value_list_rowFilterPrimitive_Year__INV_condition=['2014']
	
	if contract_invariants.check_inv_filter_rows_primitive(data_dictionary_in=rowFilterPrimitive_Year__input_dataDictionary_df,
											data_dictionary_out=rowFilterPrimitive_Year__output_dataDictionary_df,
											columns=columns_list_rowFilterPrimitive_Year__INV_condition,
											filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_Year__INV_condition,
											filter_type=FilterType.INCLUDE, origin_function="Row Filter"):
		print('INVARIANT Row Filter(Year) FilterType:INCLUDE FixValueList:[2014] VALIDATED')
	else:
		print('INVARIANT Row Filter(Year) FilterType:INCLUDE FixValueList:[2014] NOT VALIDATED')
	
	



set_logger("dataProcessing")
generateWorkflow()
