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
	rowFilterMissing_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.parquet')
	if os.path.exists('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet'):
		rowFilterMissing_Life_expectancy__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Life_expectancy')
	
	data_smells.check_integer_as_floating_point(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_types_as_string(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_suspect_precision(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_suspect_distribution(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, min_value=0.0, max_value=1.0, field='Life_expectancy')
	
	data_smells.check_date_as_datetime(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_separating_consistency(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, decimal_sep='.',  field='Life_expectancy')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	
	missing_values_rowFilterMissing_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy', 
									missing_values=missing_values_rowFilterMissing_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Row Filter"):
		print('PRECONDITION Row Filter(Life_expectancy) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Row Filter(Life_expectancy) MissingValues:[] NOT VALIDATED')
	
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
	rowFilterPrimitive_Year__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet'):
		rowFilterPrimitive_Year__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=['2010']
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, 
														missing_invalid_list=list_invalid, common_missing_invalid_list=common_invalid_list, field='Year')
	
	data_smells.check_integer_as_floating_point(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, field='Year')
	
	data_smells.check_types_as_string(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, field='Year', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, field='Year')
	
	data_smells.check_suspect_precision(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, field='Year')
	
	data_smells.check_suspect_distribution(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, min_value=9.0, max_value=202.0, field='Year')
	
	data_smells.check_date_as_datetime(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, field='Year')
	
	data_smells.check_separating_consistency(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, decimal_sep='.',  field='Year')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, field='Year')
	
	
	if contract_pre_post.check_fix_value_range(value=2010, is_substring=False, data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, belong_op=Belong(0), field='Year',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('PRECONDITION Row Filter(Year) FixValue:2010 VALIDATED')
	else:
		print('PRECONDITION Row Filter(Year) FixValue:2010 NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value=2010, is_substring=False, data_dictionary=rowFilterPrimitive_Year__output_dataDictionary_df, belong_op=Belong(0), field='Year',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('POSTCONDITION Row Filter(Year) FixValue:2010 VALIDATED')
	else:
		print('POSTCONDITION Row Filter(Year) FixValue:2010 NOT VALIDATED')
	
	
	
	columns_list_rowFilterPrimitive_Year__INV_condition=['Year']
	filter_fix_value_list_rowFilterPrimitive_Year__INV_condition=[2010]
	
	if contract_invariants.check_inv_filter_rows_primitive(data_dictionary_in=rowFilterPrimitive_Year__input_dataDictionary_df,
											data_dictionary_out=rowFilterPrimitive_Year__output_dataDictionary_df,
											columns=columns_list_rowFilterPrimitive_Year__INV_condition,
											filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_Year__INV_condition,
											filter_type=FilterType.INCLUDE, origin_function="Row Filter"):
		print('INVARIANT Row Filter(Year) FilterType:INCLUDE FixValueList:[2010] VALIDATED')
	else:
		print('INVARIANT Row Filter(Year) FilterType:INCLUDE FixValueList:[2010] NOT VALIDATED')
	
	
	#-----------------New DataProcessing-----------------
	rowFilterRange_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet'):
		rowFilterRange_Life_expectancy__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Life_expectancy')
	
	data_smells.check_integer_as_floating_point(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_types_as_string(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, field='Life_expectancy', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_suspect_precision(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_suspect_distribution(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, min_value=-1.0E9, max_value=50.0, field='Life_expectancy')
	
	data_smells.check_date_as_datetime(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_separating_consistency(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, decimal_sep='.',  field='Life_expectancy')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	
	if contract_pre_post.check_interval_range_float(left_margin=-1.0E9, right_margin=50.0, data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df,
	                                	closure_type=Closure(2), belong_op=Belong(0), field='Life_expectancy', origin_function="Row Filter"):
		print('PRECONDITION Row Filter(Life_expectancy) Interval:[-1.0E9, 50.0) VALIDATED')
	else:
		print('PRECONDITION Row Filter(Life_expectancy) Interval:[-1.0E9, 50.0) NOT VALIDATED')
	
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
	
	



set_logger("contracts")
generateWorkflow()
