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
	rowFilterRange_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet'):
		rowFilterRange_Life_expectancy__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=75.0, right_margin=1.0E9, data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df,
	                                	closure_type=Closure(2), belong_op=Belong(0), field='Life_expectancy', origin_function="Row Filter"):
		print('PRECONDITION Row Filter(Life_expectancy) Interval:[75.0, 1.0E9) VALIDATED')
	else:
		print('PRECONDITION Row Filter(Life_expectancy) Interval:[75.0, 1.0E9) NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=75.0, right_margin=1.0E9, data_dictionary=rowFilterRange_Life_expectancy__output_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='Life_expectancy', origin_function="Row Filter"):
		print('POSTCONDITION Row Filter(Life_expectancy) Interval:[75.0, 1.0E9] VALIDATED')
	else:
		print('POSTCONDITION Row Filter(Life_expectancy) Interval:[75.0, 1.0E9] NOT VALIDATED')
	
	
	
	columns_list_rowFilterRange_Life_expectancy__INV_condition=['Life_expectancy']
	left_margin_list_rowFilterRange_Life_expectancy__INV_condition=[75.0]
	right_margin_list_rowFilterRange_Life_expectancy__INV_condition=[1.0E9]
	closure_type_list_rowFilterRange_Life_expectancy__INV_condition=[Closure.closedClosed]
	
	if contract_invariants.check_inv_filter_rows_range(data_dictionary_in=rowFilterRange_Life_expectancy__input_dataDictionary_df,
											data_dictionary_out=rowFilterRange_Life_expectancy__output_dataDictionary_df,
											columns=columns_list_rowFilterRange_Life_expectancy__INV_condition,
											left_margin_list=left_margin_list_rowFilterRange_Life_expectancy__INV_condition, right_margin_list=right_margin_list_rowFilterRange_Life_expectancy__INV_condition,
											closure_type_list=closure_type_list_rowFilterRange_Life_expectancy__INV_condition,
											filter_type=FilterType.INCLUDE, origin_function="Row Filter"):
		print('INVARIANT Row Filter(Life_expectancy) FilterType:INCLUDE LeftMarginList:[75.0] RightMarginList:[1.0E9] ClosureTypeList:[Closure.closedClosed] VALIDATED')
	else:
		print('INVARIANT Row Filter(Life_expectancy) FilterType:INCLUDE LeftMarginList:[75.0] RightMarginList:[1.0E9] ClosureTypeList:[Closure.closedClosed] NOT VALIDATED')
	
	


set_logger("contracts")
generateWorkflow()
