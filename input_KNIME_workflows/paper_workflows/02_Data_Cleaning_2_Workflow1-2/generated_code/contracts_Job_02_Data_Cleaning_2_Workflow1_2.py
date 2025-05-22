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
	rowFilterRange_Latitude__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_input_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet'):
		rowFilterRange_Latitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')

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
