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
	rowFilterPrimitive_Airline__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_input_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet'):
		rowFilterPrimitive_Airline__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')

	if contract_pre_post.check_fix_value_range(value='3U', data_dictionary=rowFilterPrimitive_Airline__input_dataDictionary_df, belong_op=Belong(0), field='Airline',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('PRECONDITION Row Filter(Airline) FixValue:3U VALIDATED')
	else:
		print('PRECONDITION Row Filter(Airline) FixValue:3U NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value='3U', data_dictionary=rowFilterPrimitive_Airline__output_dataDictionary_df, belong_op=Belong(0), field='Airline',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('POSTCONDITION Row Filter(Airline) FixValue:3U VALIDATED')
	else:
		print('POSTCONDITION Row Filter(Airline) FixValue:3U NOT VALIDATED')
	
	
	
	columns_list_rowFilterPrimitive_Airline__INV_condition=['Airline']
	filter_fix_value_list_rowFilterPrimitive_Airline__INV_condition=['3U']
	
	if contract_invariants.check_inv_filter_rows_primitive(data_dictionary_in=rowFilterPrimitive_Airline__input_dataDictionary_df,
											data_dictionary_out=rowFilterPrimitive_Airline__output_dataDictionary_df,
											columns=columns_list_rowFilterPrimitive_Airline__INV_condition,
											filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_Airline__INV_condition,
											filter_type=FilterType.INCLUDE, origin_function="Row Filter"):
		print('INVARIANT Row Filter(Airline) FilterType:INCLUDE FixValueList:[3U] VALIDATED')
	else:
		print('INVARIANT Row Filter(Airline) FilterType:INCLUDE FixValueList:[3U] NOT VALIDATED')
	
	
set_logger("contracts")
generateWorkflow()
