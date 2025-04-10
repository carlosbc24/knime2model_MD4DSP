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
	mapping_Tz_database_time_zone__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mapping_input_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/mapping_output_dataDictionary.parquet'):
		mapping_Tz_database_time_zone__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mapping_output_dataDictionary.parquet')

	if contract_pre_post.check_fix_value_range(value='/', data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, belong_op=Belong(0), field='Tz database time zone',
									quant_abs=None, quant_rel=None, quant_op=None):
		print('PRECONDITION mapping(Tz database time zone)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION mapping(Tz database time zone)_PRE_valueRange NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value='/', data_dictionary=mapping_Tz_database_time_zone__output_dataDictionary_df, belong_op=Belong(0), field='Tz database time zone',
									quant_abs=None, quant_rel=None, quant_op=None):
		print('POSTCONDITION mapping(Tz database time zone)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION mapping(Tz database time zone)_POST_valueRange NOT VALIDATED')
	
	
	input_values_list_def_INV_condition=['/']
	output_values_list_def_INV_condition=['-']
	
	data_type_input_list_def_INV_condition=[DataType(0)]
	data_type_output_list_def_INV_condition=[DataType(0)]
	
	is_substring_list_def_INV_condition=[False]
	
	if contract_invariants.check_inv_fix_value_fix_value(data_dictionary_in=mapping_Tz_database_time_zone__input_dataDictionary_df,
											data_dictionary_out=mapping_Tz_database_time_zone__output_dataDictionary_df,
											input_values_list=input_values_list_def_INV_condition, 
											output_values_list=output_values_list_def_INV_condition,
											is_substring_list=is_substring_list_def_INV_condition,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											data_type_input_list=data_type_input_list_def_INV_condition,
											data_type_output_list=data_type_output_list_def_INV_condition,
											field_in='Tz database time zone', field_out='Tz database time zone'):
		print('INVARIANT mapping(Tz database time zone)_INV_condition VALIDATED')
	else:
		print('INVARIANT mapping(Tz database time zone)_INV_condition NOT VALIDATED')
	
	
	
set_logger("contracts")
generateWorkflow()
