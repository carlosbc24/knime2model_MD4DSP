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
	mapping_Tz_database_time_zone__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mapping_input_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/mapping_output_dataDictionary.parquet'):
		mapping_Tz_database_time_zone__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mapping_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Tz database time zone')
	
	data_smells.check_integer_as_floating_point(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, field='Tz database time zone')
	
	data_smells.check_types_as_string(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, field='Tz database time zone', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, field='Tz database time zone')
	
	data_smells.check_suspect_precision(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, field='Tz database time zone')
	
	
	data_smells.check_date_as_datetime(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, field='Tz database time zone')
	
	data_smells.check_separating_consistency(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, decimal_sep='.',  field='Tz database time zone')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, field='Tz database time zone')
	
	
	if contract_pre_post.check_fix_value_range(value='/', is_substring=True, data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, belong_op=Belong(0), field='Tz database time zone',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="String Manipulation"):
		print('PRECONDITION String Manipulation(Tz database time zone) FixValue:/ VALIDATED')
	else:
		print('PRECONDITION String Manipulation(Tz database time zone) FixValue:/ NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value='/', is_substring=True, data_dictionary=mapping_Tz_database_time_zone__output_dataDictionary_df, belong_op=Belong(1), field='Tz database time zone',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="String Manipulation"):
		print('POSTCONDITION String Manipulation(Tz database time zone) FixValue:/ VALIDATED')
	else:
		print('POSTCONDITION String Manipulation(Tz database time zone) FixValue:/ NOT VALIDATED')
	
	
	input_values_list_mapping_INV_condition=['/']
	output_values_list_mapping_INV_condition=['-']
	
	data_type_input_list_mapping_INV_condition=[DataType(0)]
	data_type_output_list_mapping_INV_condition=[DataType(0)]
	
	is_substring_list_mapping_INV_condition=[False]
	
	if contract_invariants.check_inv_fix_value_fix_value(data_dictionary_in=mapping_Tz_database_time_zone__input_dataDictionary_df,
											data_dictionary_out=mapping_Tz_database_time_zone__output_dataDictionary_df,
											input_values_list=input_values_list_mapping_INV_condition, 
											output_values_list=output_values_list_mapping_INV_condition,
											is_substring_list=is_substring_list_mapping_INV_condition,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											data_type_input_list=data_type_input_list_mapping_INV_condition,
											data_type_output_list=data_type_output_list_mapping_INV_condition,
											field_in='Tz database time zone', field_out='Tz database time zone', origin_function="String Manipulation"):
		print('INVARIANT String Manipulation(Tz database time zone) InputMapValues:/ OutputMapValues:- VALIDATED')
	else:
		print('INVARIANT String Manipulation(Tz database time zone) InputMapValues:/ OutputMapValues:- NOT VALIDATED')
	
	
	
set_logger("contracts")
generateWorkflow()
