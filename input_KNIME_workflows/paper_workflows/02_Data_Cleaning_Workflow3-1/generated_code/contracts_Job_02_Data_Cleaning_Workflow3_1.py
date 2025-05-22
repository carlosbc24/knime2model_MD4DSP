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
	mapping_Country__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mapping_input_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/mapping_output_dataDictionary.parquet'):
		mapping_Country__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mapping_output_dataDictionary.parquet')

	if contract_pre_post.check_fix_value_range(value='United States', data_dictionary=mapping_Country__input_dataDictionary_df, belong_op=Belong(0), field='Country',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="String Manipulation"):
		print('PRECONDITION String Manipulation(Country) FixValue:United States VALIDATED')
	else:
		print('PRECONDITION String Manipulation(Country) FixValue:United States NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value='United States', data_dictionary=mapping_Country__output_dataDictionary_df, belong_op=Belong(1), field='Country',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="String Manipulation"):
		print('POSTCONDITION String Manipulation(Country) FixValue:United States VALIDATED')
	else:
		print('POSTCONDITION String Manipulation(Country) FixValue:United States NOT VALIDATED')
	
	
	input_values_list_mapping_INV_condition=['United States']
	output_values_list_mapping_INV_condition=['USA']
	
	data_type_input_list_mapping_INV_condition=[DataType(0)]
	data_type_output_list_mapping_INV_condition=[DataType(0)]
	
	is_substring_list_mapping_INV_condition=[False]
	
	if contract_invariants.check_inv_fix_value_fix_value(data_dictionary_in=mapping_Country__input_dataDictionary_df,
											data_dictionary_out=mapping_Country__output_dataDictionary_df,
											input_values_list=input_values_list_mapping_INV_condition, 
											output_values_list=output_values_list_mapping_INV_condition,
											is_substring_list=is_substring_list_mapping_INV_condition,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											data_type_input_list=data_type_input_list_mapping_INV_condition,
											data_type_output_list=data_type_output_list_mapping_INV_condition,
											field_in='Country', field_out='Country', origin_function="String Manipulation"):
		print('INVARIANT String Manipulation(Country) InputMapValues:United States OutputMapValues:USA VALIDATED')
	else:
		print('INVARIANT String Manipulation(Country) InputMapValues:United States OutputMapValues:USA NOT VALIDATED')
	
	
	
set_logger("contracts")
generateWorkflow()
