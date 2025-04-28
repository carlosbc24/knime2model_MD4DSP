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
	join_Name_with_City__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/join_input_dataDictionary.parquet')

	join_Name_with_City__input_dataDictionary_transformed=join_Name_with_City__input_dataDictionary_df.copy()
	dictionary_join_param_join={'Name': True' - ': False'City': True}
	
	join_Name_with_City__input_dataDictionary_transformed=data_transformations.transform_join(data_dictionary=join_Name_with_City__input_dataDictionary_transformed,
																	dictionary=dictionary_join_param_join, field_out='Name with City')
	
	
	join_Name_with_City__output_dataDictionary_df=join_Name_with_City__input_dataDictionary_transformed
	join_Name_with_City__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/join_output_dataDictionary.parquet')
	join_Name_with_City__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/join_output_dataDictionary.parquet')
	
	if contract_invariants.check_inv_missing_value_missing_value(data_dictionary_in=join_Name_with_City__input_dataDictionary_df,
											data_dictionary_out=join_Name_with_City__output_dataDictionary_df,
											belong_op_out=Belong(1), field_in='Name', field_out='Name with City'):
		print('INVARIANT INV_specialValue_condition VALIDATED')
	else:
		print('INVARIANT INV_specialValue_condition NOT VALIDATED')
	if contract_invariants.check_inv_missing_value_missing_value(data_dictionary_in=join_Name_with_City__input_dataDictionary_df,
											data_dictionary_out=join_Name_with_City__output_dataDictionary_df,
											belong_op_out=Belong(1), field_in='City', field_out='Name with City'):
		print('INVARIANT INV_specialValue_condition VALIDATED')
	else:
		print('INVARIANT INV_specialValue_condition NOT VALIDATED')
	
	
	
	--------------------------------------------------------------------------------------------------------------------------------------------------
	dictionary_join_specialValue_INV_THEN={'Name': True'': False'City': True}
	if contract_invariants.check_inv_cast_type(data_dictionary_in=join_Name_with_City__input_dataDictionary_df,
								data_dictionary_out=join_Name_with_City__output_dataDictionary_df,
								dictionary=dictionary_join_specialValue_INV_THEN,
								field_in='Name', field_out='Name with City'):
		print('INVARIANT INV_join_condition_Name with City VALIDATED')
	else:
		print('INVARIANT INV_join_condition_Name with City NOT VALIDATED')
	--------------------------------------------------------------------------------------------------------------------------------------------------
	dictionary_join_specialValue_INV_THEN={'Name': True'': False'City': True}
	if contract_invariants.check_inv_cast_type(data_dictionary_in=join_Name_with_City__input_dataDictionary_df,
								data_dictionary_out=join_Name_with_City__output_dataDictionary_df,
								dictionary=dictionary_join_specialValue_INV_THEN,
								field_in='City', field_out='Name with City'):
		print('INVARIANT INV_join_condition_Name with City VALIDATED')
	else:
		print('INVARIANT INV_join_condition_Name with City NOT VALIDATED')
	
	
	
	

set_logger("dataProcessing")
generateWorkflow()
