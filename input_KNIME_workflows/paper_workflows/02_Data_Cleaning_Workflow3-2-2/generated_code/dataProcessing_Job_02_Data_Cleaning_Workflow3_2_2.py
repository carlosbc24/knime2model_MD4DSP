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
	dictionary_join_param_join={'Name': True, ' - ': False, 'City': True}
	
	join_Name_with_City__input_dataDictionary_transformed=data_transformations.transform_join(data_dictionary=join_Name_with_City__input_dataDictionary_transformed,
																	dictionary=dictionary_join_param_join, field_out='Name with City')
	
	
	join_Name_with_City__output_dataDictionary_df=join_Name_with_City__input_dataDictionary_transformed
	join_Name_with_City__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/join_output_dataDictionary.parquet')
	join_Name_with_City__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/join_output_dataDictionary.parquet')
	
	if contract_invariants.check_inv_missing_value_missing_value(data_dictionary_in=join_Name_with_City__input_dataDictionary_df,
											data_dictionary_out=join_Name_with_City__output_dataDictionary_df,
											belong_op_in=Belong(1), belong_op_out=Belong(1),
											field_in='Name', field_out='invalid', origin_function="String Manipulation"):
		print('INVARIANT String Manipulation(Name) BelongOpIn:NotBelong BelongOpOut:NotBelong VALIDATED')
	else:
		print('INVARIANT String Manipulation(Name) BelongOpIn:NotBelong BelongOpOut:NotBelong NOT VALIDATED')
	if contract_invariants.check_inv_missing_value_missing_value(data_dictionary_in=join_Name_with_City__input_dataDictionary_df,
											data_dictionary_out=join_Name_with_City__output_dataDictionary_df,
											belong_op_in=Belong(1), belong_op_out=Belong(1),
											field_in='City', field_out='invalid', origin_function="String Manipulation"):
		print('INVARIANT String Manipulation(City) BelongOpIn:NotBelong BelongOpOut:NotBelong VALIDATED')
	else:
		print('INVARIANT String Manipulation(City) BelongOpIn:NotBelong BelongOpOut:NotBelong NOT VALIDATED')
	
	
	
	
	dictionary_join_specialValue_INV_THEN={'Name': True, '': False, 'City': True}
	if contract_invariants.check_inv_join(data_dictionary_in=join_Name_with_City__input_dataDictionary_df,
								data_dictionary_out=join_Name_with_City__output_dataDictionary_df,
								dictionary=dictionary_join_specialValue_INV_THEN,
								field_in='Name', field_out='Name with City', origin_function="String Manipulation"):
		print('INVARIANT String Manipulation(Name) JoinInputs:Name VALIDATED')
	else:
		print('INVARIANT String Manipulation(Name) JoinInputs:Name NOT VALIDATED')
	
	dictionary_join_specialValue_INV_THEN={'Name': True, '': False, 'City': True}
	if contract_invariants.check_inv_join(data_dictionary_in=join_Name_with_City__input_dataDictionary_df,
								data_dictionary_out=join_Name_with_City__output_dataDictionary_df,
								dictionary=dictionary_join_specialValue_INV_THEN,
								field_in='City', field_out='Name with City', origin_function="String Manipulation"):
		print('INVARIANT String Manipulation(City) JoinInputs:Name VALIDATED')
	else:
		print('INVARIANT String Manipulation(City) JoinInputs:Name NOT VALIDATED')
	
	
	

set_logger("dataProcessing")
generateWorkflow()
