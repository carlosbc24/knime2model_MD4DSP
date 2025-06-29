import pandas as pd
import numpy as np
import functions.contract_invariants as contract_invariants
import functions.contract_pre_post as contract_pre_post
import functions.data_transformations as data_transformations
import functions.data_smells as data_smells
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	join_Name_with_City__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/join_input_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=join_Name_with_City__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Name')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=join_Name_with_City__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='City')
	
	data_smells.check_integer_as_floating_point(data_dictionary=join_Name_with_City__input_dataDictionary_df, field='Name')
	data_smells.check_integer_as_floating_point(data_dictionary=join_Name_with_City__input_dataDictionary_df, field='City')
	
	data_smells.check_types_as_string(data_dictionary=join_Name_with_City__input_dataDictionary_df, field='Name', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=join_Name_with_City__input_dataDictionary_df, field='City', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=join_Name_with_City__input_dataDictionary_df, field='Name')
	data_smells.check_special_character_spacing(data_dictionary=join_Name_with_City__input_dataDictionary_df, field='City')
	
	data_smells.check_suspect_precision(data_dictionary=join_Name_with_City__input_dataDictionary_df, field='Name')
	data_smells.check_suspect_precision(data_dictionary=join_Name_with_City__input_dataDictionary_df, field='City')
	
	
	data_smells.check_date_as_datetime(data_dictionary=join_Name_with_City__input_dataDictionary_df, field='Name')
	data_smells.check_date_as_datetime(data_dictionary=join_Name_with_City__input_dataDictionary_df, field='City')
	
	data_smells.check_separating_consistency(data_dictionary=join_Name_with_City__input_dataDictionary_df, decimal_sep='.',  field='Name')
	data_smells.check_separating_consistency(data_dictionary=join_Name_with_City__input_dataDictionary_df, decimal_sep='.',  field='City')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=join_Name_with_City__input_dataDictionary_df, field='Name')
	data_smells.check_ambiguous_datetime_format(data_dictionary=join_Name_with_City__input_dataDictionary_df, field='City')
	
	
	join_Name_with_City__input_dataDictionary_transformed=join_Name_with_City__input_dataDictionary_df.copy()
	join_Name_with_City__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=join_Name_with_City__input_dataDictionary_transformed,
																  data_type_output = DataType(0),
																  field_in = 'Name', field_out = 'Name with City')
	
	join_Name_with_City__output_dataDictionary_df=join_Name_with_City__input_dataDictionary_transformed
	join_Name_with_City__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/join_output_dataDictionary.parquet')
	join_Name_with_City__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/join_output_dataDictionary.parquet')
	dictionary_join_param_join={'Name': True, ' - ': False, 'City': True}
	
	join_Name_with_City__input_dataDictionary_transformed=data_transformations.transform_join(data_dictionary=join_Name_with_City__input_dataDictionary_transformed,
																	dictionary=dictionary_join_param_join, field_out='Name with City')
	
	
	join_Name_with_City__output_dataDictionary_df=join_Name_with_City__input_dataDictionary_transformed
	join_Name_with_City__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/join_output_dataDictionary.parquet')
	join_Name_with_City__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/join_output_dataDictionary.parquet')
	
	
	dictionary_join_check_INV_THEN={'Name': True, ' - ': False, 'City': True}
	if contract_invariants.check_inv_join(data_dictionary_in=join_Name_with_City__input_dataDictionary_df,
								data_dictionary_out=join_Name_with_City__output_dataDictionary_df,
								dictionary=dictionary_join_check_INV_THEN,
								field_out='Name with City', origin_function="String Manipulation"):
		print('INVARIANT String Manipulation(Name) JoinInputs:Name VALIDATED')
	else:
		print('INVARIANT String Manipulation(Name) JoinInputs:Name NOT VALIDATED')
	
	dictionary_join_check_INV_THEN={'Name': True, ' - ': False, 'City': True}
	if contract_invariants.check_inv_join(data_dictionary_in=join_Name_with_City__input_dataDictionary_df,
								data_dictionary_out=join_Name_with_City__output_dataDictionary_df,
								dictionary=dictionary_join_check_INV_THEN,
								field_out='Name with City', origin_function="String Manipulation"):
		print('INVARIANT String Manipulation(City) JoinInputs:Name VALIDATED')
	else:
		print('INVARIANT String Manipulation(City) JoinInputs:Name NOT VALIDATED')
	
	
	

set_logger("dataProcessing")
generateWorkflow()
