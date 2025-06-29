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
	binner_hours_per_week__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_input_dataDictionary.parquet')
	if os.path.exists('/wf_validation_python/data/output/binner_output_dataDictionary.parquet'):
		binner_hours_per_week__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=binner_hours_per_week__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='hours-per-week')
	
	data_smells.check_integer_as_floating_point(data_dictionary=binner_hours_per_week__input_dataDictionary_df, field='hours-per-week')
	
	data_smells.check_types_as_string(data_dictionary=binner_hours_per_week__input_dataDictionary_df, field='hours-per-week', expected_type=DataType.INTEGER)
	
	data_smells.check_special_character_spacing(data_dictionary=binner_hours_per_week__input_dataDictionary_df, field='hours-per-week')
	
	data_smells.check_suspect_precision(data_dictionary=binner_hours_per_week__input_dataDictionary_df, field='hours-per-week')
	
	data_smells.check_suspect_distribution(data_dictionary=binner_hours_per_week__input_dataDictionary_df, min_value=0.0, max_value=8.0, field='hours-per-week')
	
	data_smells.check_date_as_datetime(data_dictionary=binner_hours_per_week__input_dataDictionary_df, field='hours-per-week')
	
	data_smells.check_separating_consistency(data_dictionary=binner_hours_per_week__input_dataDictionary_df, decimal_sep='.',  field='hours-per-week')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=binner_hours_per_week__input_dataDictionary_df, field='hours-per-week')
	
	
	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_hours_per_week__input_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='hours-per-week', origin_function="Rule Engine"):
		print('PRECONDITION Rule Engine(hours-per-week) Interval:[0.0, 1000.0] VALIDATED')
	else:
		print('PRECONDITION Rule Engine(hours-per-week) Interval:[0.0, 1000.0] NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_hours_per_week__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='prediction', origin_function="Rule Engine"):
		print('POSTCONDITION Rule Engine(prediction) Interval:(0.0, 1000.0) VALIDATED')
	else:
		print('POSTCONDITION Rule Engine(prediction) Interval:(0.0, 1000.0) NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_hours_per_week__input_dataDictionary_df,
											data_dictionary_out=binner_hours_per_week__output_dataDictionary_df,
											left_margin=40.0, right_margin=1.0E9,
											closure_type=Closure(2),
											fix_value_output='FULL-TIME',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='hours-per-week', field_out='prediction', origin_function="Rule Engine"):
		print('INVARIANT Rule Engine(hours-per-week) Interval:[40.0, 1.0E9) FixValue:FULL-TIME VALIDATED')
	else:
		print('INVARIANT Rule Engine(hours-per-week) Interval:[40.0, 1.0E9) FixValue:FULL-TIME NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_hours_per_week__input_dataDictionary_df,
											data_dictionary_out=binner_hours_per_week__output_dataDictionary_df,
											left_margin=-1.0E9, right_margin=40.0,
											closure_type=Closure(0),
											fix_value_output='PART-TIME',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='hours-per-week', field_out='prediction', origin_function="Rule Engine"):
		print('INVARIANT Rule Engine(hours-per-week) Interval:(-1.0E9, 40.0) FixValue:PART-TIME VALIDATED')
	else:
		print('INVARIANT Rule Engine(hours-per-week) Interval:(-1.0E9, 40.0) FixValue:PART-TIME NOT VALIDATED')
	
	
	
	
	#-----------------New DataProcessing-----------------
	mapping_native_country__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/mapping_output_dataDictionary.parquet'):
		mapping_native_country__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mapping_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=mapping_native_country__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='native-country')
	
	data_smells.check_integer_as_floating_point(data_dictionary=mapping_native_country__input_dataDictionary_df, field='native-country')
	
	data_smells.check_types_as_string(data_dictionary=mapping_native_country__input_dataDictionary_df, field='native-country', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=mapping_native_country__input_dataDictionary_df, field='native-country')
	
	data_smells.check_suspect_precision(data_dictionary=mapping_native_country__input_dataDictionary_df, field='native-country')
	
	
	data_smells.check_date_as_datetime(data_dictionary=mapping_native_country__input_dataDictionary_df, field='native-country')
	
	data_smells.check_separating_consistency(data_dictionary=mapping_native_country__input_dataDictionary_df, decimal_sep='.',  field='native-country')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=mapping_native_country__input_dataDictionary_df, field='native-country')
	
	
	if contract_pre_post.check_fix_value_range(value='-', is_substring=True, data_dictionary=mapping_native_country__input_dataDictionary_df, belong_op=Belong(0), field='native-country',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="String Manipulation"):
		print('PRECONDITION String Manipulation(native-country) FixValue:- VALIDATED')
	else:
		print('PRECONDITION String Manipulation(native-country) FixValue:- NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value='-', is_substring=True, data_dictionary=mapping_native_country__output_dataDictionary_df, belong_op=Belong(1), field='native-country',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="String Manipulation"):
		print('POSTCONDITION String Manipulation(native-country) FixValue:- VALIDATED')
	else:
		print('POSTCONDITION String Manipulation(native-country) FixValue:- NOT VALIDATED')
	
	
	input_values_list_mapping_INV_condition=['-']
	output_values_list_mapping_INV_condition=[' ']
	
	data_type_input_list_mapping_INV_condition=[DataType(0)]
	data_type_output_list_mapping_INV_condition=[DataType(0)]
	
	is_substring_list_mapping_INV_condition=[False]
	
	if contract_invariants.check_inv_fix_value_fix_value(data_dictionary_in=mapping_native_country__input_dataDictionary_df,
											data_dictionary_out=mapping_native_country__output_dataDictionary_df,
											input_values_list=input_values_list_mapping_INV_condition, 
											output_values_list=output_values_list_mapping_INV_condition,
											is_substring_list=is_substring_list_mapping_INV_condition,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											data_type_input_list=data_type_input_list_mapping_INV_condition,
											data_type_output_list=data_type_output_list_mapping_INV_condition,
											field_in='native-country', field_out='native-country', origin_function="String Manipulation"):
		print('INVARIANT String Manipulation(native-country) InputMapValues:- OutputMapValues:  VALIDATED')
	else:
		print('INVARIANT String Manipulation(native-country) InputMapValues:- OutputMapValues:  NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	mathOperation_Age_of_birth__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mapping_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/mathOperation_output_dataDictionary.parquet'):
		mathOperation_Age_of_birth__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=mathOperation_Age_of_birth__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='age')
	
	data_smells.check_integer_as_floating_point(data_dictionary=mathOperation_Age_of_birth__input_dataDictionary_df, field='age')
	
	data_smells.check_types_as_string(data_dictionary=mathOperation_Age_of_birth__input_dataDictionary_df, field='age', expected_type=DataType.DOUBLE)
	
	data_smells.check_special_character_spacing(data_dictionary=mathOperation_Age_of_birth__input_dataDictionary_df, field='age')
	
	data_smells.check_suspect_precision(data_dictionary=mathOperation_Age_of_birth__input_dataDictionary_df, field='age')
	
	data_smells.check_suspect_distribution(data_dictionary=mathOperation_Age_of_birth__input_dataDictionary_df, min_value=440.0, max_value=1600.0, field='age')
	
	data_smells.check_date_as_datetime(data_dictionary=mathOperation_Age_of_birth__input_dataDictionary_df, field='age')
	
	data_smells.check_separating_consistency(data_dictionary=mathOperation_Age_of_birth__input_dataDictionary_df, decimal_sep='.',  field='age')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=mathOperation_Age_of_birth__input_dataDictionary_df, field='age')
	
	
	missing_values_mathOperation_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Age_of_birth__input_dataDictionary_df, field='age', 
									missing_values=missing_values_mathOperation_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Math Formula"):
		print('PRECONDITION Math Formula(age) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Math Formula(age) MissingValues:[] NOT VALIDATED')
	
	missing_values_mathOperation_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=mathOperation_Age_of_birth__output_dataDictionary_df, field='Age-of-birth', 
									missing_values=missing_values_mathOperation_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Math Formula"):
		print('POSTCONDITION Math Formula(Age-of-birth) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Math Formula(Age-of-birth) MissingValues:[] NOT VALIDATED')
	
	if contract_invariants.check_inv_math_operation(data_dictionary_in=mathOperation_Age_of_birth__input_dataDictionary_df,
											data_dictionary_out=mathOperation_Age_of_birth__output_dataDictionary_df,
											math_op=MathOperator(1),
											firstOperand=1994, isFieldFirst=False,secondOperand='age', isFieldSecond=True, 
											belong_op_out=Belong(0), field_in='age', field_out='Age-of-birth', origin_function="Math Formula"):
		print('INVARIANT Math Formula(Age-of-birth) substract(1994, age, ) VALIDATED')
	else:
		print('INVARIANT Math FormulaAge-of-birth substract(1994, age, ) NOT VALIDATED')
	
	
	
	
	



set_logger("contracts")
generateWorkflow()
