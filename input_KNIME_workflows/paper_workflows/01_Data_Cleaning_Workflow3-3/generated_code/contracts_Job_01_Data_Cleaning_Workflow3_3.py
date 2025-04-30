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
	binner_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_input_dataDictionary.parquet')
	binner_Life_expectancy__input_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_input_dataDictionary.parquet')
	if os.path.exists('/wf_validation_python/data/output/binner_output_dataDictionary.parquet'):
		binner_Life_expectancy__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_Life_expectancy__input_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='Life_expectancy'):
		print('PRECONDITION binner(Life_expectancy)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION binner(Life_expectancy)_PRE_valueRange NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_Life_expectancy__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(0), field='Life-Expectancy (High/Low/Avg)'):
		print('POSTCONDITION binner(Life_expectancy)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION binner(Life_expectancy)_POST_valueRange NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Life_expectancy__input_dataDictionary_df,
											data_dictionary_out=binner_Life_expectancy__output_dataDictionary_df,
											left_margin=-40.0, right_margin=40.0,
											closure_type=Closure(1),
											fix_value_output='Low',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Life_expectancy', field_out='Life-Expectancy (High/Low/Avg)'):
		print('INVARIANT binner(Life_expectancy)_INV_condition VALIDATED')
	else:
		print('INVARIANT binner(Life_expectancy)_INV_condition NOT VALIDATED')
	
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Life_expectancy__input_dataDictionary_df,
											data_dictionary_out=binner_Life_expectancy__output_dataDictionary_df,
											left_margin=70.0, right_margin=70.0,
											closure_type=Closure(2),
											fix_value_output='High',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Life_expectancy', field_out='Life-Expectancy (High/Low/Avg)'):
		print('INVARIANT binner(Life_expectancy)_INV_condition VALIDATED')
	else:
		print('INVARIANT binner(Life_expectancy)_INV_condition NOT VALIDATED')
	
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Life_expectancy__input_dataDictionary_df,
											data_dictionary_out=binner_Life_expectancy__output_dataDictionary_df,
											left_margin=-1.0E9, right_margin=-40.0,
											closure_type=Closure(0),
											fix_value_output='Average',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Life_expectancy', field_out='Life-Expectancy (High/Low/Avg)'):
		print('INVARIANT binner(Life_expectancy)_INV_condition VALIDATED')
	else:
		print('INVARIANT binner(Life_expectancy)_INV_condition NOT VALIDATED')
	
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Life_expectancy__input_dataDictionary_df,
											data_dictionary_out=binner_Life_expectancy__output_dataDictionary_df,
											left_margin=40.0, right_margin=70.0,
											closure_type=Closure(0),
											fix_value_output='Average',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Life_expectancy', field_out='Life-Expectancy (High/Low/Avg)'):
		print('INVARIANT binner(Life_expectancy)_INV_condition VALIDATED')
	else:
		print('INVARIANT binner(Life_expectancy)_INV_condition NOT VALIDATED')
	
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Life_expectancy__input_dataDictionary_df,
											data_dictionary_out=binner_Life_expectancy__output_dataDictionary_df,
											left_margin=70.0, right_margin=1.0E9,
											closure_type=Closure(0),
											fix_value_output='Average',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Life_expectancy', field_out='Life-Expectancy (High/Low/Avg)'):
		print('INVARIANT binner(Life_expectancy)_INV_condition VALIDATED')
	else:
		print('INVARIANT binner(Life_expectancy)_INV_condition NOT VALIDATED')
	
	
	
	
	
	#-----------------New DataProcessing-----------------
	rowFilterPrimitive_Region__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet'):
		rowFilterPrimitive_Region__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')

	if contract_pre_post.check_fix_value_range(value='North America', data_dictionary=rowFilterPrimitive_Region__input_dataDictionary_df, belong_op=Belong(0), field='Region',
									quant_abs=None, quant_rel=None, quant_op=None):
		print('PRECONDITION rowFilterPrimitive(Region)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION rowFilterPrimitive(Region)_PRE_valueRange NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value='North America', data_dictionary=rowFilterPrimitive_Region__output_dataDictionary_df, belong_op=Belong(0), field='Region',
									quant_abs=None, quant_rel=None, quant_op=None):
		print('POSTCONDITION rowFilterPrimitive(Region)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION rowFilterPrimitive(Region)_POST_valueRange NOT VALIDATED')
	
	#-----------------New DataProcessing-----------------
	columnFilter_Country_Region_Life_expectancy_Life_Expectancy_High_Low_Avg__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet'):
		columnFilter_Country_Region_Life_expectancy_Life_Expectancy_High_Low_Avg__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')

	field_list_columnFilter_PRE_field_range=['Country', 'Region', 'Life_expectancy', 'Life-Expectancy (High/Low/Avg)']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_PRE_field_range,
								data_dictionary=columnFilter_Country_Region_Life_expectancy_Life_Expectancy_High_Low_Avg__input_dataDictionary_df,
								belong_op=Belong(0)):
		print('PRECONDITION columnFilter(Country, Region, Life_expectancy, Life-Expectancy (High/Low/Avg))_PRE_fieldRange VALIDATED')
	else:
		print('PRECONDITION columnFilter(Country, Region, Life_expectancy, Life-Expectancy (High/Low/Avg))_PRE_fieldRange NOT VALIDATED')
	
	
	field_list_columnFilter_POST_field_range=['Country', 'Region', 'Life_expectancy', 'Life-Expectancy (High/Low/Avg)']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_POST_field_range,
								data_dictionary=columnFilter_Country_Region_Life_expectancy_Life_Expectancy_High_Low_Avg__output_dataDictionary_df,
								belong_op=Belong(0)):
		print('POSTCONDITION columnFilter(Country, Region, Life_expectancy, Life-Expectancy (High/Low/Avg))_POST_fieldRange VALIDATED')
	else:
		print('POSTCONDITION columnFilter(Country, Region, Life_expectancy, Life-Expectancy (High/Low/Avg))_POST_fieldRange NOT VALIDATED')
	
	



set_logger("contracts")
generateWorkflow()
