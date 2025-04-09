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
	mathOperation_Change__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation1_input_dataDictionary.parquet')
	mathOperation_Change__input_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mathOperation1_input_dataDictionary.parquet')

	missing_values_mathOperation_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Latitude', 
									missing_values=missing_values_mathOperation_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None):
		print('PRECONDITION mathOperation(Change)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION mathOperation(Change)_PRE_valueRange NOT VALIDATED')
	missing_values_mathOperation_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Latitude', 
									missing_values=missing_values_mathOperation_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None):
		print('PRECONDITION mathOperation(Change)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION mathOperation(Change)_PRE_valueRange NOT VALIDATED')
	
	mathOperation_Change__input_dataDictionary_transformed=mathOperation_Change__input_dataDictionary_df.copy()
	mathOperation_Change__input_dataDictionary_transformed=data_transformations.transform_math_operation(data_dictionary=mathOperation_Change__input_dataDictionary_transformed,
																math_op=MathOperator(1), field_out='Change',
																firstOperand='Latitude', isFieldFirst=True,secondOperand='Latitude', isFieldSecond=True)
	
	mathOperation_Change__output_dataDictionary_df=mathOperation_Change__input_dataDictionary_transformed
	mathOperation_Change__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet')
	mathOperation_Change__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet')
	
	missing_values_mathOperation_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Change__output_dataDictionary_df, field='Change', 
									missing_values=missing_values_mathOperation_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None):
		print('POSTCONDITION mathOperation(Change)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION mathOperation(Change)_POST_valueRange NOT VALIDATED')
	
	if contract_invariants.check_inv_math_operation(data_dictionary_in=mathOperation_Change__input_dataDictionary_df,
											data_dictionary_out=mathOperation_Change__output_dataDictionary_df,
											math_op=MathOperator(1),
											firstOperand='Latitude', isFieldFirst=True, secondOperand='Latitude', isFieldSecond=True, 
											belong_op_out=Belong(0), field_in='Change', field_out='Change'):
		print('INVARIANT mathOperation(Change)_INV_condition VALIDATED')
	else:
		print('INVARIANT mathOperation(Change)_INV_condition NOT VALIDATED')
	
	
	
	
	#-----------------New DataProcessing-----------------
	mathOperation_Percentage__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet')

	missing_values_mathOperation_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Change', 
									missing_values=missing_values_mathOperation_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None):
		print('PRECONDITION mathOperation(Percentage)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION mathOperation(Percentage)_PRE_valueRange NOT VALIDATED')
	missing_values_mathOperation_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Change', 
									missing_values=missing_values_mathOperation_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None):
		print('PRECONDITION mathOperation(Percentage)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION mathOperation(Percentage)_PRE_valueRange NOT VALIDATED')
	
	mathOperation_Percentage__input_dataDictionary_transformed=mathOperation_Percentage__input_dataDictionary_df.copy()
	mathOperation_Percentage__input_dataDictionary_transformed=data_transformations.transform_math_operation(data_dictionary=mathOperation_Percentage__input_dataDictionary_transformed,
																math_op=MathOperator(3), field_out='Percentage',
																firstOperand='Change', isFieldFirst=True,secondOperand='Change', isFieldSecond=True)
	
	mathOperation_Percentage__output_dataDictionary_df=mathOperation_Percentage__input_dataDictionary_transformed
	mathOperation_Percentage__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet')
	mathOperation_Percentage__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet')
	
	missing_values_mathOperation_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Percentage__output_dataDictionary_df, field='Percentage', 
									missing_values=missing_values_mathOperation_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None):
		print('POSTCONDITION mathOperation(Percentage)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION mathOperation(Percentage)_POST_valueRange NOT VALIDATED')
	
	if contract_invariants.check_inv_math_operation(data_dictionary_in=mathOperation_Percentage__input_dataDictionary_df,
											data_dictionary_out=mathOperation_Percentage__output_dataDictionary_df,
											math_op=MathOperator(1),
											firstOperand='Change', isFieldFirst=True, secondOperand='Change', isFieldSecond=True, 
											belong_op_out=Belong(0), field_in='Percentage', field_out='Percentage'):
		print('INVARIANT mathOperation(Percentage)_INV_condition VALIDATED')
	else:
		print('INVARIANT mathOperation(Percentage)_INV_condition NOT VALIDATED')
	
	
	
	
	#-----------------New DataProcessing-----------------
	binner_Percentage__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_Percentage__input_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='Percentage'):
		print('PRECONDITION binner(Percentage)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION binner(Percentage)_PRE_valueRange NOT VALIDATED')
	
	binner_Percentage__input_dataDictionary_transformed=binner_Percentage__input_dataDictionary_df.copy()
	binner_Percentage__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=binner_Percentage__input_dataDictionary_transformed,
																  data_type_output = DataType(0),
																  field_in = 'Percentage', field_out = 'Increase/Decrease')
	
	binner_Percentage__output_dataDictionary_df=binner_Percentage__input_dataDictionary_transformed
	binner_Percentage__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Percentage__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Percentage__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_Percentage__input_dataDictionary_transformed,
																  left_margin=-1000000.0, right_margin=1000000.0,
																  closure_type=Closure(0),
																  fix_value_output='No Change',
							                                      data_type_output = DataType(0),
																  field_in = 'Percentage',
																  field_out = 'Increase/Decrease')
	
	binner_Percentage__output_dataDictionary_df=binner_Percentage__input_dataDictionary_transformed
	binner_Percentage__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Percentage__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Percentage__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_Percentage__input_dataDictionary_transformed,
																  left_margin=-1000000.0, right_margin=0.0,
																  closure_type=Closure(0),
																  fix_value_output='Decrease',
							                                      data_type_output = DataType(0),
																  field_in = 'Percentage',
																  field_out = 'Increase/Decrease')
	
	binner_Percentage__output_dataDictionary_df=binner_Percentage__input_dataDictionary_transformed
	binner_Percentage__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Percentage__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Percentage__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_Percentage__input_dataDictionary_transformed,
																  left_margin=0.0, right_margin=1000000.0,
																  closure_type=Closure(0),
																  fix_value_output='Increase',
							                                      data_type_output = DataType(0),
																  field_in = 'Percentage',
																  field_out = 'Increase/Decrease')
	
	binner_Percentage__output_dataDictionary_df=binner_Percentage__input_dataDictionary_transformed
	binner_Percentage__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Percentage__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	
	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_Percentage__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(0), field='Increase/Decrease'):
		print('POSTCONDITION binner(Percentage)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION binner(Percentage)_POST_valueRange NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Percentage__input_dataDictionary_df,
											data_dictionary_out=binner_Percentage__output_dataDictionary_df,
											left_margin=-1000000.0, right_margin=1000000.0,
											closure_type=Closure(0),
											fix_value_output='No Change',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Percentage', field_out='Increase/Decrease'):
		print('INVARIANT binner(Percentage)_INV_condition VALIDATED')
	else:
		print('INVARIANT binner(Percentage)_INV_condition NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Percentage__input_dataDictionary_df,
											data_dictionary_out=binner_Percentage__output_dataDictionary_df,
											left_margin=-1000000.0, right_margin=0.0,
											closure_type=Closure(0),
											fix_value_output='Decrease',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Percentage', field_out='Increase/Decrease'):
		print('INVARIANT binner(Percentage)_INV_condition VALIDATED')
	else:
		print('INVARIANT binner(Percentage)_INV_condition NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Percentage__input_dataDictionary_df,
											data_dictionary_out=binner_Percentage__output_dataDictionary_df,
											left_margin=0.0, right_margin=1000000.0,
											closure_type=Closure(0),
											fix_value_output='Increase',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Percentage', field_out='Increase/Decrease'):
		print('INVARIANT binner(Percentage)_INV_condition VALIDATED')
	else:
		print('INVARIANT binner(Percentage)_INV_condition NOT VALIDATED')
	
	
	



set_logger("dataProcessing")
generateWorkflow()
