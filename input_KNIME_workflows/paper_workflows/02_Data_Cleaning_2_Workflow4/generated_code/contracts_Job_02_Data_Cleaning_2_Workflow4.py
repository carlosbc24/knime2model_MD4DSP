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
	mathOperation_Change__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation1_input_dataDictionary.parquet')
	if os.path.exists('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet'):
		mathOperation_Change__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet')

	missing_values_mathOperation_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Latitude', 
									missing_values=missing_values_mathOperation_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Math Formula"):
		print('PRECONDITION Math Formula(Latitude) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Math Formula(Latitude) MissingValues:[] NOT VALIDATED')
	missing_values_mathOperation_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Longitude', 
									missing_values=missing_values_mathOperation_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Math Formula"):
		print('PRECONDITION Math Formula(Longitude) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Math Formula(Longitude) MissingValues:[] NOT VALIDATED')
	
	missing_values_mathOperation_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=mathOperation_Change__output_dataDictionary_df, field='Change', 
									missing_values=missing_values_mathOperation_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Math Formula"):
		print('POSTCONDITION Math Formula(Change) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Math Formula(Change) MissingValues:[] NOT VALIDATED')
	
	if contract_invariants.check_inv_math_operation(data_dictionary_in=mathOperation_Change__input_dataDictionary_df,
											data_dictionary_out=mathOperation_Change__output_dataDictionary_df,
											math_op=MathOperator(1),
											firstOperand='Latitude', isFieldFirst=True, secondOperand='Longitude', isFieldSecond=True, 
											belong_op_out=Belong(0), field_in='Latitude', field_out='Change', origin_function="Math Formula"):
		print('INVARIANT Math Formula(Change) substract(Latitude, Longitude, ) VALIDATED')
	else:
		print('INVARIANT Math FormulaChange substract(Latitude, Longitude, ) NOT VALIDATED')
	
	
	
	
	
	
	#-----------------New DataProcessing-----------------
	mathOperation_Percentage__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet'):
		mathOperation_Percentage__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet')

	missing_values_mathOperation_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Change', 
									missing_values=missing_values_mathOperation_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Math Formula"):
		print('PRECONDITION Math Formula(Change) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Math Formula(Change) MissingValues:[] NOT VALIDATED')
	missing_values_mathOperation_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Latitude', 
									missing_values=missing_values_mathOperation_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Math Formula"):
		print('PRECONDITION Math Formula(Latitude) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Math Formula(Latitude) MissingValues:[] NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value=0, is_substring=False, data_dictionary=mathOperation_Percentage__input_dataDictionary_df, belong_op=Belong(1), field='Latitude',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Math Formula"):
		print('PRECONDITION Math Formula(Latitude) FixValue:0 VALIDATED')
	else:
		print('PRECONDITION Math Formula(Latitude) FixValue:0 NOT VALIDATED')
	
	missing_values_mathOperation_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=mathOperation_Percentage__output_dataDictionary_df, field='Percentage', 
									missing_values=missing_values_mathOperation_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Math Formula"):
		print('POSTCONDITION Math Formula(Percentage) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Math Formula(Percentage) MissingValues:[] NOT VALIDATED')
	
	if contract_invariants.check_inv_math_operation(data_dictionary_in=mathOperation_Percentage__input_dataDictionary_df,
											data_dictionary_out=mathOperation_Percentage__output_dataDictionary_df,
											math_op=MathOperator(3),
											firstOperand='Change', isFieldFirst=True, secondOperand='Latitude', isFieldSecond=True, 
											belong_op_out=Belong(0), field_in='Change', field_out='Percentage', origin_function="Math Formula"):
		print('INVARIANT Math Formula(Percentage) divide(Change, Latitude, ) VALIDATED')
	else:
		print('INVARIANT Math FormulaPercentage divide(Change, Latitude, ) NOT VALIDATED')
	
	
	
	
	
	
	#-----------------New DataProcessing-----------------
	binner_Change__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/binner_output_dataDictionary.parquet'):
		binner_Change__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_Change__input_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='Change', origin_function="Rule Engine"):
		print('PRECONDITION Rule Engine(Change) Interval:[0.0, 1000.0] VALIDATED')
	else:
		print('PRECONDITION Rule Engine(Change) Interval:[0.0, 1000.0] NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_Change__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='Increase/Decrease', origin_function="Rule Engine"):
		print('POSTCONDITION Rule Engine(Increase/Decrease) Interval:(0.0, 1000.0) VALIDATED')
	else:
		print('POSTCONDITION Rule Engine(Increase/Decrease) Interval:(0.0, 1000.0) NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Change__input_dataDictionary_df,
											data_dictionary_out=binner_Change__output_dataDictionary_df,
											left_margin=-1.0E9, right_margin=0.0,
											closure_type=Closure(0),
											fix_value_output='Decrease',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Change', field_out='Increase/Decrease', origin_function="Rule Engine"):
		print('INVARIANT Rule Engine(Change) Interval:(-1.0E9, 0.0) FixValue:Decrease VALIDATED')
	else:
		print('INVARIANT Rule Engine(Change) Interval:(-1.0E9, 0.0) FixValue:Decrease NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Change__input_dataDictionary_df,
											data_dictionary_out=binner_Change__output_dataDictionary_df,
											left_margin=0.0, right_margin=1.0E9,
											closure_type=Closure(0),
											fix_value_output='Increase',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Change', field_out='Increase/Decrease', origin_function="Rule Engine"):
		print('INVARIANT Rule Engine(Change) Interval:(0.0, 1.0E9) FixValue:Increase VALIDATED')
	else:
		print('INVARIANT Rule Engine(Change) Interval:(0.0, 1.0E9) FixValue:Increase NOT VALIDATED')
	
	
	
	



set_logger("contracts")
generateWorkflow()
