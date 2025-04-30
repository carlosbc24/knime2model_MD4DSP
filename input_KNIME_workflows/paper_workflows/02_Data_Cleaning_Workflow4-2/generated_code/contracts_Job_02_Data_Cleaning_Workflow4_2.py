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
	binner_Source__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_input_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/binner_output_dataDictionary.parquet'):
		binner_Source__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_Source__input_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='Source'):
		print('PRECONDITION binner(Source)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION binner(Source)_PRE_valueRange NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_Source__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(0), field='High Longitudes'):
		print('POSTCONDITION binner(Source)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION binner(Source)_POST_valueRange NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Source__input_dataDictionary_df,
											data_dictionary_out=binner_Source__output_dataDictionary_df,
											left_margin=-130.0, right_margin=130.0,
											closure_type=Closure(0),
											fix_value_output='N',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Source', field_out='High Longitudes'):
		print('INVARIANT binner(Source)_INV_condition VALIDATED')
	else:
		print('INVARIANT binner(Source)_INV_condition NOT VALIDATED')
	
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Source__input_dataDictionary_df,
											data_dictionary_out=binner_Source__output_dataDictionary_df,
											left_margin=130.0, right_margin=130.0,
											closure_type=Closure(2),
											fix_value_output='Y',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Source', field_out='High Longitudes'):
		print('INVARIANT binner(Source)_INV_condition VALIDATED')
	else:
		print('INVARIANT binner(Source)_INV_condition NOT VALIDATED')
	
	
	
	
	
set_logger("contracts")
generateWorkflow()
