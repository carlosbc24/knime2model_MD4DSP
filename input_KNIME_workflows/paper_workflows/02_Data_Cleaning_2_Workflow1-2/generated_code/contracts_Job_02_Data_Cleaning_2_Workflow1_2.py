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
	rowFilterRange_Latitude__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_input_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet'):
		rowFilterRange_Latitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=rowFilterRange_Latitude__input_dataDictionary_df,
	                                	closure_type=Closure(2), belong_op=Belong(0), field='Latitude'):
		print('PRECONDITION rowFilter(Latitude)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION rowFilter(Latitude)_PRE_valueRange NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value='-216', data_dictionary=rowFilterRange_Latitude__output_dataDictionary_df, belong_op=Belong(0), field='Latitude',
									quant_abs=None, quant_rel=None, quant_op=None):
		print('POSTCONDITION rowFilter(Latitude)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION rowFilter(Latitude)_POST_valueRange NOT VALIDATED')
	
set_logger("contracts")
generateWorkflow()
