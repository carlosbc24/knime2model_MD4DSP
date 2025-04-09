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
	rowFilterPrimitive_input_DataDictionary=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_input_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet'):
		rowFilterPrimitive_output_DataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')

	if contract_pre_post.check_fix_value_range(value='a*', data_dictionary=rowFilterPrimitive_Name__input_dataDictionary_df, belong_op=Belong(0), field='Name',
									quant_abs=None, quant_rel=None, quant_op=None):
		print('PRECONDITION rowFilterPrimitive(Name)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION rowFilterPrimitive(Name)_PRE_valueRange NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value='a*', data_dictionary=rowFilterPrimitive_Name__output_dataDictionary_df, belong_op=Belong(0), field='Name',
									quant_abs=None, quant_rel=None, quant_op=None):
		print('POSTCONDITION rowFilterPrimitive(Name)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION rowFilterPrimitive(Name)_POST_valueRange NOT VALIDATED')
	
set_logger("contracts")
generateWorkflow()
