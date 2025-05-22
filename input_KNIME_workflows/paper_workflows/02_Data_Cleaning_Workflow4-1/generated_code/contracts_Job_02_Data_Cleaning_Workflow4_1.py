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
	mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation_input_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/mathOperation_output_dataDictionary.parquet'):
		mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation_output_dataDictionary.parquet')

	missing_values_mathOperation_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Latitude', 
									missing_values=missing_values_mathOperation_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Math Formula"):
		print('PRECONDITION Math Formula(Latitude) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Math Formula(Latitude) MissingValues:[] NOT VALIDATED')
	missing_values_mathOperation_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Altitude', 
									missing_values=missing_values_mathOperation_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Math Formula"):
		print('PRECONDITION Math Formula(Altitude) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Math Formula(Altitude) MissingValues:[] NOT VALIDATED')
	
	missing_values_mathOperation_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df, field='Difference in Latitude/Altitude', 
									missing_values=missing_values_mathOperation_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Math Formula"):
		print('POSTCONDITION Math Formula(Difference in Latitude/Altitude) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Math Formula(Difference in Latitude/Altitude) MissingValues:[] NOT VALIDATED')
	
	if contract_invariants.check_inv_math_operation(data_dictionary_in=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df,
											data_dictionary_out=mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df,
											math_op=MathOperator(1),
											firstOperand='Latitude', isFieldFirst=True, secondOperand='Altitude', isFieldSecond=True, 
											belong_op_out=Belong(0), field_in='Latitude', field_out='Difference in Latitude/Altitude', origin_function="Math Formula"):
		print('INVARIANT Math Formula(Difference in Latitude/Altitude) substract(Latitude, Altitude, ) VALIDATED')
	else:
		print('INVARIANT Math FormulaDifference in Latitude/Altitude substract(Latitude, Altitude, ) NOT VALIDATED')
	
	
	
	
	
	
set_logger("contracts")
generateWorkflow()
