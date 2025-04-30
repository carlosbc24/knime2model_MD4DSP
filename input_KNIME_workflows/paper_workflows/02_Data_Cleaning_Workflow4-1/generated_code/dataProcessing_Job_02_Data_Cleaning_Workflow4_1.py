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
	mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation_input_dataDictionary.parquet')

	missing_values_mathOperation_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Latitude', 
									missing_values=missing_values_mathOperation_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None):
		print('PRECONDITION mathOperation(Difference in Latitude/Altitude)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION mathOperation(Difference in Latitude/Altitude)_PRE_valueRange NOT VALIDATED')
	missing_values_mathOperation_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Altitude', 
									missing_values=missing_values_mathOperation_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None):
		print('PRECONDITION mathOperation(Difference in Latitude/Altitude)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION mathOperation(Difference in Latitude/Altitude)_PRE_valueRange NOT VALIDATED')
	
	mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_transformed=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df.copy()
	mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_transformed,
																  data_type_output = DataType(5),
																  field_in = 'Latitude', field_out = 'Difference in Latitude/Altitude')
	
	mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_transformed
	mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mathOperation_output_dataDictionary.parquet')
	mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation_output_dataDictionary.parquet')
	mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_transformed=data_transformations.transform_math_operation(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_transformed,
																math_op=MathOperator(1), field_out='Difference in Latitude/Altitude',
																firstOperand='Latitude', isFieldFirst=True,secondOperand='Altitude', isFieldSecond=True)
	
	mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_transformed
	mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mathOperation_output_dataDictionary.parquet')
	mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation_output_dataDictionary.parquet')
	
	missing_values_mathOperation_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df, field='Difference in Latitude/Altitude', 
									missing_values=missing_values_mathOperation_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None):
		print('POSTCONDITION mathOperation(Difference in Latitude/Altitude)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION mathOperation(Difference in Latitude/Altitude)_POST_valueRange NOT VALIDATED')
	
	if contract_invariants.check_inv_math_operation(data_dictionary_in=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df,
											data_dictionary_out=mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df,
											math_op=MathOperator(1),
											firstOperand='Latitude', isFieldFirst=True, secondOperand='Altitude', isFieldSecond=True, 
											belong_op_out=Belong(0), field_in='Difference in Latitude/Altitude', field_out='Difference in Latitude/Altitude'):
		print('INVARIANT mathOperation(Difference in Latitude/Altitude)_INV_condition VALIDATED')
	else:
		print('INVARIANT mathOperation(Difference in Latitude/Altitude)_INV_condition NOT VALIDATED')
	
	
	
	
	
	
	

set_logger("dataProcessing")
generateWorkflow()
