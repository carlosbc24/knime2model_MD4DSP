import pandas as pd
import numpy as np
import functions.data_transformations as data_transformations
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():

	#-----------------New DataProcessing-----------------
	mathOperation_Change__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation1_input_dataDictionary.parquet')
	mathOperation_Change__input_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mathOperation1_input_dataDictionary.parquet')
	mathOperation_Change__input_dataDictionary_transformed=mathOperation_Change__input_dataDictionary_df.copy()
	mathOperation_Change__input_dataDictionary_transformed=data_transformations.transform_math_operation(data_dictionary=mathOperation_Change__input_dataDictionary_transformed,
																math_op=MathOperator(1), field_out='Change',
																firstOperand='Latitude', isFieldFirst=True,secondOperand='Latitude', isFieldSecond=True)
	
	mathOperation_Change__output_dataDictionary_df=mathOperation_Change__input_dataDictionary_transformed
	mathOperation_Change__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet')
	mathOperation_Change__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	mathOperation_Percentage__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet')

	mathOperation_Percentage__input_dataDictionary_transformed=mathOperation_Percentage__input_dataDictionary_df.copy()
	mathOperation_Percentage__input_dataDictionary_transformed=data_transformations.transform_math_operation(data_dictionary=mathOperation_Percentage__input_dataDictionary_transformed,
																math_op=MathOperator(3), field_out='Percentage',
																firstOperand='Change', isFieldFirst=True,secondOperand='Change', isFieldSecond=True)
	
	mathOperation_Percentage__output_dataDictionary_df=mathOperation_Percentage__input_dataDictionary_transformed
	mathOperation_Percentage__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet')
	mathOperation_Percentage__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	binner_Percentage__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet')

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
	



set_logger("transformations")
generateWorkflow()
