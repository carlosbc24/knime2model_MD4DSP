import pandas as pd
import numpy as np
import functions.data_transformations as data_transformations
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation_input_dataDictionary.parquet')

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
	

set_logger("transformations")
generateWorkflow()
