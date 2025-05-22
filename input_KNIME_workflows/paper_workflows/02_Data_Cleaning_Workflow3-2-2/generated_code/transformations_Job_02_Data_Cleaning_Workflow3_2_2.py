import pandas as pd
import numpy as np
import functions.data_transformations as data_transformations
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	join_Name_with_City__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/join_input_dataDictionary.parquet')

	join_Name_with_City__input_dataDictionary_transformed=join_Name_with_City__input_dataDictionary_df.copy()
	dictionary_join_param_join={'Name': True, ' - ': False, 'City': True}
	
	join_Name_with_City__input_dataDictionary_transformed=data_transformations.transform_join(data_dictionary=join_Name_with_City__input_dataDictionary_transformed,
																	dictionary=dictionary_join_param_join, field_out='Name with City')
	
	
	join_Name_with_City__output_dataDictionary_df=join_Name_with_City__input_dataDictionary_transformed
	join_Name_with_City__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/join_output_dataDictionary.parquet')
	join_Name_with_City__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/join_output_dataDictionary.parquet')
	

set_logger("transformations")
generateWorkflow()
