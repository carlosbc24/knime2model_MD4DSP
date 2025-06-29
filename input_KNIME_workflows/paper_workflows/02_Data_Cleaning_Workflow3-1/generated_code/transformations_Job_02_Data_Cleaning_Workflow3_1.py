import pandas as pd
import numpy as np
import functions.data_transformations as data_transformations
import functions.data_smells as data_smells
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	mapping_Country__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mapping_input_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=mapping_Country__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Country')
	
	data_smells.check_integer_as_floating_point(data_dictionary=mapping_Country__input_dataDictionary_df, field='Country')
	
	data_smells.check_types_as_string(data_dictionary=mapping_Country__input_dataDictionary_df, field='Country', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=mapping_Country__input_dataDictionary_df, field='Country')
	
	data_smells.check_suspect_precision(data_dictionary=mapping_Country__input_dataDictionary_df, field='Country')
	
	
	data_smells.check_date_as_datetime(data_dictionary=mapping_Country__input_dataDictionary_df, field='Country')
	
	data_smells.check_separating_consistency(data_dictionary=mapping_Country__input_dataDictionary_df, decimal_sep='.',  field='Country')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=mapping_Country__input_dataDictionary_df, field='Country')
	
	

	input_values_list=['United States']
	output_values_list=['USA']
	data_type_input_list=[DataType(0)]
	data_type_output_list=[DataType(0)]
	map_operation_list=[MapOperation(0)]
	mapping_Country__output_dataDictionary_df=data_transformations.transform_fix_value_fix_value(data_dictionary=mapping_Country__input_dataDictionary_df, input_values_list=input_values_list,
																  output_values_list=output_values_list,
							                                      data_type_input_list = data_type_input_list,
							                                      data_type_output_list = data_type_output_list,
																  map_operation_list = map_operation_list,
																  field_in = 'Country', field_out = 'Country')
	
	mapping_Country__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mapping_output_dataDictionary.parquet')
	mapping_Country__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mapping_output_dataDictionary.parquet')
	

set_logger("transformations")
generateWorkflow()
