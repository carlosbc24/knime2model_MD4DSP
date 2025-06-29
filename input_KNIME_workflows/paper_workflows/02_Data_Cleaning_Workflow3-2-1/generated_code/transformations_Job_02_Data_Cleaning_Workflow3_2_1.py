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
	mapping_Tz_database_time_zone__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mapping_input_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Tz database time zone')
	
	data_smells.check_integer_as_floating_point(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, field='Tz database time zone')
	
	data_smells.check_types_as_string(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, field='Tz database time zone', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, field='Tz database time zone')
	
	data_smells.check_suspect_precision(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, field='Tz database time zone')
	
	
	data_smells.check_date_as_datetime(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, field='Tz database time zone')
	
	data_smells.check_separating_consistency(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, decimal_sep='.',  field='Tz database time zone')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, field='Tz database time zone')
	
	

	input_values_list=['/']
	output_values_list=['-']
	data_type_input_list=[DataType(0)]
	data_type_output_list=[DataType(0)]
	map_operation_list=[MapOperation(1)]
	mapping_Tz_database_time_zone__output_dataDictionary_df=data_transformations.transform_fix_value_fix_value(data_dictionary=mapping_Tz_database_time_zone__input_dataDictionary_df, input_values_list=input_values_list,
																  output_values_list=output_values_list,
							                                      data_type_input_list = data_type_input_list,
							                                      data_type_output_list = data_type_output_list,
																  map_operation_list = map_operation_list,
																  field_in = 'Tz database time zone', field_out = 'Tz database time zone')
	
	mapping_Tz_database_time_zone__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mapping_output_dataDictionary.parquet')
	mapping_Tz_database_time_zone__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mapping_output_dataDictionary.parquet')
	

set_logger("transformations")
generateWorkflow()
