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
	binner_Longitude__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_input_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=binner_Longitude__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Longitude')
	
	data_smells.check_integer_as_floating_point(data_dictionary=binner_Longitude__input_dataDictionary_df, field='Longitude')
	
	data_smells.check_types_as_string(data_dictionary=binner_Longitude__input_dataDictionary_df, field='Longitude', expected_type=DataType.INTEGER)
	
	data_smells.check_special_character_spacing(data_dictionary=binner_Longitude__input_dataDictionary_df, field='Longitude')
	
	data_smells.check_suspect_precision(data_dictionary=binner_Longitude__input_dataDictionary_df, field='Longitude')
	
	data_smells.check_suspect_distribution(data_dictionary=binner_Longitude__input_dataDictionary_df, min_value=0.0, max_value=8.0, field='Longitude')
	
	data_smells.check_date_as_datetime(data_dictionary=binner_Longitude__input_dataDictionary_df, field='Longitude')
	
	data_smells.check_separating_consistency(data_dictionary=binner_Longitude__input_dataDictionary_df, decimal_sep='.',  field='Longitude')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=binner_Longitude__input_dataDictionary_df, field='Longitude')
	
	

	binner_Longitude__input_dataDictionary_transformed=binner_Longitude__input_dataDictionary_df.copy()
	binner_Longitude__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=binner_Longitude__input_dataDictionary_transformed,
																  data_type_output = DataType(0),
																  field_in = 'Longitude', field_out = 'High Longitudes')
	
	binner_Longitude__output_dataDictionary_df=binner_Longitude__input_dataDictionary_transformed
	binner_Longitude__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Longitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Longitude__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_Longitude__input_dataDictionary_transformed,
																  left_margin=-1.0E9, right_margin=130.0,
																  closure_type=Closure(0),
																  fix_value_output='N',
							                                      data_type_output = DataType(0),
																  field_in = 'Longitude',
																  field_out = 'High Longitudes')
	
	binner_Longitude__output_dataDictionary_df=binner_Longitude__input_dataDictionary_transformed
	binner_Longitude__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Longitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Longitude__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_Longitude__input_dataDictionary_transformed,
																  left_margin=130.0, right_margin=1.0E9,
																  closure_type=Closure(2),
																  fix_value_output='Y',
							                                      data_type_output = DataType(0),
																  field_in = 'Longitude',
																  field_out = 'High Longitudes')
	
	binner_Longitude__output_dataDictionary_df=binner_Longitude__input_dataDictionary_transformed
	binner_Longitude__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Longitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	

set_logger("transformations")
generateWorkflow()
