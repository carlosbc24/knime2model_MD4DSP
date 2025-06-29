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
	mathOperation_Change__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation1_input_dataDictionary.parquet')
	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=mathOperation_Change__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Latitude')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=mathOperation_Change__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Longitude')
	
	data_smells.check_integer_as_floating_point(data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Latitude')
	data_smells.check_integer_as_floating_point(data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Longitude')
	
	data_smells.check_types_as_string(data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Latitude', expected_type=DataType.DOUBLE)
	data_smells.check_types_as_string(data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Longitude', expected_type=DataType.DOUBLE)
	
	data_smells.check_special_character_spacing(data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Latitude')
	data_smells.check_special_character_spacing(data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Longitude')
	
	data_smells.check_suspect_precision(data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Latitude')
	data_smells.check_suspect_precision(data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Longitude')
	
	data_smells.check_suspect_distribution(data_dictionary=mathOperation_Change__input_dataDictionary_df, min_value=440.0, max_value=1600.0, field='Latitude')
	data_smells.check_suspect_distribution(data_dictionary=mathOperation_Change__input_dataDictionary_df, min_value=440.0, max_value=1600.0, field='Longitude')
	
	data_smells.check_date_as_datetime(data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Latitude')
	data_smells.check_date_as_datetime(data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Longitude')
	
	data_smells.check_separating_consistency(data_dictionary=mathOperation_Change__input_dataDictionary_df, decimal_sep='.',  field='Latitude')
	data_smells.check_separating_consistency(data_dictionary=mathOperation_Change__input_dataDictionary_df, decimal_sep='.',  field='Longitude')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Latitude')
	data_smells.check_ambiguous_datetime_format(data_dictionary=mathOperation_Change__input_dataDictionary_df, field='Longitude')
	
	

	mathOperation_Change__input_dataDictionary_transformed=mathOperation_Change__input_dataDictionary_df.copy()
	mathOperation_Change__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=mathOperation_Change__input_dataDictionary_transformed,
																  data_type_output = DataType(5),
																  field_in = 'Latitude', field_out = 'Change')
	
	mathOperation_Change__output_dataDictionary_df=mathOperation_Change__input_dataDictionary_transformed
	mathOperation_Change__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet')
	mathOperation_Change__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet')
	mathOperation_Change__input_dataDictionary_transformed=data_transformations.transform_math_operation(data_dictionary=mathOperation_Change__input_dataDictionary_transformed,
																math_op=MathOperator(1), field_out='Change',
																firstOperand='Latitude', isFieldFirst=True,secondOperand='Longitude', isFieldSecond=True)
	
	mathOperation_Change__output_dataDictionary_df=mathOperation_Change__input_dataDictionary_transformed
	mathOperation_Change__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet')
	mathOperation_Change__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	mathOperation_Percentage__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation1_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Change')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Latitude')
	
	data_smells.check_integer_as_floating_point(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Change')
	data_smells.check_integer_as_floating_point(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Latitude')
	
	data_smells.check_types_as_string(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Change', expected_type=DataType.DOUBLE)
	data_smells.check_types_as_string(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Latitude', expected_type=DataType.DOUBLE)
	
	data_smells.check_special_character_spacing(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Change')
	data_smells.check_special_character_spacing(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Latitude')
	
	data_smells.check_suspect_precision(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Change')
	data_smells.check_suspect_precision(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Latitude')
	
	data_smells.check_suspect_distribution(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, min_value=440.0, max_value=1600.0, field='Change')
	data_smells.check_suspect_distribution(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, min_value=440.0, max_value=1600.0, field='Latitude')
	
	data_smells.check_date_as_datetime(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Change')
	data_smells.check_date_as_datetime(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Latitude')
	
	data_smells.check_separating_consistency(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, decimal_sep='.',  field='Change')
	data_smells.check_separating_consistency(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, decimal_sep='.',  field='Latitude')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Change')
	data_smells.check_ambiguous_datetime_format(data_dictionary=mathOperation_Percentage__input_dataDictionary_df, field='Latitude')
	
	

	mathOperation_Percentage__input_dataDictionary_transformed=mathOperation_Percentage__input_dataDictionary_df.copy()
	mathOperation_Percentage__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=mathOperation_Percentage__input_dataDictionary_transformed,
																  data_type_output = DataType(5),
																  field_in = 'Change', field_out = 'Percentage')
	
	mathOperation_Percentage__output_dataDictionary_df=mathOperation_Percentage__input_dataDictionary_transformed
	mathOperation_Percentage__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet')
	mathOperation_Percentage__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet')
	mathOperation_Percentage__input_dataDictionary_transformed=data_transformations.transform_math_operation(data_dictionary=mathOperation_Percentage__input_dataDictionary_transformed,
																math_op=MathOperator(3), field_out='Percentage',
																firstOperand='Change', isFieldFirst=True,secondOperand='Latitude', isFieldSecond=True)
	
	mathOperation_Percentage__output_dataDictionary_df=mathOperation_Percentage__input_dataDictionary_transformed
	mathOperation_Percentage__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet')
	mathOperation_Percentage__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	binner_Change__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation2_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=binner_Change__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Change')
	
	data_smells.check_integer_as_floating_point(data_dictionary=binner_Change__input_dataDictionary_df, field='Change')
	
	data_smells.check_types_as_string(data_dictionary=binner_Change__input_dataDictionary_df, field='Change', expected_type=DataType.INTEGER)
	
	data_smells.check_special_character_spacing(data_dictionary=binner_Change__input_dataDictionary_df, field='Change')
	
	data_smells.check_suspect_precision(data_dictionary=binner_Change__input_dataDictionary_df, field='Change')
	
	data_smells.check_suspect_distribution(data_dictionary=binner_Change__input_dataDictionary_df, min_value=0.0, max_value=8.0, field='Change')
	
	data_smells.check_date_as_datetime(data_dictionary=binner_Change__input_dataDictionary_df, field='Change')
	
	data_smells.check_separating_consistency(data_dictionary=binner_Change__input_dataDictionary_df, decimal_sep='.',  field='Change')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=binner_Change__input_dataDictionary_df, field='Change')
	
	

	binner_Change__input_dataDictionary_transformed=binner_Change__input_dataDictionary_df.copy()
	binner_Change__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=binner_Change__input_dataDictionary_transformed,
																  data_type_output = DataType(0),
																  field_in = 'Change', field_out = 'Increase/Decrease')
	
	binner_Change__output_dataDictionary_df=binner_Change__input_dataDictionary_transformed
	binner_Change__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Change__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Change__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_Change__input_dataDictionary_transformed,
																  left_margin=-1.0E9, right_margin=0.0,
																  closure_type=Closure(0),
																  fix_value_output='Decrease',
							                                      data_type_output = DataType(0),
																  field_in = 'Change',
																  field_out = 'Increase/Decrease')
	
	binner_Change__output_dataDictionary_df=binner_Change__input_dataDictionary_transformed
	binner_Change__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Change__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Change__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_Change__input_dataDictionary_transformed,
																  left_margin=0.0, right_margin=1.0E9,
																  closure_type=Closure(0),
																  fix_value_output='Increase',
							                                      data_type_output = DataType(0),
																  field_in = 'Change',
																  field_out = 'Increase/Decrease')
	
	binner_Change__output_dataDictionary_df=binner_Change__input_dataDictionary_transformed
	binner_Change__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Change__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	



set_logger("transformations")
generateWorkflow()
