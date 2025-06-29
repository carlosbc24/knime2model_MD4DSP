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
	imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_input_dataDictionary.parquet')
	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='sex', origin_function="Missing Value")
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='IRSCHOOL', origin_function="Missing Value")
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='ETHNICITY', origin_function="Missing Value")
	
	data_smells.check_integer_as_floating_point(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='sex', origin_function="Missing Value")
	data_smells.check_integer_as_floating_point(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='IRSCHOOL', origin_function="Missing Value")
	data_smells.check_integer_as_floating_point(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='ETHNICITY', origin_function="Missing Value")
	
	data_smells.check_types_as_string(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='sex', expected_type=DataType.STRING, origin_function="Missing Value")
	data_smells.check_types_as_string(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='IRSCHOOL', expected_type=DataType.STRING, origin_function="Missing Value")
	data_smells.check_types_as_string(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='ETHNICITY', expected_type=DataType.STRING, origin_function="Missing Value")
	
	data_smells.check_special_character_spacing(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='sex', origin_function="Missing Value")
	data_smells.check_special_character_spacing(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='IRSCHOOL', origin_function="Missing Value")
	data_smells.check_special_character_spacing(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='ETHNICITY', origin_function="Missing Value")
	
	data_smells.check_suspect_precision(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='sex', origin_function="Missing Value")
	data_smells.check_suspect_precision(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='IRSCHOOL', origin_function="Missing Value")
	data_smells.check_suspect_precision(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='ETHNICITY', origin_function="Missing Value")
	
	
	data_smells.check_date_as_datetime(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='sex', origin_function="Missing Value")
	data_smells.check_date_as_datetime(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='IRSCHOOL', origin_function="Missing Value")
	data_smells.check_date_as_datetime(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='ETHNICITY', origin_function="Missing Value")
	
	data_smells.check_separating_consistency(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, decimal_sep='.',  field='sex', origin_function="Missing Value")
	data_smells.check_separating_consistency(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, decimal_sep='.',  field='IRSCHOOL', origin_function="Missing Value")
	data_smells.check_separating_consistency(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, decimal_sep='.',  field='ETHNICITY', origin_function="Missing Value")
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='sex', origin_function="Missing Value")
	data_smells.check_ambiguous_datetime_format(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='IRSCHOOL', origin_function="Missing Value")
	data_smells.check_ambiguous_datetime_format(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='ETHNICITY', origin_function="Missing Value")
	
	

	imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_transformed=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df.copy()
	missing_values_list=[]
	imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_transformed=data_transformations.transform_special_value_derived_value(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_transformed,
																  special_type_input=SpecialType(0), derived_type_output=DerivedType(0),
																  missing_values=missing_values_list,		
																  axis_param=0, field_in = 'sex', field_out = 'sex')
	
	missing_values_list=[]
	imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_transformed=data_transformations.transform_special_value_derived_value(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_transformed,
																  special_type_input=SpecialType(0), derived_type_output=DerivedType(0),
																  missing_values=missing_values_list,		
																  axis_param=0, field_in = 'IRSCHOOL', field_out = 'IRSCHOOL')
	
	missing_values_list=[]
	imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_transformed=data_transformations.transform_special_value_derived_value(data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_transformed,
																  special_type_input=SpecialType(0), derived_type_output=DerivedType(0),
																  missing_values=missing_values_list,		
																  axis_param=0, field_in = 'ETHNICITY', field_out = 'ETHNICITY')
	
	imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_transformed
	imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
	imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='ACADEMIC_INTEREST_2', origin_function="Missing Value")
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='ACADEMIC_INTEREST_1', origin_function="Missing Value")
	
	data_smells.check_integer_as_floating_point(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_2', origin_function="Missing Value")
	data_smells.check_integer_as_floating_point(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_1', origin_function="Missing Value")
	
	data_smells.check_types_as_string(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_2', expected_type=DataType.STRING, origin_function="Missing Value")
	data_smells.check_types_as_string(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_1', expected_type=DataType.STRING, origin_function="Missing Value")
	
	data_smells.check_special_character_spacing(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_2', origin_function="Missing Value")
	data_smells.check_special_character_spacing(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_1', origin_function="Missing Value")
	
	data_smells.check_suspect_precision(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_2', origin_function="Missing Value")
	data_smells.check_suspect_precision(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_1', origin_function="Missing Value")
	
	
	data_smells.check_date_as_datetime(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_2', origin_function="Missing Value")
	data_smells.check_date_as_datetime(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_1', origin_function="Missing Value")
	
	data_smells.check_separating_consistency(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, decimal_sep='.',  field='ACADEMIC_INTEREST_2', origin_function="Missing Value")
	data_smells.check_separating_consistency(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, decimal_sep='.',  field='ACADEMIC_INTEREST_1', origin_function="Missing Value")
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_2', origin_function="Missing Value")
	data_smells.check_ambiguous_datetime_format(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_1', origin_function="Missing Value")
	
	

	imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_transformed=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
	missing_values_list=[]
	imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_transformed=data_transformations.transform_special_value_fix_value(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_transformed,
																  special_type_input=SpecialType(0), fix_value_output='Unknown',
																  missing_values=missing_values_list,		
							                                      data_type_output = DataType(0),
																  axis_param=0, field_in = 'ACADEMIC_INTEREST_2', field_out = 'ACADEMIC_INTEREST_2')
	
	missing_values_list=[]
	imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_transformed=data_transformations.transform_special_value_fix_value(data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_transformed,
																  special_type_input=SpecialType(0), fix_value_output='Unknown',
																  missing_values=missing_values_list,		
							                                      data_type_output = DataType(0),
																  axis_param=0, field_in = 'ACADEMIC_INTEREST_1', field_out = 'ACADEMIC_INTEREST_1')
	
	imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__output_dataDictionary_df=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_transformed
	imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
	imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	imputeMissingByMean_avg_income_distance__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')

	data_smells.check_precision_consistency(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df,
											expected_decimals=0, field='avg_income', origin_function="Missing Value")
	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='avg_income', origin_function="Missing Value")
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='distance', origin_function="Missing Value")
	
	data_smells.check_integer_as_floating_point(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='avg_income', origin_function="Missing Value")
	data_smells.check_integer_as_floating_point(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='distance', origin_function="Missing Value")
	
	data_smells.check_types_as_string(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='avg_income', expected_type=DataType.DOUBLE, origin_function="Missing Value")
	data_smells.check_types_as_string(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='distance', expected_type=DataType.DOUBLE, origin_function="Missing Value")
	
	data_smells.check_special_character_spacing(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='avg_income', origin_function="Missing Value")
	data_smells.check_special_character_spacing(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='distance', origin_function="Missing Value")
	
	data_smells.check_suspect_precision(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='avg_income', origin_function="Missing Value")
	data_smells.check_suspect_precision(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='distance', origin_function="Missing Value")
	
	data_smells.check_suspect_distribution(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, min_value=9.0, max_value=202.0, field='avg_income', origin_function="Missing Value")
	data_smells.check_suspect_distribution(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, min_value=0.791, max_value=3882.192, field='distance', origin_function="Missing Value")
	
	data_smells.check_date_as_datetime(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='avg_income', origin_function="Missing Value")
	data_smells.check_date_as_datetime(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='distance', origin_function="Missing Value")
	
	data_smells.check_separating_consistency(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, decimal_sep='.',  field='avg_income', origin_function="Missing Value")
	data_smells.check_separating_consistency(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, decimal_sep='.',  field='distance', origin_function="Missing Value")
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='avg_income', origin_function="Missing Value")
	data_smells.check_ambiguous_datetime_format(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='distance', origin_function="Missing Value")
	
	

	imputeMissingByMean_avg_income_distance__input_dataDictionary_transformed=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
	missing_values_list=[]
	imputeMissingByMean_avg_income_distance__input_dataDictionary_transformed=data_transformations.transform_special_value_num_op(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_transformed,
																  special_type_input=SpecialType(0), num_op_output=Operation(1),
																  missing_values=missing_values_list,		
																  axis_param=0, field_in = 'avg_income', field_out = 'avg_income')
	
	missing_values_list=[]
	imputeMissingByMean_avg_income_distance__input_dataDictionary_transformed=data_transformations.transform_special_value_num_op(data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_transformed,
																  special_type_input=SpecialType(0), num_op_output=Operation(1),
																  missing_values=missing_values_list,		
																  axis_param=0, field_in = 'distance', field_out = 'distance')
	
	imputeMissingByMean_avg_income_distance__output_dataDictionary_df=imputeMissingByMean_avg_income_distance__input_dataDictionary_transformed
	imputeMissingByMean_avg_income_distance__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
	imputeMissingByMean_avg_income_distance__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')

	data_smells.check_precision_consistency(data_dictionary=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df,
											expected_decimals=0, field='satscore', origin_function="Missing Value")
	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='satscore', origin_function="Missing Value")
	
	data_smells.check_integer_as_floating_point(data_dictionary=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df, field='satscore', origin_function="Missing Value")
	
	data_smells.check_types_as_string(data_dictionary=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df, field='satscore', expected_type=DataType.INTEGER, origin_function="Missing Value")
	
	data_smells.check_special_character_spacing(data_dictionary=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df, field='satscore', origin_function="Missing Value")
	
	data_smells.check_suspect_precision(data_dictionary=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df, field='satscore', origin_function="Missing Value")
	
	data_smells.check_suspect_distribution(data_dictionary=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df, min_value=440.0, max_value=1600.0, field='satscore', origin_function="Missing Value")
	
	data_smells.check_date_as_datetime(data_dictionary=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df, field='satscore', origin_function="Missing Value")
	
	data_smells.check_separating_consistency(data_dictionary=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df, decimal_sep='.',  field='satscore', origin_function="Missing Value")
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df, field='satscore', origin_function="Missing Value")
	
	

	imputeMissingByLinearInterpolation_satscore__input_dataDictionary_transformed=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
	missing_values_list=[]
	imputeMissingByLinearInterpolation_satscore__input_dataDictionary_transformed=data_transformations.transform_special_value_num_op(data_dictionary=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_transformed,
																  special_type_input=SpecialType(0), num_op_output=Operation(0),
																  missing_values=missing_values_list,		
																  axis_param=0, field_in = 'satscore', field_out = 'satscore')
	
	imputeMissingByLinearInterpolation_satscore__output_dataDictionary_df=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_transformed
	imputeMissingByLinearInterpolation_satscore__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
	imputeMissingByLinearInterpolation_satscore__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	rowFilterRange_init_span__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=rowFilterRange_init_span__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='init_span', origin_function="Row Filter")
	
	data_smells.check_integer_as_floating_point(data_dictionary=rowFilterRange_init_span__input_dataDictionary_df, field='init_span', origin_function="Row Filter")
	
	data_smells.check_types_as_string(data_dictionary=rowFilterRange_init_span__input_dataDictionary_df, field='init_span', expected_type=DataType.INTEGER, origin_function="Row Filter")
	
	data_smells.check_special_character_spacing(data_dictionary=rowFilterRange_init_span__input_dataDictionary_df, field='init_span', origin_function="Row Filter")
	
	data_smells.check_suspect_precision(data_dictionary=rowFilterRange_init_span__input_dataDictionary_df, field='init_span', origin_function="Row Filter")
	
	data_smells.check_suspect_distribution(data_dictionary=rowFilterRange_init_span__input_dataDictionary_df, min_value=-1000.0, max_value=1000.0, field='init_span', origin_function="Row Filter")
	
	data_smells.check_date_as_datetime(data_dictionary=rowFilterRange_init_span__input_dataDictionary_df, field='init_span', origin_function="Row Filter")
	
	data_smells.check_separating_consistency(data_dictionary=rowFilterRange_init_span__input_dataDictionary_df, decimal_sep='.',  field='init_span', origin_function="Row Filter")
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=rowFilterRange_init_span__input_dataDictionary_df, field='init_span', origin_function="Row Filter")
	
	

	rowFilterRange_init_span__input_dataDictionary_transformed=rowFilterRange_init_span__input_dataDictionary_df.copy()
	columns_rowFilterRange_param_filter=['init_span']
	filter_range_left_values_list_rowFilterRange_param_filter=[-np.inf]
	filter_range_right_values_list_rowFilterRange_param_filter=[0.0]
	closure_type_list_rowFilterRange_param_filter=[Closure(3)]
	
	rowFilterRange_init_span__input_dataDictionary_transformed=data_transformations.transform_filter_rows_range(data_dictionary=rowFilterRange_init_span__input_dataDictionary_transformed,
																											columns=columns_rowFilterRange_param_filter,
																											left_margin_list=filter_range_left_values_list_rowFilterRange_param_filter,
																											right_margin_list=filter_range_right_values_list_rowFilterRange_param_filter,
																											filter_type=FilterType(0),
																											closure_type_list=closure_type_list_rowFilterRange_param_filter)
	rowFilterRange_init_span__output_dataDictionary_df=rowFilterRange_init_span__input_dataDictionary_transformed
	rowFilterRange_init_span__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/rowFilter_output_dataDictionary.parquet')
	rowFilterRange_init_span__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/rowFilter_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/rowFilter_output_dataDictionary.parquet')

	data_smells.check_precision_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df,
											expected_decimals=0, field='TRAVEL_INIT_CNTCTS', origin_function="Column Filter")
	data_smells.check_precision_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df,
											expected_decimals=0, field='REFERRAL_CNTCTS', origin_function="Column Filter")
	data_smells.check_precision_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df,
											expected_decimals=0, field='telecq', origin_function="Column Filter")
	data_smells.check_precision_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df,
											expected_decimals=0, field='interest', origin_function="Column Filter")
	data_smells.check_precision_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df,
											expected_decimals=0, field='stuemail', origin_function="Column Filter")
	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='TRAVEL_INIT_CNTCTS', origin_function="Column Filter")
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='REFERRAL_CNTCTS', origin_function="Column Filter")
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='telecq', origin_function="Column Filter")
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='interest', origin_function="Column Filter")
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='stuemail', origin_function="Column Filter")
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='CONTACT_CODE1', origin_function="Column Filter")
	
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='TRAVEL_INIT_CNTCTS', origin_function="Column Filter")
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='REFERRAL_CNTCTS', origin_function="Column Filter")
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='telecq', origin_function="Column Filter")
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='interest', origin_function="Column Filter")
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='stuemail', origin_function="Column Filter")
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='CONTACT_CODE1', origin_function="Column Filter")
	
	data_smells.check_types_as_string(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='TRAVEL_INIT_CNTCTS', expected_type=DataType.INTEGER, origin_function="Column Filter")
	data_smells.check_types_as_string(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='REFERRAL_CNTCTS', expected_type=DataType.STRING, origin_function="Column Filter")
	data_smells.check_types_as_string(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='telecq', expected_type=DataType.STRING, origin_function="Column Filter")
	data_smells.check_types_as_string(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='interest', expected_type=DataType.STRING, origin_function="Column Filter")
	data_smells.check_types_as_string(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='stuemail', expected_type=DataType.STRING, origin_function="Column Filter")
	data_smells.check_types_as_string(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='CONTACT_CODE1', expected_type=DataType.STRING, origin_function="Column Filter")
	
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='TRAVEL_INIT_CNTCTS', origin_function="Column Filter")
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='REFERRAL_CNTCTS', origin_function="Column Filter")
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='telecq', origin_function="Column Filter")
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='interest', origin_function="Column Filter")
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='stuemail', origin_function="Column Filter")
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='CONTACT_CODE1', origin_function="Column Filter")
	
	data_smells.check_suspect_precision(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='TRAVEL_INIT_CNTCTS', origin_function="Column Filter")
	data_smells.check_suspect_precision(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='REFERRAL_CNTCTS', origin_function="Column Filter")
	data_smells.check_suspect_precision(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='telecq', origin_function="Column Filter")
	data_smells.check_suspect_precision(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='interest', origin_function="Column Filter")
	data_smells.check_suspect_precision(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='stuemail', origin_function="Column Filter")
	data_smells.check_suspect_precision(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='CONTACT_CODE1', origin_function="Column Filter")
	
	data_smells.check_suspect_distribution(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, min_value=0.0, max_value=5.0, field='TRAVEL_INIT_CNTCTS', origin_function="Column Filter")
	data_smells.check_suspect_distribution(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, min_value=0.0, max_value=5.0, field='REFERRAL_CNTCTS', origin_function="Column Filter")
	data_smells.check_suspect_distribution(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, min_value=1.0, max_value=4.0, field='telecq', origin_function="Column Filter")
	data_smells.check_suspect_distribution(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, min_value=0.0, max_value=3.0, field='interest', origin_function="Column Filter")
	data_smells.check_suspect_distribution(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, min_value=0.0, max_value=1.0, field='stuemail', origin_function="Column Filter")
	
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='TRAVEL_INIT_CNTCTS', origin_function="Column Filter")
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='REFERRAL_CNTCTS', origin_function="Column Filter")
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='telecq', origin_function="Column Filter")
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='interest', origin_function="Column Filter")
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='stuemail', origin_function="Column Filter")
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='CONTACT_CODE1', origin_function="Column Filter")
	
	data_smells.check_separating_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, decimal_sep='.',  field='TRAVEL_INIT_CNTCTS', origin_function="Column Filter")
	data_smells.check_separating_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, decimal_sep='.',  field='REFERRAL_CNTCTS', origin_function="Column Filter")
	data_smells.check_separating_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, decimal_sep='.',  field='telecq', origin_function="Column Filter")
	data_smells.check_separating_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, decimal_sep='.',  field='interest', origin_function="Column Filter")
	data_smells.check_separating_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, decimal_sep='.',  field='stuemail', origin_function="Column Filter")
	data_smells.check_separating_consistency(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, decimal_sep='.',  field='CONTACT_CODE1', origin_function="Column Filter")
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='TRAVEL_INIT_CNTCTS', origin_function="Column Filter")
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='REFERRAL_CNTCTS', origin_function="Column Filter")
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='telecq', origin_function="Column Filter")
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='interest', origin_function="Column Filter")
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='stuemail', origin_function="Column Filter")
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df, field='CONTACT_CODE1', origin_function="Column Filter")
	
	

	columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_transformed=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df.copy()
	field_list_columnFilter_param_field=['TRAVEL_INIT_CNTCTS', 'REFERRAL_CNTCTS', 'telecq', 'interest', 'stuemail', 'CONTACT_CODE1']
	columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_transformed=data_transformations.transform_filter_columns(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_transformed,
																	columns=field_list_columnFilter_param_field, belong_op=Belong.BELONG)
	
	columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__output_dataDictionary_df=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_transformed
	columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/columnFilter_output_dataDictionary.parquet')
	columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/columnFilter_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	mapping_TERRITORY__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/columnFilter_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=mapping_TERRITORY__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='TERRITORY', origin_function="Rule Engine")
	
	data_smells.check_integer_as_floating_point(data_dictionary=mapping_TERRITORY__input_dataDictionary_df, field='TERRITORY', origin_function="Rule Engine")
	
	data_smells.check_types_as_string(data_dictionary=mapping_TERRITORY__input_dataDictionary_df, field='TERRITORY', expected_type=DataType.STRING, origin_function="Rule Engine")
	
	data_smells.check_special_character_spacing(data_dictionary=mapping_TERRITORY__input_dataDictionary_df, field='TERRITORY', origin_function="Rule Engine")
	
	data_smells.check_suspect_precision(data_dictionary=mapping_TERRITORY__input_dataDictionary_df, field='TERRITORY', origin_function="Rule Engine")
	
	
	data_smells.check_date_as_datetime(data_dictionary=mapping_TERRITORY__input_dataDictionary_df, field='TERRITORY', origin_function="Rule Engine")
	
	data_smells.check_separating_consistency(data_dictionary=mapping_TERRITORY__input_dataDictionary_df, decimal_sep='.',  field='TERRITORY', origin_function="Rule Engine")
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=mapping_TERRITORY__input_dataDictionary_df, field='TERRITORY', origin_function="Rule Engine")
	
	

	input_values_list=['A', 'N']
	output_values_list=['0', '0']
	data_type_input_list=[DataType(0), DataType(0)]
	data_type_output_list=[DataType(0), DataType(0)]
	map_operation_list=[MapOperation(0), MapOperation(0)]
	mapping_TERRITORY__output_dataDictionary_df=data_transformations.transform_fix_value_fix_value(data_dictionary=mapping_TERRITORY__input_dataDictionary_df, input_values_list=input_values_list,
																  output_values_list=output_values_list,
							                                      data_type_input_list = data_type_input_list,
							                                      data_type_output_list = data_type_output_list,
																  map_operation_list = map_operation_list,
																  field_in = 'TERRITORY', field_out = 'TERRITORY')
	
	mapping_TERRITORY__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/ruleEngine_territory_output_dataDictionary.parquet')
	mapping_TERRITORY__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/ruleEngine_territory_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	mapping_Instate__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/ruleEngine_territory_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=mapping_Instate__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Instate', origin_function="Rule Engine")
	
	data_smells.check_integer_as_floating_point(data_dictionary=mapping_Instate__input_dataDictionary_df, field='Instate', origin_function="Rule Engine")
	
	data_smells.check_types_as_string(data_dictionary=mapping_Instate__input_dataDictionary_df, field='Instate', expected_type=DataType.STRING, origin_function="Rule Engine")
	
	data_smells.check_special_character_spacing(data_dictionary=mapping_Instate__input_dataDictionary_df, field='Instate', origin_function="Rule Engine")
	
	data_smells.check_suspect_precision(data_dictionary=mapping_Instate__input_dataDictionary_df, field='Instate', origin_function="Rule Engine")
	
	
	data_smells.check_date_as_datetime(data_dictionary=mapping_Instate__input_dataDictionary_df, field='Instate', origin_function="Rule Engine")
	
	data_smells.check_separating_consistency(data_dictionary=mapping_Instate__input_dataDictionary_df, decimal_sep='.',  field='Instate', origin_function="Rule Engine")
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=mapping_Instate__input_dataDictionary_df, field='Instate', origin_function="Rule Engine")
	
	

	input_values_list=['Y', 'N']
	output_values_list=['1', '0']
	data_type_input_list=[DataType(0), DataType(0)]
	data_type_output_list=[DataType(0), DataType(0)]
	map_operation_list=[MapOperation(0), MapOperation(0)]
	mapping_Instate__output_dataDictionary_df=data_transformations.transform_fix_value_fix_value(data_dictionary=mapping_Instate__input_dataDictionary_df, input_values_list=input_values_list,
																  output_values_list=output_values_list,
							                                      data_type_input_list = data_type_input_list,
							                                      data_type_output_list = data_type_output_list,
																  map_operation_list = map_operation_list,
																  field_in = 'Instate', field_out = 'Instate')
	
	mapping_Instate__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/ruleEngine_instate_output_dataDictionary.parquet')
	mapping_Instate__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/ruleEngine_instate_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	stringToNumber_TERRITORY_Instate__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/ruleEngine_instate_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='TERRITORY', origin_function="String To Number")
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Instate', origin_function="String To Number")
	
	data_smells.check_integer_as_floating_point(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, field='TERRITORY', origin_function="String To Number")
	data_smells.check_integer_as_floating_point(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, field='Instate', origin_function="String To Number")
	
	data_smells.check_types_as_string(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, field='TERRITORY', expected_type=DataType.STRING, origin_function="String To Number")
	data_smells.check_types_as_string(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, field='Instate', expected_type=DataType.STRING, origin_function="String To Number")
	
	data_smells.check_special_character_spacing(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, field='TERRITORY', origin_function="String To Number")
	data_smells.check_special_character_spacing(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, field='Instate', origin_function="String To Number")
	
	data_smells.check_suspect_precision(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, field='TERRITORY', origin_function="String To Number")
	data_smells.check_suspect_precision(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, field='Instate', origin_function="String To Number")
	
	
	data_smells.check_date_as_datetime(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, field='TERRITORY', origin_function="String To Number")
	data_smells.check_date_as_datetime(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, field='Instate', origin_function="String To Number")
	
	data_smells.check_separating_consistency(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, decimal_sep='.',  field='TERRITORY', origin_function="String To Number")
	data_smells.check_separating_consistency(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, decimal_sep='.',  field='Instate', origin_function="String To Number")
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, field='TERRITORY', origin_function="String To Number")
	data_smells.check_ambiguous_datetime_format(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df, field='Instate', origin_function="String To Number")
	
	

	stringToNumber_TERRITORY_Instate__input_dataDictionary_transformed=stringToNumber_TERRITORY_Instate__input_dataDictionary_df.copy()
	stringToNumber_TERRITORY_Instate__input_dataDictionary_transformed=data_transformations.transform_cast_type(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_transformed,
																	data_type_output= DataType(2),
																	field='TERRITORY', origin_function="String To Number")
	
	stringToNumber_TERRITORY_Instate__input_dataDictionary_transformed=data_transformations.transform_cast_type(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_transformed,
																	data_type_output= DataType(2),
																	field='Instate', origin_function="String To Number")
	
	stringToNumber_TERRITORY_Instate__output_dataDictionary_df=stringToNumber_TERRITORY_Instate__input_dataDictionary_transformed
	stringToNumber_TERRITORY_Instate__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/stringToNumber_output_dataDictionary.parquet')
	stringToNumber_TERRITORY_Instate__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/stringToNumber_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/stringToNumber_output_dataDictionary.parquet')

	data_smells.check_precision_consistency(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df,
											expected_decimals=0, field='Instate', origin_function="Numeric Outliers")
	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='avg_income', origin_function="Numeric Outliers")
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='distance', origin_function="Numeric Outliers")
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Instate', origin_function="Numeric Outliers")
	
	data_smells.check_integer_as_floating_point(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='avg_income', origin_function="Numeric Outliers")
	data_smells.check_integer_as_floating_point(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='distance', origin_function="Numeric Outliers")
	data_smells.check_integer_as_floating_point(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='Instate', origin_function="Numeric Outliers")
	
	data_smells.check_types_as_string(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='avg_income', expected_type=DataType.DOUBLE, origin_function="Numeric Outliers")
	data_smells.check_types_as_string(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='distance', expected_type=DataType.DOUBLE, origin_function="Numeric Outliers")
	data_smells.check_types_as_string(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='Instate', expected_type=DataType.STRING, origin_function="Numeric Outliers")
	
	data_smells.check_special_character_spacing(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='avg_income', origin_function="Numeric Outliers")
	data_smells.check_special_character_spacing(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='distance', origin_function="Numeric Outliers")
	data_smells.check_special_character_spacing(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='Instate', origin_function="Numeric Outliers")
	
	data_smells.check_suspect_precision(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='avg_income', origin_function="Numeric Outliers")
	data_smells.check_suspect_precision(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='distance', origin_function="Numeric Outliers")
	data_smells.check_suspect_precision(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='Instate', origin_function="Numeric Outliers")
	
	data_smells.check_suspect_distribution(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, min_value=9.0, max_value=202.0, field='avg_income', origin_function="Numeric Outliers")
	data_smells.check_suspect_distribution(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, min_value=0.791, max_value=3882.192, field='distance', origin_function="Numeric Outliers")
	data_smells.check_suspect_distribution(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, min_value=0.0, max_value=1.0, field='Instate', origin_function="Numeric Outliers")
	
	data_smells.check_date_as_datetime(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='avg_income', origin_function="Numeric Outliers")
	data_smells.check_date_as_datetime(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='distance', origin_function="Numeric Outliers")
	data_smells.check_date_as_datetime(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='Instate', origin_function="Numeric Outliers")
	
	data_smells.check_separating_consistency(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, decimal_sep='.',  field='avg_income', origin_function="Numeric Outliers")
	data_smells.check_separating_consistency(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, decimal_sep='.',  field='distance', origin_function="Numeric Outliers")
	data_smells.check_separating_consistency(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, decimal_sep='.',  field='Instate', origin_function="Numeric Outliers")
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='avg_income', origin_function="Numeric Outliers")
	data_smells.check_ambiguous_datetime_format(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='distance', origin_function="Numeric Outliers")
	data_smells.check_ambiguous_datetime_format(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='Instate', origin_function="Numeric Outliers")
	
	

	imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_transformed=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df.copy()
	missing_values_list=[]
	imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_transformed=data_transformations.transform_special_value_num_op(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_transformed,
																  special_type_input=SpecialType(2), num_op_output=Operation(3),
																  missing_values=missing_values_list,		
																  axis_param=0, field_in = 'avg_income', field_out = 'avg_income')
	
	missing_values_list=[]
	imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_transformed=data_transformations.transform_special_value_num_op(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_transformed,
																  special_type_input=SpecialType(2), num_op_output=Operation(3),
																  missing_values=missing_values_list,		
																  axis_param=0, field_in = 'distance', field_out = 'distance')
	
	missing_values_list=[]
	imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_transformed=data_transformations.transform_special_value_num_op(data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_transformed,
																  special_type_input=SpecialType(2), num_op_output=Operation(3),
																  missing_values=missing_values_list,		
																  axis_param=0, field_in = 'Instate', field_out = 'Instate')
	
	imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_transformed
	imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericOutliers_output_dataDictionary.parquet')
	imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericOutliers_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericOutliers_output_dataDictionary.parquet')

	data_smells.check_precision_consistency(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											expected_decimals=0, field='TOTAL_CONTACTS', origin_function="Numeric Binner")
	data_smells.check_precision_consistency(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											expected_decimals=0, field='SELF_INIT_CNTCTS', origin_function="Numeric Binner")
	data_smells.check_precision_consistency(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											expected_decimals=0, field='SOLICITED_CNTCTS', origin_function="Numeric Binner")
	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='TOTAL_CONTACTS', origin_function="Numeric Binner")
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='SELF_INIT_CNTCTS', origin_function="Numeric Binner")
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='SOLICITED_CNTCTS', origin_function="Numeric Binner")
	
	data_smells.check_integer_as_floating_point(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='TOTAL_CONTACTS', origin_function="Numeric Binner")
	data_smells.check_integer_as_floating_point(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='SELF_INIT_CNTCTS', origin_function="Numeric Binner")
	data_smells.check_integer_as_floating_point(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='SOLICITED_CNTCTS', origin_function="Numeric Binner")
	
	data_smells.check_types_as_string(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='TOTAL_CONTACTS', expected_type=DataType.INTEGER, origin_function="Numeric Binner")
	data_smells.check_types_as_string(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='SELF_INIT_CNTCTS', expected_type=DataType.INTEGER, origin_function="Numeric Binner")
	data_smells.check_types_as_string(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='SOLICITED_CNTCTS', expected_type=DataType.INTEGER, origin_function="Numeric Binner")
	
	data_smells.check_special_character_spacing(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='TOTAL_CONTACTS', origin_function="Numeric Binner")
	data_smells.check_special_character_spacing(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='SELF_INIT_CNTCTS', origin_function="Numeric Binner")
	data_smells.check_special_character_spacing(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='SOLICITED_CNTCTS', origin_function="Numeric Binner")
	
	data_smells.check_suspect_precision(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='TOTAL_CONTACTS', origin_function="Numeric Binner")
	data_smells.check_suspect_precision(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='SELF_INIT_CNTCTS', origin_function="Numeric Binner")
	data_smells.check_suspect_precision(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='SOLICITED_CNTCTS', origin_function="Numeric Binner")
	
	data_smells.check_suspect_distribution(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, min_value=1.0, max_value=28.0, field='TOTAL_CONTACTS', origin_function="Numeric Binner")
	data_smells.check_suspect_distribution(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, min_value=0.0, max_value=21.0, field='SELF_INIT_CNTCTS', origin_function="Numeric Binner")
	data_smells.check_suspect_distribution(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, min_value=0.0, max_value=9.0, field='SOLICITED_CNTCTS', origin_function="Numeric Binner")
	
	data_smells.check_date_as_datetime(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='TOTAL_CONTACTS', origin_function="Numeric Binner")
	data_smells.check_date_as_datetime(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='SELF_INIT_CNTCTS', origin_function="Numeric Binner")
	data_smells.check_date_as_datetime(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='SOLICITED_CNTCTS', origin_function="Numeric Binner")
	
	data_smells.check_separating_consistency(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, decimal_sep='.',  field='TOTAL_CONTACTS', origin_function="Numeric Binner")
	data_smells.check_separating_consistency(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, decimal_sep='.',  field='SELF_INIT_CNTCTS', origin_function="Numeric Binner")
	data_smells.check_separating_consistency(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, decimal_sep='.',  field='SOLICITED_CNTCTS', origin_function="Numeric Binner")
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='TOTAL_CONTACTS', origin_function="Numeric Binner")
	data_smells.check_ambiguous_datetime_format(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='SELF_INIT_CNTCTS', origin_function="Numeric Binner")
	data_smells.check_ambiguous_datetime_format(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df, field='SOLICITED_CNTCTS', origin_function="Numeric Binner")
	
	

	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df.copy()
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed,
																  data_type_output = DataType(0),
																  field_in = 'TOTAL_CONTACTS', field_out = 'TOTAL_CONTACTS_binned')
	
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed,
																  data_type_output = DataType(0),
																  field_in = 'SELF_INIT_CNTCTS', field_out = 'SELF_INIT_CNTCTS_binned')
	
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed,
																  data_type_output = DataType(0),
																  field_in = 'SOLICITED_CNTCTS', field_out = 'SOLICITED_CNTCTS_binned')
	
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed,
																  left_margin=-1000.0, right_margin=1.0,
																  closure_type=Closure(0),
																  fix_value_output='Low',
							                                      data_type_output = DataType(0),
																  field_in = 'TOTAL_CONTACTS',
																  field_out = 'TOTAL_CONTACTS_binned')
	
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed,
																  left_margin=-1000.0, right_margin=1.0,
																  closure_type=Closure(0),
																  fix_value_output='Low',
							                                      data_type_output = DataType(0),
																  field_in = 'SELF_INIT_CNTCTS',
																  field_out = 'SELF_INIT_CNTCTS_binned')
	
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed,
																  left_margin=-1000.0, right_margin=1.0,
																  closure_type=Closure(0),
																  fix_value_output='Low',
							                                      data_type_output = DataType(0),
																  field_in = 'SOLICITED_CNTCTS',
																  field_out = 'SOLICITED_CNTCTS_binned')
	
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed,
																  left_margin=1.0, right_margin=4.0,
																  closure_type=Closure(2),
																  fix_value_output='Moderate',
							                                      data_type_output = DataType(0),
																  field_in = 'TOTAL_CONTACTS',
																  field_out = 'TOTAL_CONTACTS_binned')
	
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed,
																  left_margin=1.0, right_margin=4.0,
																  closure_type=Closure(2),
																  fix_value_output='Moderate',
							                                      data_type_output = DataType(0),
																  field_in = 'SELF_INIT_CNTCTS',
																  field_out = 'SELF_INIT_CNTCTS_binned')
	
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed,
																  left_margin=1.0, right_margin=4.0,
																  closure_type=Closure(2),
																  fix_value_output='Moderate',
							                                      data_type_output = DataType(0),
																  field_in = 'SOLICITED_CNTCTS',
																  field_out = 'SOLICITED_CNTCTS_binned')
	
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed,
																  left_margin=4.0, right_margin=1000.0,
																  closure_type=Closure(2),
																  fix_value_output='High',
							                                      data_type_output = DataType(0),
																  field_in = 'TOTAL_CONTACTS',
																  field_out = 'TOTAL_CONTACTS_binned')
	
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed,
																  left_margin=4.0, right_margin=1000.0,
																  closure_type=Closure(2),
																  fix_value_output='High',
							                                      data_type_output = DataType(0),
																  field_in = 'SELF_INIT_CNTCTS',
																  field_out = 'SELF_INIT_CNTCTS_binned')
	
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed,
																  left_margin=4.0, right_margin=1000.0,
																  closure_type=Closure(2),
																  fix_value_output='High',
							                                      data_type_output = DataType(0),
																  field_in = 'SOLICITED_CNTCTS',
																  field_out = 'SOLICITED_CNTCTS_binned')
	
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_transformed
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	binner_TERRITORY__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')

	data_smells.check_precision_consistency(data_dictionary=binner_TERRITORY__input_dataDictionary_df,
											expected_decimals=0, field='TERRITORY', origin_function="Numeric Binner")
	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=binner_TERRITORY__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='TERRITORY', origin_function="Numeric Binner")
	
	data_smells.check_integer_as_floating_point(data_dictionary=binner_TERRITORY__input_dataDictionary_df, field='TERRITORY', origin_function="Numeric Binner")
	
	data_smells.check_types_as_string(data_dictionary=binner_TERRITORY__input_dataDictionary_df, field='TERRITORY', expected_type=DataType.INTEGER, origin_function="Numeric Binner")
	
	data_smells.check_special_character_spacing(data_dictionary=binner_TERRITORY__input_dataDictionary_df, field='TERRITORY', origin_function="Numeric Binner")
	
	data_smells.check_suspect_precision(data_dictionary=binner_TERRITORY__input_dataDictionary_df, field='TERRITORY', origin_function="Numeric Binner")
	
	data_smells.check_suspect_distribution(data_dictionary=binner_TERRITORY__input_dataDictionary_df, min_value=0.0, max_value=8.0, field='TERRITORY', origin_function="Numeric Binner")
	
	data_smells.check_date_as_datetime(data_dictionary=binner_TERRITORY__input_dataDictionary_df, field='TERRITORY', origin_function="Numeric Binner")
	
	data_smells.check_separating_consistency(data_dictionary=binner_TERRITORY__input_dataDictionary_df, decimal_sep='.',  field='TERRITORY', origin_function="Numeric Binner")
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=binner_TERRITORY__input_dataDictionary_df, field='TERRITORY', origin_function="Numeric Binner")
	
	

	binner_TERRITORY__input_dataDictionary_transformed=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TERRITORY__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=binner_TERRITORY__input_dataDictionary_transformed,
																  data_type_output = DataType(0),
																  field_in = 'TERRITORY', field_out = 'TERRITORY_binned')
	
	binner_TERRITORY__output_dataDictionary_df=binner_TERRITORY__input_dataDictionary_transformed
	binner_TERRITORY__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TERRITORY__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TERRITORY__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_TERRITORY__input_dataDictionary_transformed,
																  left_margin=-1000.0, right_margin=1.0,
																  closure_type=Closure(0),
																  fix_value_output='Unknown',
							                                      data_type_output = DataType(0),
																  field_in = 'TERRITORY',
																  field_out = 'TERRITORY_binned')
	
	binner_TERRITORY__output_dataDictionary_df=binner_TERRITORY__input_dataDictionary_transformed
	binner_TERRITORY__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TERRITORY__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TERRITORY__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_TERRITORY__input_dataDictionary_transformed,
																  left_margin=1.0, right_margin=3.0,
																  closure_type=Closure(2),
																  fix_value_output='Zone 1',
							                                      data_type_output = DataType(0),
																  field_in = 'TERRITORY',
																  field_out = 'TERRITORY_binned')
	
	binner_TERRITORY__output_dataDictionary_df=binner_TERRITORY__input_dataDictionary_transformed
	binner_TERRITORY__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TERRITORY__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TERRITORY__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_TERRITORY__input_dataDictionary_transformed,
																  left_margin=3.0, right_margin=5.0,
																  closure_type=Closure(2),
																  fix_value_output='Zone 2',
							                                      data_type_output = DataType(0),
																  field_in = 'TERRITORY',
																  field_out = 'TERRITORY_binned')
	
	binner_TERRITORY__output_dataDictionary_df=binner_TERRITORY__input_dataDictionary_transformed
	binner_TERRITORY__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TERRITORY__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TERRITORY__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_TERRITORY__input_dataDictionary_transformed,
																  left_margin=5.0, right_margin=7.0,
																  closure_type=Closure(2),
																  fix_value_output='Zone 3',
							                                      data_type_output = DataType(0),
																  field_in = 'TERRITORY',
																  field_out = 'TERRITORY_binned')
	
	binner_TERRITORY__output_dataDictionary_df=binner_TERRITORY__input_dataDictionary_transformed
	binner_TERRITORY__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TERRITORY__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TERRITORY__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_TERRITORY__input_dataDictionary_transformed,
																  left_margin=7.0, right_margin=1000.0,
																  closure_type=Closure(3),
																  fix_value_output='Zone 4',
							                                      data_type_output = DataType(0),
																  field_in = 'TERRITORY',
																  field_out = 'TERRITORY_binned')
	
	binner_TERRITORY__output_dataDictionary_df=binner_TERRITORY__input_dataDictionary_transformed
	binner_TERRITORY__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_TERRITORY__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	binner_satscore__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=binner_satscore__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='satscore', origin_function="Numeric Binner")
	
	data_smells.check_integer_as_floating_point(data_dictionary=binner_satscore__input_dataDictionary_df, field='satscore', origin_function="Numeric Binner")
	
	data_smells.check_types_as_string(data_dictionary=binner_satscore__input_dataDictionary_df, field='satscore', expected_type=DataType.INTEGER, origin_function="Numeric Binner")
	
	data_smells.check_special_character_spacing(data_dictionary=binner_satscore__input_dataDictionary_df, field='satscore', origin_function="Numeric Binner")
	
	data_smells.check_suspect_precision(data_dictionary=binner_satscore__input_dataDictionary_df, field='satscore', origin_function="Numeric Binner")
	
	data_smells.check_suspect_distribution(data_dictionary=binner_satscore__input_dataDictionary_df, min_value=-1000.0, max_value=2000.0, field='satscore', origin_function="Numeric Binner")
	
	data_smells.check_date_as_datetime(data_dictionary=binner_satscore__input_dataDictionary_df, field='satscore', origin_function="Numeric Binner")
	
	data_smells.check_separating_consistency(data_dictionary=binner_satscore__input_dataDictionary_df, decimal_sep='.',  field='satscore', origin_function="Numeric Binner")
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=binner_satscore__input_dataDictionary_df, field='satscore', origin_function="Numeric Binner")
	
	

	binner_satscore__input_dataDictionary_transformed=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_satscore__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=binner_satscore__input_dataDictionary_transformed,
																  data_type_output = DataType(0),
																  field_in = 'satscore', field_out = 'satscore_binned')
	
	binner_satscore__output_dataDictionary_df=binner_satscore__input_dataDictionary_transformed
	binner_satscore__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_satscore__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_satscore__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_satscore__input_dataDictionary_transformed,
																  left_margin=-1000.0, right_margin=1040.0,
																  closure_type=Closure(1),
																  fix_value_output='54 Percentile and Under',
							                                      data_type_output = DataType(0),
																  field_in = 'satscore',
																  field_out = 'satscore_binned')
	
	binner_satscore__output_dataDictionary_df=binner_satscore__input_dataDictionary_transformed
	binner_satscore__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_satscore__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_satscore__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_satscore__input_dataDictionary_transformed,
																  left_margin=1040.0, right_margin=1160.0,
																  closure_type=Closure(0),
																  fix_value_output='55-75 Percentile',
							                                      data_type_output = DataType(0),
																  field_in = 'satscore',
																  field_out = 'satscore_binned')
	
	binner_satscore__output_dataDictionary_df=binner_satscore__input_dataDictionary_transformed
	binner_satscore__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_satscore__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_satscore__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_satscore__input_dataDictionary_transformed,
																  left_margin=1160.0, right_margin=1340.0,
																  closure_type=Closure(3),
																  fix_value_output='76-93 Percentile',
							                                      data_type_output = DataType(0),
																  field_in = 'satscore',
																  field_out = 'satscore_binned')
	
	binner_satscore__output_dataDictionary_df=binner_satscore__input_dataDictionary_transformed
	binner_satscore__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_satscore__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_satscore__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_satscore__input_dataDictionary_transformed,
																  left_margin=1340.0, right_margin=2000.0,
																  closure_type=Closure(1),
																  fix_value_output='94+ percentile',
							                                      data_type_output = DataType(0),
																  field_in = 'satscore',
																  field_out = 'satscore_binned')
	
	binner_satscore__output_dataDictionary_df=binner_satscore__input_dataDictionary_transformed
	binner_satscore__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_satscore__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	binner_avg_income__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=binner_avg_income__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='avg_income', origin_function="Numeric Binner")
	
	data_smells.check_integer_as_floating_point(data_dictionary=binner_avg_income__input_dataDictionary_df, field='avg_income', origin_function="Numeric Binner")
	
	data_smells.check_types_as_string(data_dictionary=binner_avg_income__input_dataDictionary_df, field='avg_income', expected_type=DataType.FLOAT, origin_function="Numeric Binner")
	
	data_smells.check_special_character_spacing(data_dictionary=binner_avg_income__input_dataDictionary_df, field='avg_income', origin_function="Numeric Binner")
	
	data_smells.check_suspect_precision(data_dictionary=binner_avg_income__input_dataDictionary_df, field='avg_income', origin_function="Numeric Binner")
	
	data_smells.check_suspect_distribution(data_dictionary=binner_avg_income__input_dataDictionary_df, min_value=9.0, max_value=100000.0, field='avg_income', origin_function="Numeric Binner")
	
	data_smells.check_date_as_datetime(data_dictionary=binner_avg_income__input_dataDictionary_df, field='avg_income', origin_function="Numeric Binner")
	
	data_smells.check_separating_consistency(data_dictionary=binner_avg_income__input_dataDictionary_df, decimal_sep='.',  field='avg_income', origin_function="Numeric Binner")
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=binner_avg_income__input_dataDictionary_df, field='avg_income', origin_function="Numeric Binner")
	
	

	binner_avg_income__input_dataDictionary_transformed=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_avg_income__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=binner_avg_income__input_dataDictionary_transformed,
																  data_type_output = DataType(0),
																  field_in = 'avg_income', field_out = 'avg_income_binned')
	
	binner_avg_income__output_dataDictionary_df=binner_avg_income__input_dataDictionary_transformed
	binner_avg_income__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_avg_income__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_avg_income__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_avg_income__input_dataDictionary_transformed,
																  left_margin=9.0, right_margin=42830.0,
																  closure_type=Closure(1),
																  fix_value_output='low',
							                                      data_type_output = DataType(0),
																  field_in = 'avg_income',
																  field_out = 'avg_income_binned')
	
	binner_avg_income__output_dataDictionary_df=binner_avg_income__input_dataDictionary_transformed
	binner_avg_income__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_avg_income__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_avg_income__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_avg_income__input_dataDictionary_transformed,
																  left_margin=42830.0, right_margin=55590.0,
																  closure_type=Closure(1),
																  fix_value_output='Moderate',
							                                      data_type_output = DataType(0),
																  field_in = 'avg_income',
																  field_out = 'avg_income_binned')
	
	binner_avg_income__output_dataDictionary_df=binner_avg_income__input_dataDictionary_transformed
	binner_avg_income__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_avg_income__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_avg_income__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_avg_income__input_dataDictionary_transformed,
																  left_margin=55590.0, right_margin=100000.0,
																  closure_type=Closure(2),
																  fix_value_output='High',
							                                      data_type_output = DataType(0),
																  field_in = 'avg_income',
																  field_out = 'avg_income_binned')
	
	binner_avg_income__output_dataDictionary_df=binner_avg_income__input_dataDictionary_transformed
	binner_avg_income__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	binner_avg_income__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
	














set_logger("transformations")
generateWorkflow()
