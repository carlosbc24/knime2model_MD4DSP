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
	imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_input_dataDictionary.parquet')

	missing_values_imputeByDerivedValue_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='sex', 
									missing_values=missing_values_imputeByDerivedValue_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Missing Value"):
		print('PRECONDITION Missing Value(sex) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Missing Value(sex) MissingValues:[] NOT VALIDATED')
	
	missing_values_imputeByDerivedValue_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='IRSCHOOL', 
									missing_values=missing_values_imputeByDerivedValue_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Missing Value"):
		print('PRECONDITION Missing Value(IRSCHOOL) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Missing Value(IRSCHOOL) MissingValues:[] NOT VALIDATED')
	
	missing_values_imputeByDerivedValue_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='ETHNICITY', 
									missing_values=missing_values_imputeByDerivedValue_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Missing Value"):
		print('PRECONDITION Missing Value(ETHNICITY) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Missing Value(ETHNICITY) MissingValues:[] NOT VALIDATED')
	
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
	
	missing_values_imputeByDerivedValue_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df, field='sex', 
									missing_values=missing_values_imputeByDerivedValue_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION Missing Value(sex) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Missing Value(sex) MissingValues:[] NOT VALIDATED')
	
	missing_values_imputeByDerivedValue_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df, field='IRSCHOOL', 
									missing_values=missing_values_imputeByDerivedValue_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION Missing Value(IRSCHOOL) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Missing Value(IRSCHOOL) MissingValues:[] NOT VALIDATED')
	
	missing_values_imputeByDerivedValue_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df, field='ETHNICITY', 
									missing_values=missing_values_imputeByDerivedValue_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION Missing Value(ETHNICITY) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Missing Value(ETHNICITY) MissingValues:[] NOT VALIDATED')
	
	missing_values_imputeByDerivedValue_INV_condition=[]
	if contract_invariants.check_inv_special_value_derived_value(data_dictionary_in=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df,
								data_dictionary_out=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df,
								belong_op_in=Belong(0),
								belong_op_out=Belong(0),
								special_type_input=SpecialType(0),
								derived_type_output=DerivedType(0),
								missing_values=missing_values_imputeByDerivedValue_INV_condition, axis_param=0, field_in='sex', field_out='sex', origin_function="Missing Value"):
		print('INVARIANT Missing Value(sex) MISSING_to_MostFrequent VALIDATED')
	else:
		print('INVARIANT Missing Value(sex) MISSING_to_MostFrequent NOT VALIDATED')
	
	
	
	missing_values_imputeByDerivedValue_INV_condition=[]
	if contract_invariants.check_inv_special_value_derived_value(data_dictionary_in=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df,
								data_dictionary_out=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df,
								belong_op_in=Belong(0),
								belong_op_out=Belong(0),
								special_type_input=SpecialType(0),
								derived_type_output=DerivedType(0),
								missing_values=missing_values_imputeByDerivedValue_INV_condition, axis_param=0, field_in='IRSCHOOL', field_out='IRSCHOOL', origin_function="Missing Value"):
		print('INVARIANT Missing Value(IRSCHOOL) MISSING_to_MostFrequent VALIDATED')
	else:
		print('INVARIANT Missing Value(IRSCHOOL) MISSING_to_MostFrequent NOT VALIDATED')
	
	
	
	missing_values_imputeByDerivedValue_INV_condition=[]
	if contract_invariants.check_inv_special_value_derived_value(data_dictionary_in=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df,
								data_dictionary_out=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df,
								belong_op_in=Belong(0),
								belong_op_out=Belong(0),
								special_type_input=SpecialType(0),
								derived_type_output=DerivedType(0),
								missing_values=missing_values_imputeByDerivedValue_INV_condition, axis_param=0, field_in='ETHNICITY', field_out='ETHNICITY', origin_function="Missing Value"):
		print('INVARIANT Missing Value(ETHNICITY) MISSING_to_MostFrequent VALIDATED')
	else:
		print('INVARIANT Missing Value(ETHNICITY) MISSING_to_MostFrequent NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_input_dataDictionary.parquet')

	missing_values_imputeByFixValue_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_2', 
									missing_values=missing_values_imputeByFixValue_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Missing Value"):
		print('PRECONDITION Missing Value(ACADEMIC_INTEREST_2) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Missing Value(ACADEMIC_INTEREST_2) MissingValues:[] NOT VALIDATED')
	
	missing_values_imputeByFixValue_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_1', 
									missing_values=missing_values_imputeByFixValue_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Missing Value"):
		print('PRECONDITION Missing Value(ACADEMIC_INTEREST_1) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Missing Value(ACADEMIC_INTEREST_1) MissingValues:[] NOT VALIDATED')
	
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
	
	missing_values_imputeByFixValue_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__output_dataDictionary_df, field='ACADEMIC_INTEREST_2', 
									missing_values=missing_values_imputeByFixValue_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION Missing Value(ACADEMIC_INTEREST_2) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Missing Value(ACADEMIC_INTEREST_2) MissingValues:[] NOT VALIDATED')
	
	missing_values_imputeByFixValue_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__output_dataDictionary_df, field='ACADEMIC_INTEREST_1', 
									missing_values=missing_values_imputeByFixValue_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION Missing Value(ACADEMIC_INTEREST_1) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Missing Value(ACADEMIC_INTEREST_1) MissingValues:[] NOT VALIDATED')
	
	missing_values_imputeByFixValue_INV_condition=[]
	if contract_invariants.check_inv_special_value_fix_value(data_dictionary_in=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df,
								data_dictionary_out=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__output_dataDictionary_df,
								special_type_input=SpecialType(0),
								fix_value_output='Unknown',
								belong_op_in=Belong(0),
								belong_op_out=Belong(0),
								data_type_output=DataType(0),
								missing_values=missing_values_imputeByFixValue_INV_condition, 
								axis_param=0, field_in='ACADEMIC_INTEREST_2', field_out='ACADEMIC_INTEREST_2', origin_function="Missing Value"):
		print('INVARIANT Missing Value(ACADEMIC_INTEREST_2) MISSING FixValue:Unknown VALIDATED')
	else:
		print('INVARIANT Missing Value(ACADEMIC_INTEREST_2) MISSING FixValue:Unknown NOT VALIDATED')
	
	
	
	missing_values_imputeByFixValue_INV_condition=[]
	if contract_invariants.check_inv_special_value_fix_value(data_dictionary_in=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df,
								data_dictionary_out=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__output_dataDictionary_df,
								special_type_input=SpecialType(0),
								fix_value_output='Unknown',
								belong_op_in=Belong(0),
								belong_op_out=Belong(0),
								data_type_output=DataType(0),
								missing_values=missing_values_imputeByFixValue_INV_condition, 
								axis_param=0, field_in='ACADEMIC_INTEREST_1', field_out='ACADEMIC_INTEREST_1', origin_function="Missing Value"):
		print('INVARIANT Missing Value(ACADEMIC_INTEREST_1) MISSING FixValue:Unknown VALIDATED')
	else:
		print('INVARIANT Missing Value(ACADEMIC_INTEREST_1) MISSING FixValue:Unknown NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	imputeMissingByMean_avg_income_distance__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_input_dataDictionary.parquet')

	missing_values_imputeByNumericOp_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='avg_income', 
									missing_values=missing_values_imputeByNumericOp_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Missing Value"):
		print('PRECONDITION Missing Value(avg_income) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Missing Value(avg_income) MissingValues:[] NOT VALIDATED')
	
	missing_values_imputeByNumericOp_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='distance', 
									missing_values=missing_values_imputeByNumericOp_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('PRECONDITION Missing Value(distance) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Missing Value(distance) MissingValues:[] NOT VALIDATED')
	
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
	
	missing_values_imputeByNumericOp_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByMean_avg_income_distance__output_dataDictionary_df, field='avg_income', 
									missing_values=missing_values_imputeByNumericOp_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION Missing Value(avg_income) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Missing Value(avg_income) MissingValues:[] NOT VALIDATED')
	
	missing_values_imputeByNumericOp_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByMean_avg_income_distance__output_dataDictionary_df, field='distance', 
									missing_values=missing_values_imputeByNumericOp_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION Missing Value(distance) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Missing Value(distance) MissingValues:[] NOT VALIDATED')
	
	missing_values_imputeByNumericOp_INV_condition=[]
	if contract_invariants.check_inv_special_value_num_op(data_dictionary_in=imputeMissingByMean_avg_income_distance__input_dataDictionary_df,
											data_dictionary_out=imputeMissingByMean_avg_income_distance__output_dataDictionary_df,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											special_type_input=SpecialType(0),
											num_op_output=Operation(1),
											missing_values=missing_values_imputeByNumericOp_INV_condition, axis_param=0, field_in='avg_income', field_out='avg_income', origin_function="Missing Value"):
		print('INVARIANT Missing Value(avg_income) MISSING_to_Mean VALIDATED')
	else:
		print('INVARIANT Missing Value(avg_income) MISSING_to_Mean NOT VALIDATED')
	
	
	
	missing_values_imputeByNumericOp_INV_condition=[]
	if contract_invariants.check_inv_special_value_num_op(data_dictionary_in=imputeMissingByMean_avg_income_distance__input_dataDictionary_df,
											data_dictionary_out=imputeMissingByMean_avg_income_distance__output_dataDictionary_df,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											special_type_input=SpecialType(0),
											num_op_output=Operation(1),
											missing_values=missing_values_imputeByNumericOp_INV_condition, axis_param=0, field_in='distance', field_out='distance', origin_function="Missing Value"):
		print('INVARIANT Missing Value(distance) MISSING_to_Mean VALIDATED')
	else:
		print('INVARIANT Missing Value(distance) MISSING_to_Mean NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_input_dataDictionary.parquet')

	missing_values_imputeByNumericOp_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df, field='satscore', 
									missing_values=missing_values_imputeByNumericOp_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Missing Value"):
		print('PRECONDITION Missing Value(satscore) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Missing Value(satscore) MissingValues:[] NOT VALIDATED')
	
	imputeMissingByLinearInterpolation_satscore__input_dataDictionary_transformed=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
	missing_values_list=[]
	
	imputeMissingByLinearInterpolation_satscore__input_dataDictionary_transformed=data_transformations.transform_special_value_num_op(data_dictionary=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_transformed,
																  special_type_input=SpecialType(0), num_op_output=Operation(0),
																  missing_values=missing_values_list,		
																  axis_param=0, field_in = 'satscore', field_out = 'satscore')
	
	imputeMissingByLinearInterpolation_satscore__output_dataDictionary_df=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_transformed
	imputeMissingByLinearInterpolation_satscore__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
	imputeMissingByLinearInterpolation_satscore__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
	
	missing_values_imputeByNumericOp_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByLinearInterpolation_satscore__output_dataDictionary_df, field='satscore', 
									missing_values=missing_values_imputeByNumericOp_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION Missing Value(satscore) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Missing Value(satscore) MissingValues:[] NOT VALIDATED')
	
	missing_values_imputeByNumericOp_INV_condition=[]
	if contract_invariants.check_inv_special_value_num_op(data_dictionary_in=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df,
											data_dictionary_out=imputeMissingByLinearInterpolation_satscore__output_dataDictionary_df,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											special_type_input=SpecialType(0),
											num_op_output=Operation(0),
											missing_values=missing_values_imputeByNumericOp_INV_condition, axis_param=0, field_in='satscore', field_out='satscore', origin_function="Missing Value"):
		print('INVARIANT Missing Value(satscore) MISSING_to_Interpolation VALIDATED')
	else:
		print('INVARIANT Missing Value(satscore) MISSING_to_Interpolation NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	rowFilterRange_init_span__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=1000.0, data_dictionary=rowFilterRange_init_span__input_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(0), field='init_span', origin_function="Row Filter"):
		print('PRECONDITION Row Filter(init_span) Interval:(-1000.0, 1000.0) VALIDATED')
	else:
		print('PRECONDITION Row Filter(init_span) Interval:(-1000.0, 1000.0) NOT VALIDATED')
	
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
	
	if contract_pre_post.check_fix_value_range(value=-216, is_substring=False, data_dictionary=rowFilterRange_init_span__output_dataDictionary_df, belong_op=Belong(1), field='init_span', numDecimals=None,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('POSTCONDITION Row Filter(init_span) FixValue:-216 VALIDATED')
	else:
		print('POSTCONDITION Row Filter(init_span) FixValue:-216 NOT VALIDATED')
	
	
	
	columns_list_rowFilterRange_init_span__INV_condition=['init_span']
	left_margin_list_rowFilterRange_init_span__INV_condition=[-10000.0]
	right_margin_list_rowFilterRange_init_span__INV_condition=[0.0]
	closure_type_list_rowFilterRange_init_span__INV_condition=[Closure.closedClosed]
	
	if contract_invariants.check_inv_filter_rows_range(data_dictionary_in=rowFilterRange_init_span__input_dataDictionary_df,
											data_dictionary_out=rowFilterRange_init_span__output_dataDictionary_df,
											columns=columns_list_rowFilterRange_init_span__INV_condition,
											left_margin_list=left_margin_list_rowFilterRange_init_span__INV_condition, right_margin_list=right_margin_list_rowFilterRange_init_span__INV_condition,
											closure_type_list=closure_type_list_rowFilterRange_init_span__INV_condition,
											filter_type=FilterType.EXCLUDE, origin_function="Row Filter"):
		print('INVARIANT Row Filter(init_span) FilterType:EXCLUDE LeftMarginList:[-10000.0] RightMarginList:[0.0] ClosureTypeList:[Closure.closedClosed] VALIDATED')
	else:
		print('INVARIANT Row Filter(init_span) FilterType:EXCLUDE LeftMarginList:[-10000.0] RightMarginList:[0.0] ClosureTypeList:[Closure.closedClosed] NOT VALIDATED')
	
	
	#-----------------New DataProcessing-----------------
	columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/rowFilter_output_dataDictionary.parquet')

	field_list_columnFilter_PRE_field_range=['TRAVEL_INIT_CNTCTS', 'REFERRAL_CNTCTS', 'telecq', 'stuemail', 'interest', 'CONTACT_CODE1']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_PRE_field_range,
								data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df,
								belong_op=Belong(0), origin_function="Column Filter"):
		print('PRECONDITION Column Filter(TRAVEL_INIT_CNTCTS, REFERRAL_CNTCTS, telecq, stuemail, interest, CONTACT_CODE1) VALIDATED')
	else:
		print('PRECONDITION Column Filter(TRAVEL_INIT_CNTCTS, REFERRAL_CNTCTS, telecq, stuemail, interest, CONTACT_CODE1) NOT VALIDATED')
	
	
	columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_transformed=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df.copy()
	field_list_columnFilter_param_field=['TRAVEL_INIT_CNTCTS', 'REFERRAL_CNTCTS', 'telecq', 'interest', 'stuemail', 'CONTACT_CODE1']
	
	columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_transformed=data_transformations.transform_filter_columns(data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_transformed,
																	columns=field_list_columnFilter_param_field, belong_op=Belong.BELONG)
	
	columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__output_dataDictionary_df=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_transformed
	columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__output_dataDictionary_df.to_parquet('/wf_validation_contracts/data/columnFilter_output_dataDictionary.parquet')
	columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/columnFilter_output_dataDictionary.parquet')
	
	field_list_columnFilter_POST_field_range=['TRAVEL_INIT_CNTCTS', 'REFERRAL_CNTCTS', 'telecq', 'stuemail', 'interest', 'CONTACT_CODE1']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_POST_field_range,
								data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__output_dataDictionary_df,
								belong_op=Belong(1), origin_function="Column Filter"):
		print('POSTCONDITION Column Filter(TRAVEL_INIT_CNTCTS, REFERRAL_CNTCTS, telecq, stuemail, interest, CONTACT_CODE1) VALIDATED')
	else:
		print('POSTCONDITION Column Filter(TRAVEL_INIT_CNTCTS, REFERRAL_CNTCTS, telecq, stuemail, interest, CONTACT_CODE1) NOT VALIDATED')
	
	
	columns_list_columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__INV_condition = ['TRAVEL_INIT_CNTCTS', 'REFERRAL_CNTCTS', 'telecq', 'stuemail', 'interest', 'CONTACT_CODE1']
	
	if contract_invariants.check_inv_filter_columns(data_dictionary_in=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df,
							data_dictionary_out=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__output_dataDictionary_df,
							columns=columns_list_columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__INV_condition,
							belong_op=Belong(0), origin_function="Column Filter"):
		print('INVARIANT Column Filter(TRAVEL_INIT_CNTCTS, REFERRAL_CNTCTS, telecq, stuemail, interest, CONTACT_CODE1) VALIDATED')
	else:
		print('INVARIANT Column Filter(TRAVEL_INIT_CNTCTS, REFERRAL_CNTCTS, telecq, stuemail, interest, CONTACT_CODE1) NOT VALIDATED')
	
	
	
	
	
	#-----------------New DataProcessing-----------------
	mapping_TERRITORY__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/columnFilter_output_dataDictionary.parquet')

	if contract_pre_post.check_fix_value_range(value='A', is_substring=False, data_dictionary=mapping_TERRITORY__input_dataDictionary_df, belong_op=Belong(0), field='TERRITORY', numDecimals=None,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('PRECONDITION Rule Engine(TERRITORY) FixValue:A VALIDATED')
	else:
		print('PRECONDITION Rule Engine(TERRITORY) FixValue:A NOT VALIDATED')
	if contract_pre_post.check_fix_value_range(value='N', is_substring=False, data_dictionary=mapping_TERRITORY__input_dataDictionary_df, belong_op=Belong(0), field='TERRITORY', numDecimals=None,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('PRECONDITION Rule Engine(TERRITORY) FixValue:N VALIDATED')
	else:
		print('PRECONDITION Rule Engine(TERRITORY) FixValue:N NOT VALIDATED')
	
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
	
	if contract_pre_post.check_fix_value_range(value='A', is_substring=False, data_dictionary=mapping_TERRITORY__output_dataDictionary_df, belong_op=Belong(1), field='TERRITORY', numDecimals=None,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('POSTCONDITION Rule Engine(TERRITORY) FixValue:A VALIDATED')
	else:
		print('POSTCONDITION Rule Engine(TERRITORY) FixValue:A NOT VALIDATED')
	if contract_pre_post.check_fix_value_range(value='N', is_substring=False, data_dictionary=mapping_TERRITORY__output_dataDictionary_df, belong_op=Belong(1), field='TERRITORY', numDecimals=None,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('POSTCONDITION Rule Engine(TERRITORY) FixValue:N VALIDATED')
	else:
		print('POSTCONDITION Rule Engine(TERRITORY) FixValue:N NOT VALIDATED')
	
	
	input_values_list_mapping_INV_condition=['A', 'N']
	output_values_list_mapping_INV_condition=['0', '0']
	
	data_type_input_list_mapping_INV_condition=[DataType(0), DataType(0)]
	data_type_output_list_mapping_INV_condition=[DataType(0), DataType(0)]
	
	is_substring_list_mapping_INV_condition=[False, False]
	
	if contract_invariants.check_inv_fix_value_fix_value(data_dictionary_in=mapping_TERRITORY__input_dataDictionary_df,
											data_dictionary_out=mapping_TERRITORY__output_dataDictionary_df,
											input_values_list=input_values_list_mapping_INV_condition, 
											output_values_list=output_values_list_mapping_INV_condition,
											is_substring_list=is_substring_list_mapping_INV_condition,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											data_type_input_list=data_type_input_list_mapping_INV_condition,
											data_type_output_list=data_type_output_list_mapping_INV_condition,
											field_in='TERRITORY', field_out='TERRITORY', origin_function="Rule Engine"):
		print('INVARIANT Rule Engine(TERRITORY) InputMapValues:A, N OutputMapValues:0, 0 VALIDATED')
	else:
		print('INVARIANT Rule Engine(TERRITORY) InputMapValues:A, N OutputMapValues:0, 0 NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	mapping_Instate__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/ruleEngine_territory_output_dataDictionary.parquet')

	if contract_pre_post.check_fix_value_range(value='Y', is_substring=False, data_dictionary=mapping_Instate__input_dataDictionary_df, belong_op=Belong(0), field='Instate', numDecimals=None,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('PRECONDITION Rule Engine(Instate) FixValue:Y VALIDATED')
	else:
		print('PRECONDITION Rule Engine(Instate) FixValue:Y NOT VALIDATED')
	if contract_pre_post.check_fix_value_range(value='N', is_substring=False, data_dictionary=mapping_Instate__input_dataDictionary_df, belong_op=Belong(0), field='Instate', numDecimals=None,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('PRECONDITION Rule Engine(Instate) FixValue:N VALIDATED')
	else:
		print('PRECONDITION Rule Engine(Instate) FixValue:N NOT VALIDATED')
	
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
	
	if contract_pre_post.check_fix_value_range(value='Y', is_substring=False, data_dictionary=mapping_Instate__output_dataDictionary_df, belong_op=Belong(1), field='Instate', numDecimals=None,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('POSTCONDITION Rule Engine(Instate) FixValue:Y VALIDATED')
	else:
		print('POSTCONDITION Rule Engine(Instate) FixValue:Y NOT VALIDATED')
	if contract_pre_post.check_fix_value_range(value='N', is_substring=False, data_dictionary=mapping_Instate__output_dataDictionary_df, belong_op=Belong(1), field='Instate', numDecimals=None,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('POSTCONDITION Rule Engine(Instate) FixValue:N VALIDATED')
	else:
		print('POSTCONDITION Rule Engine(Instate) FixValue:N NOT VALIDATED')
	
	
	input_values_list_mapping_INV_condition=['Y', 'N']
	output_values_list_mapping_INV_condition=['1', '0']
	
	data_type_input_list_mapping_INV_condition=[DataType(0), DataType(0)]
	data_type_output_list_mapping_INV_condition=[DataType(0), DataType(0)]
	
	is_substring_list_mapping_INV_condition=[False, False]
	
	if contract_invariants.check_inv_fix_value_fix_value(data_dictionary_in=mapping_Instate__input_dataDictionary_df,
											data_dictionary_out=mapping_Instate__output_dataDictionary_df,
											input_values_list=input_values_list_mapping_INV_condition, 
											output_values_list=output_values_list_mapping_INV_condition,
											is_substring_list=is_substring_list_mapping_INV_condition,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											data_type_input_list=data_type_input_list_mapping_INV_condition,
											data_type_output_list=data_type_output_list_mapping_INV_condition,
											field_in='Instate', field_out='Instate', origin_function="Rule Engine"):
		print('INVARIANT Rule Engine(Instate) InputMapValues:Y, N OutputMapValues:1, 0 VALIDATED')
	else:
		print('INVARIANT Rule Engine(Instate) InputMapValues:Y, N OutputMapValues:1, 0 NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	stringToNumber_TERRITORY_Instate__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/ruleEngine_instate_output_dataDictionary.parquet')

	if contract_pre_post.check_field_type(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df,
	                                	  field='TERRITORY', field_type=DataType(0), origin_function="String To Number"):
		print('PRECONDITION String To Number(TERRITORY) Type:String VALIDATED')
	else:
		print('PRECONDITION String To Number(TERRITORY) Type:String NOT VALIDATED')
	
	if contract_pre_post.check_field_type(data_dictionary=stringToNumber_TERRITORY_Instate__input_dataDictionary_df,
	                                	  field='Instate', field_type=DataType(0), origin_function="String To Number"):
		print('PRECONDITION String To Number(Instate) Type:String VALIDATED')
	else:
		print('PRECONDITION String To Number(Instate) Type:String NOT VALIDATED')
	
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
	
	if contract_pre_post.check_field_type(data_dictionary=stringToNumber_TERRITORY_Instate__output_dataDictionary_df,
	                                	  field='TERRITORY', field_type=DataType(2), origin_function="String To Number"):
		print('POSTCONDITION String To Number(TERRITORY) Type:Integer VALIDATED')
	else:
		print('POSTCONDITION String To Number(TERRITORY) Type:Integer NOT VALIDATED')
	
	if contract_pre_post.check_field_type(data_dictionary=stringToNumber_TERRITORY_Instate__output_dataDictionary_df,
	                                	  field='Instate', field_type=DataType(2), origin_function="String To Number"):
		print('POSTCONDITION String To Number(Instate) Type:Integer VALIDATED')
	else:
		print('POSTCONDITION String To Number(Instate) Type:Integer NOT VALIDATED')
	
	if contract_invariants.check_inv_missing_value_missing_value(data_dictionary_in=stringToNumber_TERRITORY_Instate__input_dataDictionary_df,
											data_dictionary_out=stringToNumber_TERRITORY_Instate__output_dataDictionary_df,
											belong_op_in=Belong(1), belong_op_out=Belong(1),
											field_in = 'TERRITORY', field_out='TERRITORY', origin_function="String To Number"):
		print('INVARIANT String To Number(TERRITORY) BelongOpIn:NotBelong BelongOpOut:NotBelong VALIDATED')
	else:
		print('INVARIANT String To Number(TERRITORY) BelongOpIn:NotBelong BelongOpOut:NotBelong NOT VALIDATED')
	
	
	
	if contract_invariants.check_inv_cast_type(data_dictionary_in=stringToNumber_TERRITORY_Instate__input_dataDictionary_df,
								data_dictionary_out=stringToNumber_TERRITORY_Instate__output_dataDictionary_df,
								cast_type_in=DataType(0),
								cast_type_out=DataType(2),
								belong_op_out=Belong(0),
								field_in='TERRITORY', field_out='TERRITORY', origin_function="String To Number"):
		print('INVARIANT String To Number(TERRITORY) InputType:String OutputType:Integer VALIDATED')
	else:
		print('INVARIANT String To Number(TERRITORY) InputType:String OutputType:Integer NOT VALIDATED')
	
	
	
	if contract_invariants.check_inv_missing_value_missing_value(data_dictionary_in=stringToNumber_TERRITORY_Instate__input_dataDictionary_df,
											data_dictionary_out=stringToNumber_TERRITORY_Instate__output_dataDictionary_df,
											belong_op_in=Belong(1), belong_op_out=Belong(1),
											field_in = 'Instate', field_out='Instate', origin_function="String To Number"):
		print('INVARIANT String To Number(Instate) BelongOpIn:NotBelong BelongOpOut:NotBelong VALIDATED')
	else:
		print('INVARIANT String To Number(Instate) BelongOpIn:NotBelong BelongOpOut:NotBelong NOT VALIDATED')
	
	
	
	if contract_invariants.check_inv_cast_type(data_dictionary_in=stringToNumber_TERRITORY_Instate__input_dataDictionary_df,
								data_dictionary_out=stringToNumber_TERRITORY_Instate__output_dataDictionary_df,
								cast_type_in=DataType(0),
								cast_type_out=DataType(2),
								belong_op_out=Belong(0),
								field_in='Instate', field_out='Instate', origin_function="String To Number"):
		print('INVARIANT String To Number(Instate) InputType:String OutputType:Integer VALIDATED')
	else:
		print('INVARIANT String To Number(Instate) InputType:String OutputType:Integer NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/stringToNumber_output_dataDictionary.parquet')

	if contract_pre_post.check_outliers(belong_op=Belong(0), data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='avg_income', 
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Numeric Outliers"):
		print('PRECONDITION Numeric Outliers(avg_income) VALIDATED')
	else:
		print('PRECONDITION Numeric Outliers(avg_income) NOT VALIDATED')
	
	if contract_pre_post.check_outliers(belong_op=Belong(0), data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='distance', 
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Numeric Outliers"):
		print('PRECONDITION Numeric Outliers(distance) VALIDATED')
	else:
		print('PRECONDITION Numeric Outliers(distance) NOT VALIDATED')
	
	if contract_pre_post.check_outliers(belong_op=Belong(0), data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='Instate', 
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Numeric Outliers"):
		print('PRECONDITION Numeric Outliers(Instate) VALIDATED')
	else:
		print('PRECONDITION Numeric Outliers(Instate) NOT VALIDATED')
	
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
	
	if contract_pre_post.check_outliers(belong_op=Belong(1), data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df, field='avg_income', 
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Numeric Outliers"):
		print('POSTCONDITION Numeric Outliers(avg_income) VALIDATED')
	else:
		print('POSTCONDITION Numeric Outliers(avg_income) NOT VALIDATED')
	
	if contract_pre_post.check_outliers(belong_op=Belong(1), data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df, field='distance', 
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Numeric Outliers"):
		print('POSTCONDITION Numeric Outliers(distance) VALIDATED')
	else:
		print('POSTCONDITION Numeric Outliers(distance) NOT VALIDATED')
	
	if contract_pre_post.check_outliers(belong_op=Belong(1), data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df, field='Instate', 
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Numeric Outliers"):
		print('POSTCONDITION Numeric Outliers(Instate) VALIDATED')
	else:
		print('POSTCONDITION Numeric Outliers(Instate) NOT VALIDATED')
	
	if contract_invariants.check_inv_special_value_num_op(data_dictionary_in=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df,
											data_dictionary_out=imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											special_type_input=SpecialType(2),
											num_op_output=Operation(3),
											missing_values=None, axis_param=0, field_in='avg_income', field_out='avg_income', origin_function="Numeric Outliers"):
		print('INVARIANT Numeric Outliers(avg_income) OUTLIER_to_Closest VALIDATED')
	else:
		print('INVARIANT Numeric Outliers(avg_income) OUTLIER_to_Closest NOT VALIDATED')
	
	
	
	if contract_invariants.check_inv_special_value_num_op(data_dictionary_in=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df,
											data_dictionary_out=imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											special_type_input=SpecialType(2),
											num_op_output=Operation(3),
											missing_values=None, axis_param=0, field_in='distance', field_out='distance', origin_function="Numeric Outliers"):
		print('INVARIANT Numeric Outliers(distance) OUTLIER_to_Closest VALIDATED')
	else:
		print('INVARIANT Numeric Outliers(distance) OUTLIER_to_Closest NOT VALIDATED')
	
	
	
	if contract_invariants.check_inv_special_value_num_op(data_dictionary_in=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df,
											data_dictionary_out=imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											special_type_input=SpecialType(2),
											num_op_output=Operation(3),
											missing_values=None, axis_param=0, field_in='Instate', field_out='Instate', origin_function="Numeric Outliers"):
		print('INVARIANT Numeric Outliers(Instate) OUTLIER_to_Closest VALIDATED')
	else:
		print('INVARIANT Numeric Outliers(Instate) OUTLIER_to_Closest NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericOutliers_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=1000.0, data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(0), field='TOTAL_CONTACTS', origin_function="Numeric Binner"):
		print('PRECONDITION Numeric Binner(TOTAL_CONTACTS) Interval:(-1000.0, 1000.0) VALIDATED')
	else:
		print('PRECONDITION Numeric Binner(TOTAL_CONTACTS) Interval:(-1000.0, 1000.0) NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=1000.0, data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(0), field='SELF_INIT_CNTCTS', origin_function="Numeric Binner"):
		print('PRECONDITION Numeric Binner(SELF_INIT_CNTCTS) Interval:(-1000.0, 1000.0) VALIDATED')
	else:
		print('PRECONDITION Numeric Binner(SELF_INIT_CNTCTS) Interval:(-1000.0, 1000.0) NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=1000.0, data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(0), field='SOLICITED_CNTCTS', origin_function="Numeric Binner"):
		print('PRECONDITION Numeric Binner(SOLICITED_CNTCTS) Interval:(-1000.0, 1000.0) VALIDATED')
	else:
		print('PRECONDITION Numeric Binner(SOLICITED_CNTCTS) Interval:(-1000.0, 1000.0) NOT VALIDATED')
	
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
	
	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=1000.0, data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='TOTAL_CONTACTS_binned', origin_function="Numeric Binner"):
		print('POSTCONDITION Numeric Binner(TOTAL_CONTACTS_binned) Interval:(-1000.0, 1000.0) VALIDATED')
	else:
		print('POSTCONDITION Numeric Binner(TOTAL_CONTACTS_binned) Interval:(-1000.0, 1000.0) NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=1000.0, data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='SELF_INIT_CNTCTS_binned', origin_function="Numeric Binner"):
		print('POSTCONDITION Numeric Binner(SELF_INIT_CNTCTS_binned) Interval:(-1000.0, 1000.0) VALIDATED')
	else:
		print('POSTCONDITION Numeric Binner(SELF_INIT_CNTCTS_binned) Interval:(-1000.0, 1000.0) NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=1000.0, data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='SOLICITED_CNTCTS_binned', origin_function="Numeric Binner"):
		print('POSTCONDITION Numeric Binner(SOLICITED_CNTCTS_binned) Interval:(-1000.0, 1000.0) VALIDATED')
	else:
		print('POSTCONDITION Numeric Binner(SOLICITED_CNTCTS_binned) Interval:(-1000.0, 1000.0) NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=-1000.0, right_margin=1.0,
											closure_type=Closure(0),
											fix_value_output='Low',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TOTAL_CONTACTS', field_out='TOTAL_CONTACTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(TOTAL_CONTACTS) Interval:(-1000.0, 1.0) FixValue:Low VALIDATED')
	else:
		print('INVARIANT Numeric Binner(TOTAL_CONTACTS) Interval:(-1000.0, 1.0) FixValue:Low NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=1.0, right_margin=4.0,
											closure_type=Closure(2),
											fix_value_output='Moderate',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TOTAL_CONTACTS', field_out='TOTAL_CONTACTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(TOTAL_CONTACTS) Interval:[1.0, 4.0) FixValue:Moderate VALIDATED')
	else:
		print('INVARIANT Numeric Binner(TOTAL_CONTACTS) Interval:[1.0, 4.0) FixValue:Moderate NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=4.0, right_margin=1000.0,
											closure_type=Closure(2),
											fix_value_output='High',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TOTAL_CONTACTS', field_out='TOTAL_CONTACTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(TOTAL_CONTACTS) Interval:[4.0, 1000.0) FixValue:High VALIDATED')
	else:
		print('INVARIANT Numeric Binner(TOTAL_CONTACTS) Interval:[4.0, 1000.0) FixValue:High NOT VALIDATED')
	
	
	
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=-1000.0, right_margin=1.0,
											closure_type=Closure(0),
											fix_value_output='Low',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='SELF_INIT_CNTCTS', field_out='SELF_INIT_CNTCTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(SELF_INIT_CNTCTS) Interval:(-1000.0, 1.0) FixValue:Low VALIDATED')
	else:
		print('INVARIANT Numeric Binner(SELF_INIT_CNTCTS) Interval:(-1000.0, 1.0) FixValue:Low NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=1.0, right_margin=4.0,
											closure_type=Closure(2),
											fix_value_output='Moderate',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='SELF_INIT_CNTCTS', field_out='SELF_INIT_CNTCTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(SELF_INIT_CNTCTS) Interval:[1.0, 4.0) FixValue:Moderate VALIDATED')
	else:
		print('INVARIANT Numeric Binner(SELF_INIT_CNTCTS) Interval:[1.0, 4.0) FixValue:Moderate NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=4.0, right_margin=1000.0,
											closure_type=Closure(2),
											fix_value_output='High',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='SELF_INIT_CNTCTS', field_out='SELF_INIT_CNTCTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(SELF_INIT_CNTCTS) Interval:[4.0, 1000.0) FixValue:High VALIDATED')
	else:
		print('INVARIANT Numeric Binner(SELF_INIT_CNTCTS) Interval:[4.0, 1000.0) FixValue:High NOT VALIDATED')
	
	
	
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=-1000.0, right_margin=1.0,
											closure_type=Closure(0),
											fix_value_output='Low',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='SOLICITED_CNTCTS', field_out='SOLICITED_CNTCTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(SOLICITED_CNTCTS) Interval:(-1000.0, 1.0) FixValue:Low VALIDATED')
	else:
		print('INVARIANT Numeric Binner(SOLICITED_CNTCTS) Interval:(-1000.0, 1.0) FixValue:Low NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=1.0, right_margin=4.0,
											closure_type=Closure(2),
											fix_value_output='Moderate',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='SOLICITED_CNTCTS', field_out='SOLICITED_CNTCTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(SOLICITED_CNTCTS) Interval:[1.0, 4.0) FixValue:Moderate VALIDATED')
	else:
		print('INVARIANT Numeric Binner(SOLICITED_CNTCTS) Interval:[1.0, 4.0) FixValue:Moderate NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=4.0, right_margin=1000.0,
											closure_type=Closure(2),
											fix_value_output='High',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='SOLICITED_CNTCTS', field_out='SOLICITED_CNTCTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(SOLICITED_CNTCTS) Interval:[4.0, 1000.0) FixValue:High VALIDATED')
	else:
		print('INVARIANT Numeric Binner(SOLICITED_CNTCTS) Interval:[4.0, 1000.0) FixValue:High NOT VALIDATED')
	
	
	
	
	#-----------------New DataProcessing-----------------
	binner_TERRITORY__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericOutliers_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_TERRITORY__input_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='TERRITORY', origin_function="Numeric Binner"):
		print('PRECONDITION Numeric Binner(TERRITORY) Interval:[0.0, 1000.0] VALIDATED')
	else:
		print('PRECONDITION Numeric Binner(TERRITORY) Interval:[0.0, 1000.0] NOT VALIDATED')
	
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
	
	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_TERRITORY__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='TERRITORY_binned', origin_function="Numeric Binner"):
		print('POSTCONDITION Numeric Binner(TERRITORY_binned) Interval:(0.0, 1000.0) VALIDATED')
	else:
		print('POSTCONDITION Numeric Binner(TERRITORY_binned) Interval:(0.0, 1000.0) NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TERRITORY__input_dataDictionary_df,
											data_dictionary_out=binner_TERRITORY__output_dataDictionary_df,
											left_margin=-1000.0, right_margin=1.0,
											closure_type=Closure(0),
											fix_value_output='Unknown',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TERRITORY', field_out='TERRITORY_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(TERRITORY) Interval:(-1000.0, 1.0) FixValue:Unknown VALIDATED')
	else:
		print('INVARIANT Numeric Binner(TERRITORY) Interval:(-1000.0, 1.0) FixValue:Unknown NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TERRITORY__input_dataDictionary_df,
											data_dictionary_out=binner_TERRITORY__output_dataDictionary_df,
											left_margin=1.0, right_margin=3.0,
											closure_type=Closure(2),
											fix_value_output='Zone 1',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TERRITORY', field_out='TERRITORY_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(TERRITORY) Interval:[1.0, 3.0) FixValue:Zone 1 VALIDATED')
	else:
		print('INVARIANT Numeric Binner(TERRITORY) Interval:[1.0, 3.0) FixValue:Zone 1 NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TERRITORY__input_dataDictionary_df,
											data_dictionary_out=binner_TERRITORY__output_dataDictionary_df,
											left_margin=3.0, right_margin=5.0,
											closure_type=Closure(2),
											fix_value_output='Zone 2',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TERRITORY', field_out='TERRITORY_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(TERRITORY) Interval:[3.0, 5.0) FixValue:Zone 2 VALIDATED')
	else:
		print('INVARIANT Numeric Binner(TERRITORY) Interval:[3.0, 5.0) FixValue:Zone 2 NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TERRITORY__input_dataDictionary_df,
											data_dictionary_out=binner_TERRITORY__output_dataDictionary_df,
											left_margin=5.0, right_margin=7.0,
											closure_type=Closure(2),
											fix_value_output='Zone 3',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TERRITORY', field_out='TERRITORY_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(TERRITORY) Interval:[5.0, 7.0) FixValue:Zone 3 VALIDATED')
	else:
		print('INVARIANT Numeric Binner(TERRITORY) Interval:[5.0, 7.0) FixValue:Zone 3 NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TERRITORY__input_dataDictionary_df,
											data_dictionary_out=binner_TERRITORY__output_dataDictionary_df,
											left_margin=7.0, right_margin=1000.0,
											closure_type=Closure(2),
											fix_value_output='Zone 4',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TERRITORY', field_out='TERRITORY_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(TERRITORY) Interval:[7.0, 1000.0) FixValue:Zone 4 VALIDATED')
	else:
		print('INVARIANT Numeric Binner(TERRITORY) Interval:[7.0, 1000.0) FixValue:Zone 4 NOT VALIDATED')
	
	
	
	
	#-----------------New DataProcessing-----------------
	binner_satscore__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericOutliers_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=2000.0, data_dictionary=binner_satscore__input_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='satscore', origin_function="Numeric Binner"):
		print('PRECONDITION Numeric Binner(satscore) Interval:[-1000.0, 2000.0] VALIDATED')
	else:
		print('PRECONDITION Numeric Binner(satscore) Interval:[-1000.0, 2000.0] NOT VALIDATED')
	
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
	
	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=2000.0, data_dictionary=binner_satscore__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='satscore_binned', origin_function="Numeric Binner"):
		print('POSTCONDITION Numeric Binner(satscore_binned) Interval:(-1000.0, 2000.0) VALIDATED')
	else:
		print('POSTCONDITION Numeric Binner(satscore_binned) Interval:(-1000.0, 2000.0) NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_satscore__input_dataDictionary_df,
											data_dictionary_out=binner_satscore__output_dataDictionary_df,
											left_margin=-1000.0, right_margin=1040.0,
											closure_type=Closure(1),
											fix_value_output='54 Percentile and Under',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='satscore', field_out='satscore_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(satscore) Interval:(-1000.0, 1040.0] FixValue:54 Percentile and Under VALIDATED')
	else:
		print('INVARIANT Numeric Binner(satscore) Interval:(-1000.0, 1040.0] FixValue:54 Percentile and Under NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_satscore__input_dataDictionary_df,
											data_dictionary_out=binner_satscore__output_dataDictionary_df,
											left_margin=1040.0, right_margin=1160.0,
											closure_type=Closure(0),
											fix_value_output='55-75 Percentile',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='satscore', field_out='satscore_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(satscore) Interval:(1040.0, 1160.0) FixValue:55-75 Percentile VALIDATED')
	else:
		print('INVARIANT Numeric Binner(satscore) Interval:(1040.0, 1160.0) FixValue:55-75 Percentile NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_satscore__input_dataDictionary_df,
											data_dictionary_out=binner_satscore__output_dataDictionary_df,
											left_margin=1160.0, right_margin=1340.0,
											closure_type=Closure(2),
											fix_value_output='76-93 Percentile',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='satscore', field_out='satscore_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(satscore) Interval:[1160.0, 1340.0) FixValue:76-93 Percentile VALIDATED')
	else:
		print('INVARIANT Numeric Binner(satscore) Interval:[1160.0, 1340.0) FixValue:76-93 Percentile NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_satscore__input_dataDictionary_df,
											data_dictionary_out=binner_satscore__output_dataDictionary_df,
											left_margin=1340.0, right_margin=2000.0,
											closure_type=Closure(1),
											fix_value_output='94+ percentile',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='satscore', field_out='satscore_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(satscore) Interval:(1340.0, 2000.0] FixValue:94+ percentile VALIDATED')
	else:
		print('INVARIANT Numeric Binner(satscore) Interval:(1340.0, 2000.0] FixValue:94+ percentile NOT VALIDATED')
	
	
	
	
	#-----------------New DataProcessing-----------------
	binner_avg_income__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericOutliers_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=9.0, right_margin=100000.0, data_dictionary=binner_avg_income__input_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='avg_income', origin_function="Numeric Binner"):
		print('PRECONDITION Numeric Binner(avg_income) Interval:[9.0, 100000.0] VALIDATED')
	else:
		print('PRECONDITION Numeric Binner(avg_income) Interval:[9.0, 100000.0] NOT VALIDATED')
	
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
	
	if contract_pre_post.check_interval_range_float(left_margin=9.0, right_margin=100000.0, data_dictionary=binner_avg_income__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='avg_income_binned', origin_function="Numeric Binner"):
		print('POSTCONDITION Numeric Binner(avg_income_binned) Interval:(9.0, 100000.0) VALIDATED')
	else:
		print('POSTCONDITION Numeric Binner(avg_income_binned) Interval:(9.0, 100000.0) NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_avg_income__input_dataDictionary_df,
											data_dictionary_out=binner_avg_income__output_dataDictionary_df,
											left_margin=9.0, right_margin=42830.0,
											closure_type=Closure(0),
											fix_value_output='low',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='avg_income', field_out='avg_income_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(avg_income) Interval:(9.0, 42830.0) FixValue:low VALIDATED')
	else:
		print('INVARIANT Numeric Binner(avg_income) Interval:(9.0, 42830.0) FixValue:low NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_avg_income__input_dataDictionary_df,
											data_dictionary_out=binner_avg_income__output_dataDictionary_df,
											left_margin=42830.0, right_margin=55559.0,
											closure_type=Closure(2),
											fix_value_output='Moderate',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='avg_income', field_out='avg_income_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(avg_income) Interval:[42830.0, 55559.0) FixValue:Moderate VALIDATED')
	else:
		print('INVARIANT Numeric Binner(avg_income) Interval:[42830.0, 55559.0) FixValue:Moderate NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_avg_income__input_dataDictionary_df,
											data_dictionary_out=binner_avg_income__output_dataDictionary_df,
											left_margin=55590.0, right_margin=100000.0,
											closure_type=Closure(2),
											fix_value_output='High',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='avg_income', field_out='avg_income_binned', origin_function="Numeric Binner"):
		print('INVARIANT Numeric Binner(avg_income) Interval:[55590.0, 100000.0) FixValue:High VALIDATED')
	else:
		print('INVARIANT Numeric Binner(avg_income) Interval:[55590.0, 100000.0) FixValue:High NOT VALIDATED')
	
	
	
	














set_logger("dataProcessing")
generateWorkflow()
