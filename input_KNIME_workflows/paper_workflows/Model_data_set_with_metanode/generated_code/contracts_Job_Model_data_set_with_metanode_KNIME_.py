import os

import pandas as pd
import numpy as np
import functions.contract_invariants as contract_invariants
import functions.contract_pre_post as contract_pre_post
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_input_dataDictionary.parquet')
	if os.path.exists('/wf_validation_contracts/data/missing_output_dataDictionary.parquet'):
		imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')

	missing_values_imputeByDerivedValue_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='sex', 
									missing_values=missing_values_imputeByDerivedValue_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Missing Value"):
		print('PRECONDITION imputeMissingByMostFrequent(sex)_PRE_value_range_missing_values_[] VALIDATED')
	else:
		print('PRECONDITION imputeMissingByMostFrequent(sex)_PRE_value_range_missing_values_[] NOT VALIDATED')
	
	missing_values_imputeByDerivedValue_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='IRSCHOOL', 
									missing_values=missing_values_imputeByDerivedValue_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Missing Value"):
		print('PRECONDITION imputeMissingByMostFrequent(IRSCHOOL)_PRE_valueRange_missing_values_[] VALIDATED')
	else:
		print('PRECONDITION imputeMissingByMostFrequent(IRSCHOOL)_PRE_valueRange_missing_values_[] NOT VALIDATED')
	
	missing_values_imputeByDerivedValue_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df, field='ETHNICITY', 
									missing_values=missing_values_imputeByDerivedValue_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Missing Value"):
		print('PRECONDITION imputeMissingByMostFrequent(ETHNICITY)_PRE_valueRange_missing_values_[] VALIDATED')
	else:
		print('PRECONDITION imputeMissingByMostFrequent(ETHNICITY)_PRE_valueRange_missing_values_[] NOT VALIDATED')
	
	missing_values_imputeByDerivedValue_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df, field='sex', 
									missing_values=missing_values_imputeByDerivedValue_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION POST_value_range_impute_sex_columns_missing_values_[] VALIDATED')
	else:
		print('POSTCONDITION POST_value_range_impute_sex_columns_missing_values_[] NOT VALIDATED')
	
	missing_values_imputeByDerivedValue_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df, field='IRSCHOOL', 
									missing_values=missing_values_imputeByDerivedValue_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION POST_value_range_impute_IRSCHOOL_columns_missing_values_[] VALIDATED')
	else:
		print('POSTCONDITION POST_value_range_impute_IRSCHOOL_columns_missing_values_[] NOT VALIDATED')
	
	missing_values_imputeByDerivedValue_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df, field='ETHNICITY', 
									missing_values=missing_values_imputeByDerivedValue_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION POST_value_range_impute_ETHNICITY_columns_missing_values_[] VALIDATED')
	else:
		print('POSTCONDITION POST_value_range_impute_ETHNICITY_columns_missing_values_[] NOT VALIDATED')
	
	missing_values_imputeByDerivedValue_INV_condition=[]
	if contract_invariants.check_inv_special_value_derived_value(data_dictionary_in=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df,
								data_dictionary_out=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df,
								belong_op_in=Belong(0),
								belong_op_out=Belong(0),
								special_type_input=SpecialType(0),
								derived_type_output=DerivedType(0),
								missing_values=missing_values_imputeByDerivedValue_INV_condition, axis_param=0, field_in='sex', field_out='sex', origin_function="Missing Value"):
		print('INVARIANT INV_condition_impute_sex_columns_MISSING_to_MostFrequent VALIDATED')
	else:
		print('INVARIANT INV_condition_impute_sex_columns_MISSING_to_MostFrequent NOT VALIDATED')
	
	
	
	missing_values_imputeByDerivedValue_INV_condition=[]
	if contract_invariants.check_inv_special_value_derived_value(data_dictionary_in=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df,
								data_dictionary_out=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df,
								belong_op_in=Belong(0),
								belong_op_out=Belong(0),
								special_type_input=SpecialType(0),
								derived_type_output=DerivedType(0),
								missing_values=missing_values_imputeByDerivedValue_INV_condition, axis_param=0, field_in='IRSCHOOL', field_out='IRSCHOOL', origin_function="Missing Value"):
		print('INVARIANT INV_condition_impute_IRSCHOOL_columns_MISSING_to_MostFrequent VALIDATED')
	else:
		print('INVARIANT INV_condition_impute_IRSCHOOL_columns_MISSING_to_MostFrequent NOT VALIDATED')
	
	
	
	missing_values_imputeByDerivedValue_INV_condition=[]
	if contract_invariants.check_inv_special_value_derived_value(data_dictionary_in=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary_df,
								data_dictionary_out=imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__output_dataDictionary_df,
								belong_op_in=Belong(0),
								belong_op_out=Belong(0),
								special_type_input=SpecialType(0),
								derived_type_output=DerivedType(0),
								missing_values=missing_values_imputeByDerivedValue_INV_condition, axis_param=0, field_in='ETHNICITY', field_out='ETHNICITY', origin_function="Missing Value"):
		print('INVARIANT INV_condition_impute_ETHNICITY_columns_MISSING_to_MostFrequent VALIDATED')
	else:
		print('INVARIANT INV_condition_impute_ETHNICITY_columns_MISSING_to_MostFrequent NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_contracts/data/missing_output_dataDictionary.parquet'):
		imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')

	missing_values_imputeByFixValue_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_2', 
									missing_values=missing_values_imputeByFixValue_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Missing Value"):
		print('PRECONDITION imputeMissingByFixValue(ACADEMIC_INTEREST_2)_PRE_valueRange_missing_values_[] VALIDATED')
	else:
		print('PRECONDITION imputeMissingByFixValue(ACADEMIC_INTEREST_2)_PRE_valueRange_missing_values_[] NOT VALIDATED')
	
	missing_values_imputeByFixValue_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary_df, field='ACADEMIC_INTEREST_1', 
									missing_values=missing_values_imputeByFixValue_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Missing Value"):
		print('PRECONDITION imputeMissingByFixValue(ACADEMIC_INTEREST_1)_PRE_valueRange_missing_values_[] VALIDATED')
	else:
		print('PRECONDITION imputeMissingByFixValue(ACADEMIC_INTEREST_1)_PRE_valueRange_missing_values_[] NOT VALIDATED')
	
	missing_values_imputeByFixValue_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__output_dataDictionary_df, field='ACADEMIC_INTEREST_2', 
									missing_values=missing_values_imputeByFixValue_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION imputeMissingByFixValue(ACADEMIC_INTEREST_2)_POST_valueRange_missing_values_[] VALIDATED')
	else:
		print('POSTCONDITION imputeMissingByFixValue(ACADEMIC_INTEREST_2)_POST_valueRange_missing_values_[] NOT VALIDATED')
	
	missing_values_imputeByFixValue_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__output_dataDictionary_df, field='ACADEMIC_INTEREST_1', 
									missing_values=missing_values_imputeByFixValue_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION imputeMissingByFixValue(ACADEMIC_INTEREST_1)_POST_valueRange_missing_values_[] VALIDATED')
	else:
		print('POSTCONDITION imputeMissingByFixValue(ACADEMIC_INTEREST_1)_POST_valueRange_missing_values_[] NOT VALIDATED')
	
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
		print('INVARIANT imputeMissingByFixValue(ACADEMIC_INTEREST_2)_INV_condition_MISSING_to_fixValue_Unknown VALIDATED')
	else:
		print('INVARIANT imputeMissingByFixValue(ACADEMIC_INTEREST_2)_INV_condition_MISSING_to_fixValue_Unknown NOT VALIDATED')
	
	
	
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
		print('INVARIANT imputeMissingByFixValue(ACADEMIC_INTEREST_1)_INV_condition_MISSING_to_fixValue_Unknown VALIDATED')
	else:
		print('INVARIANT imputeMissingByFixValue(ACADEMIC_INTEREST_1)_INV_condition_MISSING_to_fixValue_Unknown NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	imputeMissingByMean_avg_income_distance__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_contracts/data/missing_output_dataDictionary.parquet'):
		imputeMissingByMean_avg_income_distance__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')

	missing_values_imputeByNumericOp_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='avg_income', 
									missing_values=missing_values_imputeByNumericOp_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Missing Value"):
		print('PRECONDITION imputeMissingByMean(avg_income)_PRE_valueRange_missing_values_[] VALIDATED')
	else:
		print('PRECONDITION imputeMissingByMean(avg_income)_PRE_valueRange_missing_values_[] NOT VALIDATED')
	
	missing_values_imputeByNumericOp_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByMean_avg_income_distance__input_dataDictionary_df, field='distance', 
									missing_values=missing_values_imputeByNumericOp_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('PRECONDITION imputeMissingByMean(distance)_PRE_valueRange_missing_values_[] VALIDATED')
	else:
		print('PRECONDITION imputeMissingByMean(distance)_PRE_valueRange_missing_values_[] NOT VALIDATED')
	
	missing_values_imputeByNumericOp_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByMean_avg_income_distance__output_dataDictionary_df, field='avg_income', 
									missing_values=missing_values_imputeByNumericOp_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION imputeMissingByMean(avg_income)_POST_valueRange_missing_values_[] VALIDATED')
	else:
		print('POSTCONDITION imputeMissingByMean(avg_income)_POST_valueRange_missing_values_[] NOT VALIDATED')
	
	missing_values_imputeByNumericOp_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByMean_avg_income_distance__output_dataDictionary_df, field='distance', 
									missing_values=missing_values_imputeByNumericOp_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION imputeMissingByMean(distance)_POST_valueRange_missing_values_[] VALIDATED')
	else:
		print('POSTCONDITION imputeMissingByMean(distance)_POST_valueRange_missing_values_[] NOT VALIDATED')
	
	missing_values_imputeByNumericOp_INV_condition=[]
	if contract_invariants.check_inv_special_value_num_op(data_dictionary_in=imputeMissingByMean_avg_income_distance__input_dataDictionary_df,
											data_dictionary_out=imputeMissingByMean_avg_income_distance__output_dataDictionary_df,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											special_type_input=SpecialType(0),
											num_op_output=Operation(1),
											missing_values=missing_values_imputeByNumericOp_INV_condition, axis_param=0, field_in='avg_income', field_out='avg_income', origin_function="Missing Value"):
		print('INVARIANT imputeMissingByMean(avg_income)_INV_condition_MISSING_to_Mean VALIDATED')
	else:
		print('INVARIANT imputeMissingByMean(avg_income)_INV_condition_MISSING_to_Mean NOT VALIDATED')
	
	
	
	missing_values_imputeByNumericOp_INV_condition=[]
	if contract_invariants.check_inv_special_value_num_op(data_dictionary_in=imputeMissingByMean_avg_income_distance__input_dataDictionary_df,
											data_dictionary_out=imputeMissingByMean_avg_income_distance__output_dataDictionary_df,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											special_type_input=SpecialType(0),
											num_op_output=Operation(1),
											missing_values=missing_values_imputeByNumericOp_INV_condition, axis_param=0, field_in='distance', field_out='distance', origin_function="Missing Value"):
		print('INVARIANT imputeMissingByMean(distance)_INV_condition_MISSING_to_Mean VALIDATED')
	else:
		print('INVARIANT imputeMissingByMean(distance)_INV_condition_MISSING_to_Mean NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_contracts/data/missing_output_dataDictionary.parquet'):
		imputeMissingByLinearInterpolation_satscore__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')

	missing_values_imputeByNumericOp_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df, field='satscore', 
									missing_values=missing_values_imputeByNumericOp_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Missing Value"):
		print('PRECONDITION imputeMissingByLinearInterpolation(satscore)_PRE_valueRange_missing_values_[] VALIDATED')
	else:
		print('PRECONDITION imputeMissingByLinearInterpolation(satscore)_PRE_valueRange_missing_values_[] NOT VALIDATED')
	
	missing_values_imputeByNumericOp_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=imputeMissingByLinearInterpolation_satscore__output_dataDictionary_df, field='satscore', 
									missing_values=missing_values_imputeByNumericOp_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Missing Value"):
		print('POSTCONDITION imputeMissingByLinearInterpolation(satscore)_POST_valueRange_missing_values_[] VALIDATED')
	else:
		print('POSTCONDITION imputeMissingByLinearInterpolation(satscore)_POST_valueRange_missing_values_[] NOT VALIDATED')
	
	missing_values_imputeByNumericOp_INV_condition=[]
	if contract_invariants.check_inv_special_value_num_op(data_dictionary_in=imputeMissingByLinearInterpolation_satscore__input_dataDictionary_df,
											data_dictionary_out=imputeMissingByLinearInterpolation_satscore__output_dataDictionary_df,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											special_type_input=SpecialType(0),
											num_op_output=Operation(0),
											missing_values=missing_values_imputeByNumericOp_INV_condition, axis_param=0, field_in='satscore', field_out='satscore', origin_function="Missing Value"):
		print('INVARIANT imputeMissingByLinearInterpolation(satscore)_INV_condition_MISSING_to_Interpolation VALIDATED')
	else:
		print('INVARIANT imputeMissingByLinearInterpolation(satscore)_INV_condition_MISSING_to_Interpolation NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	rowFilterRange_init_span__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_contracts/data/rowFilter_output_dataDictionary.parquet'):
		rowFilterRange_init_span__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/rowFilter_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=rowFilterRange_init_span__input_dataDictionary_df,
	                                	closure_type=Closure(2), belong_op=Belong(1), field='init_span', origin_function="Row Filter"):
		print('PRECONDITION rowFilterRange(init_span)_PRE_valueRange_[0.0, 1000.0) VALIDATED')
	else:
		print('PRECONDITION rowFilterRange(init_span)_PRE_valueRange_[0.0, 1000.0) NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value=-216, data_dictionary=rowFilterRange_init_span__output_dataDictionary_df, belong_op=Belong(1), field='init_span',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('POSTCONDITION rowFilterRange(init_span)_POST_valueRange_-216 VALIDATED')
	else:
		print('POSTCONDITION rowFilterRange(init_span)_POST_valueRange_-216 NOT VALIDATED')
	
	
	
	columns_list_rowFilterRange_init_span__INV_condition=['init_span']
	left_margin_list_rowFilterRange_init_span__INV_condition=[-10000.0]
	right_margin_list_rowFilterRange_init_span__INV_condition=[0.0]
	closure_type_list_rowFilterRange_init_span__INV_condition=[Closure.closedClosed]
	
	if contract_invariants.check_inv_filter_rows_range(data_dictionary_in=rowFilterRange_init_span__input_dataDictionary_df,
											data_dictionary_out=rowFilterRange_init_span__output_dataDictionary_df,
											columns=columns_list_rowFilterRange_init_span__INV_condition,
											left_margin_list=left_margin_list_rowFilterRange_init_span__INV_condition, right_margin_list=right_margin_list_rowFilterRange_init_span__INV_condition,
											closure_type_list=closure_type_list_rowFilterRange_init_span__INV_condition,
											filter_type=FilterType.EXCLUDE):
		print('INVARIANT rowFilterRange(init_span)_INV_condition VALIDATED')
	else:
		print('INVARIANT rowFilterRange(init_span)_INV_condition NOT VALIDATED')
	
	
	#-----------------New DataProcessing-----------------
	columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/rowFilter_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_contracts/data/columnFilter_output_dataDictionary.parquet'):
		columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/columnFilter_output_dataDictionary.parquet')

	field_list_columnFilter_PRE_field_range=['TRAVEL_INIT_CNTCTS', 'REFERRAL_CNTCTS', 'telecq', 'stuemail', 'interest', 'CONTACT_CODE1']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_PRE_field_range,
								data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df,
								belong_op=Belong(0), origin_function="Column Filter"):
		print('PRECONDITION columnFilter(TRAVEL_INIT_CNTCTS, REFERRAL_CNCTS, telecq, interest, stuemail, CONTACT_CODE1)_PRE_fieldRange VALIDATED')
	else:
		print('PRECONDITION columnFilter(TRAVEL_INIT_CNTCTS, REFERRAL_CNCTS, telecq, interest, stuemail, CONTACT_CODE1)_PRE_fieldRange NOT VALIDATED')
	
	
	field_list_columnFilter_POST_field_range=['TRAVEL_INIT_CNTCTS', 'REFERRAL_CNTCTS', 'telecq', 'stuemail', 'interest', 'CONTACT_CODE1']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_POST_field_range,
								data_dictionary=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__output_dataDictionary_df,
								belong_op=Belong(1), origin_function="Column Filter"):
		print('POSTCONDITION columnFilter(TRAVEL_INIT_CNTCTS, REFERRAL_CNCTS, telecq, interest, stuemail, CONTACT_CODE1)_POST_fieldRange VALIDATED')
	else:
		print('POSTCONDITION columnFilter(TRAVEL_INIT_CNTCTS, REFERRAL_CNCTS, telecq, interest, stuemail, CONTACT_CODE1)_POST_fieldRange NOT VALIDATED')
	
	
	columns_list_columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__INV_condition = ['TRAVEL_INIT_CNTCTS', 'REFERRAL_CNTCTS', 'telecq', 'stuemail', 'interest', 'CONTACT_CODE1']
	
	if contract_invariants.check_inv_filter_columns(data_dictionary_in=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary_df,
							data_dictionary_out=columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__output_dataDictionary_df,
							columns=columns_list_columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__INV_condition,
							belong_op=Belong(0), origin_function="Column Filter"):
		print('INVARIANT columnFilter(TRAVEL_INIT_CNTCTS, REFERRAL_CNCTS, telecq, interest, stuemail, CONTACT_CODE1)_INV_condition VALIDATED')
	else:
		print('INVARIANT columnFilter(TRAVEL_INIT_CNTCTS, REFERRAL_CNCTS, telecq, interest, stuemail, CONTACT_CODE1)_INV_condition NOT VALIDATED')
	
	
	
	
	
	#-----------------New DataProcessing-----------------
	mapping_TERRITORY__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/columnFilter_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_contracts/data/ruleEngine_territory_output_dataDictionary.parquet'):
		mapping_TERRITORY__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/ruleEngine_territory_output_dataDictionary.parquet')

	if contract_pre_post.check_fix_value_range(value='A', data_dictionary=mapping_TERRITORY__input_dataDictionary_df, belong_op=Belong(0), field='TERRITORY',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('PRECONDITION mapping(TERRITORY)_PRE_valueRange_A VALIDATED')
	else:
		print('PRECONDITION mapping(TERRITORY)_PRE_valueRange_A NOT VALIDATED')
	if contract_pre_post.check_fix_value_range(value='N', data_dictionary=mapping_TERRITORY__input_dataDictionary_df, belong_op=Belong(0), field='TERRITORY',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('PRECONDITION mapping(TERRITORY)_PRE_valueRange_N VALIDATED')
	else:
		print('PRECONDITION mapping(TERRITORY)_PRE_valueRange_N NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value='A', data_dictionary=mapping_TERRITORY__output_dataDictionary_df, belong_op=Belong(1), field='TERRITORY',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('POSTCONDITION mapping(TERRITORY)_POST_valueRange_A VALIDATED')
	else:
		print('POSTCONDITION mapping(TERRITORY)_POST_valueRange_A NOT VALIDATED')
	if contract_pre_post.check_fix_value_range(value='N', data_dictionary=mapping_TERRITORY__output_dataDictionary_df, belong_op=Belong(1), field='TERRITORY',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('POSTCONDITION mapping(TERRITORY)_POST_valueRange_N VALIDATED')
	else:
		print('POSTCONDITION mapping(TERRITORY)_POST_valueRange_N NOT VALIDATED')
	
	
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
		print('INVARIANT Mapping(TERRITORY)_INV_condition_Map_input_values_[A, N]_map_output_values_[0, 0] VALIDATED')
	else:
		print('INVARIANT Mapping(TERRITORY)_INV_condition_Map_input_values_[A, N]_map_output_values_[0, 0] NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	mapping_Instate__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/ruleEngine_territory_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_contracts/data/ruleEngine_instate_output_dataDictionary.parquet'):
		mapping_Instate__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/ruleEngine_instate_output_dataDictionary.parquet')

	if contract_pre_post.check_fix_value_range(value='Y', data_dictionary=mapping_Instate__input_dataDictionary_df, belong_op=Belong(0), field='Instate',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('PRECONDITION mapping(Instate)_PRE_valueRange_Y VALIDATED')
	else:
		print('PRECONDITION mapping(Instate)_PRE_valueRange_Y NOT VALIDATED')
	if contract_pre_post.check_fix_value_range(value='N', data_dictionary=mapping_Instate__input_dataDictionary_df, belong_op=Belong(0), field='Instate',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('PRECONDITION mapping(Instate)_PRE_valueRange_N VALIDATED')
	else:
		print('PRECONDITION mapping(Instate)_PRE_valueRange_N NOT VALIDATED')
	
	if contract_pre_post.check_fix_value_range(value='Y', data_dictionary=mapping_Instate__output_dataDictionary_df, belong_op=Belong(1), field='Instate',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('POSTCONDITION mapping(Instate)_POST_valueRange_Y VALIDATED')
	else:
		print('POSTCONDITION mapping(Instate)_POST_valueRange_Y NOT VALIDATED')
	if contract_pre_post.check_fix_value_range(value='N', data_dictionary=mapping_Instate__output_dataDictionary_df, belong_op=Belong(1), field='Instate',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Rule Engine"):
		print('POSTCONDITION mapping(Instate)_POST_valueRange_N VALIDATED')
	else:
		print('POSTCONDITION mapping(Instate)_POST_valueRange_N NOT VALIDATED')
	
	
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
		print('INVARIANT mapping(Instate)_INV_condition_Map_input_values_[Y, N]_map_output_values_[1, 0] VALIDATED')
	else:
		print('INVARIANT mapping(Instate)_INV_condition_Map_input_values_[Y, N]_map_output_values_[1, 0] NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	stringToNumber_TERRITORY_Instate__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/ruleEngine_instate_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_contracts/data/stringToNumber_output_dataDictionary.parquet'):
		stringToNumber_TERRITORY_Instate__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/stringToNumber_output_dataDictionary.parquet')

	if contract_invariants.check_inv_missing_value_missing_value(data_dictionary_in=stringToNumber_TERRITORY_Instate__input_dataDictionary_df,
											data_dictionary_out=stringToNumber_TERRITORY_Instate__output_dataDictionary_df,
											belong_op_in=Belong(1), belong_op_out=Belong(1),
											field_in='TERRITORY', field_out='TERRITORY', origin_function="String To Number"):
		print('INVARIANT INV_specialValue_condition_TERRITORY_belongOpIn_NotBelong_belongOpOut_NotBelong VALIDATED')
	else:
		print('INVARIANT INV_specialValue_condition_TERRITORY_belongOpIn_NotBelong_belongOpOut_NotBelong NOT VALIDATED')
	
	
	
	if contract_invariants.check_inv_missing_value_missing_value(data_dictionary_in=stringToNumber_TERRITORY_Instate__input_dataDictionary_df,
											data_dictionary_out=stringToNumber_TERRITORY_Instate__output_dataDictionary_df,
											belong_op_in=Belong(1), belong_op_out=Belong(1),
											field_in='Instate', field_out='Instate', origin_function="String To Number"):
		print('INVARIANT INV_specialValue_condition_Instate_belongOpIn_NotBelong_belongOpOut_NotBelong VALIDATED')
	else:
		print('INVARIANT INV_specialValue_condition_Instate_belongOpIn_NotBelong_belongOpOut_NotBelong NOT VALIDATED')
	
	
	
	if contract_invariants.check_inv_cast_type(data_dictionary_in=stringToNumber_TERRITORY_Instate__input_dataDictionary_df,
								data_dictionary_out=stringToNumber_TERRITORY_Instate__output_dataDictionary_df,
								cast_type_in=DataType(0),
								cast_type_out=DataType(2),
								belong_op_out=Belong(0),
								field_in='TERRITORY', field_out='TERRITORY'):
		print('INVARIANT INV_castType_condition_TERRITORY_String_to_Integer VALIDATED')
	else:
		print('INVARIANT INV_castType_condition_TERRITORY_String_to_Integer NOT VALIDATED')
	
	
	
	if contract_invariants.check_inv_cast_type(data_dictionary_in=stringToNumber_TERRITORY_Instate__input_dataDictionary_df,
								data_dictionary_out=stringToNumber_TERRITORY_Instate__output_dataDictionary_df,
								cast_type_in=DataType(0),
								cast_type_out=DataType(2),
								belong_op_out=Belong(0),
								field_in='Instate', field_out='Instate'):
		print('INVARIANT INV_castType_condition_Instate_String_to_Integer VALIDATED')
	else:
		print('INVARIANT INV_castType_condition_Instate_String_to_Integer NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/stringToNumber_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_contracts/data/numericOutliers_output_dataDictionary.parquet'):
		imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericOutliers_output_dataDictionary.parquet')

	if contract_pre_post.check_outliers(belong_op=Belong(0), data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='avg_income', 
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Numeric Outliers"):
		print('PRECONDITION imputeOutlierByClosest(avg_income)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION imputeOutlierByClosest(avg_income)_PRE_valueRange NOT VALIDATED')
	
	if contract_pre_post.check_outliers(belong_op=Belong(0), data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='distance', 
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Numeric Outliers"):
		print('PRECONDITION imputeOutlierByClosest(distance)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION imputeOutlierByClosest(distance)_PRE_valueRange NOT VALIDATED')
	
	if contract_pre_post.check_outliers(belong_op=Belong(0), data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df, field='Instate', 
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Numeric Outliers"):
		print('PRECONDITION imputeOutlierByClosest(Instate)_PRE_valueRange VALIDATED')
	else:
		print('PRECONDITION imputeOutlierByClosest(Instate)_PRE_valueRange NOT VALIDATED')
	
	if contract_pre_post.check_outliers(belong_op=Belong(1), data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df, field='avg_income', 
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Numeric Outliers"):
		print('POSTCONDITION imputeOutlierByClosest(avg_income)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION imputeOutlierByClosest(avg_income)_POST_valueRange NOT VALIDATED')
	
	if contract_pre_post.check_outliers(belong_op=Belong(1), data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df, field='distance', 
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Numeric Outliers"):
		print('POSTCONDITION imputeOutlierByClosest(distance)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION imputeOutlierByClosest(distance)_POST_valueRange NOT VALIDATED')
	
	if contract_pre_post.check_outliers(belong_op=Belong(1), data_dictionary=imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df, field='Instate', 
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Numeric Outliers"):
		print('POSTCONDITION imputeOutlierByClosest(Instate)_POST_valueRange VALIDATED')
	else:
		print('POSTCONDITION imputeOutlierByClosest(Instate)_POST_valueRange NOT VALIDATED')
	
	if contract_invariants.check_inv_special_value_num_op(data_dictionary_in=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df,
											data_dictionary_out=imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											special_type_input=SpecialType(2),
											num_op_output=Operation(3),
											missing_values=None, axis_param=0, field_in='avg_income', field_out='avg_income', origin_function="Numeric Outliers"):
		print('INVARIANT imputeOutlierByClosest(avg_income)_INV_condition_OUTLIER_to_Closest VALIDATED')
	else:
		print('INVARIANT imputeOutlierByClosest(avg_income)_INV_condition_OUTLIER_to_Closest NOT VALIDATED')
	
	
	
	if contract_invariants.check_inv_special_value_num_op(data_dictionary_in=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df,
											data_dictionary_out=imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											special_type_input=SpecialType(2),
											num_op_output=Operation(3),
											missing_values=None, axis_param=0, field_in='distance', field_out='distance', origin_function="Numeric Outliers"):
		print('INVARIANT imputeOutlierByClosest(distance)_INV_condition_OUTLIER_to_Closest VALIDATED')
	else:
		print('INVARIANT imputeOutlierByClosest(distance)_INV_condition_OUTLIER_to_Closest NOT VALIDATED')
	
	
	
	if contract_invariants.check_inv_special_value_num_op(data_dictionary_in=imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary_df,
											data_dictionary_out=imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary_df,
											belong_op_in=Belong(0),
											belong_op_out=Belong(0),
											special_type_input=SpecialType(2),
											num_op_output=Operation(3),
											missing_values=None, axis_param=0, field_in='Instate', field_out='Instate', origin_function="Numeric Outliers"):
		print('INVARIANT imputeOutlierByClosest(Instate)_INV_condition_OUTLIER_to_Closest VALIDATED')
	else:
		print('INVARIANT imputeOutlierByClosest(Instate)_INV_condition_OUTLIER_to_Closest NOT VALIDATED')
	
	
	
	#-----------------New DataProcessing-----------------
	binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericOutliers_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet'):
		binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=1000.0, data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(0), field='TOTAL_CONTACTS', origin_function="Numeric Binner"):
		print('PRECONDITION binner(TOTAL_CONTACTS)_PRE_valueRange_(-1000.0, 1000.0) VALIDATED')
	else:
		print('PRECONDITION binner(TOTAL_CONTACTS)_PRE_valueRange_(-1000.0, 1000.0) NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=1000.0, data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(0), field='SELF_INIT_CNTCTS', origin_function="Numeric Binner"):
		print('PRECONDITION binner(SELF_INIT_CNTCTS)_PRE_valueRange_(-1000.0, 1000.0) VALIDATED')
	else:
		print('PRECONDITION binner(SELF_INIT_CNTCTS)_PRE_valueRange_(-1000.0, 1000.0) NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=1000.0, data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(0), field='SOLICITED_CNTCTS', origin_function="Numeric Binner"):
		print('PRECONDITION binner(SOLICITED_CNTCTS)_PRE_valueRange_(-1000.0, 1000.0) VALIDATED')
	else:
		print('PRECONDITION binner(SOLICITED_CNTCTS)_PRE_valueRange_(-1000.0, 1000.0) NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=1000.0, data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='TOTAL_CONTACTS_binned', origin_function="Numeric Binner"):
		print('POSTCONDITION binner(TOTAL_CONTACTS)_POST_valueRange_(-1000.0, 1000.0) VALIDATED')
	else:
		print('POSTCONDITION binner(TOTAL_CONTACTS)_POST_valueRange_(-1000.0, 1000.0) NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=1000.0, data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='SELF_INIT_CNTCTS_binned', origin_function="Numeric Binner"):
		print('POSTCONDITION binner(SELF_INIT_CNTCTS)_POST_valueRange_(-1000.0, 1000.0) VALIDATED')
	else:
		print('POSTCONDITION binner(SELF_INIT_CNTCTS)_POST_valueRange_(-1000.0, 1000.0) NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=1000.0, data_dictionary=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='SOLICITED_CNTCTS_binned', origin_function="Numeric Binner"):
		print('POSTCONDITION binner(SOLICITED_CNTCTS)_POST_valueRange_(-1000.0, 1000.0) VALIDATED')
	else:
		print('POSTCONDITION binner(SOLICITED_CNTCTS)_POST_valueRange_(-1000.0, 1000.0) NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=-1000.0, right_margin=1.0,
											closure_type=Closure(0),
											fix_value_output='Low',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TOTAL_CONTACTS', field_out='TOTAL_CONTACTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(TOTAL_CONTACTS)_INV_condition_interval_(-1000.0, 1.0)_fixValue_Low VALIDATED')
	else:
		print('INVARIANT binner(TOTAL_CONTACTS)_INV_condition_interval_(-1000.0, 1.0)_fixValue_Low NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=1.0, right_margin=4.0,
											closure_type=Closure(2),
											fix_value_output='Moderate',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TOTAL_CONTACTS', field_out='TOTAL_CONTACTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(TOTAL_CONTACTS)_INV_condition_interval_[1.0, 4.0)_fixValue_Moderate VALIDATED')
	else:
		print('INVARIANT binner(TOTAL_CONTACTS)_INV_condition_interval_[1.0, 4.0)_fixValue_Moderate NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=4.0, right_margin=1000.0,
											closure_type=Closure(2),
											fix_value_output='High',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TOTAL_CONTACTS', field_out='TOTAL_CONTACTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(TOTAL_CONTACTS)_INV_condition_interval_[4.0, 1000.0)_fixValue_High VALIDATED')
	else:
		print('INVARIANT binner(TOTAL_CONTACTS)_INV_condition_interval_[4.0, 1000.0)_fixValue_High NOT VALIDATED')
	
	
	
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=-1000.0, right_margin=1.0,
											closure_type=Closure(0),
											fix_value_output='Low',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='SELF_INIT_CNTCTS', field_out='SELF_INIT_CNTCTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT INV_binner_condition_SELF_INIT_CNTCTS_interval_(-1000.0, 1.0)_fixValue_Low VALIDATED')
	else:
		print('INVARIANT INV_binner_condition_SELF_INIT_CNTCTS_interval_(-1000.0, 1.0)_fixValue_Low NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=1.0, right_margin=4.0,
											closure_type=Closure(2),
											fix_value_output='Moderate',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='SELF_INIT_CNTCTS', field_out='SELF_INIT_CNTCTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT INV_binner_condition_SELF_INIT_CNTCTS_interval_[1.0, 4.0)_fixValue_Moderate VALIDATED')
	else:
		print('INVARIANT INV_binner_condition_SELF_INIT_CNTCTS_interval_[1.0, 4.0)_fixValue_Moderate NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=4.0, right_margin=1000.0,
											closure_type=Closure(2),
											fix_value_output='High',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='SELF_INIT_CNTCTS', field_out='SELF_INIT_CNTCTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT INV_binner_condition_SELF_INIT_CNTCTS_interval_[4.0, 1000.0)_fixValue_High VALIDATED')
	else:
		print('INVARIANT INV_binner_condition_SELF_INIT_CNTCTS_interval_[4.0, 1000.0)_fixValue_High NOT VALIDATED')
	
	
	
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=-1000.0, right_margin=1.0,
											closure_type=Closure(0),
											fix_value_output='Low',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='SOLICITED_CNTCTS', field_out='SOLICITED_CNTCTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(SOLICITED_CNTCTS)_INV_condition_interval_(-1000.0, 1.0)_fixValue_Low VALIDATED')
	else:
		print('INVARIANT binner(SOLICITED_CNTCTS)_INV_condition_interval_(-1000.0, 1.0)_fixValue_Low NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=1.0, right_margin=4.0,
											closure_type=Closure(2),
											fix_value_output='Moderate',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='SOLICITED_CNTCTS', field_out='SOLICITED_CNTCTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(SOLICITED_CNTCTS)_INV_condition_interval_[1.0, 4.0)_fixValue_Moderate VALIDATED')
	else:
		print('INVARIANT binner(SOLICITED_CNTCTS)_INV_condition_interval_[1.0, 4.0)_fixValue_Moderate NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary_df,
											data_dictionary_out=binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__output_dataDictionary_df,
											left_margin=4.0, right_margin=1000.0,
											closure_type=Closure(2),
											fix_value_output='High',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='SOLICITED_CNTCTS', field_out='SOLICITED_CNTCTS_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(SOLICITED_CNTCTS)_INV_condition_interval_[4.0, 1000.0)_fixValue_High VALIDATED')
	else:
		print('INVARIANT binner(SOLICITED_CNTCTS)_INV_condition_interval_[4.0, 1000.0)_fixValue_High NOT VALIDATED')
	
	
	
	
	#-----------------New DataProcessing-----------------
	binner_TERRITORY__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet'):
		binner_TERRITORY__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_TERRITORY__input_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='TERRITORY', origin_function="Numeric Binner"):
		print('PRECONDITION binner(TERRITORY)_PRE_valueRange_[0.0, 1000.0] VALIDATED')
	else:
		print('PRECONDITION binner(TERRITORY)_PRE_valueRange_[0.0, 1000.0] NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_TERRITORY__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='TERRITORY_binned', origin_function="Numeric Binner"):
		print('POSTCONDITION binner(TERRITORY)_POST_valueRange_(0.0, 1000.0) VALIDATED')
	else:
		print('POSTCONDITION binner(TERRITORY)_POST_valueRange_(0.0, 1000.0) NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TERRITORY__input_dataDictionary_df,
											data_dictionary_out=binner_TERRITORY__output_dataDictionary_df,
											left_margin=-1000.0, right_margin=1.0,
											closure_type=Closure(0),
											fix_value_output='Unknown',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TERRITORY', field_out='TERRITORY_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(TERRITORY)_INV_condition_interval_(-1000.0, 1.0)_fixValue_Unknown VALIDATED')
	else:
		print('INVARIANT binner(TERRITORY)_INV_condition_interval_(-1000.0, 1.0)_fixValue_Unknown NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TERRITORY__input_dataDictionary_df,
											data_dictionary_out=binner_TERRITORY__output_dataDictionary_df,
											left_margin=1.0, right_margin=3.0,
											closure_type=Closure(2),
											fix_value_output='Zone 1',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TERRITORY', field_out='TERRITORY_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(TERRITORY)_INV_condition_interval_[1.0, 3.0)_fixValue_Zone 1 VALIDATED')
	else:
		print('INVARIANT binner(TERRITORY)_INV_condition_interval_[1.0, 3.0)_fixValue_Zone 1 NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TERRITORY__input_dataDictionary_df,
											data_dictionary_out=binner_TERRITORY__output_dataDictionary_df,
											left_margin=3.0, right_margin=5.0,
											closure_type=Closure(2),
											fix_value_output='Zone 2',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TERRITORY', field_out='TERRITORY_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(TERRITORY)_INV_condition_interval_[3.0, 5.0)_fixValue_Zone 2 VALIDATED')
	else:
		print('INVARIANT binner(TERRITORY)_INV_condition_interval_[3.0, 5.0)_fixValue_Zone 2 NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TERRITORY__input_dataDictionary_df,
											data_dictionary_out=binner_TERRITORY__output_dataDictionary_df,
											left_margin=5.0, right_margin=7.0,
											closure_type=Closure(2),
											fix_value_output='Zone 3',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TERRITORY', field_out='TERRITORY_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(TERRITORY)_INV_condition_interval_[5.0, 7.0)_fixValue_Zone 3 VALIDATED')
	else:
		print('INVARIANT binner(TERRITORY)_INV_condition_interval_[5.0, 7.0)_fixValue_Zone 3 NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_TERRITORY__input_dataDictionary_df,
											data_dictionary_out=binner_TERRITORY__output_dataDictionary_df,
											left_margin=7.0, right_margin=1000.0,
											closure_type=Closure(2),
											fix_value_output='Zone 4',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='TERRITORY', field_out='TERRITORY_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(TERRITORY)_INV_condition_interval_[7.0, 1000.0)_fixValue_Zone 4 VALIDATED')
	else:
		print('INVARIANT binner(TERRITORY)_INV_condition_interval_[7.0, 1000.0)_fixValue_Zone 4 NOT VALIDATED')
	
	
	
	
	#-----------------New DataProcessing-----------------
	binner_satscore__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet'):
		binner_satscore__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=2000.0, data_dictionary=binner_satscore__input_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='satscore', origin_function="Numeric Binner"):
		print('PRECONDITION binner(satscore)_PRE_valueRange_[-1000.0, 2000.0] VALIDATED')
	else:
		print('PRECONDITION binner(satscore)_PRE_valueRange_[-1000.0, 2000.0] NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=-1000.0, right_margin=2000.0, data_dictionary=binner_satscore__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='satscore_binned', origin_function="Numeric Binner"):
		print('POSTCONDITION binner(satscore)_POST_valueRange_(-1000.0, 2000.0) VALIDATED')
	else:
		print('POSTCONDITION binner(satscore)_POST_valueRange_(-1000.0, 2000.0) NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_satscore__input_dataDictionary_df,
											data_dictionary_out=binner_satscore__output_dataDictionary_df,
											left_margin=-1000.0, right_margin=1040.0,
											closure_type=Closure(1),
											fix_value_output='54 Percentile and Under',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='satscore', field_out='satscore_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(satscore)_INV_condition_interval_(-1000.0, 1040.0]_fixValue_54 Percentile and Under VALIDATED')
	else:
		print('INVARIANT binner(satscore)_INV_condition_interval_(-1000.0, 1040.0]_fixValue_54 Percentile and Under NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_satscore__input_dataDictionary_df,
											data_dictionary_out=binner_satscore__output_dataDictionary_df,
											left_margin=1040.0, right_margin=1160.0,
											closure_type=Closure(0),
											fix_value_output='55-75 Percentile',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='satscore', field_out='satscore_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(satscore)_INV_condition_interval_(1040.0, 1160.0)_fixValue_55-75 Percentile VALIDATED')
	else:
		print('INVARIANT binner(satscore)_INV_condition_interval_(1040.0, 1160.0)_fixValue_55-75 Percentile NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_satscore__input_dataDictionary_df,
											data_dictionary_out=binner_satscore__output_dataDictionary_df,
											left_margin=1160.0, right_margin=1340.0,
											closure_type=Closure(2),
											fix_value_output='76-93 Percentile',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='satscore', field_out='satscore_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(satscore)_INV_condition_interval_[1160.0, 1340.0)_fixValue_76-93 Percentile VALIDATED')
	else:
		print('INVARIANT binner(satscore)_INV_condition_interval_[1160.0, 1340.0)_fixValue_76-93 Percentile NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_satscore__input_dataDictionary_df,
											data_dictionary_out=binner_satscore__output_dataDictionary_df,
											left_margin=1340.0, right_margin=2000.0,
											closure_type=Closure(1),
											fix_value_output='94+ percentile',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='satscore', field_out='satscore_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(satscore)_INV_condition_interval_(1340.0, 2000.0]_fixValue_94+ percentile VALIDATED')
	else:
		print('INVARIANT binner(satscore)_INV_condition_interval_(1340.0, 2000.0]_fixValue_94+ percentile NOT VALIDATED')
	
	
	
	
	#-----------------New DataProcessing-----------------
	binner_avg_income__input_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')

	if os.path.exists('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet'):
		binner_avg_income__output_dataDictionary_df=pd.read_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=9.0, right_margin=100000.0, data_dictionary=binner_avg_income__input_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='avg_income', origin_function="Numeric Binner"):
		print('PRECONDITION binner(avg_income)_PRE_valueRange_[9.0, 100000.0] VALIDATED')
	else:
		print('PRECONDITION binner(avg_income)_PRE_valueRange_[9.0, 100000.0] NOT VALIDATED')
	
	if contract_pre_post.check_interval_range_float(left_margin=9.0, right_margin=100000.0, data_dictionary=binner_avg_income__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='avg_income_binned', origin_function="Numeric Binner"):
		print('POSTCONDITION binner(avg_income)_POST_valueRange_(9.0, 100000.0) VALIDATED')
	else:
		print('POSTCONDITION binner(avg_income)_POST_valueRange_(9.0, 100000.0) NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_avg_income__input_dataDictionary_df,
											data_dictionary_out=binner_avg_income__output_dataDictionary_df,
											left_margin=9.0, right_margin=42830.0,
											closure_type=Closure(0),
											fix_value_output='low',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='avg_income', field_out='avg_income_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(avg_income)_INV_condition_interval_(9.0, 42830.0)_fixValue_low VALIDATED')
	else:
		print('INVARIANT binner(avg_income)_INV_condition_interval_(9.0, 42830.0)_fixValue_low NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_avg_income__input_dataDictionary_df,
											data_dictionary_out=binner_avg_income__output_dataDictionary_df,
											left_margin=42830.0, right_margin=55559.0,
											closure_type=Closure(2),
											fix_value_output='Moderate',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='avg_income', field_out='avg_income_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(avg_income)_INV_condition_interval_[42830.0, 55559.0)_fixValue_Moderate VALIDATED')
	else:
		print('INVARIANT binner(avg_income)_INV_condition_interval_[42830.0, 55559.0)_fixValue_Moderate NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_avg_income__input_dataDictionary_df,
											data_dictionary_out=binner_avg_income__output_dataDictionary_df,
											left_margin=55590.0, right_margin=100000.0,
											closure_type=Closure(2),
											fix_value_output='High',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='avg_income', field_out='avg_income_binned', origin_function="Numeric Binner"):
		print('INVARIANT binner(avg_income)_INV_condition_interval_[55590.0, 100000.0)_fixValue_High VALIDATED')
	else:
		print('INVARIANT binner(avg_income)_INV_condition_interval_[55590.0, 100000.0)_fixValue_High NOT VALIDATED')
	
	
	
	














set_logger("contracts")
generateWorkflow()
