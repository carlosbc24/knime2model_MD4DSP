import pandas as pd
import numpy as np
import functions.contract_invariants as contract_invariants
import functions.contract_pre_post as contract_pre_post
import functions.data_transformations as data_transformations
import functions.data_smells as data_smells
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	rowFilterMissing_Equipment__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=rowFilterMissing_Equipment__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Equipment')
	
	data_smells.check_integer_as_floating_point(data_dictionary=rowFilterMissing_Equipment__input_dataDictionary_df, field='Equipment')
	
	data_smells.check_types_as_string(data_dictionary=rowFilterMissing_Equipment__input_dataDictionary_df, field='Equipment', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=rowFilterMissing_Equipment__input_dataDictionary_df, field='Equipment')
	
	data_smells.check_suspect_precision(data_dictionary=rowFilterMissing_Equipment__input_dataDictionary_df, field='Equipment')
	
	data_smells.check_suspect_distribution(data_dictionary=rowFilterMissing_Equipment__input_dataDictionary_df, min_value=0.0, max_value=1.0, field='Equipment')
	
	data_smells.check_date_as_datetime(data_dictionary=rowFilterMissing_Equipment__input_dataDictionary_df, field='Equipment')
	
	data_smells.check_separating_consistency(data_dictionary=rowFilterMissing_Equipment__input_dataDictionary_df, decimal_sep='.',  field='Equipment')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=rowFilterMissing_Equipment__input_dataDictionary_df, field='Equipment')
	
	
	missing_values_rowFilterMissing_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=rowFilterMissing_Equipment__input_dataDictionary_df, field='Equipment', 
									missing_values=missing_values_rowFilterMissing_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Row Filter"):
		print('PRECONDITION Row Filter(Equipment) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Row Filter(Equipment) MissingValues:[] NOT VALIDATED')
	
	rowFilterMissing_Equipment__input_dataDictionary_transformed=rowFilterMissing_Equipment__input_dataDictionary_df.copy()
	columns_rowFilterMissing_param_filter=['Equipment']
	dicc_rowFilterMissing_param_filter={'Equipment':{'missing': []}}
	
	rowFilterMissing_Equipment__input_dataDictionary_transformed=data_transformations.transform_filter_rows_special_values(data_dictionary=rowFilterMissing_Equipment__input_dataDictionary_transformed,
																											cols_special_type_values=dicc_rowFilterMissing_param_filter,
																											filter_type=FilterType(1))
	rowFilterMissing_Equipment__output_dataDictionary_df=rowFilterMissing_Equipment__input_dataDictionary_transformed
	rowFilterMissing_Equipment__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')
	rowFilterMissing_Equipment__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')
	
	missing_values_rowFilterMissing_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=rowFilterMissing_Equipment__output_dataDictionary_df, field='Equipment', 
									missing_values=missing_values_rowFilterMissing_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('POSTCONDITION Row Filter(Equipment) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Row Filter(Equipment) MissingValues:[] NOT VALIDATED')
	
	
	
	cols_special_type_values_rowFilterMissing_Equipment__INV_condition={'Equipment':{'missing': []}}
	
	if contract_invariants.check_inv_filter_rows_special_values(data_dictionary_in=rowFilterMissing_Equipment__input_dataDictionary_df,
											data_dictionary_out=rowFilterMissing_Equipment__output_dataDictionary_df,
											cols_special_type_values=cols_special_type_values_rowFilterMissing_Equipment__INV_condition,
											filter_type=FilterType.INCLUDE, origin_function="Row Filter"):
		print('INVARIANT Row Filter(Equipment) FilterType:INCLUDE SpecialValues: EquipmentMISSING:[] VALIDATED')
	else:
		print('INVARIANT Row Filter(Equipment) FilterType:INCLUDE SpecialValues: EquipmentMISSING:[] NOT VALIDATED')
	
	

set_logger("dataProcessing")
generateWorkflow()
