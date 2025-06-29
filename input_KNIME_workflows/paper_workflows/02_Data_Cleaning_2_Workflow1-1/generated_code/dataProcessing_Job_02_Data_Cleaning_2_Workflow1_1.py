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
	rowFilterPrimitive_Country__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_input_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=['China']
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=rowFilterPrimitive_Country__input_dataDictionary_df, 
														missing_invalid_list=list_invalid, common_missing_invalid_list=common_invalid_list, field='Country')
	
	data_smells.check_integer_as_floating_point(data_dictionary=rowFilterPrimitive_Country__input_dataDictionary_df, field='Country')
	
	data_smells.check_types_as_string(data_dictionary=rowFilterPrimitive_Country__input_dataDictionary_df, field='Country', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=rowFilterPrimitive_Country__input_dataDictionary_df, field='Country')
	
	data_smells.check_suspect_precision(data_dictionary=rowFilterPrimitive_Country__input_dataDictionary_df, field='Country')
	
	data_smells.check_suspect_distribution(data_dictionary=rowFilterPrimitive_Country__input_dataDictionary_df, min_value=9.0, max_value=202.0, field='Country')
	
	data_smells.check_date_as_datetime(data_dictionary=rowFilterPrimitive_Country__input_dataDictionary_df, field='Country')
	
	data_smells.check_separating_consistency(data_dictionary=rowFilterPrimitive_Country__input_dataDictionary_df, decimal_sep='.',  field='Country')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=rowFilterPrimitive_Country__input_dataDictionary_df, field='Country')
	
	
	if contract_pre_post.check_fix_value_range(value='China', is_substring=False, data_dictionary=rowFilterPrimitive_Country__input_dataDictionary_df, belong_op=Belong(0), field='Country',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('PRECONDITION Row Filter(Country) FixValue:China VALIDATED')
	else:
		print('PRECONDITION Row Filter(Country) FixValue:China NOT VALIDATED')
	
	rowFilterPrimitive_Country__input_dataDictionary_transformed=rowFilterPrimitive_Country__input_dataDictionary_df.copy()
	columns_rowFilterPrimitive_param_filter=['Country']
	filter_fix_value_list_rowFilterPrimitive_param_filter=['China']
	
	rowFilterPrimitive_Country__input_dataDictionary_transformed=data_transformations.transform_filter_rows_primitive(data_dictionary=rowFilterPrimitive_Country__input_dataDictionary_transformed,
																											columns=columns_rowFilterPrimitive_param_filter,
																		                                    filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_param_filter,
																											filter_type=FilterType(1))
	rowFilterPrimitive_Country__output_dataDictionary_df=rowFilterPrimitive_Country__input_dataDictionary_transformed
	rowFilterPrimitive_Country__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	rowFilterPrimitive_Country__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	
	if contract_pre_post.check_fix_value_range(value='China', is_substring=False, data_dictionary=rowFilterPrimitive_Country__output_dataDictionary_df, belong_op=Belong(0), field='Country',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('POSTCONDITION Row Filter(Country) FixValue:China VALIDATED')
	else:
		print('POSTCONDITION Row Filter(Country) FixValue:China NOT VALIDATED')
	
	
	
	columns_list_rowFilterPrimitive_Country__INV_condition=['Country']
	filter_fix_value_list_rowFilterPrimitive_Country__INV_condition=['China']
	
	if contract_invariants.check_inv_filter_rows_primitive(data_dictionary_in=rowFilterPrimitive_Country__input_dataDictionary_df,
											data_dictionary_out=rowFilterPrimitive_Country__output_dataDictionary_df,
											columns=columns_list_rowFilterPrimitive_Country__INV_condition,
											filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_Country__INV_condition,
											filter_type=FilterType.INCLUDE, origin_function="Row Filter"):
		print('INVARIANT Row Filter(Country) FilterType:INCLUDE FixValueList:[China] VALIDATED')
	else:
		print('INVARIANT Row Filter(Country) FilterType:INCLUDE FixValueList:[China] NOT VALIDATED')
	
	

set_logger("dataProcessing")
generateWorkflow()
