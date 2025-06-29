import os

import pandas as pd
import numpy as np
import functions.contract_invariants as contract_invariants
import functions.contract_pre_post as contract_pre_post
import functions.data_smells as data_smells
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_input_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet'):
		columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Thinness_ten_nineteen_years')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Thinness_five_nine_years')
	
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, field='Thinness_ten_nineteen_years')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, field='Thinness_five_nine_years')
	
	data_smells.check_types_as_string(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, field='Thinness_ten_nineteen_years', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, field='Thinness_five_nine_years', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, field='Thinness_ten_nineteen_years')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, field='Thinness_five_nine_years')
	
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, field='Thinness_ten_nineteen_years')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, field='Thinness_five_nine_years')
	
	
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, field='Thinness_ten_nineteen_years')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, field='Thinness_five_nine_years')
	
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, decimal_sep='.',  field='Thinness_ten_nineteen_years')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, decimal_sep='.',  field='Thinness_five_nine_years')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, field='Thinness_ten_nineteen_years')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df, field='Thinness_five_nine_years')
	
	
	columns_list_columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__INV_condition = ['Thinness_ten_nineteen_years', 'Thinness_five_nine_years']
	
	if contract_invariants.check_inv_filter_columns(data_dictionary_in=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df,
							data_dictionary_out=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__output_dataDictionary_df,
							columns=columns_list_columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__INV_condition,
							belong_op=Belong(0), origin_function="Column Filter"):
		print('INVARIANT Column Filter(Thinness_ten_nineteen_years, Thinness_five_nine_years) VALIDATED')
	else:
		print('INVARIANT Column Filter(Thinness_ten_nineteen_years, Thinness_five_nine_years) NOT VALIDATED')
	
	
	
	
	
set_logger("contracts")
generateWorkflow()
