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
	columnFilter_Latitude_Longitude__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_input_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet'):
		columnFilter_Latitude_Longitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Latitude')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Longitude')
	
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, field='Latitude')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, field='Longitude')
	
	data_smells.check_types_as_string(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, field='Latitude', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, field='Longitude', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, field='Latitude')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, field='Longitude')
	
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, field='Latitude')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, field='Longitude')
	
	
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, field='Latitude')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, field='Longitude')
	
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, decimal_sep='.',  field='Latitude')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, decimal_sep='.',  field='Longitude')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, field='Latitude')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Latitude_Longitude__input_dataDictionary_df, field='Longitude')
	
	
	columns_list_columnFilter_Latitude_Longitude__INV_condition = ['Latitude', 'Longitude']
	
	if contract_invariants.check_inv_filter_columns(data_dictionary_in=columnFilter_Latitude_Longitude__input_dataDictionary_df,
							data_dictionary_out=columnFilter_Latitude_Longitude__output_dataDictionary_df,
							columns=columns_list_columnFilter_Latitude_Longitude__INV_condition,
							belong_op=Belong(0), origin_function="Column Filter"):
		print('INVARIANT Column Filter(Latitude, Longitude) VALIDATED')
	else:
		print('INVARIANT Column Filter(Latitude, Longitude) NOT VALIDATED')
	
	
	
	
	
set_logger("contracts")
generateWorkflow()
