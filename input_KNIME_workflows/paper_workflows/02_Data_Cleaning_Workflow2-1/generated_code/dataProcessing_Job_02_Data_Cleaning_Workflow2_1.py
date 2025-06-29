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
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_input_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Airline ID')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Name')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Alias')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='IATA')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='ICAO')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Callsign')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Country')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Active')
	
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Airline ID')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Name')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Alias')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='IATA')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='ICAO')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Callsign')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Country')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Active')
	
	data_smells.check_types_as_string(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Airline ID', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Name', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Alias', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='IATA', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='ICAO', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Callsign', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Country', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Active', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Airline ID')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Name')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Alias')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='IATA')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='ICAO')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Callsign')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Country')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Active')
	
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Airline ID')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Name')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Alias')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='IATA')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='ICAO')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Callsign')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Country')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Active')
	
	
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Airline ID')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Name')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Alias')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='IATA')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='ICAO')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Callsign')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Country')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Active')
	
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, decimal_sep='.',  field='Airline ID')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, decimal_sep='.',  field='Name')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, decimal_sep='.',  field='Alias')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, decimal_sep='.',  field='IATA')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, decimal_sep='.',  field='ICAO')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, decimal_sep='.',  field='Callsign')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, decimal_sep='.',  field='Country')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, decimal_sep='.',  field='Active')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Airline ID')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Name')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Alias')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='IATA')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='ICAO')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Callsign')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Country')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df, field='Active')
	
	
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_transformed=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df.copy()
	field_list_columnFilter_param_field=['Airline ID', 'Name', 'Alias', 'IATA', 'ICAO', 'Callsign', 'Country', 'Active']
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_transformed=data_transformations.transform_filter_columns(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_transformed,
																	columns=field_list_columnFilter_param_field, belong_op=Belong.NOTBELONG)
	
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__output_dataDictionary_df=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_transformed
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	
	columns_list_columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__INV_condition = ['Airline ID', 'Name', 'Alias', 'IATA', 'ICAO', 'Callsign', 'Country', 'Active']
	
	if contract_invariants.check_inv_filter_columns(data_dictionary_in=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df,
							data_dictionary_out=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__output_dataDictionary_df,
							columns=columns_list_columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__INV_condition,
							belong_op=Belong(1), origin_function="Column Filter"):
		print('INVARIANT Column Filter(Airline ID, Name, Alias, IATA, ICAO, Callsign, Country, Active) VALIDATED')
	else:
		print('INVARIANT Column Filter(Airline ID, Name, Alias, IATA, ICAO, Callsign, Country, Active) NOT VALIDATED')
	
	
	
	
	

set_logger("dataProcessing")
generateWorkflow()
