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
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_input_dataDictionary.parquet')

	field_list_columnFilter_PRE_field_range=['Airline ID', 'Name', 'Alias', 'IATA', 'ICAO', 'Callsign', 'Country', 'Active']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_PRE_field_range,
								data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df,
								belong_op=Belong(0), origin_function="Column Filter"):
		print('PRECONDITION Column Filter(Airline ID, Name, Alias, IATA, ICAO, Callsign, Country, Active) VALIDATED')
	else:
		print('PRECONDITION Column Filter(Airline ID, Name, Alias, IATA, ICAO, Callsign, Country, Active) NOT VALIDATED')
	
	
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_transformed=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df.copy()
	field_list_columnFilter_param_field=['Airline ID', 'Name', 'Alias', 'IATA', 'ICAO', 'Callsign', 'Country', 'Active']
	
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_transformed=data_transformations.transform_filter_columns(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_transformed,
																	columns=field_list_columnFilter_param_field, belong_op=Belong.NOTBELONG)
	
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__output_dataDictionary_df=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_transformed
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	
	field_list_columnFilter_POST_field_range=['Airline ID', 'Name', 'Alias', 'IATA', 'ICAO', 'Callsign', 'Country', 'Active']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_POST_field_range,
								data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__output_dataDictionary_df,
								belong_op=Belong(0), origin_function="Column Filter"):
		print('POSTCONDITION Column Filter(Airline ID, Name, Alias, IATA, ICAO, Callsign, Country, Active) VALIDATED')
	else:
		print('POSTCONDITION Column Filter(Airline ID, Name, Alias, IATA, ICAO, Callsign, Country, Active) NOT VALIDATED')
	
	
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
