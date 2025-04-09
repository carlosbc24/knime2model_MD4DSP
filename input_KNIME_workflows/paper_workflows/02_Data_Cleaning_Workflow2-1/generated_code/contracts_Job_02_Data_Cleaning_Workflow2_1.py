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
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_input_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet'):
		columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')

	field_list_columnFilter_PRE_field_range=['Airline ID', 'Name', 'Alias', 'IATA', 'ICAO', 'Callsign', 'Country', 'Active']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_PRE_field_range,
								data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df,
								belong_op=Belong(0)):
		print('PRECONDITION columnFilter(Airline ID, Name, Alias, IATA, ICAO, Callsign, Country, Active)_PRE_fieldRange VALIDATED')
	else:
		print('PRECONDITION columnFilter(Airline ID, Name, Alias, IATA, ICAO, Callsign, Country, Active)_PRE_fieldRange NOT VALIDATED')
	
	
	field_list_columnFilter_POST_field_range=['Airline ID', 'Name', 'Alias', 'IATA', 'ICAO', 'Callsign', 'Country', 'Active']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_POST_field_range,
								data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__output_dataDictionary_df,
								belong_op=Belong(0)):
		print('POSTCONDITION columnFilter(Airline ID, Name, Alias, IATA, ICAO, Callsign, Country, Active)_POST_fieldRange VALIDATED')
	else:
		print('POSTCONDITION columnFilter(Airline ID, Name, Alias, IATA, ICAO, Callsign, Country, Active)_POST_fieldRange NOT VALIDATED')
	
	
set_logger("contracts")
generateWorkflow()
