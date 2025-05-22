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
	columnFilter_Airline_Airline_ID_Source_airport_Source_airport_ID_Destination_airport_Destination_airport_ID_Codeshare_Stops_Equipment__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_input_dataDictionary.parquet')

	if os.path.exists('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet'):
		columnFilter_Airline_Airline_ID_Source_airport_Source_airport_ID_Destination_airport_Destination_airport_ID_Codeshare_Stops_Equipment__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')

	field_list_columnFilter_PRE_field_range=['Airline', 'Airline ID', 'Source airport', 'Source airport ID', 'Destination airport', 'Destination airport ID', 'Codeshare', 'Stops', 'Equipment']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_PRE_field_range,
								data_dictionary=columnFilter_Airline_Airline_ID_Source_airport_Source_airport_ID_Destination_airport_Destination_airport_ID_Codeshare_Stops_Equipment__input_dataDictionary_df,
								belong_op=Belong(0), origin_function="Column Filter"):
		print('PRECONDITION Column Filter(Airline, Airline ID, Source airport, Source airport ID, Destination airport, Destination airport ID, Codeshare, Stops, Equipment) VALIDATED')
	else:
		print('PRECONDITION Column Filter(Airline, Airline ID, Source airport, Source airport ID, Destination airport, Destination airport ID, Codeshare, Stops, Equipment) NOT VALIDATED')
	
	
	field_list_columnFilter_POST_field_range=['Airline', 'Airline ID', 'Source airport', 'Source airport ID', 'Destination airport', 'Destination airport ID', 'Codeshare', 'Stops', 'Equipment']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_POST_field_range,
								data_dictionary=columnFilter_Airline_Airline_ID_Source_airport_Source_airport_ID_Destination_airport_Destination_airport_ID_Codeshare_Stops_Equipment__output_dataDictionary_df,
								belong_op=Belong(0), origin_function="Column Filter"):
		print('POSTCONDITION Column Filter(Airline, Airline ID, Source airport, Source airport ID, Destination airport, Destination airport ID, Codeshare, Stops, Equipment) VALIDATED')
	else:
		print('POSTCONDITION Column Filter(Airline, Airline ID, Source airport, Source airport ID, Destination airport, Destination airport ID, Codeshare, Stops, Equipment) NOT VALIDATED')
	
	
	columns_list_columnFilter_Airline_Airline_ID_Source_airport_Source_airport_ID_Destination_airport_Destination_airport_ID_Codeshare_Stops_Equipment__INV_condition = ['Airline', 'Airline ID', 'Source airport', 'Source airport ID', 'Destination airport', 'Destination airport ID', 'Codeshare', 'Stops', 'Equipment']
	
	if contract_invariants.check_inv_filter_columns(data_dictionary_in=columnFilter_Airline_Airline_ID_Source_airport_Source_airport_ID_Destination_airport_Destination_airport_ID_Codeshare_Stops_Equipment__input_dataDictionary_df,
							data_dictionary_out=columnFilter_Airline_Airline_ID_Source_airport_Source_airport_ID_Destination_airport_Destination_airport_ID_Codeshare_Stops_Equipment__output_dataDictionary_df,
							columns=columns_list_columnFilter_Airline_Airline_ID_Source_airport_Source_airport_ID_Destination_airport_Destination_airport_ID_Codeshare_Stops_Equipment__INV_condition,
							belong_op=Belong(1), origin_function="Column Filter"):
		print('INVARIANT Column Filter(Airline, Airline ID, Source airport, Source airport ID, Destination airport, Destination airport ID, Codeshare, Stops, Equipment) VALIDATED')
	else:
		print('INVARIANT Column Filter(Airline, Airline ID, Source airport, Source airport ID, Destination airport, Destination airport ID, Codeshare, Stops, Equipment) NOT VALIDATED')
	
	
	
	
	
set_logger("contracts")
generateWorkflow()
