import pandas as pd
import numpy as np
import functions.data_transformations as data_transformations
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_input_dataDictionary.parquet')

	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_transformed=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_df.copy()
	field_list_columnFilter_param_field=['Airline ID', 'Name', 'Alias', 'IATA', 'ICAO', 'Callsign', 'Country', 'Active']
	
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_transformed=data_transformations.transform_filter_columns(data_dictionary=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_transformed,
																	columns=field_list_columnFilter_param_field, belong_op=Belong.NOTBELONG)
	
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__output_dataDictionary_df=columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__input_dataDictionary_transformed
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	columnFilter_Airline_ID_Name_Alias_IATA_ICAO_Callsign_Country_Active__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	

set_logger("transformations")
generateWorkflow()
