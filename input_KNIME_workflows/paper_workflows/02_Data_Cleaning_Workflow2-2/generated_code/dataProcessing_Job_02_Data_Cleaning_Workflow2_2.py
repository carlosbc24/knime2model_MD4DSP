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
	columnFilter_IATA_code_ICAO_code__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_input_dataDictionary.parquet')

	field_list_columnFilter_PRE_field_range=['IATA code', 'ICAO code']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_PRE_field_range,
								data_dictionary=columnFilter_IATA_code_ICAO_code__input_dataDictionary_df,
								belong_op=Belong(0)):
		print('PRECONDITION columnFilter(IATA code, ICAO code)_PRE_fieldRange VALIDATED')
	else:
		print('PRECONDITION columnFilter(IATA code, ICAO code)_PRE_fieldRange NOT VALIDATED')
	
	
	columnFilter_IATA_code_ICAO_code__input_dataDictionary_transformed=columnFilter_IATA_code_ICAO_code__input_dataDictionary_df.copy()
	field_list_columnFilter_param_field=['IATA code', 'ICAO code']
	
	columnFilter_IATA_code_ICAO_code__input_dataDictionary_transformed=data_transformations.transform_filter_columns(data_dictionary=columnFilter_IATA_code_ICAO_code__input_dataDictionary_transformed,
																	columns=field_list_columnFilter_param_field, belong_op=Belong.BELONG)
	
	columnFilter_IATA_code_ICAO_code__output_dataDictionary_df=columnFilter_IATA_code_ICAO_code__input_dataDictionary_transformed
	columnFilter_IATA_code_ICAO_code__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	columnFilter_IATA_code_ICAO_code__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	
	field_list_columnFilter_POST_field_range=['IATA code', 'ICAO code']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_POST_field_range,
								data_dictionary=columnFilter_IATA_code_ICAO_code__output_dataDictionary_df,
								belong_op=Belong(0)):
		print('POSTCONDITION columnFilter(IATA code, ICAO code)_POST_fieldRange VALIDATED')
	else:
		print('POSTCONDITION columnFilter(IATA code, ICAO code)_POST_fieldRange NOT VALIDATED')
	
	

set_logger("dataProcessing")
generateWorkflow()
