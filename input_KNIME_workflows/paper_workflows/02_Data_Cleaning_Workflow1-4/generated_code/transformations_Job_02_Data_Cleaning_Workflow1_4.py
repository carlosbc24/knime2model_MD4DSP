import pandas as pd
import numpy as np
import functions.data_transformations as data_transformations
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	rowFilterMissing_Equipment__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.parquet')

	rowFilterMissing_Equipment__input_dataDictionary_transformed=rowFilterMissing_Equipment__input_dataDictionary_df.copy()
	columns_rowFilterMissing_param_filter=['Equipment']
	
	dicc_rowFilterMissing_param_filter={'Equipment':{'missing': []}}
	
	rowFilterMissing_Equipment__input_dataDictionary_transformed=data_transformations.transform_filter_rows_special_values(data_dictionary=rowFilterMissing_Equipment__input_dataDictionary_transformed,
																											cols_special_type_values=dicc_rowFilterMissing_param_filter,
																											filter_type=FilterType(0))
	rowFilterMissing_Equipment__output_dataDictionary_df=rowFilterMissing_Equipment__input_dataDictionary_transformed
	rowFilterMissing_Equipment__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')
	rowFilterMissing_Equipment__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')
	

set_logger("transformations")
generateWorkflow()
