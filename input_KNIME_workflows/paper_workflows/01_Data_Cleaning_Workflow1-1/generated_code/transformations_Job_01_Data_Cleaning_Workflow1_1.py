import pandas as pd
import numpy as np
import functions.data_transformations as data_transformations
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():

	#-----------------New DataProcessing-----------------
	rowFilterMissing_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.parquet')
	rowFilterMissing_Life_expectancy__input_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.parquet')
	rowFilterMissing_Life_expectancy__input_dataDictionary_transformed=rowFilterMissing_Life_expectancy__input_dataDictionary_df.copy()
	columns_rowFilterMissing_param_filter=['Life_expectancy']
	
	dicc_rowFilterMissing_param_filter={'Life_expectancy':{'missing': []}}
	
	rowFilterMissing_Life_expectancy__input_dataDictionary_transformed=data_transformations.transform_filter_rows_special_values(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_transformed,
																											cols_special_type_values=dicc_rowFilterMissing_param_filter,
																											filter_type=FilterType(0))
	rowFilterMissing_Life_expectancy__output_dataDictionary_df=rowFilterMissing_Life_expectancy__input_dataDictionary_transformed
	rowFilterMissing_Life_expectancy__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')
	rowFilterMissing_Life_expectancy__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	rowFilterRange_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')

	rowFilterRange_Life_expectancy__input_dataDictionary_transformed=rowFilterRange_Life_expectancy__input_dataDictionary_df.copy()
	columns_rowFilterRange_param_filter=['Life_expectancy']
	
	filter_range_left_values_list_rowFilterRange_param_filter=[0.0]
	filter_range_right_values_list_rowFilterRange_param_filter=[0.0]
	closure_type_list_rowFilterRange_param_filter=[Closure(3)]
	
	rowFilterRange_Life_expectancy__input_dataDictionary_transformed=data_transformations.transform_filter_rows_range(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_transformed,
																											columns=columns_rowFilterRange_param_filter,
																											left_margin_list=filter_range_left_values_list_rowFilterRange_param_filter,
																											right_margin_list=filter_range_right_values_list_rowFilterRange_param_filter,
																											filter_type=FilterType(1),
																											closure_type_list=closure_type_list_rowFilterRange_param_filter)
	rowFilterRange_Life_expectancy__output_dataDictionary_df=rowFilterRange_Life_expectancy__input_dataDictionary_transformed
	rowFilterRange_Life_expectancy__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')
	rowFilterRange_Life_expectancy__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterRange_output_dataDictionary.parquet')
	


set_logger("transformations")
generateWorkflow()
