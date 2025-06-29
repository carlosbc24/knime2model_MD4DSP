import pandas as pd
import numpy as np
import functions.data_transformations as data_transformations
import functions.data_smells as data_smells
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():

	#-----------------New DataProcessing-----------------
	rowFilterMissing_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.parquet')
	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Life_expectancy')
	
	data_smells.check_integer_as_floating_point(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_types_as_string(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_suspect_precision(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_suspect_distribution(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, min_value=0.0, max_value=1.0, field='Life_expectancy')
	
	data_smells.check_date_as_datetime(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_separating_consistency(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, decimal_sep='.',  field='Life_expectancy')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=rowFilterMissing_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	

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
	rowFilterPrimitive_Year__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=['2010']
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, 
														missing_invalid_list=list_invalid, common_missing_invalid_list=common_invalid_list, field='Year')
	
	data_smells.check_integer_as_floating_point(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, field='Year')
	
	data_smells.check_types_as_string(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, field='Year', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, field='Year')
	
	data_smells.check_suspect_precision(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, field='Year')
	
	data_smells.check_suspect_distribution(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, min_value=9.0, max_value=202.0, field='Year')
	
	data_smells.check_date_as_datetime(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, field='Year')
	
	data_smells.check_separating_consistency(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, decimal_sep='.',  field='Year')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_df, field='Year')
	
	

	rowFilterPrimitive_Year__input_dataDictionary_transformed=rowFilterPrimitive_Year__input_dataDictionary_df.copy()
	columns_rowFilterPrimitive_param_filter=['Year']
	filter_fix_value_list_rowFilterPrimitive_param_filter=[2010]
	
	rowFilterPrimitive_Year__input_dataDictionary_transformed=data_transformations.transform_filter_rows_primitive(data_dictionary=rowFilterPrimitive_Year__input_dataDictionary_transformed,
																											columns=columns_rowFilterPrimitive_param_filter,
																		                                    filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_param_filter,
																											filter_type=FilterType(1))
	rowFilterPrimitive_Year__output_dataDictionary_df=rowFilterPrimitive_Year__input_dataDictionary_transformed
	rowFilterPrimitive_Year__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	rowFilterPrimitive_Year__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	
	#-----------------New DataProcessing-----------------
	rowFilterRange_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Life_expectancy')
	
	data_smells.check_integer_as_floating_point(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_types_as_string(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, field='Life_expectancy', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_suspect_precision(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_suspect_distribution(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, min_value=-1.0E9, max_value=50.0, field='Life_expectancy')
	
	data_smells.check_date_as_datetime(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_separating_consistency(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, decimal_sep='.',  field='Life_expectancy')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=rowFilterRange_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	

	rowFilterRange_Life_expectancy__input_dataDictionary_transformed=rowFilterRange_Life_expectancy__input_dataDictionary_df.copy()
	columns_rowFilterRange_param_filter=['Life_expectancy']
	filter_range_left_values_list_rowFilterRange_param_filter=[-np.inf]
	filter_range_right_values_list_rowFilterRange_param_filter=[50.0]
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
