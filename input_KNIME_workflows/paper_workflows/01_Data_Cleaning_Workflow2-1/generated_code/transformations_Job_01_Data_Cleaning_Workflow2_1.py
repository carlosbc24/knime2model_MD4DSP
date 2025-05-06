import pandas as pd
import numpy as np
import functions.data_transformations as data_transformations
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_input_dataDictionary.parquet')

	columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_transformed=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df.copy()
	field_list_columnFilter_param_field=['Thinness_ten_nineteen_years', 'Thinness_five_nine_years']
	
	columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_transformed=data_transformations.transform_filter_columns(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_transformed,
																	columns=field_list_columnFilter_param_field, belong_op=Belong.BELONG)
	
	columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__output_dataDictionary_df=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_transformed
	columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	

set_logger("transformations")
generateWorkflow()
