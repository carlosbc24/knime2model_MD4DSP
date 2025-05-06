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
	columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_input_dataDictionary.parquet')

	field_list_columnFilter_PRE_field_range=['Thinness_ten_nineteen_years', 'Thinness_five_nine_years']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_PRE_field_range,
								data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df,
								belong_op=Belong(0)):
		print('PRECONDITION columnFilter(Thinness_ten_nineteen_years, Thinness_five_nine_years)_PRE_fieldRange VALIDATED')
	else:
		print('PRECONDITION columnFilter(Thinness_ten_nineteen_years, Thinness_five_nine_years)_PRE_fieldRange NOT VALIDATED')
	
	
	columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_transformed=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df.copy()
	field_list_columnFilter_param_field=['Thinness_ten_nineteen_years', 'Thinness_five_nine_years']
	
	columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_transformed=data_transformations.transform_filter_columns(data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_transformed,
																	columns=field_list_columnFilter_param_field, belong_op=Belong.BELONG)
	
	columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__output_dataDictionary_df=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_transformed
	columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	
	field_list_columnFilter_POST_field_range=['Thinness_ten_nineteen_years', 'Thinness_five_nine_years']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_POST_field_range,
								data_dictionary=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__output_dataDictionary_df,
								belong_op=Belong(1)):
		print('POSTCONDITION columnFilter(Thinness_ten_nineteen_years, Thinness_five_nine_years)_POST_fieldRange VALIDATED')
	else:
		print('POSTCONDITION columnFilter(Thinness_ten_nineteen_years, Thinness_five_nine_years)_POST_fieldRange NOT VALIDATED')
	
	
	columns_list_columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__INV_condition = ['Thinness_ten_nineteen_years', 'Thinness_five_nine_years']
	
	if contract_invariants.check_inv_filter_columns(data_dictionary_in=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__input_dataDictionary_df,
							data_dictionary_out=columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__output_dataDictionary_df,
							columns=columns_list_columnFilter_Thinness_ten_nineteen_years_Thinness_five_nine_years__INV_condition,
							belong_op=Belong(0)):
		print('INVARIANT columnFilter(Thinness_ten_nineteen_years, Thinness_five_nine_years)_INV_condition VALIDATED')
	else:
		print('INVARIANT columnFilter(Thinness_ten_nineteen_years, Thinness_five_nine_years)_INV_condition NOT VALIDATED')
	
	
	
	
	

set_logger("dataProcessing")
generateWorkflow()
