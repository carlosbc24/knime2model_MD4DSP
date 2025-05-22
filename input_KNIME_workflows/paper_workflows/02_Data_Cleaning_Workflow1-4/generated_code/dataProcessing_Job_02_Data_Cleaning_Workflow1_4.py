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
	rowFilterMissing_Equipment__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_input_dataDictionary.parquet')

	missing_values_rowFilterMissing_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=rowFilterMissing_Equipment__input_dataDictionary_df, field='Equipment', 
									missing_values=missing_values_rowFilterMissing_PRE_valueRange,
									quant_op=Operator(3), quant_rel=60.0/100, origin_function="Row Filter"):
		print('PRECONDITION Row Filter(Equipment) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Row Filter(Equipment) MissingValues:[] NOT VALIDATED')
	
	rowFilterMissing_Equipment__input_dataDictionary_transformed=rowFilterMissing_Equipment__input_dataDictionary_df.copy()
	columns_rowFilterMissing_param_filter=['Equipment']
	
	dicc_rowFilterMissing_param_filter={'Equipment':{'missing': []}}
	
	rowFilterMissing_Equipment__input_dataDictionary_transformed=data_transformations.transform_filter_rows_special_values(data_dictionary=rowFilterMissing_Equipment__input_dataDictionary_transformed,
																											cols_special_type_values=dicc_rowFilterMissing_param_filter,
																											filter_type=FilterType(0))
	rowFilterMissing_Equipment__output_dataDictionary_df=rowFilterMissing_Equipment__input_dataDictionary_transformed
	rowFilterMissing_Equipment__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')
	rowFilterMissing_Equipment__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterMissing_output_dataDictionary.parquet')
	
	missing_values_rowFilterMissing_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=rowFilterMissing_Equipment__output_dataDictionary_df, field='Equipment', 
									missing_values=missing_values_rowFilterMissing_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('POSTCONDITION Row Filter(Equipment) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Row Filter(Equipment) MissingValues:[] NOT VALIDATED')
	
	
	
	cols_special_type_values_rowFilterMissing_Equipment__INV_condition={'Equipment':{'missing': []}}
	
	if contract_invariants.check_inv_filter_rows_special_values(data_dictionary_in=rowFilterMissing_Equipment__input_dataDictionary_df,
											data_dictionary_out=rowFilterMissing_Equipment__output_dataDictionary_df,
											cols_special_type_values=cols_special_type_values_rowFilterMissing_Equipment__INV_condition,
											filter_type=FilterType.EXCLUDE, origin_function="Row Filter"):
		print('INVARIANT Row Filter(Equipment) FilterType:EXCLUDE SpecialValues: EquipmentMISSING:[] VALIDATED')
	else:
		print('INVARIANT Row Filter(Equipment) FilterType:EXCLUDE SpecialValues: EquipmentMISSING:[] NOT VALIDATED')
	
	

set_logger("dataProcessing")
generateWorkflow()
