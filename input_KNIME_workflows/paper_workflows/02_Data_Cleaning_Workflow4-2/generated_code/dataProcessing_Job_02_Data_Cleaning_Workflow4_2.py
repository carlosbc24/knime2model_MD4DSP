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
	binner_Longitude__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_input_dataDictionary.parquet')

	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_Longitude__input_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='Longitude', origin_function="Rule Engine"):
		print('PRECONDITION Rule Engine(Longitude) Interval:[0.0, 1000.0] VALIDATED')
	else:
		print('PRECONDITION Rule Engine(Longitude) Interval:[0.0, 1000.0] NOT VALIDATED')
	
	binner_Longitude__input_dataDictionary_transformed=binner_Longitude__input_dataDictionary_df.copy()
	binner_Longitude__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=binner_Longitude__input_dataDictionary_transformed,
																  data_type_output = DataType(0),
																  field_in = 'Longitude', field_out = 'High Longitudes')
	
	binner_Longitude__output_dataDictionary_df=binner_Longitude__input_dataDictionary_transformed
	binner_Longitude__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Longitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Longitude__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_Longitude__input_dataDictionary_transformed,
																  left_margin=-1.0E9, right_margin=130.0,
																  closure_type=Closure(0),
																  fix_value_output='N',
							                                      data_type_output = DataType(0),
																  field_in = 'Longitude',
																  field_out = 'High Longitudes')
	
	binner_Longitude__output_dataDictionary_df=binner_Longitude__input_dataDictionary_transformed
	binner_Longitude__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Longitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Longitude__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_Longitude__input_dataDictionary_transformed,
																  left_margin=130.0, right_margin=1.0E9,
																  closure_type=Closure(2),
																  fix_value_output='Y',
							                                      data_type_output = DataType(0),
																  field_in = 'Longitude',
																  field_out = 'High Longitudes')
	
	binner_Longitude__output_dataDictionary_df=binner_Longitude__input_dataDictionary_transformed
	binner_Longitude__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Longitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	
	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_Longitude__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='High Longitudes', origin_function="Rule Engine"):
		print('POSTCONDITION Rule Engine(High Longitudes) Interval:(0.0, 1000.0) VALIDATED')
	else:
		print('POSTCONDITION Rule Engine(High Longitudes) Interval:(0.0, 1000.0) NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Longitude__input_dataDictionary_df,
											data_dictionary_out=binner_Longitude__output_dataDictionary_df,
											left_margin=-1.0E9, right_margin=130.0,
											closure_type=Closure(0),
											fix_value_output='N',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Longitude', field_out='High Longitudes', origin_function="Rule Engine"):
		print('INVARIANT Rule Engine(Longitude) Interval:(-1.0E9, 130.0) FixValue:N VALIDATED')
	else:
		print('INVARIANT Rule Engine(Longitude) Interval:(-1.0E9, 130.0) FixValue:N NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Longitude__input_dataDictionary_df,
											data_dictionary_out=binner_Longitude__output_dataDictionary_df,
											left_margin=130.0, right_margin=1.0E9,
											closure_type=Closure(2),
											fix_value_output='Y',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Longitude', field_out='High Longitudes', origin_function="Rule Engine"):
		print('INVARIANT Rule Engine(Longitude) Interval:[130.0, 1.0E9) FixValue:Y VALIDATED')
	else:
		print('INVARIANT Rule Engine(Longitude) Interval:[130.0, 1.0E9) FixValue:Y NOT VALIDATED')
	
	
	
	

set_logger("dataProcessing")
generateWorkflow()
