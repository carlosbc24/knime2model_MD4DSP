import pandas as pd
import numpy as np
import functions.contract_invariants as contract_invariants
import functions.contract_pre_post as contract_pre_post
import functions.data_transformations as data_transformations
import functions.data_smells as data_smells
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():
	#-----------------New DataProcessing-----------------
	mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation_input_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Latitude')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Altitude')
	
	data_smells.check_integer_as_floating_point(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Latitude')
	data_smells.check_integer_as_floating_point(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Altitude')
	
	data_smells.check_types_as_string(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Latitude', expected_type=DataType.DOUBLE)
	data_smells.check_types_as_string(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Altitude', expected_type=DataType.DOUBLE)
	
	data_smells.check_special_character_spacing(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Latitude')
	data_smells.check_special_character_spacing(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Altitude')
	
	data_smells.check_suspect_precision(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Latitude')
	data_smells.check_suspect_precision(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Altitude')
	
	data_smells.check_suspect_distribution(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, min_value=440.0, max_value=1600.0, field='Latitude')
	data_smells.check_suspect_distribution(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, min_value=440.0, max_value=1600.0, field='Altitude')
	
	data_smells.check_date_as_datetime(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Latitude')
	data_smells.check_date_as_datetime(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Altitude')
	
	data_smells.check_separating_consistency(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, decimal_sep='.',  field='Latitude')
	data_smells.check_separating_consistency(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, decimal_sep='.',  field='Altitude')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Latitude')
	data_smells.check_ambiguous_datetime_format(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Altitude')
	
	
	missing_values_mathOperation_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Latitude', 
									missing_values=missing_values_mathOperation_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Math Formula"):
		print('PRECONDITION Math Formula(Latitude) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Math Formula(Latitude) MissingValues:[] NOT VALIDATED')
	missing_values_mathOperation_PRE_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(0), data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df, field='Altitude', 
									missing_values=missing_values_mathOperation_PRE_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Math Formula"):
		print('PRECONDITION Math Formula(Altitude) MissingValues:[] VALIDATED')
	else:
		print('PRECONDITION Math Formula(Altitude) MissingValues:[] NOT VALIDATED')
	
	mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_transformed=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df.copy()
	mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_transformed,
																  data_type_output = DataType(5),
																  field_in = 'Latitude', field_out = 'Difference in Latitude/Altitude')
	
	mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_transformed
	mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mathOperation_output_dataDictionary.parquet')
	mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation_output_dataDictionary.parquet')
	mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_transformed=data_transformations.transform_math_operation(data_dictionary=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_transformed,
																math_op=MathOperator(1), field_out='Difference in Latitude/Altitude',
																firstOperand='Latitude', isFieldFirst=True,secondOperand='Altitude', isFieldSecond=True)
	
	mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_transformed
	mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/mathOperation_output_dataDictionary.parquet')
	mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/mathOperation_output_dataDictionary.parquet')
	
	missing_values_mathOperation_POST_valueRange=[]
	if contract_pre_post.check_missing_range(belong_op=Belong(1), data_dictionary=mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df, field='Difference in Latitude/Altitude', 
									missing_values=missing_values_mathOperation_POST_valueRange,
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Math Formula"):
		print('POSTCONDITION Math Formula(Difference in Latitude/Altitude) MissingValues:[] VALIDATED')
	else:
		print('POSTCONDITION Math Formula(Difference in Latitude/Altitude) MissingValues:[] NOT VALIDATED')
	
	if contract_invariants.check_inv_math_operation(data_dictionary_in=mathOperation_Difference_in_Latitude_Altitude__input_dataDictionary_df,
											data_dictionary_out=mathOperation_Difference_in_Latitude_Altitude__output_dataDictionary_df,
											math_op=MathOperator(1),
											firstOperand='Latitude', isFieldFirst=True, secondOperand='Altitude', isFieldSecond=True, 
											belong_op_out=Belong(0), field_in='Latitude', field_out='Difference in Latitude/Altitude', origin_function="Math Formula"):
		print('INVARIANT Math Formula(Difference in Latitude/Altitude) substract(Latitude, Altitude, ) VALIDATED')
	else:
		print('INVARIANT Math FormulaDifference in Latitude/Altitude substract(Latitude, Altitude, ) NOT VALIDATED')
	
	
	
	
	
	

set_logger("dataProcessing")
generateWorkflow()
