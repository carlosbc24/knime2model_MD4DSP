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
	binner_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_input_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=binner_Life_expectancy__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Life_expectancy')
	
	data_smells.check_integer_as_floating_point(data_dictionary=binner_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_types_as_string(data_dictionary=binner_Life_expectancy__input_dataDictionary_df, field='Life_expectancy', expected_type=DataType.INTEGER)
	
	data_smells.check_special_character_spacing(data_dictionary=binner_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_suspect_precision(data_dictionary=binner_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_suspect_distribution(data_dictionary=binner_Life_expectancy__input_dataDictionary_df, min_value=0.0, max_value=8.0, field='Life_expectancy')
	
	data_smells.check_date_as_datetime(data_dictionary=binner_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	data_smells.check_separating_consistency(data_dictionary=binner_Life_expectancy__input_dataDictionary_df, decimal_sep='.',  field='Life_expectancy')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=binner_Life_expectancy__input_dataDictionary_df, field='Life_expectancy')
	
	
	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_Life_expectancy__input_dataDictionary_df,
	                                	closure_type=Closure(3), belong_op=Belong(0), field='Life_expectancy', origin_function="Rule Engine"):
		print('PRECONDITION Rule Engine(Life_expectancy) Interval:[0.0, 1000.0] VALIDATED')
	else:
		print('PRECONDITION Rule Engine(Life_expectancy) Interval:[0.0, 1000.0] NOT VALIDATED')
	
	binner_Life_expectancy__input_dataDictionary_transformed=binner_Life_expectancy__input_dataDictionary_df.copy()
	binner_Life_expectancy__input_dataDictionary_transformed=data_transformations.transform_derived_field(data_dictionary=binner_Life_expectancy__input_dataDictionary_transformed,
																  data_type_output = DataType(0),
																  field_in = 'Life_expectancy', field_out = 'Life-Expectancy (High/Low/Avg)')
	
	binner_Life_expectancy__output_dataDictionary_df=binner_Life_expectancy__input_dataDictionary_transformed
	binner_Life_expectancy__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Life_expectancy__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Life_expectancy__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_Life_expectancy__input_dataDictionary_transformed,
																  left_margin=-1.0E9, right_margin=40.0,
																  closure_type=Closure(1),
																  fix_value_output='Low',
							                                      data_type_output = DataType(0),
																  field_in = 'Life_expectancy',
																  field_out = 'Life-Expectancy (High/Low/Avg)')
	
	binner_Life_expectancy__output_dataDictionary_df=binner_Life_expectancy__input_dataDictionary_transformed
	binner_Life_expectancy__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Life_expectancy__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Life_expectancy__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_Life_expectancy__input_dataDictionary_transformed,
																  left_margin=70.0, right_margin=1.0E9,
																  closure_type=Closure(2),
																  fix_value_output='High',
							                                      data_type_output = DataType(0),
																  field_in = 'Life_expectancy',
																  field_out = 'Life-Expectancy (High/Low/Avg)')
	
	binner_Life_expectancy__output_dataDictionary_df=binner_Life_expectancy__input_dataDictionary_transformed
	binner_Life_expectancy__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Life_expectancy__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Life_expectancy__input_dataDictionary_transformed=data_transformations.transform_interval_fix_value(data_dictionary=binner_Life_expectancy__input_dataDictionary_transformed,
																  left_margin=40.0, right_margin=70.0,
																  closure_type=Closure(0),
																  fix_value_output='Average',
							                                      data_type_output = DataType(0),
																  field_in = 'Life_expectancy',
																  field_out = 'Life-Expectancy (High/Low/Avg)')
	
	binner_Life_expectancy__output_dataDictionary_df=binner_Life_expectancy__input_dataDictionary_transformed
	binner_Life_expectancy__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	binner_Life_expectancy__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')
	
	if contract_pre_post.check_interval_range_float(left_margin=0.0, right_margin=1000.0, data_dictionary=binner_Life_expectancy__output_dataDictionary_df,
	                                	closure_type=Closure(0), belong_op=Belong(1), field='Life-Expectancy (High/Low/Avg)', origin_function="Rule Engine"):
		print('POSTCONDITION Rule Engine(Life-Expectancy (High/Low/Avg)) Interval:(0.0, 1000.0) VALIDATED')
	else:
		print('POSTCONDITION Rule Engine(Life-Expectancy (High/Low/Avg)) Interval:(0.0, 1000.0) NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Life_expectancy__input_dataDictionary_df,
											data_dictionary_out=binner_Life_expectancy__output_dataDictionary_df,
											left_margin=-1.0E9, right_margin=40.0,
											closure_type=Closure(1),
											fix_value_output='Low',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Life_expectancy', field_out='Life-Expectancy (High/Low/Avg)', origin_function="Rule Engine"):
		print('INVARIANT Rule Engine(Life_expectancy) Interval:(-1.0E9, 40.0] FixValue:Low VALIDATED')
	else:
		print('INVARIANT Rule Engine(Life_expectancy) Interval:(-1.0E9, 40.0] FixValue:Low NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Life_expectancy__input_dataDictionary_df,
											data_dictionary_out=binner_Life_expectancy__output_dataDictionary_df,
											left_margin=70.0, right_margin=1.0E9,
											closure_type=Closure(2),
											fix_value_output='High',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Life_expectancy', field_out='Life-Expectancy (High/Low/Avg)', origin_function="Rule Engine"):
		print('INVARIANT Rule Engine(Life_expectancy) Interval:[70.0, 1.0E9) FixValue:High VALIDATED')
	else:
		print('INVARIANT Rule Engine(Life_expectancy) Interval:[70.0, 1.0E9) FixValue:High NOT VALIDATED')
	
	if contract_invariants.check_inv_interval_fix_value(data_dictionary_in=binner_Life_expectancy__input_dataDictionary_df,
											data_dictionary_out=binner_Life_expectancy__output_dataDictionary_df,
											left_margin=40.0, right_margin=70.0,
											closure_type=Closure(0),
											fix_value_output='Average',
											belong_op_in=Belong(0), belong_op_out=Belong(0),
											data_type_output=DataType(0),
											field_in='Life_expectancy', field_out='Life-Expectancy (High/Low/Avg)', origin_function="Rule Engine"):
		print('INVARIANT Rule Engine(Life_expectancy) Interval:(40.0, 70.0) FixValue:Average VALIDATED')
	else:
		print('INVARIANT Rule Engine(Life_expectancy) Interval:(40.0, 70.0) FixValue:Average NOT VALIDATED')
	
	
	
	
	#-----------------New DataProcessing-----------------
	rowFilterPrimitive_Region__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=['Africa']
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=rowFilterPrimitive_Region__input_dataDictionary_df, 
														missing_invalid_list=list_invalid, common_missing_invalid_list=common_invalid_list, field='Region')
	
	data_smells.check_integer_as_floating_point(data_dictionary=rowFilterPrimitive_Region__input_dataDictionary_df, field='Region')
	
	data_smells.check_types_as_string(data_dictionary=rowFilterPrimitive_Region__input_dataDictionary_df, field='Region', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=rowFilterPrimitive_Region__input_dataDictionary_df, field='Region')
	
	data_smells.check_suspect_precision(data_dictionary=rowFilterPrimitive_Region__input_dataDictionary_df, field='Region')
	
	data_smells.check_suspect_distribution(data_dictionary=rowFilterPrimitive_Region__input_dataDictionary_df, min_value=9.0, max_value=202.0, field='Region')
	
	data_smells.check_date_as_datetime(data_dictionary=rowFilterPrimitive_Region__input_dataDictionary_df, field='Region')
	
	data_smells.check_separating_consistency(data_dictionary=rowFilterPrimitive_Region__input_dataDictionary_df, decimal_sep='.',  field='Region')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=rowFilterPrimitive_Region__input_dataDictionary_df, field='Region')
	
	
	if contract_pre_post.check_fix_value_range(value='Africa', is_substring=False, data_dictionary=rowFilterPrimitive_Region__input_dataDictionary_df, belong_op=Belong(0), field='Region',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('PRECONDITION Row Filter(Region) FixValue:Africa VALIDATED')
	else:
		print('PRECONDITION Row Filter(Region) FixValue:Africa NOT VALIDATED')
	
	rowFilterPrimitive_Region__input_dataDictionary_transformed=rowFilterPrimitive_Region__input_dataDictionary_df.copy()
	columns_rowFilterPrimitive_param_filter=['Region']
	filter_fix_value_list_rowFilterPrimitive_param_filter=['Africa']
	
	rowFilterPrimitive_Region__input_dataDictionary_transformed=data_transformations.transform_filter_rows_primitive(data_dictionary=rowFilterPrimitive_Region__input_dataDictionary_transformed,
																											columns=columns_rowFilterPrimitive_param_filter,
																		                                    filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_param_filter,
																											filter_type=FilterType(1))
	rowFilterPrimitive_Region__output_dataDictionary_df=rowFilterPrimitive_Region__input_dataDictionary_transformed
	rowFilterPrimitive_Region__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	rowFilterPrimitive_Region__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	
	if contract_pre_post.check_fix_value_range(value='Africa', is_substring=False, data_dictionary=rowFilterPrimitive_Region__output_dataDictionary_df, belong_op=Belong(0), field='Region',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('POSTCONDITION Row Filter(Region) FixValue:Africa VALIDATED')
	else:
		print('POSTCONDITION Row Filter(Region) FixValue:Africa NOT VALIDATED')
	
	
	
	columns_list_rowFilterPrimitive_Region__INV_condition=['Region']
	filter_fix_value_list_rowFilterPrimitive_Region__INV_condition=['Africa']
	
	if contract_invariants.check_inv_filter_rows_primitive(data_dictionary_in=rowFilterPrimitive_Region__input_dataDictionary_df,
											data_dictionary_out=rowFilterPrimitive_Region__output_dataDictionary_df,
											columns=columns_list_rowFilterPrimitive_Region__INV_condition,
											filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_Region__INV_condition,
											filter_type=FilterType.INCLUDE, origin_function="Row Filter"):
		print('INVARIANT Row Filter(Region) FilterType:INCLUDE FixValueList:[Africa] VALIDATED')
	else:
		print('INVARIANT Row Filter(Region) FilterType:INCLUDE FixValueList:[Africa] NOT VALIDATED')
	
	
	#-----------------New DataProcessing-----------------
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')

	
	common_invalid_list=['inf', '-inf', 'nan']
	common_missing_list=['', '?', '.','null','none','na']
	
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Year')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Infant_deaths')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Under_five_deaths')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Adult_mortality')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Alcohol_consumption')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Hepatitis_B')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Measles')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='BMI')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Polio')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Diphtheria')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Incidents_HIV')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='GDP_per_capita')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Population_mln')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Thinness_ten_nineteen_years')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Thinness_five_nine_years')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Schooling')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Economy_status_Developed')
	list_missing=[]
	list_invalid=[]
	
	data_smells.check_missing_invalid_value_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, 
														missing_invalid_list=[], common_missing_invalid_list=common_missing_list, field='Economy_status_Developing')
	
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Year')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Infant_deaths')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Under_five_deaths')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Adult_mortality')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Alcohol_consumption')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Hepatitis_B')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Measles')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='BMI')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Polio')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Diphtheria')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Incidents_HIV')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='GDP_per_capita')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Population_mln')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Thinness_ten_nineteen_years')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Thinness_five_nine_years')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Schooling')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Economy_status_Developed')
	data_smells.check_integer_as_floating_point(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Economy_status_Developing')
	
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Year', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Infant_deaths', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Under_five_deaths', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Adult_mortality', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Alcohol_consumption', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Hepatitis_B', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Measles', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='BMI', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Polio', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Diphtheria', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Incidents_HIV', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='GDP_per_capita', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Population_mln', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Thinness_ten_nineteen_years', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Thinness_five_nine_years', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Schooling', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Economy_status_Developed', expected_type=DataType.STRING)
	data_smells.check_types_as_string(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Economy_status_Developing', expected_type=DataType.STRING)
	
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Year')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Infant_deaths')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Under_five_deaths')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Adult_mortality')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Alcohol_consumption')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Hepatitis_B')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Measles')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='BMI')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Polio')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Diphtheria')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Incidents_HIV')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='GDP_per_capita')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Population_mln')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Thinness_ten_nineteen_years')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Thinness_five_nine_years')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Schooling')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Economy_status_Developed')
	data_smells.check_special_character_spacing(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Economy_status_Developing')
	
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Year')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Infant_deaths')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Under_five_deaths')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Adult_mortality')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Alcohol_consumption')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Hepatitis_B')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Measles')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='BMI')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Polio')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Diphtheria')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Incidents_HIV')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='GDP_per_capita')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Population_mln')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Thinness_ten_nineteen_years')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Thinness_five_nine_years')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Schooling')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Economy_status_Developed')
	data_smells.check_suspect_precision(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Economy_status_Developing')
	
	
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Year')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Infant_deaths')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Under_five_deaths')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Adult_mortality')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Alcohol_consumption')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Hepatitis_B')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Measles')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='BMI')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Polio')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Diphtheria')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Incidents_HIV')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='GDP_per_capita')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Population_mln')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Thinness_ten_nineteen_years')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Thinness_five_nine_years')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Schooling')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Economy_status_Developed')
	data_smells.check_date_as_datetime(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Economy_status_Developing')
	
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Year')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Infant_deaths')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Under_five_deaths')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Adult_mortality')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Alcohol_consumption')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Hepatitis_B')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Measles')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='BMI')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Polio')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Diphtheria')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Incidents_HIV')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='GDP_per_capita')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Population_mln')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Thinness_ten_nineteen_years')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Thinness_five_nine_years')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Schooling')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Economy_status_Developed')
	data_smells.check_separating_consistency(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, decimal_sep='.',  field='Economy_status_Developing')
	
	
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Year')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Infant_deaths')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Under_five_deaths')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Adult_mortality')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Alcohol_consumption')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Hepatitis_B')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Measles')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='BMI')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Polio')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Diphtheria')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Incidents_HIV')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='GDP_per_capita')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Population_mln')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Thinness_ten_nineteen_years')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Thinness_five_nine_years')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Schooling')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Economy_status_Developed')
	data_smells.check_ambiguous_datetime_format(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df, field='Economy_status_Developing')
	
	
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_transformed=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df.copy()
	field_list_columnFilter_param_field=['Year', 'Infant_deaths', 'Under_five_deaths', 'Adult_mortality', 'Alcohol_consumption', 'Hepatitis_B', 'Measles', 'BMI', 'Polio', 'Diphtheria', 'Incidents_HIV', 'GDP_per_capita', 'Population_mln', 'Thinness_ten_nineteen_years', 'Thinness_five_nine_years', 'Schooling', 'Economy_status_Developed', 'Economy_status_Developing']
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_transformed=data_transformations.transform_filter_columns(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_transformed,
																	columns=field_list_columnFilter_param_field, belong_op=Belong.BELONG)
	
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__output_dataDictionary_df=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_transformed
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	
	columns_list_columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__INV_condition = ['Year', 'Infant_deaths', 'Under_five_deaths', 'Adult_mortality', 'Alcohol_consumption', 'Hepatitis_B', 'Measles', 'BMI', 'Polio', 'Diphtheria', 'Incidents_HIV', 'GDP_per_capita', 'Population_mln', 'Thinness_ten_nineteen_years', 'Thinness_five_nine_years', 'Schooling', 'Economy_status_Developed', 'Economy_status_Developing']
	
	if contract_invariants.check_inv_filter_columns(data_dictionary_in=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df,
							data_dictionary_out=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__output_dataDictionary_df,
							columns=columns_list_columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__INV_condition,
							belong_op=Belong(0), origin_function="Column Filter"):
		print('INVARIANT Column Filter(Year, Infant_deaths, Under_five_deaths, Adult_mortality, Alcohol_consumption, Hepatitis_B, Measles, BMI, Polio, Diphtheria, Incidents_HIV, GDP_per_capita, Population_mln, Thinness_ten_nineteen_years, Thinness_five_nine_years, Schooling, Economy_status_Developed, Economy_status_Developing) VALIDATED')
	else:
		print('INVARIANT Column Filter(Year, Infant_deaths, Under_five_deaths, Adult_mortality, Alcohol_consumption, Hepatitis_B, Measles, BMI, Polio, Diphtheria, Incidents_HIV, GDP_per_capita, Population_mln, Thinness_ten_nineteen_years, Thinness_five_nine_years, Schooling, Economy_status_Developed, Economy_status_Developing) NOT VALIDATED')
	
	
	
	
	



set_logger("dataProcessing")
generateWorkflow()
