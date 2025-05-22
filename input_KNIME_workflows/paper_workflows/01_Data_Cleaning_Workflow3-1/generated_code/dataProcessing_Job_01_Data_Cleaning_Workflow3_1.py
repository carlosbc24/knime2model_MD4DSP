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
	binner_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_input_dataDictionary.parquet')

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

	if contract_pre_post.check_fix_value_range(value='Asia', data_dictionary=rowFilterPrimitive_Region__input_dataDictionary_df, belong_op=Belong(0), field='Region',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('PRECONDITION Row Filter(Region) FixValue:Asia VALIDATED')
	else:
		print('PRECONDITION Row Filter(Region) FixValue:Asia NOT VALIDATED')
	
	rowFilterPrimitive_Region__input_dataDictionary_transformed=rowFilterPrimitive_Region__input_dataDictionary_df.copy()
	columns_rowFilterPrimitive_param_filter=['Region']
	
	filter_fix_value_list_rowFilterPrimitive_param_filter=['Asia']
	
	rowFilterPrimitive_Region__input_dataDictionary_transformed=data_transformations.transform_filter_rows_primitive(data_dictionary=rowFilterPrimitive_Region__input_dataDictionary_transformed,
																											columns=columns_rowFilterPrimitive_param_filter,
																		                                    filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_param_filter,
																											filter_type=FilterType(1))
	rowFilterPrimitive_Region__output_dataDictionary_df=rowFilterPrimitive_Region__input_dataDictionary_transformed
	rowFilterPrimitive_Region__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	rowFilterPrimitive_Region__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')
	
	if contract_pre_post.check_fix_value_range(value='Asia', data_dictionary=rowFilterPrimitive_Region__output_dataDictionary_df, belong_op=Belong(0), field='Region',
									quant_abs=None, quant_rel=None, quant_op=None, origin_function="Row Filter"):
		print('POSTCONDITION Row Filter(Region) FixValue:Asia VALIDATED')
	else:
		print('POSTCONDITION Row Filter(Region) FixValue:Asia NOT VALIDATED')
	
	
	
	columns_list_rowFilterPrimitive_Region__INV_condition=['Region']
	filter_fix_value_list_rowFilterPrimitive_Region__INV_condition=['Asia']
	
	if contract_invariants.check_inv_filter_rows_primitive(data_dictionary_in=rowFilterPrimitive_Region__input_dataDictionary_df,
											data_dictionary_out=rowFilterPrimitive_Region__output_dataDictionary_df,
											columns=columns_list_rowFilterPrimitive_Region__INV_condition,
											filter_fix_value_list=filter_fix_value_list_rowFilterPrimitive_Region__INV_condition,
											filter_type=FilterType.INCLUDE, origin_function="Row Filter"):
		print('INVARIANT Row Filter(Region) FilterType:INCLUDE FixValueList:[Asia] VALIDATED')
	else:
		print('INVARIANT Row Filter(Region) FilterType:INCLUDE FixValueList:[Asia] NOT VALIDATED')
	
	
	#-----------------New DataProcessing-----------------
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')

	field_list_columnFilter_PRE_field_range=['Year', 'Infant_deaths', 'Under_five_deaths', 'Adult_mortality', 'Alcohol_consumption', 'Hepatitis_B', 'Measles', 'BMI', 'Polio', 'Diphtheria', 'Incidents_HIV', 'GDP_per_capita', 'Population_mln', 'Thinness_ten_nineteen_years', 'Thinness_five_nine_years', 'Schooling', 'Economy_status_Developed', 'Economy_status_Developing']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_PRE_field_range,
								data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df,
								belong_op=Belong(0), origin_function="Column Filter"):
		print('PRECONDITION Column Filter(Year, Infant_deaths, Under_five_deaths, Adult_mortality, Alcohol_consumption, Hepatitis_B, Measles, BMI, Polio, Diphtheria, Incidents_HIV, GDP_per_capita, Population_mln, Thinness_ten_nineteen_years, Thinness_five_nine_years, Schooling, Economy_status_Developed, Economy_status_Developing) VALIDATED')
	else:
		print('PRECONDITION Column Filter(Year, Infant_deaths, Under_five_deaths, Adult_mortality, Alcohol_consumption, Hepatitis_B, Measles, BMI, Polio, Diphtheria, Incidents_HIV, GDP_per_capita, Population_mln, Thinness_ten_nineteen_years, Thinness_five_nine_years, Schooling, Economy_status_Developed, Economy_status_Developing) NOT VALIDATED')
	
	
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_transformed=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df.copy()
	field_list_columnFilter_param_field=['Year', 'Infant_deaths', 'Under_five_deaths', 'Adult_mortality', 'Alcohol_consumption', 'Hepatitis_B', 'Measles', 'BMI', 'Polio', 'Diphtheria', 'Incidents_HIV', 'GDP_per_capita', 'Population_mln', 'Thinness_ten_nineteen_years', 'Thinness_five_nine_years', 'Schooling', 'Economy_status_Developed', 'Economy_status_Developing']
	
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_transformed=data_transformations.transform_filter_columns(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_transformed,
																	columns=field_list_columnFilter_param_field, belong_op=Belong.BELONG)
	
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__output_dataDictionary_df=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_transformed
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	
	field_list_columnFilter_POST_field_range=['Year', 'Infant_deaths', 'Under_five_deaths', 'Adult_mortality', 'Alcohol_consumption', 'Hepatitis_B', 'Measles', 'BMI', 'Polio', 'Diphtheria', 'Incidents_HIV', 'GDP_per_capita', 'Population_mln', 'Thinness_ten_nineteen_years', 'Thinness_five_nine_years', 'Schooling', 'Economy_status_Developed', 'Economy_status_Developing']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_POST_field_range,
								data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__output_dataDictionary_df,
								belong_op=Belong(1), origin_function="Column Filter"):
		print('POSTCONDITION Column Filter(Year, Infant_deaths, Under_five_deaths, Adult_mortality, Alcohol_consumption, Hepatitis_B, Measles, BMI, Polio, Diphtheria, Incidents_HIV, GDP_per_capita, Population_mln, Thinness_ten_nineteen_years, Thinness_five_nine_years, Schooling, Economy_status_Developed, Economy_status_Developing) VALIDATED')
	else:
		print('POSTCONDITION Column Filter(Year, Infant_deaths, Under_five_deaths, Adult_mortality, Alcohol_consumption, Hepatitis_B, Measles, BMI, Polio, Diphtheria, Incidents_HIV, GDP_per_capita, Population_mln, Thinness_ten_nineteen_years, Thinness_five_nine_years, Schooling, Economy_status_Developed, Economy_status_Developing) NOT VALIDATED')
	
	
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
