import pandas as pd
import numpy as np
import functions.data_transformations as data_transformations
from helpers.enumerations import Belong, Operator, Operation, SpecialType, DataType, DerivedType, Closure, FilterType, MapOperation, MathOperator
from helpers.logger import set_logger
import pyarrow
from functions.PMML import PMMLModel

def generateWorkflow():

	#-----------------New DataProcessing-----------------
	binner_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_input_dataDictionary.parquet')
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
	
	#-----------------New DataProcessing-----------------
	rowFilterPrimitive_Region__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/binner_output_dataDictionary.parquet')

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
	
	#-----------------New DataProcessing-----------------
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/rowFilterPrimitive_output_dataDictionary.parquet')

	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_transformed=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_df.copy()
	field_list_columnFilter_param_field=['Year', 'Infant_deaths', 'Under_five_deaths', 'Adult_mortality', 'Alcohol_consumption', 'Hepatitis_B', 'Measles', 'BMI', 'Polio', 'Diphtheria', 'Incidents_HIV', 'GDP_per_capita', 'Population_mln', 'Thinness_ten_nineteen_years', 'Thinness_five_nine_years', 'Schooling', 'Economy_status_Developed', 'Economy_status_Developing']
	
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_transformed=data_transformations.transform_filter_columns(data_dictionary=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_transformed,
																	columns=field_list_columnFilter_param_field, belong_op=Belong.BELONG)
	
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__output_dataDictionary_df=columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__input_dataDictionary_transformed
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	columnFilter_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Thinness_ten_nineteen_years_Thinness_five_nine_years_Schooling_Economy_status_Developed_Economy_status_Developing__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	



set_logger("transformations")
generateWorkflow()
