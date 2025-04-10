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
	columnFilter_Country_Region_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Schooling_Economy_status_Developed_Economy_status_Developing_Life_expectancy__input_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_input_dataDictionary.parquet')

	field_list_columnFilter_PRE_field_range=['Country', 'Region', 'Year', 'Infant_deaths', 'Under_five_deaths', 'Adult_mortality', 'Alcohol_consumption', 'Hepatitis_B', 'Measles', 'BMI', 'Polio', 'Diphtheria', 'Incidents_HIV', 'GDP_per_capita', 'Population_mln', 'Schooling', 'Economy_status_Developed', 'Economy_status_Developing', 'Life_expectancy']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_PRE_field_range,
								data_dictionary=columnFilter_Country_Region_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Schooling_Economy_status_Developed_Economy_status_Developing_Life_expectancy__input_dataDictionary_df,
								belong_op=Belong(0)):
		print('PRECONDITION columnFilter(Country, Region, Year, Infant_deaths, Under_five_deaths, Adult_mortality, Alcohol_consumption, Hepatitis_B, Measles, BMI, Polio, Diphtheria, Incidents_HIV, GDP_per_capita, Population_mln, Schooling, Economy_status_Developed, Economy_status_Developing, Life_expectancy)_PRE_fieldRange VALIDATED')
	else:
		print('PRECONDITION columnFilter(Country, Region, Year, Infant_deaths, Under_five_deaths, Adult_mortality, Alcohol_consumption, Hepatitis_B, Measles, BMI, Polio, Diphtheria, Incidents_HIV, GDP_per_capita, Population_mln, Schooling, Economy_status_Developed, Economy_status_Developing, Life_expectancy)_PRE_fieldRange NOT VALIDATED')
	
	
	columnFilter_Country_Region_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Schooling_Economy_status_Developed_Economy_status_Developing_Life_expectancy__input_dataDictionary_transformed=columnFilter_Country_Region_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Schooling_Economy_status_Developed_Economy_status_Developing_Life_expectancy__input_dataDictionary_df.copy()
	field_list_columnFilter_param_field=['Country', 'Region', 'Year', 'Infant_deaths', 'Under_five_deaths', 'Adult_mortality', 'Alcohol_consumption', 'Hepatitis_B', 'Measles', 'BMI', 'Polio', 'Diphtheria', 'Incidents_HIV', 'GDP_per_capita', 'Population_mln', 'Schooling', 'Economy_status_Developed', 'Economy_status_Developing', 'Life_expectancy']
	
	columnFilter_Country_Region_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Schooling_Economy_status_Developed_Economy_status_Developing_Life_expectancy__input_dataDictionary_transformed=data_transformations.transform_filter_columns(data_dictionary=columnFilter_Country_Region_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Schooling_Economy_status_Developed_Economy_status_Developing_Life_expectancy__input_dataDictionary_transformed,
																	columns=field_list_columnFilter_param_field, belong_op=Belong.BELONG)
	
	columnFilter_Country_Region_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Schooling_Economy_status_Developed_Economy_status_Developing_Life_expectancy__output_dataDictionary_df=columnFilter_Country_Region_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Schooling_Economy_status_Developed_Economy_status_Developing_Life_expectancy__input_dataDictionary_transformed
	columnFilter_Country_Region_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Schooling_Economy_status_Developed_Economy_status_Developing_Life_expectancy__output_dataDictionary_df.to_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	columnFilter_Country_Region_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Schooling_Economy_status_Developed_Economy_status_Developing_Life_expectancy__output_dataDictionary_df=pd.read_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
	
	field_list_columnFilter_POST_field_range=['Country', 'Region', 'Year', 'Infant_deaths', 'Under_five_deaths', 'Adult_mortality', 'Alcohol_consumption', 'Hepatitis_B', 'Measles', 'BMI', 'Polio', 'Diphtheria', 'Incidents_HIV', 'GDP_per_capita', 'Population_mln', 'Schooling', 'Economy_status_Developed', 'Economy_status_Developing', 'Life_expectancy']
	if contract_pre_post.check_field_range(fields=field_list_columnFilter_POST_field_range,
								data_dictionary=columnFilter_Country_Region_Year_Infant_deaths_Under_five_deaths_Adult_mortality_Alcohol_consumption_Hepatitis_B_Measles_BMI_Polio_Diphtheria_Incidents_HIV_GDP_per_capita_Population_mln_Schooling_Economy_status_Developed_Economy_status_Developing_Life_expectancy__output_dataDictionary_df,
								belong_op=Belong(0)):
		print('POSTCONDITION columnFilter(Country, Region, Year, Infant_deaths, Under_five_deaths, Adult_mortality, Alcohol_consumption, Hepatitis_B, Measles, BMI, Polio, Diphtheria, Incidents_HIV, GDP_per_capita, Population_mln, Schooling, Economy_status_Developed, Economy_status_Developing, Life_expectancy)_POST_fieldRange VALIDATED')
	else:
		print('POSTCONDITION columnFilter(Country, Region, Year, Infant_deaths, Under_five_deaths, Adult_mortality, Alcohol_consumption, Hepatitis_B, Measles, BMI, Polio, Diphtheria, Incidents_HIV, GDP_per_capita, Population_mln, Schooling, Economy_status_Developed, Economy_status_Developing, Life_expectancy)_POST_fieldRange NOT VALIDATED')
	
	

set_logger("dataProcessing")
generateWorkflow()
