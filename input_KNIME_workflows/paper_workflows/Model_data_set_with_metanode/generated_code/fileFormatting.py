import pandas as pd
import json
import h5py
import pyarrow
				
stringToNumber_TERRITORY_Instate__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/ruleEngine_instate_output_dataDictionary.csv', sep = ',')
stringToNumber_TERRITORY_Instate__input_dataDictionary.to_parquet('/wf_validation_python/data/output/ruleEngine_instate_output_dataDictionary.parquet')
				
stringToNumber_TERRITORY_Instate__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/stringToNumber_output_dataDictionary.csv', sep = ',')
stringToNumber_TERRITORY_Instate__output_dataDictionary.to_parquet('/wf_validation_python/data/output/stringToNumber_output_dataDictionary.parquet')
				
mapping_Instate__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/ruleEngine_territory_output_dataDictionary.csv', sep = ',')
mapping_Instate__input_dataDictionary.to_parquet('/wf_validation_python/data/output/ruleEngine_territory_output_dataDictionary.parquet')
				
mapping_Instate__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/ruleEngine_instate_output_dataDictionary.csv', sep = ',')
mapping_Instate__output_dataDictionary.to_parquet('/wf_validation_python/data/output/ruleEngine_instate_output_dataDictionary.parquet')
				
mapping_TERRITORY__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/columnFilter_output_dataDictionary.csv', sep = ',')
mapping_TERRITORY__input_dataDictionary.to_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
				
mapping_TERRITORY__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/ruleEngine_territory_output_dataDictionary.csv', sep = ',')
mapping_TERRITORY__output_dataDictionary.to_parquet('/wf_validation_python/data/output/ruleEngine_territory_output_dataDictionary.parquet')
				
rowFilterRange_init_span__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/missing_output_dataDictionary.csv', sep = ',')
rowFilterRange_init_span__input_dataDictionary.to_parquet('/wf_validation_python/data/output/missing_output_dataDictionary.parquet')
				
rowFilterRange_init_span__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilter_output_dataDictionary.csv', sep = ',')
rowFilterRange_init_span__output_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilter_output_dataDictionary.parquet')
				
columnFilter_ETHNICITY_TERRITORY_ACADEMIC_INTEREST_1_ACADEMIC_INTEREST_2_Enroll_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS_CAMPUS_VISIT_IRSCHOOL_satscore_sex_mailq_premiere_init_span_int1rat_int2rat_hscrat_avg_income_distance_Instate__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/rowFilter_output_dataDictionary.csv', sep = ',')
columnFilter_ETHNICITY_TERRITORY_ACADEMIC_INTEREST_1_ACADEMIC_INTEREST_2_Enroll_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS_CAMPUS_VISIT_IRSCHOOL_satscore_sex_mailq_premiere_init_span_int1rat_int2rat_hscrat_avg_income_distance_Instate__input_dataDictionary.to_parquet('/wf_validation_python/data/output/rowFilter_output_dataDictionary.parquet')
				
columnFilter_ETHNICITY_TERRITORY_ACADEMIC_INTEREST_1_ACADEMIC_INTEREST_2_Enroll_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS_CAMPUS_VISIT_IRSCHOOL_satscore_sex_mailq_premiere_init_span_int1rat_int2rat_hscrat_avg_income_distance_Instate__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/columnFilter_output_dataDictionary.csv', sep = ',')
columnFilter_ETHNICITY_TERRITORY_ACADEMIC_INTEREST_1_ACADEMIC_INTEREST_2_Enroll_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS_CAMPUS_VISIT_IRSCHOOL_satscore_sex_mailq_premiere_init_span_int1rat_int2rat_hscrat_avg_income_distance_Instate__output_dataDictionary.to_parquet('/wf_validation_python/data/output/columnFilter_output_dataDictionary.parquet')
				
imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/stringToNumber_output_dataDictionary.csv', sep = ',')
imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary.to_parquet('/wf_validation_python/data/output/stringToNumber_output_dataDictionary.parquet')
				
imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/numericOutliers_output_dataDictionary.csv', sep = ',')
imputeOutlierByClosest_avg_income_distance_Instate__output_dataDictionary.to_parquet('/wf_validation_python/data/output/numericOutliers_output_dataDictionary.parquet')
				
imputeMissingByMostFrequent_sex_ETHNICITY_IRSCHOOL__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/missing_input_dataDictionary.csv', sep = ',')
imputeMissingByMostFrequent_sex_ETHNICITY_IRSCHOOL__input_dataDictionary.to_parquet('/wf_validation_python/data/output/missing_input_dataDictionary.parquet')
				
imputeMissingByMostFrequent_sex_ETHNICITY_IRSCHOOL__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/missing_output_dataDictionary.csv', sep = ',')
imputeMissingByMostFrequent_sex_ETHNICITY_IRSCHOOL__output_dataDictionary.to_parquet('/wf_validation_python/data/output/missing_output_dataDictionary.parquet')
				
imputeMissingByMean_satscore__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/missing_output_dataDictionary.csv', sep = ',')
imputeMissingByMean_satscore__input_dataDictionary.to_parquet('/wf_validation_python/data/output/missing_output_dataDictionary.parquet')
				
imputeMissingByMean_satscore__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/missing_output_dataDictionary.csv', sep = ',')
imputeMissingByMean_satscore__output_dataDictionary.to_parquet('/wf_validation_python/data/output/missing_output_dataDictionary.parquet')
				
imputeMissingByMean_avg_income_distance__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/missing_output_dataDictionary.csv', sep = ',')
imputeMissingByMean_avg_income_distance__input_dataDictionary.to_parquet('/wf_validation_python/data/output/missing_output_dataDictionary.parquet')
				
imputeMissingByMean_avg_income_distance__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/missing_output_dataDictionary.csv', sep = ',')
imputeMissingByMean_avg_income_distance__output_dataDictionary.to_parquet('/wf_validation_python/data/output/missing_output_dataDictionary.parquet')
				
imputeMissingByFixValue_ACADEMIC_INTEREST_1_ACADEMIC_INTEREST_2__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/missing_output_dataDictionary.csv', sep = ',')
imputeMissingByFixValue_ACADEMIC_INTEREST_1_ACADEMIC_INTEREST_2__input_dataDictionary.to_parquet('/wf_validation_python/data/output/missing_output_dataDictionary.parquet')
				
imputeMissingByFixValue_ACADEMIC_INTEREST_1_ACADEMIC_INTEREST_2__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/missing_output_dataDictionary.csv', sep = ',')
imputeMissingByFixValue_ACADEMIC_INTEREST_1_ACADEMIC_INTEREST_2__output_dataDictionary.to_parquet('/wf_validation_python/data/output/missing_output_dataDictionary.parquet')
				
binner_satscore__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/numericOutliers_output_dataDictionary.csv', sep = ',')
binner_satscore__input_dataDictionary.to_parquet('/wf_validation_python/data/output/numericOutliers_output_dataDictionary.parquet')
				
binner_satscore__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/numericBinner_output_dataDictionary.csv', sep = ',')
binner_satscore__output_dataDictionary.to_parquet('/wf_validation_python/data/output/numericBinner_output_dataDictionary.parquet')
				
binner_TERRITORY__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/numericBinner_output_dataDictionary.csv', sep = ',')
binner_TERRITORY__input_dataDictionary.to_parquet('/wf_validation_python/data/output/numericBinner_output_dataDictionary.parquet')
				
binner_TERRITORY__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/numericBinner_output_dataDictionary.csv', sep = ',')
binner_TERRITORY__output_dataDictionary.to_parquet('/wf_validation_python/data/output/numericBinner_output_dataDictionary.parquet')
				
binner_avg_income__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/numericBinner_output_dataDictionary.csv', sep = ',')
binner_avg_income__input_dataDictionary.to_parquet('/wf_validation_python/data/output/numericBinner_output_dataDictionary.parquet')
				
binner_avg_income__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/numericBinner_output_dataDictionary.csv', sep = ',')
binner_avg_income__output_dataDictionary.to_parquet('/wf_validation_python/data/output/numericBinner_output_dataDictionary.parquet')
				
binner_TOTAL_CONTACTS__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/numericBinner_output_dataDictionary.csv', sep = ',')
binner_TOTAL_CONTACTS__input_dataDictionary.to_parquet('/wf_validation_python/data/output/numericBinner_output_dataDictionary.parquet')
				
binner_TOTAL_CONTACTS__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/numericBinner_output_dataDictionary.csv', sep = ',')
binner_TOTAL_CONTACTS__output_dataDictionary.to_parquet('/wf_validation_python/data/output/numericBinner_output_dataDictionary.parquet')
				
binner_SOLICITED_CNTCTS__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/numericBinner_output_dataDictionary.csv', sep = ',')
binner_SOLICITED_CNTCTS__input_dataDictionary.to_parquet('/wf_validation_python/data/output/numericBinner_output_dataDictionary.parquet')
				
binner_SOLICITED_CNTCTS__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/numericBinner_output_dataDictionary.csv', sep = ',')
binner_SOLICITED_CNTCTS__output_dataDictionary.to_parquet('/wf_validation_python/data/output/numericBinner_output_dataDictionary.parquet')
				
binner_SELF_INIT_CNTCTS__input_dataDictionary=pd.read_csv('/wf_validation_python/data/output/numericBinner_output_dataDictionary.csv', sep = ',')
binner_SELF_INIT_CNTCTS__input_dataDictionary.to_parquet('/wf_validation_python/data/output/numericBinner_output_dataDictionary.parquet')
				
binner_SELF_INIT_CNTCTS__output_dataDictionary=pd.read_csv('/wf_validation_python/data/output/numericBinner_output_dataDictionary.csv', sep = ',')
binner_SELF_INIT_CNTCTS__output_dataDictionary.to_parquet('/wf_validation_python/data/output/numericBinner_output_dataDictionary.parquet')
