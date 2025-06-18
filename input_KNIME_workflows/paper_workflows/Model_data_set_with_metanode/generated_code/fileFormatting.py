import pandas as pd
import json
import h5py
import pyarrow
				
imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary=pd.read_csv('/wf_validation_contracts/data/missing_input_dataDictionary.csv', sep = ',', decimal = '.')
imputeMissingByMostFrequent_sex_IRISCHOOL_ETHNICITY__input_dataDictionary.to_parquet('/wf_validation_contracts/data/missing_input_dataDictionary.parquet')
				
imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary=pd.read_csv('/wf_validation_contracts/data/missing_output_dataDictionary.csv', sep = ',', decimal = '.')
imputeMissingByFixValue_ACADEMIC_INTEREST_2_ACADEMIC_INTEREST_1__input_dataDictionary.to_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
				
imputeMissingByMean_avg_income_distance__input_dataDictionary=pd.read_csv('/wf_validation_contracts/data/missing_output_dataDictionary.csv', sep = ',', decimal = '.')
imputeMissingByMean_avg_income_distance__input_dataDictionary.to_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
				
imputeMissingByLinearInterpolation_satscore__input_dataDictionary=pd.read_csv('/wf_validation_contracts/data/missing_output_dataDictionary.csv', sep = ',', decimal = '.')
imputeMissingByLinearInterpolation_satscore__input_dataDictionary.to_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
				
rowFilterRange_init_span__input_dataDictionary=pd.read_csv('/wf_validation_contracts/data/missing_output_dataDictionary.csv', sep = ',', decimal = '.')
rowFilterRange_init_span__input_dataDictionary.to_parquet('/wf_validation_contracts/data/missing_output_dataDictionary.parquet')
				
columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary=pd.read_csv('/wf_validation_contracts/data/rowFilter_output_dataDictionary.csv', sep = ',', decimal = '.')
columnFilter_TRAVEL_INIT_CNTCTS_REFERRAL_CNCTS_telecq_interest_stuemail_CONTACT_CODE1__input_dataDictionary.to_parquet('/wf_validation_contracts/data/rowFilter_output_dataDictionary.parquet')
				
mapping_TERRITORY__input_dataDictionary=pd.read_csv('/wf_validation_contracts/data/columnFilter_output_dataDictionary.csv', sep = ',', decimal = '.')
mapping_TERRITORY__input_dataDictionary.to_parquet('/wf_validation_contracts/data/columnFilter_output_dataDictionary.parquet')
				
mapping_Instate__input_dataDictionary=pd.read_csv('/wf_validation_contracts/data/ruleEngine_territory_output_dataDictionary.csv', sep = ',', decimal = '.')
mapping_Instate__input_dataDictionary.to_parquet('/wf_validation_contracts/data/ruleEngine_territory_output_dataDictionary.parquet')
				
stringToNumber_TERRITORY_Instate__input_dataDictionary=pd.read_csv('/wf_validation_contracts/data/ruleEngine_instate_output_dataDictionary.csv', sep = ',', decimal = '.')
stringToNumber_TERRITORY_Instate__input_dataDictionary.to_parquet('/wf_validation_contracts/data/ruleEngine_instate_output_dataDictionary.parquet')
				
imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary=pd.read_csv('/wf_validation_contracts/data/stringToNumber_output_dataDictionary.csv', sep = ',', decimal = '.')
imputeOutlierByClosest_avg_income_distance_Instate__input_dataDictionary.to_parquet('/wf_validation_contracts/data/stringToNumber_output_dataDictionary.parquet')
				
binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary=pd.read_csv('/wf_validation_contracts/data/numericOutliers_output_dataDictionary.csv', sep = ',', decimal = '.')
binner_TOTAL_CONTACTS_SELF_INIT_CNTCTS_SOLICITED_CNTCTS__input_dataDictionary.to_parquet('/wf_validation_contracts/data/numericOutliers_output_dataDictionary.parquet')
				
binner_TERRITORY__input_dataDictionary=pd.read_csv('/wf_validation_contracts/data/numericBinner_output_dataDictionary.csv', sep = ',', decimal = '.')
binner_TERRITORY__input_dataDictionary.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
				
binner_satscore__input_dataDictionary=pd.read_csv('/wf_validation_contracts/data/numericBinner_output_dataDictionary.csv', sep = ',', decimal = '.')
binner_satscore__input_dataDictionary.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
				
binner_avg_income__input_dataDictionary=pd.read_csv('/wf_validation_contracts/data/numericBinner_output_dataDictionary.csv', sep = ',', decimal = '.')
binner_avg_income__input_dataDictionary.to_parquet('/wf_validation_contracts/data/numericBinner_output_dataDictionary.parquet')
