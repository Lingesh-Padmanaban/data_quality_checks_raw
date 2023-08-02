import json
import re
import pandas as pd
from curator_methods import dtype_curator as cu
from mapper_select import mapper_select
import itertools

class raw_to_OMFI_checks():
    empty_df = pd.DataFrame([])
    def __init__(self, feed_name):
        ms = mapper_select(feed_name)
        self.column_mapper_obj = ms.return_mapper()
        self.zip_obj = json.load(open("zip.json"))

    def census(self, input_census_path):
        df = pd.read_csv(input_census_path, sep = "|", dtype=str, low_memory=False)
        #OMFI map
        omfi_census_map = self.column_mapper_obj["census_name_mapping"]
        key_with_same_value = cu().check_duplicate_values_in_dict(omfi_census_map)
        key_with_same_value_list = list(itertools.chain(*key_with_same_value.values()))
        for key in omfi_census_map.keys():
            if key in key_with_same_value_list:
                pass
            else:
                df = df.rename(columns = {omfi_census_map[key]:key})

        for key in key_with_same_value.keys():
            for field in key_with_same_value[key]:
                df[field] = df[key]
            df = df.drop(columns=key)

        for key in omfi_census_map.keys():
            if key not in df.columns:
                df[key] = "*"

        df = df[list(omfi_census_map.keys())]

        cs = cu().clean_string
        current_date  = cu().get_current_date()
        level_of_coverage_standardize = cu().level_of_coverage_standardize()

        df["employer_group_name"] = df["employer_group_name"].astype(str).str.replace("_", " ").apply(lambda x: x.strip())
        df["employer_group_id"] = df["employer_group_id"].astype(str).str.replace("_", " ").apply(lambda x: x.strip())
        df["member_unique_id"] = df["member_unique_id"].apply(lambda x: x.strip())
        df["member_first_name"] = df["member_first_name"].apply(lambda x: cs(x))
        df["member_last_name"] = df["member_last_name"].apply(lambda x: cs(x))
        df["member_relationship_code"] = df["member_relationship_code"].apply(lambda x:"01" if pd.to_numeric(x, errors="coerce")==1 else "02")
        df["member_relationship_description"] = df["member_relationship_description"].apply(lambda x:"Employee" if "self" in x.lower() else "Dependent")
        df["member_gender"] = df["member_gender"].apply(lambda x:"M" if "f" not in x.lower() else "F")
        df["member_date_of_birth"] = df["member_date_of_birth"].apply(lambda x:str(pd.to_datetime(x, errors="coerce"))[:10])
        df["member_zip_code"] = df["member_zip_code"].apply(lambda x:cs(x)[:5])
        df["termination_date"] = df["termination_date"].apply(lambda x:pd.to_datetime(x, errors="coerce"))
        df["enrollment_date"][df["enrollment_date"].astype(str).str.lower() == "nat"] = df["enrollment_date"][df["enrollment_date"].astype(str).str.lower() == "nat"].apply(lambda x: current_date)
        df["enrollment_date"] = df["enrollment_date"].apply(lambda x:pd.to_datetime(x, errors="coerce"))
        df["enrollment_date"][df["enrollment_date"].astype(str).str.lower() == "nat"] = df["enrollment_date"][df["enrollment_date"].astype(str).str.lower() == "nat"].apply(lambda x: current_date)
        df["level_of_coverage_description"] = df["level_of_coverage_description"].replace(level_of_coverage_standardize)
        members_with_multiple_dobs_and_genders_df = df.groupby(["member_unique_id"]).agg({"member_date_of_birth":lambda x:x.nunique(),
                                                                                      "member_gender":lambda x:x.nunique()}).reset_index()
        members_with_multiple_dobs_and_gender_list = list(members_with_multiple_dobs_and_genders_df[(members_with_multiple_dobs_and_genders_df["member_date_of_birth"]>1) | (members_with_multiple_dobs_and_genders_df["member_gender"]>1)]["member_unique_id"].unique())
        membs_with_multiple_dobs_and_gender_list = {"anamolic_members":members_with_multiple_dobs_and_gender_list,
                                                      "error_type":["multiple_dobs_and_gender" for i in range(len(members_with_multiple_dobs_and_gender_list))]}
        members_without_multiple_dobs_and_gender_list = list(members_with_multiple_dobs_and_genders_df[(members_with_multiple_dobs_and_genders_df["member_date_of_birth"]==1) & (members_with_multiple_dobs_and_genders_df["member_gender"]==1)]["member_unique_id"].unique())

        #Filter census with members with multiple dobs and genders
        df = df.merge(pd.DataFrame({"member_unique_id":members_without_multiple_dobs_and_gender_list}),
                     on = ["member_unique_id"],
                     how = "inner")

        #Assign first plan enrollmet date and last termination date
        df["enrollment_date"] = df.groupby(["member_unique_id"])["enrollment_date"].transform("min").astype(str)
        df["termination_date"] = df.groupby(["member_unique_id"])["termination_date"].transform("max").astype(str)

        #Assign first zip_code
        df["member_zip_code"] = df.groupby(["member_unique_id"])["member_zip_code"].transform("first").astype(str)

        #Assign member_state
        df["member_state"] = df["member_zip_code"].apply(lambda x: self.zip_obj[str(x)]["state_name"] if str(x) in self.zip_obj.keys() else "*")

        # Assign first level_of_coverage_description, plan_type , member_relationship_code and member_relationship_description
        df["level_of_coverage_description"] = df.groupby(["member_unique_id"])["level_of_coverage_description"].transform("last").astype(str)
        df["member_relationship_code"] = df.groupby(["member_unique_id"])["member_relationship_code"].transform("last").astype(str)
        df["member_relationship_description"] = df.groupby(["member_unique_id"])["member_relationship_description"].transform("last").astype(str)
        df["plan_type"] = df["plan_type"].apply(lambda x:cs(x))
        df["plan_type"] = df.groupby(["member_unique_id"])["plan_type"].transform(lambda x: ','.join(set(x)))
        df["plan_name"] = df.groupby(["member_unique_id"])["plan_name"].transform(lambda x: ','.join(set(x)))
        df = df.drop_duplicates()
        return df,membs_with_multiple_dobs_and_gender_list

    def medical_claims(self, input_medical_path):
        df = pd.read_csv(input_medical_path, sep="|", dtype=str, low_memory=False)
        print(df)

