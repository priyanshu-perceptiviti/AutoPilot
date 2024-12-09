import pandas as pd
import numpy as np
import re
import mysql.connector
from file_cnv_config import activity_type_dict, encounter_type_dict, tgt_col,provider_id_dict

class ClaimsDataProcessor:
    def __init__(self, file_path, cntrl_db, client):
        # Load Excel data and set up database connection details
        self.df = pd.read_excel(file_path)
        self.cntrl_db = cntrl_db
        self.client = client
        self.activity_type_dict = activity_type_dict
        self.encounter_type_dict = encounter_type_dict
        self.provider_id_dict = provider_id_dict
        self.mysql_conn, self.mysql_cur = None, None
        self.total_records = len(self.df)
        print("Total records:", self.total_records)
        # Add is_valid flag
        self.df['is_valid'] = 1
    
    def connect_to_db(self):
        # Initialize MySQL connection
        try:
            self.mysql_conn = mysql.connector.connect(
                host="172.19.6.209",
                user="perceptiviti",
                password="P01012020",
                database=self.cntrl_db
            )
            self.mysql_cur = self.mysql_conn.cursor()
            print("Connected to the MySQL database")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
    
    def close_db(self):
        # Close MySQL connection
        if self.mysql_cur:
            self.mysql_cur.close()
        if self.mysql_conn:
            self.mysql_conn.close()
        print("MySQL connection closed")
    
    def execute_query(self, query):
        # Execute a query and return the result
        try:
            self.connect_to_db()
            self.mysql_cur.execute(query)
            result = self.mysql_cur.fetchall()
            self.close_db()
            return result
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            self.close_db()
            return None
    
    def fetch_src_column(self, target_col_name):
        # Query to fetch the source column name for a given target column name
        query = f"SELECT src_field_name FROM workarea.pilot_mapping_table WHERE target_field_name = '{target_col_name}' and client = '{self.client}'"
        result = self.execute_query(query)
        return result[0][0] if result else ''
    
    def is_mandatory_present(self):
        # Check for missing mandatory columns
        query = "SELECT COUNT(*) FROM workarea.pilot_mapping_table WHERE is_mandatory = 1 AND (src_field_name = '' OR src_field_name IS NULL)"
        result = self.execute_query(query)
        print('results fetched successfully')
        if result and result[0][0] > 0:
            print("Mandatory columns are missing")
        else:
            print("All mandatory columns are present")
    
    def cpt_profiling(self, attribute):
        # Perform CPT profiling with a pattern match for 5-digit numeric codes
        column_name = self.fetch_src_column(attribute)
        pattern = r'^\d{5}$|^[A-Za-z]\d{4}$'
        self.df['is_5_digit_numeric'] = self.df[column_name].astype(str).apply(lambda x: bool(re.match(pattern, x)))
        self.df['is_valid'] = np.where(
        (self.df['is_valid'] != 0) & (self.df['is_5_digit_numeric']),1,0)
        pattern_match_count = self.df['is_5_digit_numeric'].sum()
        print('is_5_digit_numeric = ', pattern_match_count)
    
    def icd_profiling(self, attribute):
        # Perform ICD profiling with a pattern match for codes (e.g., "A00.0")
        column_name = self.fetch_src_column(attribute)
        pattern = r'^[A-Za-z]\d{2}(\.\d{1,4})?$'
        self.df['matches_pattern'] = self.df[column_name].astype(str).apply(lambda x: bool(re.match(pattern, x)))
        self.df['is_valid'] = np.where(
        (self.df['is_valid'] != 0) & (self.df['matches_pattern']),1,0)
        pattern_match_count = self.df['matches_pattern'].sum()
        print('valid icd codes = ', pattern_match_count)
    
    def activity_type_data_analysis(self, attribute):
        # Perform data analysis on activity type by counting occurrences
        column_name = self.fetch_src_column(attribute)
        grouped_counts = self.df.groupby(column_name).size()
        print(grouped_counts)
    
    def date_comparison(self, attribute1, attribute2):
        # Compare two date attributes to check if the first is less than or equal to the second
        date_col_1 = self.fetch_src_column(attribute1)
        date_col_2 = self.fetch_src_column(attribute2)
        
        # Convert columns to datetime
        self.df[date_col_1] = pd.to_datetime(self.df[date_col_1], errors='coerce')
        self.df[date_col_2] = pd.to_datetime(self.df[date_col_2], errors='coerce')
        
        # Perform date comparison
        self.df['date_check'] = self.df[date_col_1] <= self.df[date_col_2]
        invalid_records = self.df[self.df['date_check'] == False][[date_col_1, date_col_2]]
        self.df.loc[self.df['date_check'] == False, 'is_valid'] = 0
        print('number of invalid dates: ', len(invalid_records))

    def transform_data(self):
        # Valid data
        filtered_df = self.df[self.df['is_valid'] == 1]
        filtered_df_length = len(filtered_df)
        print("Total records in filtered df:", filtered_df_length)

        # Get the mapping for columns
        query = f"SELECT src_field_name, target_field_name FROM workarea.pilot_mapping_table WHERE client = '{self.client}' and src_field_name != '' "
        result = self.execute_query(query)
        target_columns = tgt_col

        # Mapping the columns
        target_df = pd.DataFrame(columns=target_columns)
        for row in result:
            target_df[row[1]] = filtered_df[row[0]]

        # Fill missing values with default values   
        is_resubmission_present = any(row[1] == 'IS_RESUBMISSION' for row in result)
        if not is_resubmission_present:
            target_df['IS_RESUBMISSION'] = 'No'
        is_resubmissionhash_present = any(row[1] == 'RESUBMISSION#' for row in result)
        if not is_resubmissionhash_present:    
            target_df['RESUBMISSION#'] = 0
        is_emirate_present = any(row[1] == 'EMIRATE' for row in result)
        if not is_emirate_present:
            target_df['EMIRATE'] = 'Abu Dhabi'
        is_dateofintimation_present = any(row[1] == 'DATE_OF_INTIMATION' for row in result)
        if not is_dateofintimation_present:    
            target_df['DATE_OF_INTIMATION'] = '01/01/2015 00:00:00'
        is_dateofcmmencement_present = any(row[1] == 'DATE_OF_COMMENCEMENT' for row in result)
        if not is_dateofcmmencement_present:    
            target_df['DATE_OF_COMMENCEMENT'] = '01/01/2025 00:00:00'

        is_physician_present = any(row[1] == 'PhysicianName' for row in result)
        if is_physician_present:
            target_df['PhysicianName'] = '[ ' + target_df['PhysicianName'] + ' ]'

        target_df['DOB'] = target_df['DOB'].dt.strftime('%d/%m/%Y %I:%M:%S %p')
        target_df['DATE_OF_ADMISSION'] = target_df['DATE_OF_ADMISSION'].dt.strftime('%d/%m/%Y %I:%M:%S %p')
        target_df['DATE_OF_DISCHARGE'] = target_df['DATE_OF_DISCHARGE'].dt.strftime('%d/%m/%Y %I:%M:%S %p')
        target_df['DATE_OF_INTIMATION'] = target_df['DATE_OF_INTIMATION'].dt.strftime('%d/%m/%Y %I:%M:%S %p')
        target_df['DATE_OF_COMMENCEMENT'] = target_df['DATE_OF_COMMENCEMENT'].dt.strftime('%d/%m/%Y %I:%M:%S %p')
        target_df['DATE_OF_EXPIRY'] = target_df['DATE_OF_EXPIRY'].dt.strftime('%d/%m/%Y %I:%M:%S %p')
        target_df['CLAIM_NUMBER'] = target_df['CLAIM_NUMBER'].astype(str)
        target_df['MEMBER_ID'] = target_df['MEMBER_ID'].astype(str)
        target_df['ICD_CODE'] = target_df['ICD_CODE'].astype(str)
        target_df['activity type'] = target_df['activity type'].astype(str) 
        target_df['Primary diagnosis'] = target_df['Primary diagnosis'].astype(str)
        target_df['IS_RESUBMISSION'] = target_df['IS_RESUBMISSION'].astype(str)
        target_df["RESUBMISSION#"] = pd.to_numeric(target_df["RESUBMISSION#"], errors="coerce")
        target_df["RESUBMISSION#"] = target_df["RESUBMISSION#"].astype("Int64")
        target_df["PAYABLE_TO_CLINIC"] = pd.to_numeric(target_df["PAYABLE_TO_CLINIC"], errors="coerce")
        target_df["PAYABLE_TO_CLINIC"] = target_df["PAYABLE_TO_CLINIC"].astype("Float64")
        target_df["PAYABLE_TO_CLINIC"] = target_df["PAYABLE_TO_CLINIC"].fillna(0).astype(int)
        target_df["encounter type code"] = pd.to_numeric(target_df["encounter type code"], errors="coerce")
        target_df["encounter type code"] = target_df["encounter type code"].astype("Int64")

        # Exporting the mapped data
        target_df.to_excel("converted_file.xlsx", index=False)
        print("Data transformed and saved to 'converted_file.xlsx'")

    def get_activity_type(self, attribute):
        # Create a case-insensitive mapping by converting dictionary keys to lowercase
        case_insensitive_dict = {k.lower(): v for k, v in self.activity_type_dict.items()}

        # Map values in the attribute column to lowercase before applying the mapping
        self.df['activity_type'] = self.df[attribute].str.lower().map(case_insensitive_dict)
        
    def get_encounter_type(self, attribute):
        self.df['encounter_type'] = self.df[attribute].map(self.encounter_type_dict)
    
    def get_provider_id(self, attribute):
        self.df['Provider'] = self.df[attribute].map(self.provider_id_dict)

    def year_to_date(self, attribute):
        self.df[attribute] = pd.to_datetime('01/01/' + self.df[attribute].astype(str))
    
    def sec_diag_formatting(self, attribute):
        self.df[attribute] = self.df[attribute].str.replace(' - ', ', ')
    
    def split_code_desc(self, attribute):
        self.df[attribute] = self.df['attribute'].fillna('').apply(lambda x: x.split()[0] if x.strip() else None)
    
    def join_icd_cols(self, attribute):
        query = f"SELECT src_field_name FROM workarea.pilot_mapping_table WHERE client = '{self.client}' and target_field_name != 'secondary diagnosis'"
        result = self.execute_query(query)  # Assume `result` is a DataFrame returned from this query.

        # Ensure result is a DataFrame
        if isinstance(result, pd.DataFrame):
            # Iterate over rows and join all column values row by row into a comma-separated string
            self.df['joined_values'] = result.apply(
                    lambda row: ', '.join(str(x).strip() for x in row if pd.notna(x)), axis=1
                    )
        else:
            raise ValueError("Query result is not a DataFrame")
        
# Example usage:
file_path = '/Users/tusharchoudhary/Downloads/perceptiviti/pilot_automation/automate_pilot/Claims Report_Closed_January-June 2024 sample 2.xls'
cntrl_db = 'sherlock_control'
client = 'sharq'

processor = ClaimsDataProcessor(file_path, cntrl_db, client)
print('calling is_mandatory_present()')
processor.is_mandatory_present()
# print('calling cpt_profiling()')
processor.cpt_profiling('ICD_CODE')
# print('calling icd_profiling()')
processor.icd_profiling('Primary diagnosis')
processor.year_to_date('DOB')
#processor.get_activity_type('ITEM_DESCRIPTION') # commented by PL not req in walah
# processor.get_encounter_type('CATEG') # commented by PL not req in walah
processor.get_provider_id('Provider')

#processor.sec_diag_formatting('SECOND DISEASE') # commented by PL not req in walah

# processor.activity_type_data_analysis('DESCRIPTION.2')
# processor.date_comparison('DOB', 'ADMISSION DATE')
# print('calling transform_data()')
processor.transform_data()
