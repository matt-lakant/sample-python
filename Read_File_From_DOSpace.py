import re
from datetime import datetime

import shared_lib.db_queries
import shared_lib.file_space_access
from shared_lib.utils import print_section_heading, print_subsection_heading, valid_date

############################################################################################################################
print_section_heading("Parameters")
############################################################################################################################

data_provider_mnemo = 'SOLA'
data_datetimed_file_name = '2024-09-26T08-03-08.665224_FAZI-CLOSING-EN-2024-09-03.csv'

original_file_name = data_datetimed_file_name[27:]
print("original_file_name: ", original_file_name)

############################################################################################################################
print_section_heading("Error Code Initialization")
############################################################################################################################
EXIT_CODE = 0
EXIT_MSG = ""

############################################################################################################################
print_section_heading("main")
############################################################################################################################

if __name__ == "__main__":
    first_lines_df = shared_lib.file_space_access.read_first_n_lines_from_file(data_provider_mnemo, data_datetimed_file_name, 5)

    if first_lines_df is not None:
        print("first_lines_df: ", first_lines_df)

    ####################################################################
    print_subsection_heading("Get data_date by Reading the File")
    ####################################################################

    # --------GET data_date----------
    data_date_str = first_lines_df.iloc[0, 1]
    print("data_date_str: " + data_date_str)

    # Check the date is valid
    date_format_from_file = "%Y-%m-%d"
    data_date = valid_date(data_date_str, date_format_from_file)
    if data_date == False:
        EXIT_CODE = 1
        EXIT_MSG = f"Cannot acquire '{original_file_name}' ('{data_datetimed_file_name}') because data date '{data_date_str}' extracted from index file is NOT a valid date"
        data_date = None
        print("EXIT_CODE: " + str(EXIT_CODE))
        print("EXIT_MSG: " + EXIT_MSG)
    else:
        print("data_date:", data_date)

    ####################################################################
    print_subsection_heading("Get data_date by Reading the File")
    ####################################################################
    data_provider_id = 4
    if EXIT_CODE == 0:
        rows = list(shared_lib.db_queries.get_rows_radix_regex_query(data_provider_id).itertuples(index=False, name=None))
        
        match_found = False
        row_index = 0

        while not match_found and row_index < len(rows):
            pattern = rows[row_index][1]  # Access the regex pattern
            print("Trying pattern: ", pattern)
            match = re.match(pattern, original_file_name)

            if match:
                print("The pattern of the file name is: ", pattern)

                if "index_ISIN" in pattern:
                    dim_index_ts_field_name = "isin_code"
                    index_ts_identifier = match.group('index_ISIN')
                    print("dim_index_ts_field_name: ", dim_index_ts_field_name)
                    print("index_identifier: ", index_ts_identifier)

                if "index_RIC" in pattern:
                    dim_index_ts_field_name = "ric_code"
                    index_ts_identifier = match.group('index_RIC')
                    print("dim_index_ts_field_name: ", dim_index_ts_field_name)
                    print("index_identifier: ", index_ts_identifier)

                year = int(match.group('year'))
                month = int(match.group('month'))
                day = int(match.group('day'))
                data_date_from_filename = datetime(year, month, day).date()
                print("data_date_from_filename: ", data_date_from_filename)

                # Double-check data_date from inside the file with data_date from the file name
                if data_date != data_date_from_filename:
                    EXIT_CODE = 3
                    EXIT_MSG = f"Date from file name '{data_date_from_filename}' is different from date from inside file '{data_date}'"
                    data_date_int = 0
                    print("EXIT_CODE: " + str(EXIT_CODE))
                    print("EXIT_MSG: " + EXIT_MSG)

                file_radix_id = rows[row_index][0]  # Access the index_file_radix_id TO BE REMOVED ONCE ORM IMPLEMENTED
                index_file_radix_id = rows[row_index][0]  # Access the index_file_radix_id
                match_found = True

            row_index += 1

        if not match_found:
            EXIT_CODE = 2
            EXIT_MSG = f"Cannot match '{original_file_name}' with any known file name pattern"
            print("EXIT_CODE: " + str(EXIT_CODE))
            print("EXIT_MSG: " + EXIT_MSG)

    ####################################################################
    print_subsection_heading("Get index_ts_id")
    ####################################################################

    if EXIT_CODE == 0:
        # Get the list of index_ts_id which have this identifier (can be a RIC code or an ISIN code)
        # It can potentially return several records if there is a config issue in the database, i.e. several time series with the identifier
        index_ts_id_list = shared_lib.db_queries.get_index_ts_id_query(dim_index_ts_field_name, index_ts_identifier)['index_ts_id'].tolist()

        if len(index_ts_id_list) == 0:
            EXIT_CODE = 4
            EXIT_MSG = f"No index matches {dim_index_ts_field_name} = '{index_ts_identifier}'"
            print("EXIT_CODE:", EXIT_CODE)
            print("EXIT_MSG:", EXIT_MSG)

        elif len(index_ts_id_list) > 1:
            EXIT_CODE = 5
            EXIT_MSG = f"Several indexes {index_ts_id_list} match {dim_index_ts_field_name} = '{index_ts_identifier}'"
            print("EXIT_CODE:", EXIT_CODE)
            print("EXIT_MSG:", EXIT_MSG)

        else:
            index_ts_id = index_ts_id_list[0]
            print("index_ts_id:", index_ts_id)

    ####################################################################
    print_subsection_heading("Check List of Index File Fields")
    ####################################################################

    if EXIT_CODE == 0:
        # Extract the header row of the Constituents section
        list_file_fields = first_lines_df.iloc[4].tolist()
        print("List of fields in the index file: ", list_file_fields)

        list_file_fields_expected = shared_lib.db_queries.get_list_index_file_fields(data_date, index_file_radix_id)
        print("List of expected fields in the index file: ", list_file_fields_expected)

        if len(list_file_fields_expected) == 0:
            EXIT_CODE = 4
            EXIT_MSG = f"Cannot retrieve list of expected data header fields from database (index_file_radix_id = '{file_radix_id}')"
            print("EXIT_CODE:", EXIT_CODE)
            print("EXIT_MSG:", EXIT_MSG)

        # Check if the fields from the index file are consistent with the expected list
        if list_file_fields != list_file_fields_expected:
            order_id = 1
            list_unexpected_field_names = []

            # Building the list of unexpected column names
            for file_field in list_file_fields:
                if file_field not in list_file_fields_expected:
                    list_unexpected_field_names.append(f"'{file_field}'")
                order_id += 1

            if len(list_unexpected_field_names) > 0:
                EXIT_CODE = 6
                EXIT_MSG = f"Cannot acquire '{original_file_name}' because field(s) {', '.join(list_unexpected_field_names)} are NOT expected"
                print("EXIT_CODE:", EXIT_CODE)
                print("EXIT_MSG:", EXIT_MSG)
            else:
                EXIT_CODE = 7
                EXIT_MSG = f"Cannot acquire '{original_file_name}' because the list of fields in the index file ({list_file_fields}) are not ordered as expected ({list_file_fields_expected})"
                print("EXIT_CODE:", EXIT_CODE)
                print("EXIT_MSG:", EXIT_MSG)
        else:
            print("The fields from the index file are as expected")

    ####################################################################
    print_subsection_heading("Writing to database")
    ####################################################################
    
    # Specify the name of the table
    table_name = 'your_table_name2'

    # Save the DataFrame to PostgreSQL, auto-creating the table if it doesn't exist
    try:
        # first_lines_df.to_sql(table_name, engine, index=False, if_exists='replace')  # Change 'replace' to 'append' if needed
        print(f"DataFrame saved to table '{table_name}' successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")