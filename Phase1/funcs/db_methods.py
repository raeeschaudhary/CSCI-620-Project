from funcs.globals import chunk_size, data_directory, input_files
from funcs.db_utils import *
import pandas as pd

# This method removes entries with non-existent FK Ids from chunk_data.
def remove_invalid_entries_links(chunk_data, valid_ids, loc):
    # return only data that contains valid ids
    return [data for data in chunk_data if int(data[loc]) in valid_ids]

# This methods removes entries with non-existent FK Ids from chunk_data but ignore entries where the FK Id is None.
def remove_invalid_entries_links_ignore_none(chunk_data, valid_ids, loc):
    # return only data that contains valid ids and None ids
    return [data for data in chunk_data if data[loc] is None or int(data[loc]) in valid_ids]

# This method extract ids from chunk_data on give location to compare FK ids with existing Ids in the database.
def extract_ids_from_chunk(chunk_data, loc):
    # return ids extracted from given location from chunk_data
    return [int(data[loc]) for data in chunk_data]


# This method extract ids from chunk_data on give location to compare FK ids with existing Ids in the database. Only includes non-None values.
def extract_ids_from_chunk_none(chunk_data, loc):
    # # return ids extracted from given location from chunk_data only where is not None
    return [int(data[loc]) for data in chunk_data if data[loc] is not None]

# This method removes entries with non-existent FK Ids from chunk_data.
def remove_invalid_entries_links_chunked(chunk_data, valid_ids, loc, chunk_size=5000):
    # return only data that contains valid ids and None ids
    valid_ids_set = set(valid_ids)
    filtered_data = []
    # Process in chunks
    for i in range(0, len(chunk_data), chunk_size):
        chunk = chunk_data[i:i + chunk_size]
        filtered_data.extend(data for data in chunk if int(data[loc]) in valid_ids_set)
    
    return filtered_data

# This method checks which ids are valid by querying the database and returns only valid_ids to ensure fk is not violated.
def check_valid_fk_ids(table, ids):
    valid_ids = set()
    if ids:
        # Make sure that only to check for valid table names to avoid SQL injection. 
        if table.lower() not in {t.lower() for t in input_files}:
            raise ValueError("Invalid table name")
        sql = "SELECT Id FROM " + table + " WHERE Id IN %s"
        result = exec_get_all(sql, (tuple(ids),))
        valid_ids = [row[0] for row in result]
    return valid_ids

# This method takes an sql script file from root; and executes it
def run_schema_script(file):
    # Run the script file to create schema. 
    exec_sql_file(file)

# This method takes an sql query; and executes it
def run_commit_query(query):
    # Run single query
    exec_commit(query)

# This method Inserst users
def insert_users(input_file, query, max_chunk=10):
    csv_file = data_directory + input_file
    chunks = pd.read_csv(csv_file, chunksize=chunk_size)

    c_count = 1
    for chunk in chunks:
        # print(chunk) 
        df_values = list(chunk.itertuples(index=False, name=None))
        execute_df_values(query, df_values)
        c_count += 1

        # if c_count >= max_chunk:
        #     break


def report_db_statistics():
    # loop over all the tables
    for table in input_files:
        # input files are known to avoid SQL injection
        query = "SELECT COUNT(*) FROM " + table + ";"
        result = exec_get_one(query)
        if result:
            print("Table: ", table, " Record Inserted: ", result[0])
