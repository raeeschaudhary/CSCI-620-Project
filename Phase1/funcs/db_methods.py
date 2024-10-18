from funcs.globals import chunk_size, data_directory, cleaned_files
from funcs.db_utils import *
import pandas as pd

def extract_ids_from_chunk(chunk_data, loc):
    """
    Extract ids from chunk_data on give location to compare FK ids with existing Ids in the database.
    
    :param chunk_data: the chunk of the data to be processed.
    :param loc: location of the attribute in the chunk_data tuple to be extracted
    :return: list of values extracted from chunk_data at loc.
    """
    # return ids extracted from given location from chunk_data
    return [int(data[loc]) for data in chunk_data]

def check_valid_fk_ids(table, ids):
    """
    Check which ids are valid by querying the database and returns only valid_ids to ensure fk is not violated.
    
    :param table: the name of the table to extract ids from.
    :param ids: list of ids to compare against. 
    :return: list of valid ids that match with existing table.
    """
    valid_ids = set()
    if ids:
        # Make sure that only to check for valid table names to avoid SQL injection. 
        if table.lower() not in {t.lower() for t in cleaned_files}:
            raise ValueError("Invalid table name")
        sql = "SELECT Id FROM " + table + " WHERE Id IN %s"
        result = exec_get_all(sql, (tuple(ids),))
        valid_ids = [row[0] for row in result]
    return valid_ids

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

def run_schema_script(file):
    """
    Run the script file to create schema. 
    
    :param file: Name of the sql file. e.g., create_schema.sql
    """
    # call call db util methods to execute query
    exec_sql_file(file)

def run_commit_query(query):
    """
    Takes a sql query; and executes it
    
    :param query: SQL query as input.
    """
    # Run single query
    exec_commit(query)

def get_csv_chunker(csv_file):
    """
    Takes a input csv file and reads it into chunks.
    
    :param csv_file: Path to input csv file.
    :return: pandas chunks to read data in chunks.
    """
    try:
        # chunk_size is a global variable set it globals.py
        # read the chunks based on chunk size and return to function call.
        chunks = pd.read_csv(csv_file, chunksize=chunk_size)
        return chunks
    except:
        # print the error if the file read fails. 
        print("=======================================================")
        print("Wrong file or file path; ", csv_file, " Does not Exists")
        print("=======================================================")
        return None

def insert_users(input_file, query):
    """
    This method Insert users
    
    :param input_file: Name of the CSV file.
    :param query: Query to be executed.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # insert tuple into database
        execute_df_values(query, df_values)

def insert_tags(input_file, query):
    """
    This method Insert tags
    
    :param input_file: Name of the CSV file.
    :param query: Query to be executed.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # insert tuple into database
        execute_df_values(query, df_values)

def insert_forums(input_file, query):
    """
    This method Insert forums
    
    :param input_file: Name of the CSV file.
    :param query: Query to be executed.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # insert tuple into database
        execute_df_values(query, df_values)

def insert_organizations(input_file, query):
    """
    This method Insert organizations
    
    :param input_file: Name of the CSV file.
    :param query: Query to be executed.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # insert tuple into database
        execute_df_values(query, df_values)

def insert_user_organizations(input_file, query):
    """
    This method Insert user organizations
    
    :param input_file: Name of the CSV file.
    :param query: Query to be executed.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # get the FK ids from the chunck; provide the FK index
        users_ids = extract_ids_from_chunk(df_values, -3)
        # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
        valid_users_ids = check_valid_fk_ids('Users', users_ids)
        # if length of valid_ids and set of Ids is same it means all ids are present in the db. 
        if len(set(users_ids)) == len(set(valid_users_ids)):
            # Proceed to insert all chuncked data; as it is valid
            execute_df_values(query, df_values)
        else:
            # In case some ids are are invalid (do not link to primary table), filter out invalid ids; provide the FK index 
            valid_df_values = remove_invalid_entries_links_chunked(df_values, valid_users_ids, -3)
            # insert tuple into database
            execute_df_values(query, valid_df_values)

def insert_user_followers(input_file, query):
    """
    This method Insert user followers
    
    :param input_file: Name of the CSV file.
    :param query: Query to be executed.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # get the FK ids from the chunck; provide the FK index
        follower_ids = extract_ids_from_chunk(df_values, -2)
        # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
        valid_follower_ids = check_valid_fk_ids('Users', follower_ids)
        # In case some ids are are invalid (do not link to primary table), filter out invalid ids; provide the FK index 
        valid_df_values = remove_invalid_entries_links_chunked(df_values, valid_follower_ids, -2)
        # get the FK ids from the chunck; provide the FK index
        user_ids = extract_ids_from_chunk(valid_df_values, -3)
        # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
        valid_user_ids = check_valid_fk_ids('Users', user_ids)
        # In case some ids are are invalid (do not link to primary table), filter out invalid ids; provide the FK index 
        valid_df_values = remove_invalid_entries_links_chunked(valid_df_values, valid_user_ids, -3)
        # insert tuple into database
        execute_df_values(query, valid_df_values)

def insert_cleaned_datasets(input_file, query):
    """
    This method Insert datasets
    
    :param input_file: Name of the CSV file.
    :param query: Query to be executed.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # get the FK ids from the chunck; provide the FK index
        forums_ids = extract_ids_from_chunk(df_values, -7)
        # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
        valid_forums_ids = check_valid_fk_ids('Forums', forums_ids)
        # In case some ids are are invalid (do not link to primary table), filter out invalid ids; provide the FK index 
        valid_df_values = remove_invalid_entries_links_chunked(df_values, valid_forums_ids, -7)
        # get the FK ids from the chunck; provide the FK index
        user_ids = extract_ids_from_chunk(valid_df_values, -8)
        # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
        valid_user_ids = check_valid_fk_ids('Users', user_ids)
        # In case some ids are are invalid (do not link to primary table), filter out invalid ids; provide the FK index 
        valid_df_values = remove_invalid_entries_links_chunked(valid_df_values, valid_user_ids, -8)
        # insert tuple into database
        execute_df_values(query, valid_df_values)

def insert_dataset_tags(input_file, query):
    """
    This method Insert dataset tags
    
    :param input_file: Name of the CSV file.
    :param query: Query to be executed.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # get the FK ids from the chunck; provide the FK index
        tag_ids = extract_ids_from_chunk(df_values, -1)
        # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
        valid_tag_ids = check_valid_fk_ids('Tags', tag_ids)
        # In case some ids are are invalid (do not link to primary table), filter out invalid ids; provide the FK index 
        valid_df_values = remove_invalid_entries_links_chunked(df_values, valid_tag_ids, -1)
        # get the FK ids from the chunck; provide the FK index
        dataset_ids = extract_ids_from_chunk(valid_df_values, -2)
        # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
        valid_dataset_ids = check_valid_fk_ids('DatasetsCleaned', dataset_ids)
        # In case some ids are are invalid (do not link to primary table), filter out invalid ids; provide the FK index 
        valid_df_values = remove_invalid_entries_links_chunked(valid_df_values, valid_dataset_ids, -2)
        # insert tuple into database
        execute_df_values(query, valid_df_values)

def insert_cleaned_competitions(input_file, query):
    """
    This method Insert competitions
    
    :param input_file: Name of the CSV file.
    :param query: Query to be executed.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # get the FK ids from the chunck; provide the FK index
        forum_ids = extract_ids_from_chunk(df_values, -9)
        # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
        valid_forum_ids = check_valid_fk_ids('Forums', forum_ids)
        # if length of valid_ids and set of Ids is same it means all ids are present in the db. 
        if len(set(forum_ids)) == len(set(valid_forum_ids)):
            # Proceed to insert all chuncked data; as it is valid
            execute_df_values(query, df_values)
        else:
            # In case some ids are are invalid (do not link to primary table), filter out invalid ids; provide the FK index 
            valid_df_values = remove_invalid_entries_links_chunked(df_values, valid_forum_ids, -9)
            # insert tuple into database
            execute_df_values(query, valid_df_values)

def insert_competition_tags(input_file, query):
    """
    This method Insert competition tags
    
    :param input_file: Name of the CSV file.
    :param query: Query to be executed.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # get the FK ids from the chunck; provide the FK index
        tag_ids = extract_ids_from_chunk(df_values, -1)
        # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
        valid_tag_ids = check_valid_fk_ids('Tags', tag_ids)
        # In case some ids are are invalid (do not link to primary table), filter out invalid ids; provide the FK index 
        valid_df_values = remove_invalid_entries_links_chunked(df_values, valid_tag_ids, -1)
        # get the FK ids from the chunck; provide the FK index
        compt_ids = extract_ids_from_chunk(valid_df_values, -2)
        # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
        valid_compt_ids = check_valid_fk_ids('CompetitionsCleaned', compt_ids)
        # In case some ids are are invalid (do not link to primary table), filter out invalid ids; provide the FK index 
        valid_df_values = remove_invalid_entries_links_chunked(valid_df_values, valid_compt_ids, -2)
        # insert tuple into database
        execute_df_values(query, valid_df_values)

def insert_teams(input_file, query):
    """
    This method Insert teams
    
    :param input_file: Name of the CSV file.
    :param query: Query to be executed.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # get the FK ids from the chunck; provide the FK index
        competition_ids = extract_ids_from_chunk(df_values, -3)
        # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
        valid_competition_ids = check_valid_fk_ids('CompetitionsCleaned', competition_ids)
        # if length of valid_ids and set of Ids is same it means all ids are present in the db. 
        if len(set(competition_ids)) == len(set(valid_competition_ids)):
            # Proceed to insert all chuncked data; as it is valid
            execute_df_values(query, df_values)
        else:
            # In case some ids are are invalid (do not link to primary table), filter out invalid ids; provide the FK index 
            valid_df_values = remove_invalid_entries_links_chunked(df_values, valid_competition_ids, -3)
            # insert tuple into database
            execute_df_values(query, valid_df_values)


def insert_submissions(input_file, query):
    """
    This method Insert submissions
    
    :param input_file: Name of the CSV file.
    :param query: Query to be executed.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # get the FK ids from the chunck; provide the FK index
        team_ids = extract_ids_from_chunk(df_values, -5)
        # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
        valid_team_ids = check_valid_fk_ids('TeamsCleaned', team_ids)
        # if length of valid_ids and set of Ids is same it means all ids are present in the db. 
        if len(set(team_ids)) == len(set(valid_team_ids)):
            # Proceed to insert all chuncked data; as it is valid
            execute_df_values(query, df_values)
        else:
            # In case some ids are are invalid (do not link to primary table), filter out invalid ids; provide the FK index 
            valid_df_values = remove_invalid_entries_links_chunked(df_values, valid_team_ids, -5)
            # insert tuple into database
            execute_df_values(query, valid_df_values)

def insert_user_achievements(input_file, query):
    """
    This method Insert user achievements
    
    :param input_file: Name of the CSV file.
    :param query: Query to be executed.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # get the FK ids from the chunck; provide the FK index
        users_ids = extract_ids_from_chunk(df_values, -10)
        # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
        valid_users_ids = check_valid_fk_ids('Users', users_ids)
        # if length of valid_ids and set of Ids is same it means all ids are present in the db. 
        if len(set(users_ids)) == len(set(valid_users_ids)):
            # Proceed to insert all chuncked data; as it is valid
            execute_df_values(query, df_values)
        else:
            # In case some ids are are invalid (do not link to primary table), filter out invalid ids; provide the FK index 
            valid_df_values = remove_invalid_entries_links_chunked(df_values, valid_users_ids, -10)
            # insert tuple into database
            execute_df_values(query, valid_df_values)

def report_db_statistics():
    # loop over all the tables
    for table in cleaned_files:
        # input files are known to avoid SQL injection
        query = "SELECT COUNT(*) FROM " + table + ";"
        result = exec_get_one(query)
        if result:
            print("Table: ", table, " Record Inserted: ", result[0])
