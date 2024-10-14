from funcs.globals import chunk_size, data_directory, cleaned_files
from funcs.db_utils import *
import pandas as pd

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
        # insert tuple into database
        execute_df_values(query, df_values)

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
        # insert tuple into database
        execute_df_values(query, df_values)

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
        # insert tuple into database
        execute_df_values(query, df_values)

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
        # insert tuple into database
        execute_df_values(query, df_values)

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
        # insert tuple into database
        execute_df_values(query, df_values)

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
        # insert tuple into database
        execute_df_values(query, df_values)
        
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
        # insert tuple into database
        execute_df_values(query, df_values)

        
def report_db_statistics():
    # loop over all the tables
    for table in cleaned_files:
        # input files are known to avoid SQL injection
        query = "SELECT COUNT(*) FROM " + table + ";"
        result = exec_get_one(query)
        if result:
            print("Table: ", table, " Record Inserted: ", result[0])
