from globals import cleaned_files
from sql.db_utils import *
from sql.queries import all_queries, index_queries, drop_queries
import time

def execute_query(query, query_name, columns):
    """
    Execute an SQL query and display results.

    This function executes a provided SQL query using the given database
    connection. It measures and returns the execution time, displays the first 10 rows of the result.

    Args:
        query (str): The SQL query to be executed.
        query_name (str): A descriptive name for the query, used in logging.
        columns (str): A description of columns for the query, used in logging.

    Returns:
        float: The time taken to execute the query in seconds, or None if an error occurred.

    Raises:
        Exception: If any error occurs during query execution, the error is caught and printed.
    """
    try:
        start_time = time.time()
        results = exec_get_all(query)
        execution_time = time.time() - start_time
        # Display results, showing only the first 10 rows for brevity
        counter = 1
        print(f"\n{columns}")
        for res in results:
            print(res)
            counter += 1
            if counter > 11: break

        return execution_time
    except Exception as e:
        print(f"An error occurred while executing {query_name}: {e}")
        return None
    
def run_all_queries():
    """
    Execute a series of predefined SQL queries against the database and display results.

    This function executes multiple SQL queries that analyze user achievements,
    competition tags, and dataset engagements. Each query is executed sequentially and
    the execution time is displayed. 
    """
    for q in all_queries:
        print(f"\nExecuting query: {q['name']}")
        execution_time = execute_query(q['query'], q['name'], q['columns'])
        if execution_time is not None:
            print(f"{q['name']} completed in {execution_time:.2f} seconds.\n")

def create_indexes():
    """
    Create indexes on the database tables to speed up the queries.
    """
    
    for index in index_queries:
        try:
            exec_commit(index)
            print(f"Index created: {index}")
        except Exception as e:
            print(f"Error creating index: {e}")

def drop_indexes():
    """
    Drops the indexes created by Phase 2.
    """
    
    for index in drop_queries:
        try:
            exec_commit(index)
            print(f"Index dropped: {index}")
        except Exception as e:
            print(f"Error dropping index: {e}")

def report_db_statistics():
    # loop over all the tables
    for table in cleaned_files:
        # input files are known to avoid SQL injection
        query = "SELECT COUNT(*) FROM " + table + ";"
        result = exec_get_one(query)
        if result:
            print("Table: ", table, " Record Inserted: ", result[0])
