import psycopg2
import psycopg2.extras as extras
import os

from funcs.globals import *

"""
This file contains methods for connecting and executing operations on the database.
Each method is self explanatory
"""

def connect():
    """
    Makes the connection based on the database credentials given in gloabls.py.

    :return: connection to postgres database server.
    """
    return psycopg2.connect(
        dbname=db_config['database'],
        user=db_config['user'],
        password=db_config['password'],
        host=db_config['host'],
        port=db_config['port']
    )

def exec_sql_file(path):
    """
    Takes a file path and execute all queries inside that. 
    
    :param path: Name of input sql file. expect it to be in root directory.
    """
    # gets the currect directory from execution; root
    current_directory = os.getcwd()
    # joins the directory with the file name.
    full_path = os.path.join(current_directory, f'./{path}')
    # make the connection
    conn = connect()
    cur = conn.cursor()
    # read the file and execute it
    with open(full_path, 'r') as file:
        cur.execute(file.read())
    # commit and close connection
    conn.commit()
    conn.close()

def exec_get_one(sql, args={}):
    """
    get one record from db; based on provide query
    
    :param sql: Sql query.
    :param args: list of sql arguments.
    :return: one record read from database.
    """
    # make the connection
    conn = connect()
    cur = conn.cursor()
    cur.execute(sql, args)
    # read one record
    one = cur.fetchone()
    # close connection and return one record
    conn.close()
    return one

def exec_get_all(sql, args={}):
    """
    get all records from db; based on provide query
    
    :param sql: Sql query.
    :param args: list of sql arguments.
    :return: list of tuples read from the database.
    """
    # make the connection
    conn = connect()
    cur = conn.cursor()
    cur.execute(sql, args)
    # https://www.psycopg.org/docs/cursor.html#cursor.fetchall
    # read all records
    list_of_tuples = cur.fetchall()
    # close connection and return all records
    conn.close()
    return list_of_tuples

def exec_commit(sql, args={}):
    """
    execute a query; based on provided query
    
    :param sql: Sql query.
    :param args: list of sql arguments.
    :return: return the response from the database.
    """
    # make the connection
    conn = connect()
    cur = conn.cursor()
    # execute the query
    result = cur.execute(sql, args)
    # close connection and return the response of query
    conn.commit()
    conn.close()
    return result

def execute_df_values(sql, tuples):
    """
    excute a bulk query (i.e., to insert chunk of data togather) based on provided query and input data
    
    :param sql: Sql query.
    :param tuples: list of tuples for processing.
    :return: return the response from the database.
    """
    # make the connection
    conn = connect()
    cur = conn.cursor()
    # execute the query
    result = extras.execute_values(cur, sql, tuples)
    # close connection and return the response of query
    conn.commit()
    conn.close()
    return result