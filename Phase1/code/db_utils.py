import psycopg2
import psycopg2.extras as extras
import os

from code.globals import *

# This file contains methods for connecting and executing operations on the database.
# Each method is self explanatory

# method to connect with database
def connect():
    return psycopg2.connect(
        dbname=db_config['database'],
        user=db_config['user'],
        password=db_config['password'],
        host=db_config['host'],
        port=db_config['port']
    )

# method to connect and execute an sql file script for database
def exec_sql_file(path):
    # full_path = os.path.join(os.path.dirname(__file__), f'./{path}')
    current_directory = os.getcwd()
    full_path = os.path.join(current_directory, f'./{path}')
    conn = connect()
    cur = conn.cursor()
    with open(full_path, 'r') as file:
        cur.execute(file.read())
    conn.commit()
    conn.close()

# method to connect and get one record from db; based on provide query
def exec_get_one(sql, args={}):
    conn = connect()
    cur = conn.cursor()
    cur.execute(sql, args)
    one = cur.fetchone()
    conn.close()
    return one

# method to connect and get all records from db; based on provide query
def exec_get_all(sql, args={}):
    conn = connect()
    cur = conn.cursor()
    cur.execute(sql, args)
    # https://www.psycopg.org/docs/cursor.html#cursor.fetchall
    list_of_tuples = cur.fetchall()
    conn.close()
    return list_of_tuples

# method to execute a query; based on provided query
def exec_commit(sql, args={}):
    conn = connect()
    cur = conn.cursor()
    result = cur.execute(sql, args)
    conn.commit()
    conn.close()
    return result

# method to excute a bulk query (i.e., to insert chunk of data togather) based on provided query and input data
def execute_df_values(sql, tuples):
    conn = connect()
    cur = conn.cursor()
    result = extras.execute_values(cur, sql, tuples)
    conn.commit()
    conn.close()
    return result