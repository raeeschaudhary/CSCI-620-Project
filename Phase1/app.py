from funcs.db_methods import *
from funcs.queries import *
import time

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Creating Database Schema")
    run_schema_script('create_schema.sql')
    print("Creating Database Schema")

    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Users")
    insert_users("Users.csv", users_insert_query)
    print("Creating Database Schema")

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
