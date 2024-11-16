from mongo.db_methods import *
import time

glo = "hello"

if __name__=="__main__":
    start_time = time.time()

    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # removing keys used for mapping 
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Removing old primary keys and mapping keys")
    remove_mapping_keys()
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
