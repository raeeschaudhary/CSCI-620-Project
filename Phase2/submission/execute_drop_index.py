from sql.db_methods import drop_indexes
import time

if __name__=="__main__":
    start_time = time.time()

    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # delete all Indexes 
    print('Drop All Indexes.')
    drop_indexes()
    print("Indexes Dropped Successfully.")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    
    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
