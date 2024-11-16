from sql.db_methods import create_indexes
import time

if __name__=="__main__":
    start_time = time.time()

    print('++++++++++++++++++++++++++++++++++++++++++++++')


    # creating all indedex 
    print('Create All Indexes.')
    create_indexes()
    print("Indexes Created Successfully.")
    print("Restart the database and run queries again.")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    
    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
