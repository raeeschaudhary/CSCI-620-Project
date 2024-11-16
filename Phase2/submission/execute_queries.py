from sql.db_methods import run_all_queries
import time

if __name__=="__main__":
    start_time = time.time()

    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # running all queries and reporting results on console, along with execution time. 
    print('Running All 5 Queries.')
    run_all_queries()
    print("Queries Run Success.")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    
    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
