from funcs.db_methods import *
from funcs.queries import *
import time

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print("=================================================")

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
