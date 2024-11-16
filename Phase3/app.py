from sql.db_methods import *
from sql.queries import *
import time

glo = "hello"

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
