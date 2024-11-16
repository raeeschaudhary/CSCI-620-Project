from globals import cleaned_files
from sql.fds import FunctionalDependencies
import time


if __name__=="__main__":
    start_time = time.time()

    # run fds on the database. 
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # running all queries and reporting results on console, along with execution time. 
    print('Running Checking FDs.')
    all_fds = {}
    for table_name in cleaned_files:
        fd = FunctionalDependencies(table_name)
        fd.find_functional_dependency()
        # Store FDs for each table
        all_fds[table_name] = fd.fds 
    # write to file 
    FunctionalDependencies.write_all_fds_to_file(all_fds)

    print('FDs Check Complete.')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")