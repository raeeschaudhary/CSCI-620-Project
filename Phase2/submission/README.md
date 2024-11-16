# Running SQL Queries and Dependency Check. 

1. Navigate to root folder.
    - We assume that the SQL database is popoulated from phase 1. If it is not populated, please use the project Phase 1 to populate the database. 
2. Activate your virtual environment. (unless you have or can install `psycopg2==2.9.9` and `pandas==2.2.2` in your existing environment)
3. Install the python package from requirements.txt e.g., `pip install psycopg2==2.9.9` and `pip install pandas==2.2.2` (if you do not have these.)
4. Navigate to `globals.py` in the root directory to provide SQL database path and credentials using `sql_db_config`. Save the file.
5. Run queries by running `execute_queries.py` e.g., `python execute_queries.py`. (~1 minute)
6. Run Indexes. `execute_indexes.py` e.g., `python execute_indexes.py`. Output will be displayed on consolve. (3 minutes)
7. Restart the db.
    -  Example to restart database server; `pg_ctl.exe restart -D "C:\Program Files\PostgreSQL\16\data` from the path your binary is installed for postgres (on Windows)
8. Re-Run the queries. `execute_queries.py` e.g., `python execute_queries.py`. Output will be displayed on consolve. (1 minute)
    - Optional Run `execute_drop_index.py` e.g., `python execute_drop_index.py` to drop indexes before re-creating. Output will be displayed on console. (1 second)
9. Run FDs by running `generate_fds.py`, e.g., `python generate_fds.py` to examine fds on a subset of data. The output will be saved in `all_tables_fds.txt` file in the root directory. (2 minute)
    - Navigate to `globals.py` in the root directory to provide limit on the count of records used for calculating FDs in each table by setting `limit_size`. Save the file and re-run step 9.

# Populate Mongo DB.
1. Navigate to root folder. Setup the dataset if you do not have it from phase 1. 
    - Navigate to  [Meta Kaggle](https://www.kaggle.com/datasets/kaggle/meta-kaggle)
    - Download these csv files and place them in a data directory; ["Users", "Tags", "Forums", "Organizations", "UserOrganizations", "UserFollowers", "Datasets", "DatasetTags", "Competitions", "CompetitionTags", "Teams", "Submissions", "UserAchievements"]
2. Activate your virtual environment. (unless you have or can install `pymongo==4.8.0` and `pandas==2.2.2` in your existing environment)
3. Install the python package from requirements.txt e.g., `pip install pymongo==4.8.0` and `pip install pandas==2.2.2` (if you do not have these.)
4. Navigate to `globals.py` in the root directory to provide database path and credentials using `mongo_config`. Save the file.
5. Navigate to `globals.py` in the root directory to provide the dataset path. Make sure the path contains the final slash. Also, the code expects to all input files from step 1 exist in the data folder. Save the file.
7. Navigage back to root folder.
7. Some cleaning is necessary for some files; Run the clean_files.py, e.g., `python clean_files.py` (takes about 95 seconds)
    - This will clean and re-write some files in the same data directory where the files are located (provided in step 5).
    - This requires that all necessary from files `input_files`  (Step 1) are present in the data directory.
8. Run `mongo_app.py` e.g., `python mongo_app.py` from the terminal to create and populate the database. (Expect all to run in 3.5 hours)
    - Insertion upto teams would be fast. (around 45 minutes)
    - Updates in Teams (7.6M) for Submissions (15M) could take around 75 minutes. 1-2 Failures occur where document size exceeds MongoDB specified limit. Error unknown.
    - Updates in Users (20M) for Achievements (81M) could take more than 95 minutes. Code can be stopped after this.
    - Reporting documents and sub-documents count takes around 75 minutes as it iterates over all documents and sub-documents to count. If not needed, stop the code. 
9. If the code is run fully, given the amount of time the evaluator has. We confirm that the number of inserted records match with Phase 1 (except few missings in team submissions). 
10. Optional - To remove keys like IDs that were used to map records with _id attribute run `mongo_rem_keys.py` e.g., `python mongo_rem_keys.py` from the terminal.

## Directory structures
    ├── mongo
    │   ├── __init__.py
    │   └── db_methods.py
    ├── sql
    │   ├── __init__.py
	│   ├── db_methods.py
    │   ├── db_utils.py
    │   ├── fds.py
	│   └── queries.py
    ├── all_tables_fds.txt
    ├── clean_files.py
    ├── execute_drop_index.py
    ├── execute_indexes.py
    ├── execute_queries.py
    ├── generate_fds.py
    ├── globals.py
    ├── mongo_app.py
    ├── mongo_rem_keys.py
    ├── README.md
    └── requirements.txt