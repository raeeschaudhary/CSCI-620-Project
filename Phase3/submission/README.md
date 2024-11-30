# Analyzing, Cleaning, Inserting Data. 

1. Navigate to root folder. Setup the dataset if you do not have it from phase 1. 
    - Navigate to  [Meta Kaggle](https://www.kaggle.com/datasets/kaggle/meta-kaggle)
    - Download these csv files and place them in a `data_directory`; ["Users", "Tags", "Forums", "Organizations", "UserOrganizations", "UserFollowers", "Datasets", "DatasetTags", "Competitions", "CompetitionTags", "Teams", "Submissions", "UserAchievements"]
2. Activate your virtual environment. (unless you have or can install `psycopg2==2.9.9` and `pandas==2.2.2` in your existing environment)
3. Install the python package from requirements.txt e.g., `pip install psycopg2==2.9.9` and `pip install pandas==2.2.2` (if you do not have these.)
4. Navigate to `globals.py` in the root directory to provide database path and credentials using `sql_db_config`. Save the file.
5. Navigate to `globals.py` in the root directory to provide the `data_directory`. Make sure the path contains the final slash. Also, the code expects to all input files from step 1 exist in the data folder. Save the file.
7. Navigage back to root folder.
8. Run `analyze_data.py` e.g., `python analyze_data.py` to analyze original dataset before any operations. (11 minutes)
    - The output will be saved in `file_info.txt` in the root folder. 
9. Run `filter_data.py` e.g., `python filter_data.py` to filter the dataset into subset columns (as used in Phase 1). (5 minutes)
    - This will filter data and re-write some files in the same `data_directory` where the files are located (provided in step 5).
    - This requires that all necessary from files `input_files` (Step 1) are present in the data directory.
10. Run `insert_data_clean.py` e.g., `python insert_data_clean.py` from to apply cleaning, insertion, and database constraints. (expect all to run in 90 minutes)

# Itemset Mining
1. After following upto Step 10 from above. Navigate back to the root.
    - This assumes that database is populated in `postgres` and credentials are provided in `sql_db_config` from globals.py
2. Run `itemset_mining.py` e.g., `python itemset_mining.py` to run itemset mining. (3 minutes)
    - Observe the output on the console. 


## Directory structures
    ├── sql
    │   ├── __init__.py
	│   ├── db_methods.py
    │   ├── db_utils.py
    │   ├── filtering_methods.py
	│   └── queries.py
    ├── analyze_data.py
    ├── create_schema.sql
    ├── file_info.txt
    ├── filter_data.py
    ├── globals.py
    ├── insert_data_clean.py
    ├── README.md
    ├── requirements.txt
    └── sql_final.sql