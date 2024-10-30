# Running SQL Queries and Dependency Check. 

1. Navigate to root folder.
    - We assume that the database is popoulated. If it is not populated, please use the project Phase 1 to populate the database. 
2. Activate your virtual environment. (unless you have or can install psycopg2==2.9.9 and pandas==2.2.2 in your existing environment)
3. Install the python package from requirements.txt e.g., `pip install psycopg2==2.9.9` and `pip install pandas==2.2.2` (if you do not have these.)
4. Navigate to `globals.py` in the root directory to provide SQL database path and credentials using `sql_config`. Save the file.
5. Run queries. 
6. Run Indexes.
7. Restart the db.
8. Re-Run the queries.
9. Run FDs. 

# Populate Mongo DB.
1. Navigate to root folder. Setup the dataset if you do not have it from phase 1. 
    - Navigate to  [Meta Kaggle](https://www.kaggle.com/datasets/kaggle/meta-kaggle)
    - Download these csv files and place them in a data directory; ["Users", "Tags", "Forums", "Organizations", "UserOrganizations", "UserFollowers", "Datasets", "DatasetTags", "Competitions", "CompetitionTags", "Teams", "Submissions", "UserAchievements"]
2. Activate your virtual environment. (unless you have or can install pymongo==4.8.0 and pandas==2.2.2 in your existing environment)
3. Install the python package from requirements.txt e.g., `pip install pymongo==4.8.0` and `pip install pandas==2.2.2` (if you do not have these.)
4. Navigate to `globals.py` in the root directory to provide database path and credentials using `mongo_config`. Save the file.
5. Navigate to `globals.py` in the root directory to provide the dataset path. Make sure the path contains the final slash. Also, the code expects to all input files from step 1 exist in the data folder. Save the file.
7. Navigage back to root folder.
7. Some cleaning is necessary for some files; Run the clean_files.py, e.g., `python clean_files.py` (takes about 95 seconds)
    - This will clean and re-write some files in the same data directory where the files are located (provided in step 5).
    - This requires that all necessary from files `input_files`  (Step 1) are present in the data directory.
8. Run `mongo_app.py` e.g., `python mongo_app.py` from the terminal to create and populate the database.
    - Insertion upto teams would be fast. (around 25 minutes)
    - Updates in Teams (7.6M) for Submissions (15M) could take around 2 hours.
        - To test features only, stop the code once few chunks of `Submissions` are processed. Comment the code in `mongo_app.py` upto `insert_submissions_in_teams()` method and rerun.
    - Updates in Users (20M) for Achievements (81M) could take more than 12 hours.
        - To test features only, stop the code once few chunks of `Achievements` are processed. Comment the code in `mongo_app.py` upto `insert_user_achievements()` method and rerun.
9. If the code is run fully, given the amount of time the evaluator has. We confirm that the number of inserted records match with Phase 1. 

## Directory structures
    ├── mongo
    │   ├── __init__.py
	│   ├── db_methods.py
    │   └── queries.py
    ├── funcs
    │   ├── __init__.py
	│   ├── db_methods.py
    │   ├── db_utils.py
	│   └── queries.py
    ├── app.py
    ├── clean_files.py
    ├── mongo_app.py
    ├── create_schema.sql
    ├── queries.py
    ├── README.md
    └── requirements.txt