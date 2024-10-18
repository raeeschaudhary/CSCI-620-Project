# Download and Setup the local storage for database. 

1. Navigate to  [Meta Kaggle](https://www.kaggle.com/datasets/kaggle/meta-kaggle)
2. Download these csv files and place them in a data directory; ["Users", "Tags", "Forums", "Organizations", "UserOrganizations", "UserFollowers", "Datasets", "DatasetTags",
               "Competitions", "CompetitionTags", "Teams", "Submissions", "UserAchievements"]

# Running the Code for Phase 1

1. Navigate to root folder.
2. Activate your virtual environment. (unless you have or can install psycopg2==2.9.9 and pandas==2.2.2 in your existing environment)
3. Install the python package from requirements.txt e.g., `pip install psycopg2==2.9.9` and `pip install pandas==2.2.2` (if you do not have these.)
4. Navigate to `funcs/globals.py` to provide database path and credentials. Save the file.
5. Navigate to `funcs/globals.py` to provide the dataset path. Make sure the path contains the final slash. Also, the code expects to all input files in the data folder. Save the file.
6. Navigage back to root folder.
7. Some cleaning is necessary for some files; Run the clean_files.py, e.g., `python clean_files.py` (takes about 95 seconds)
    - This will clean and re-write some files in the same data directory where the files are located (provided in step 5).
    - This requires that all necessary input_files (Step 2: Download and Setup the local storage for database.) files are present in the data directory.
8. Optional: To analyze data run `python analyze_data.py`. This will provide descriptive statistics by analyzing each file. Results will be stored in "file_info.txt" in the root folder. 
9. Run `app.py` e.g., `python app.py` from the terminal to create and populate the database. (takes about 1.5 hours)

## Directory structures
    ├── funcs
    │   ├── __init__.py
	│   ├── db_methods.py
    │   ├── db_utils.py
	│   ├── globals.py
	│   └── queries.py
    ├── analyze_data.py
    ├── app.py
    ├── clean_files.py
    ├── create_schema.sql
    ├── README.md
    └── requirements.txt