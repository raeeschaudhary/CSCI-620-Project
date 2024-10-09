# Running the Code for Phase 1

1. Navigate to root folder.
2. Activate your virtual environment. (unless you have or can install psycopg2==2.9.9 in your existing environment)
3. Install the python package from requirements.txt e.g., pip install psycopg2==2.9.9 (if you do not have it)
4. Navigate to `code/globals.py` to provide database path and credentials. Save the file.
5. Navigate to `code/globals.py` to provide the dataset path. Make sure the path contains the final slash. Also, the code expects to all input files in the data folder. Save the file.
6. Navigage back to root folder.
7. Run `app.py` e.g., `python app.py` from the terminal to create and populate the database.

## Directory structures
    ├── code
    │   ├── __init__.py
	│   ├── db_methods.py
    │   ├── db_utils.py
	│   ├── globals.py
	│   └── queries.py
    ├── create_schema.sql
    ├── app.py
    ├── README.md
    └── requirements.txt