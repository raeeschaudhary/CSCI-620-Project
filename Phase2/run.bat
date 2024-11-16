@echo off

python execute_queries.py

python execute_indexes.py
python execute_drop_index.py
python execute_indexes.py

python execute_queries.py

python generate_fds.py

python clean_files.py

python mongo_app.py

python mongo_rem_keys.py

pause