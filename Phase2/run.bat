@echo off
python test1.py
python test2.py
pause

@echo off
python clean_files.py
python analyze_data.py
python app.py
pause