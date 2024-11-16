# Provide the SQL db credentials for your machine
sql_db_config = {
    'host': 'localhost',
    'database': 'bdp1',
    'user': 'postgres',
    'password': 'root',
    'port': '5432'
}

# Provide the mongodb credentials for your machine
mongo_db_config = {
    'host': 'localhost',
    'port': 27017,
    'database': 'bdp2'
}

# provide the dataset folder; make sure to include the last slash(es).
# data_directory = 'C:\\Users\\Muhammad Raees\\Desktop\\venvs\\pdata\\'
data_directory = 'C:\\Users\\mr2714\\Desktop\\venvs\\pdata\\'

# this is static; increasing uses more memory but less connections to db; reducing make more connection to db
chunk_size = 100000

# this is the limit on generate FDs code, can be increaed or decreased. 
limit_size = 100000

# these are the list of tables in the db; We use it check table results; to avoid sql injection
cleaned_files = ["Users", "Tags", "Forums", "Organizations", "UserOrganizations", "UserFollowers", "DatasetsCleaned", "DatasetTags", 
                 "CompetitionsCleaned", "CompetitionTags", "TeamsCleaned", "SubmissionsCleaned", "UserAchievements"]

# input files to be processed for cleaning
input_files = ["Users", "Tags", "Forums", "Organizations", "UserOrganizations", "UserFollowers", "Datasets", "DatasetTags",
               "Competitions", "CompetitionTags", "Teams", "Submissions", "UserAchievements"]

collections = ['organizations', 'forums', 'tags', 'competitions', 'users', 'datasets', 'teams']