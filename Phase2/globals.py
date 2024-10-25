# Provide the db credentials for your machine
db_config = {
    'host': 'localhost',
    'database': 'bdp1',
    'user': 'postgres',
    'password': 'root',
    'port': '5432'
}

# provide the dataset folder; make sure to include the last slash(es).
# data_directory = 'C:\\Users\\Muhammad Raees\\Desktop\\venvs\\pdata\\'
data_directory = 'C:\\Users\\mr2714\\Desktop\\venvs\\pdata\\'

# this is static; increasing uses more memory but less connections to db; reducing make more connection to db
chunk_size = 100000

# these are the list of tables in the db; We use it check table results; to avoid sql injection
cleaned_files = ["Users", "Tags", "Forums", "Organizations", "UserOrganizations", "UserFollowers", "DatasetsCleaned", "DatasetTags", 
                 "CompetitionsCleaned", "CompetitionTags", "TeamsCleaned", "SubmissionsCleaned", "UserAchievements"]

# input files to be processed for cleaning
input_files = ["Users", "Tags", "Forums", "Organizations", "UserOrganizations", "UserFollowers", "Datasets", "DatasetTags",
               "Competitions", "CompetitionTags", "Teams", "Submissions", "UserAchievements"]
