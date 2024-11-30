import time
from sql.filtering_methods import *

if __name__=="__main__":
    start_time = time.time()
    
    ### Clean Datasets
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Cleaning Datasets")
    columns_to_keep = [
    'Id', 'CreatorUserId', 'ForumId', 'CreationDate',
    'LastActivityDate', 'TotalViews', 'TotalDownloads', 'TotalVotes', 'TotalKernels'
    ]
    input_file = 'Datasets.csv'
    output_file = 'DatasetsCleaned.csv'
    clean_csv_columns_to_keep(input_file, output_file, columns_to_keep)
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    ## Clean Competitions
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Cleaning Competitions")
    columns_to_keep = [
    'Id', 'Slug', 'Title', 'ForumId', 'EnabledDate', 'DeadlineDate',
    'EvaluationAlgorithmName', 'MaxTeamSize', 'NumPrizes', 'TotalTeams', 'TotalCompetitors', 'TotalSubmissions'
    ]
    input_file = 'Competitions.csv'
    output_file = 'CompetitionsCleaned.csv'
    clean_csv_columns_to_keep(input_file, output_file, columns_to_keep)
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    ### Clean Teams
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Cleaning Teams")
    columns_to_keep = [
    'Id', 'CompetitionId', 'TeamLeaderId', 'TeamName'
    ]   
    input_file = 'Teams.csv'
    output_file = 'TeamsCleaned.csv'
    clean_csv_columns_to_keep(input_file, output_file, columns_to_keep)
    print('++++++++++++++++++++++++++++++++++++++++++++++')


    ### Clean Submissions
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Cleaning Submissions")
    columns_to_keep = [
        'Id', 'SubmittedUserId', 'TeamId', 'SubmissionDate', 'IsAfterDeadline', 'PublicScoreLeaderboardDisplay', 'PrivateScoreLeaderboardDisplay'
    ] 
    input_file = 'Submissions.csv'
    output_file = 'SubmissionsCleaned.csv'
    clean_csv_columns_to_keep(input_file, output_file, columns_to_keep)
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    ### check for duplicates
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Checking for Duplicates")
    check_print_duplicates()
    print('++++++++++++++++++++++++++++++++++++++++++++++')


    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
