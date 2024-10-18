from funcs.db_methods import *
from funcs.queries import *
import time
import pandas as pd


def clean_csv_columns_to_keep(csv_file, output_file, columns_to_keep):
    """
    Takes a input csv file and removes the columns not provided and stores the cleaned file.
    
    :param csv_file: Name to input csv file.
    :param output_file: Name to output csv file.
    :param columns_to_keep: List of columns to keep in the output csv file.
    """
    # combine the csv_file and output_file, with data_directory given in the globals.py
    input_file = data_directory + csv_file
    output_file = data_directory + output_file
    # Read the entire CSV file into a DataFrame
    df = pd.read_csv(input_file)
    # Filter the DataFrame to keep only the specified columns
    cleaned_df = df[columns_to_keep]
    # Save the cleaned DataFrame to a new CSV file
    cleaned_df.to_csv(output_file, index=False)


if __name__=="__main__":
    start_time = time.time()
    
    ### Clean Competitions
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

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
