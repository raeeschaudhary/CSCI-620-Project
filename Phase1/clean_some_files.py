from funcs.db_methods import *
from funcs.queries import *
import time
import pandas as pd


def clean_csv_columns_to_keep(csv_file, output_file, columns_to_keep):
    input_file = data_directory + csv_file
    output_file = data_directory + output_file
    # Read the entire CSV file into a DataFrame
    df = pd.read_csv(input_file)
    
    # Filter the DataFrame to keep only the specified columns
    cleaned_df = df[columns_to_keep]
    
    # Save the cleaned DataFrame to a new CSV file
    cleaned_df.to_csv(output_file, index=False)

def clean_csv_dates(csv_file, output_file, chunk_size=100000):
    input_file = data_directory + csv_file
    output_file = data_directory + output_file
    # Function to check if a date is valid
    def is_valid_date(date_str):
        try:
            pd.to_datetime(date_str, format='%m/%d/%Y', errors='raise')
            return True
        except ValueError:
            return False

    # Use an empty file or create the output file with headers
    first_chunk = True

    # Read the CSV in chunks
    for chunk in pd.read_csv(input_file, chunksize=chunk_size):
        # Filter out rows with invalid dates
        valid_chunk = chunk[chunk['TierAchievementDate'].apply(is_valid_date)]
        
        # Write the valid chunk to the output file
        valid_chunk.to_csv(output_file, mode='a', header=first_chunk, index=False)
        
        # Set header to False for subsequent writes
        first_chunk = False


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
    output_file = 'CleanedCompetitions.csv'
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
    output_file = 'CleanedDatasets.csv'
    clean_csv_columns_to_keep(input_file, output_file, columns_to_keep)
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    ### Clean UserAchievements Dates cleaning need to be checked later; so far it is not processing well
    # therefore keeping the type of this field as varchar 30
    # print('++++++++++++++++++++++++++++++++++++++++++++++')
    # print("Cleaning UserAchievements")
    # input_file = 'UserAchievements.csv'
    # output_file = 'CleanedUserAchievements.csv'
    # clean_csv_dates(input_file, output_file)
    # print('++++++++++++++++++++++++++++++++++++++++++++++')


    ### Clean Teams
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Cleaning Teams")
    columns_to_keep = [
    'Id', 'CompetitionId', 'TeamLeaderId', 'TeamName'
    ]   
    input_file = 'Teams.csv'
    output_file = 'CleanedTeams.csv'
    clean_csv_columns_to_keep(input_file, output_file, columns_to_keep)
    print('++++++++++++++++++++++++++++++++++++++++++++++')
     

    ### Clean Submissions
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Cleaning Submissions")
    columns_to_keep = [
        'Id', 'SubmittedUserId', 'TeamId', 'SubmissionDate', 'IsAfterDeadline', 'PublicScoreLeaderboardDisplay', 'PrivateScoreLeaderboardDisplay'
    ] 
    input_file = 'Submissions.csv'
    output_file = 'CleanedSubmissions.csv'
    clean_csv_columns_to_keep(input_file, output_file, columns_to_keep)
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
