from funcs.db_methods import *
from funcs.queries import *
import time

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Creating Database Schema")
    run_schema_script('create_schema.sql')
    print("Database Schema Created")

    # # Users
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Users")
    insert_users("Users.csv", users_insert_query)
    print("Users Inserted")

    # # Organizations
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Organizations")
    insert_organizations("Organizations.csv", organizations_insert_query)
    print("Organizations Created")

    # # UserOrganizations
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting User Organizations")
    insert_user_organizations("UserOrganizations.csv", user_organizations_insert_query)
    print("UserOrganizations Created")

    # # Followers
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting User Followers")
    insert_user_followers("UserFollowers.csv", user_followers_insert_query)
    print("UserFollowers Created")

    # # UserAchievements
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting User Achievements")
    insert_user_achievements("UserAchievements.csv", user_achievements_insert_query)
    print("UserAchievements Created")

    # # CleanedCompetitions
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Cleaned Competitions")
    insert_cleaned_competitions("CleanedCompetitions.csv", competitions_insert_query)
    print("CleanedCompetitions Created")

    # # CleanedCompetitions
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Tags")
    insert_tags("Tags.csv", tags_insert_query)
    print("Tags Created")

    # # CompetitionTags
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting CompetitionTags")
    insert_competition_tags("CompetitionTags.csv", competition_tags_insert_query)
    print("CompetitionTags Created")

    # # Datasets
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Datasets")
    insert_cleaned_datasets("CleanedDatasets.csv", dataset_insert_query)
    print("Datasets Created")

    # # DatasetTags
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting DatasetTags")
    insert_dataset_tags("DatasetTags.csv", dataset_tags_insert_query)
    print("DatasetTags Created")

    # # Forums
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Forums")
    insert_forums("Forums.csv", forums_insert_query)
    print("DatasetTags Created")

    # # Teams
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Teams")
    insert_teams("CleanedTeams.csv", teams_insert_query)
    print("Teams Created")

    # # CleanedSubmissions
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Submissions")
    insert_submissions("CleanedSubmissions.csv", submission_insert_query)
    print("Submissions Created")


    print("=================================================")
    print("Reporting Database Statistics")
    report_db_statistics()
    print("=================================================")

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
