from sql.db_methods import *
from sql.queries import *
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
    print("\nUsers Inserted")

    # # Tags
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Tags")
    insert_tags("Tags.csv", tags_insert_query)
    print("\nTags Created")

    # # Forums
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Forums")
    insert_forums("Forums.csv", forums_insert_query)
    print("\nForums Created")

     # # Organizations
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Organizations")
    insert_organizations("Organizations.csv", organizations_insert_query)
    print("\nOrganizations Created")

    # # UserOrganizations
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting User Organizations")
    insert_user_organizations("UserOrganizations.csv", user_organizations_insert_query)
    print("\nUserOrganizations Created")

    # # UserFollowers
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting User Followers")
    insert_user_followers("UserFollowers.csv", user_followers_insert_query)
    print("\nUserFollowers Created")

    # # Datasets
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Datasets")
    insert_cleaned_datasets("DatasetsCleaned.csv", dataset_insert_query)
    print("\nDatasets Created")

    # # DatasetTags
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting DatasetTags")
    insert_dataset_tags("DatasetTags.csv", dataset_tags_insert_query)
    print("\nDatasetTags Created")

    # # CompetitionsCleaned
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Cleaned Competitions")
    insert_cleaned_competitions("CompetitionsCleaned.csv", competitions_insert_query)
    print("\nCompetitionsCleaned Created")

    # # CompetitionTags
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting CompetitionTags")
    insert_competition_tags("CompetitionTags.csv", competition_tags_insert_query)
    print("\nCompetitionTags Created")
    
    # # TeamsCleaned
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Teams")
    insert_teams("TeamsCleaned.csv", teams_insert_query)
    print("\nTeams Created")

    # # SubmissionsCleaned
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Submissions")
    insert_submissions("SubmissionsCleaned.csv", submission_insert_query)
    print("\nSubmissions Created")

    # # UserAchievements
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting User Achievements")
    insert_user_achievements("UserAchievements.csv", user_achievements_insert_query)
    print("\nUserAchievements Created")

    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Finalizing the Schema")
    run_schema_script('sql_final.sql')
    print("Database Schema Finalized")

    print("=================================================")
    print("Reporting Database Statistics")
    report_db_statistics()
    print("=================================================")

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
