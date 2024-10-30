from mongo.db_methods import *
import time

glo = "hello"

if __name__=="__main__":
    start_time = time.time()

    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # delete all collections
    print('Deleting all data and collections in the database.')
    cleaning_database()
    print("Database cleaned. Ready for fresh insertion.")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # create collections given in gloabls.py
    print('Recreating Collections Fresh.')
    creating_collections()
    print("New Collections Created.")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Organizations
    print('Inserting Organizations')
    insert_organizations("Organizations.csv")
    print("\nOrganizations inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Forums
    print('Inserting Forums')
    insert_forums("Forums.csv")
    print("\nForums inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Tags
    print('Inserting Tags')
    insert_tags("Tags.csv")
    print("\nTags inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    # CompetitionsCleaned
    print('Inserting Competitions')
    insert_competitions("CompetitionsCleaned.csv")
    print("\nCompetitions inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # CompetitionTags
    print('Inserting Competition Tags In Competitions')
    insert_competition_tags("CompetitionTags.csv")
    print("\nCompetitions Tags inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Users
    print('Inserting Users')
    insert_users("Users.csv")
    print("\nUsers inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    # UserOrganizations
    print('Inserting User Organizations In Users')
    insert_user_organizations("UserOrganizations.csv")
    print("\nUsers Organizations inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # UserFollowers
    print('Inserting User Followers In Users')
    insert_user_followers("UserFollowers.csv")
    print("\nUsers Followers inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Datasets
    print('Inserting Datasets')
    insert_datasets("DatasetsCleaned.csv")
    print("\nDatasets inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # DatasetTags
    print('Inserting Tags in Datasets')
    insert_dataset_tags("DatasetTags.csv")
    print("\nDataset Tags inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Teams
    print('Inserting Teams')
    insert_teams("TeamsCleaned.csv")
    print("\nTeams inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Submissions
    print('Inserting Submissions')
    insert_submissions_in_teams("SubmissionsCleaned.csv")
    print("\nSubmissions inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    # UserAchievements
    print('Inserting User Achievements in Users')
    print('******************************************************************************************************************************')
    print("This is the last insertion. There are around 810 chunks of 100K each. Could take 40 hours inserting 81M records in 20M users.")
    print('******************************************************************************************************************************')
    insert_user_achievements("UserAchievements.csv")
    print("\nUsers Achievements inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # report inserted statistics 
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Report DB Statistics")
    report_db_statistics()
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # removing keys used for mapping 
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Removing old primary keys and mapping keys")
    remove_mapping_keys()
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
