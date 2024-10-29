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
    print("Organizations inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Forums
    print('Inserting Forums')
    insert_forums("Forums.csv")
    print("Forums inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Tags
    print('Inserting Tags')
    insert_tags("Tags.csv")
    print("Tags inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    # CompetitionsCleaned
    print('Inserting Competitions')
    insert_competitions("CompetitionsCleaned.csv")
    print("Competitions inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # CompetitionTags
    print('Inserting Competition Tags In Competitions')
    insert_competition_tags("CompetitionTags.csv")
    print("Competitions Tags inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Users
    print('Inserting Users')
    insert_users("Users.csv")
    print("Users inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    # UserOrganizations
    print('Inserting User Organizations In Users')
    insert_user_organizations("UserOrganizations.csv")
    print("Users Organizations inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # UserFollowers
    print('Inserting User Followers In Users')
    insert_user_followers("UserFollowers.csv")
    print("Users Followers inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # UserAchievements
    print('Inserting User Achievements In Users')
    insert_user_achievements("UserAchievements.csv")
    print("Users Achievements inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Datasets
    print('Inserting Datasets')
    insert_datasets("DatasetsCleaned.csv")
    print("Datasets inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # DatasetTags
    print('Inserting Tags in Datasets')
    insert_dataset_tags("DatasetTags.csv")
    print("Dataset Tags inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Teams
    print('Inserting Teams')
    insert_teams("TeamsCleaned.csv")
    print("Teams inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Submissions
    print('Inserting Submissions')
    insert_submissions_in_teams("SubmissionsCleaned.csv")
    print("Submissions inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    # report inserted statistics 
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Report DB Statistics")
    report_db_statistics()
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
