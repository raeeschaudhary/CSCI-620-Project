from funcs.db_methods import *
from funcs.queries import *
import time

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Creating Database Schema")
    run_schema_script('create_schema.sql')
    print("Database Schema Created")

    # print('++++++++++++++++++++++++++++++++++++++++++++++')
    # print("Inserting Users")
    # insert_users("Users.csv", users_insert_query)
    # print("Users Inserted")


    # print('++++++++++++++++++++++++++++++++++++++++++++++')
    # print("Inserting Organizations")
    # insert_organizations("Organizations.csv", organizations_insert_query)
    # print("Organizations Created")

    # print('++++++++++++++++++++++++++++++++++++++++++++++')
    # print("Inserting User Organizations")
    # insert_user_organizations("UserOrganizations.csv", user_organizations_insert_query)
    # print("UserOrganizations Created")

    # print('++++++++++++++++++++++++++++++++++++++++++++++')
    # print("Inserting User Followers")
    # insert_user_followers("UserFollowers.csv", user_followers_insert_query)
    # print("UserFollowers Created")

    # UserAchievements
    # print('++++++++++++++++++++++++++++++++++++++++++++++')
    # print("Inserting User Achievements")
    # insert_user_achievements("UserAchievements.csv", user_achievements_insert_query)
    # print("UserAchievements Created")

    # CleanedCompetitions
    # print('++++++++++++++++++++++++++++++++++++++++++++++')
    # print("Inserting Cleaned Competitions")
    # insert_cleaned_competitions("CleanedCompetitions.csv", cleaned_competitions_insert_query)
    # print("CleanedCompetitions Created")

    # CleanedCompetitions
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Inserting Tags")
    insert_tags("Tags.csv", tags_insert_query)
    print("Tags Created")

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
