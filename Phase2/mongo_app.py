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
    
    # report inserted statistics 
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Report DB Statistics")
    report_db_statistics()
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
