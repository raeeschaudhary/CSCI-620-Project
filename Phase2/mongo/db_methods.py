from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo import UpdateOne
import pandas as pd
from globals import chunk_size, data_directory, mongo_db_config, collections
from datetime import datetime

def connect():
    """
    Make the connection with the database and return the connection. 
    
    :returns a database client object.
    """
    try:
        # create a mongodb connection using the mongo_db_config provided in globals.py
        client = MongoClient(host=mongo_db_config['host'], 
                         port=mongo_db_config['port'])
        return client[mongo_db_config['database']]
    except PyMongoError as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def get_csv_chunker(csv_file):
    """
    Takes an input csv file and reads it into chunks.
    
    :param csv_file: Path to input csv file.
    :return: pandas chunks to read data in chunks.
    """
    try:
        # chunk_size is a global variable set in globals.py
        # read the chunks based on chunk size and return to function call.
        chunks = pd.read_csv(csv_file, chunksize=chunk_size)
        return chunks
    except:
        # print the error if the file read fails. 
        print("=======================================================")
        print(f"Wrong file or file path; {csv_file} Does not Exists")
        print("=======================================================")
        return None

def fetch_existing_ids(collection, key):
    """
    fetches the existing keys (primary) from an existing collection. To get user ids; collection = 'users', key = 'Id'
    
    :param collection: the name of the collection to search keys.
    :param key: the name of the key to fetch data from the collection. 
    :return: dictionary of found data (existing keys) (key, _id).
    """
    # Make a connection
    db = connect()
    # find _id for all values matching with give key (e.g., Id)
    existing_data = db[collection].find({}, {'_id': 1, key: 1})
    # retun key:value mapping for found keys to access _id for each found value
    return {str(doc[key]): doc['_id'] for doc in existing_data}

def filter_and_replace_ids(chunk_data, existing_ids, key):
    """
    filters and replaces existing ids (e.g., UserId = 1) with _id (ObjectId) at given key to establish the foreign key relationship in the chunk data.
    
    :param chunk_data: the document data in chunk to replace key with existing keys in the collection (as FK).
    :param existing_ids: the dictionary of fetched keys from the collection (e.g., valid keys in the collection). 
    :param key: the key to be replaced with _id to make the relationship valid (e.g., UserId).
    :return: valid chunk data that replaced Ids for key with valid existing ids from the collection to be referenced.
    """
    # prepare valid data
    valid_chunk_data = []
    # process each entry in the chunk to replace keys
    for entry in chunk_data:
        # find the key to replace 
        test_id = str(entry.get(key))
        # Ensure to consider test_id is present in chunk data (not none) and valid with existing_ids
        if test_id is not None and str(test_id) in existing_ids:
            # Find and update the key (e.g., UserId) with corresponding _id from the collection; mapped by provided dictionary
            entry[key] = existing_ids[str(test_id)] 
            # add it to valid data
            valid_chunk_data.append(entry)
    # return all valid chunk data by only having entries with valid (existing ids)
    return valid_chunk_data

def filter_and_replace_ids_chunk(chunk_data, existing_ids, key, swap_key=False):
    """
    filters and replaces existing ids (e.g., UserId = 1) with _id (ObjectId) at in the chunk data in cases of updating existing collection.
    
    :param chunk_data: the list of tuples in chunk (id: document) to replace key with existing keys in the collection (as FK).
    :param existing_ids: the dictionary of fetched keys from the collection (e.g., valid keys in the collection). 
    :param key: the key to be replaced with _id to make the relationship valid (e.g., UserId).
    :param swap_key: True or False to swap the key value with _id to map to entry in the same collection.
    :return: valid chunk data that replaced Ids for key with valid existing ids from the collection to be referenced.
    """
    # prepare valid data
    valid_chunk_data = []
    # process each entry in the chunk to replace keys; now chunk is key: document (userId: Achievement)
    for _id, sub_chunk_data in chunk_data:
        # Get the key to replace (UserId) from chunk_data
        test_id = str(sub_chunk_data.get(key))
        # Ensure to consider test_id is present in chunk data (not none) and valid with existing_ids
        if test_id is not None and str(test_id) in existing_ids:
            # Find and update the key (e.g., UserId) with corresponding _id from the collection; mapped by provided dictionary
            sub_chunk_data[key] = existing_ids[str(test_id)]
            # replace the existing _id with object ID to match insertion; if swap_key==True
            # if swap_key == True:
            #     _id = existing_ids[_id]
            if swap_key and str(_id) in existing_ids:
                _id = existing_ids[str(_id)]
            # else:
            #     print(f"Warning: ID {_id} not found in existing_ids.")
            # add it to valid data; keep the tuple formation
            valid_chunk_data.append((_id, sub_chunk_data))
    # return all valid chunk data by only having entries with valid (existing ids)
    return valid_chunk_data

def clean_chunk_data(chunk_data, keys_to_check):
    """
    cleans the chunk of data by excluding the results where any of the given key is none.
    
    :param chunk_data: the list of tuples in chunk (id: document) to check for completeness.
    :param keys_to_check: the list of keys to check for nulls. 
    :return: valid chunk data that does not contain any null values.
    """
    # store the cleaned data
    cleaned_data = []
    # process each document
    for document in chunk_data:
        # Check if any of the specified keys are None
        if all(document.get(key) is not None for key in keys_to_check):
            cleaned_data.append(document)
    # return the cleaned set
    return cleaned_data

def cleaning_database():
    """
    clean the database (drop collections) for fresh insertion of the record. 
    """
    # make a database connection
    db = connect()
    # get a list of database connection names
    collections = db.list_collection_names()
    # iterate over collections and drop each and print a message on console.
    for collection_name in collections:
        db[collection_name].drop()
        print(f"Deleted collection: {collection_name}")

def creating_collections():
    """
    Create a list of collections provided by names in collections variable in the globals.py.
    """
    # make a database connection
    db = connect()
    # take the list of collections from globals.py connections
    for collection_name in collections:
        db.create_collection(collection_name)
    # get a list of database connection names
    all_collections = db.list_collection_names()
    # iterate over collections and drop each and print a message on console.
    for collection_name in all_collections:
        print(f"Created collection: {collection_name}")

def insert_organizations(input_file):
    """
    This method Insert organizations
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # connect to the database
    db = connect()
    # get a reference to users collection
    collection = db['organizations']
    # process each chunk
    for chunk in chunks:
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            document = {
                "Id": elem[0],
                "Name": elem[1],
                "Slug": elem[2], 
                "CreationDate": datetime.strptime(elem[3], '%m/%d/%Y'),
                "Description": elem[4]
            }
            chunk_data.append(document)
        # insert tuple into database
        collection.insert_many(chunk_data)

def insert_forums(input_file):
    """
    This method Insert Forums
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # connect to the database
    db = connect()
    # get a reference to users collection
    collection = db['forums']
    # process each chunk
    for chunk in chunks:
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            document = {
                "Id": elem[0],
                "ParForumentId": elem[1],
                "Title": elem[2]
            }
            chunk_data.append(document)
        # insert tuple into database
        collection.insert_many(chunk_data)

def insert_tags(input_file):
    """
    This method Insert Tags
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # connect to the database
    db = connect()
    # get a reference to users collection
    collection = db['tags']
    # process each chunk
    for chunk in chunks:
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            document = {
                "Id": elem[0],
                "ParentTagId": elem[1],
                "Name": elem[2],
                "Slug": elem[3],
                "FullPath": elem[4],
                "Description": elem[5],
                "DatasetCount": elem[6],
                "CompetitionCount": elem[7],
                "KernelCount": elem[8],
            }
            chunk_data.append(document)
        # insert tuple into database
        collection.insert_many(chunk_data)

def insert_competitions(input_file):
    """
    This method Insert Competitions
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # connect to the database
    db = connect()
    # get a reference to users collection
    collection = db['competitions']
    # process each chunk
    for chunk in chunks:
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            document = {
                "Id": elem[0],
                "Slug": elem[1],
                "Title": elem[2],
                "ForumId": elem[3],
                "EnabledDate": elem[4],
                "DeadlineDate": elem[5],
                "EvaluationAlgorithmName": elem[6],
                "MaxTeamSize": elem[7],
                "TotalTeams": elem[8],
                "TotalSubmissions": elem[9],
                "TotalCompetitors": elem[10],
                "TotalSubmissions": elem[11],
                "CompetitionTags": []
            }
            chunk_data.append(document)
        # first check existing forums ids to check for valid entries
        existing_ids = fetch_existing_ids('forums', 'Id')
        # filter up the chunk data based on valid existing ids; replace _id with ForumId field
        valid_chunk_data = filter_and_replace_ids(chunk_data, existing_ids, 'ForumId')
        # Insert valid data is not empty; then insert record
        if valid_chunk_data:
            collection.insert_many(valid_chunk_data)

def bulk_update_competitions_with_tags(chunk_data):
    """
    This method updates competitions collection with tags insertion.
    
    :param chunk_data: the chunk of data containing tags read from the competition tags input file.
    """
    # connect to the database
    db = connect()
    # get a reference to posts collection
    collection = db['competitions']
    # Create a list to hold all update operations
    bulk_operations = []
    # process each chunk as tuple pair of post id and comment data
    for comp_id, comp_tag in chunk_data:
        # Update using the _id, which is already replaced in chunk data, Don't insert if post doesn't exist
        operation = UpdateOne(
            {'_id': comp_id}, 
            {'$addToSet': {'CompetitionTags': comp_tag}},
            upsert=False
        )
        bulk_operations.append(operation)
    try:
        # Execute all operations in a single bulk call
        collection.bulk_write(bulk_operations)
    # throw an error if there an an issue.
    except Exception as e:
        print(f"Error in bulk update: {e}")

def insert_competition_tags(input_file):
    """
    This method Insert Competition Tags
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            comp_tag_data = {
                "CompetitionId": elem[1], # will be removed after mapping 
                "TagId": elem[2]
                }
            # collect comp id as key for each competition entry
            comp_id = elem[1]
            chunk_data.append((comp_id, comp_tag_data))
        # first check existing tags ids to check for valid entries
        existing_tag_ids = fetch_existing_ids('tags', 'Id')
        # filter up the chunk data based on valid existing user ids; do not replace _id with TagId as competetions relates to competetions
        valid_chunk_data = filter_and_replace_ids_chunk(chunk_data, existing_tag_ids, 'TagId', False)
        # secondly check existing competetitions ids to check for valid entries against competetitions collection
        existing_comp_ids = fetch_existing_ids('competitions', 'Id')
        # filter up the chunk data based on valid existing competitions ids; replace _id with CompetitionId 
        valid_chunk_data = filter_and_replace_ids_chunk(valid_chunk_data, existing_comp_ids, 'CompetitionId', True)
        # Insert valid data is not empty; then insert record
        if valid_chunk_data:
            bulk_update_competitions_with_tags(valid_chunk_data)

def insert_users(input_file):
    """
    This method Insert Users
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # connect to the database
    db = connect()
    # get a reference to users collection
    collection = db['users']
    # process each chunk
    for chunk in chunks:
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            document = {
                "Id": elem[0],
                "UserName": elem[1],
                "DisplayName": elem[2],
                "RegisterDate": datetime.strptime(elem[3], '%m/%d/%Y'),
                "PerformanceTier": elem[4],
                "Country": elem[5],
                "Organizations": [],
                "Followers": [],
                "Achievements": []
            }
            chunk_data.append(document)
        collection.insert_many(chunk_data)

def bulk_update_users_with_organizations(chunk_data):
    """
    This method updates users collection with orgnizations insertion.
    
    :param chunk_data: the chunk of data containing orgniazations read from the user organizations input file.
    """
    # connect to the database
    db = connect()
    # get a reference to posts collection
    collection = db['users']
    # Create a list to hold all update operations
    bulk_operations = []
    # process each chunk as tuple pair of post id and comment data
    for user_id, user_org in chunk_data:
        # Update using the _id, which is already replaced in chunk data, Don't insert if post doesn't exist
        operation = UpdateOne(
            {'_id': user_id}, 
            {'$addToSet': {'Organizations': user_org}},
            upsert=False
        )
        bulk_operations.append(operation)
    try:
        # Execute all operations in a single bulk call
        collection.bulk_write(bulk_operations)
    # throw an error if there an an issue.
    except Exception as e:
        print(f"Error in bulk update: {e}")

def insert_user_organizations(input_file):
    """
    This method Insert User Organizations
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            user_org_data = {
                "UserId": elem[1], # will be removed after mapping 
                "OrganizationId": elem[2],
                "JoinDate": elem[3]
                }
            # collect comp id as key for each competition entry
            user_id = elem[1]
            chunk_data.append((user_id, user_org_data))
        # first check existing tags ids to check for valid entries
        existing_org_ids = fetch_existing_ids('organizations', 'Id')
        # filter up the chunk data based on valid existing ids; do not replace _id with OrganizationId as organizations relates to users
        valid_chunk_data = filter_and_replace_ids_chunk(chunk_data, existing_org_ids, 'OrganizationId', False)
        # secondly check existing competetitions ids to check for valid entries against competetitions collection
        existing_user_ids = fetch_existing_ids('users', 'Id')
        # filter up the chunk data based on valid existing competitions ids; replace _id with CompetitionId 
        valid_chunk_data = filter_and_replace_ids_chunk(valid_chunk_data, existing_user_ids, 'UserId', True)
        # Insert valid data is not empty; then insert record
        if valid_chunk_data:
            bulk_update_users_with_organizations(valid_chunk_data)

def bulk_update_users_with_followers(chunk_data):
    """
    This method updates users collection with followers insertion.
    
    :param chunk_data: the chunk of data containing followers read from the user followers input file.
    """
    # connect to the database
    db = connect()
    # get a reference to posts collection
    collection = db['users']
    # Create a list to hold all update operations
    bulk_operations = []
    # process each chunk as tuple pair of post id and comment data
    for user_id, user_follower in chunk_data:
        # Update using the _id, which is already replaced in chunk data, Don't insert if post doesn't exist
        operation = UpdateOne(
            {'_id': user_id}, 
            {'$addToSet': {'Followers': user_follower}},
            upsert=False
        )
        bulk_operations.append(operation)
    try:
        # Execute all operations in a single bulk call
        collection.bulk_write(bulk_operations)
    # throw an error if there an an issue.
    except Exception as e:
        print(f"Error in bulk update: {e}")

def insert_user_followers(input_file):
    """
    This method Insert User Followers.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            user_fol_data = {
                "UserId": elem[1], # will be removed after mapping 
                "FollowingUserId": elem[2],
                "CreationDate": elem[3]
                }
            # collect comp id as key for each competition entry
            user_id = elem[1]
            chunk_data.append((user_id, user_fol_data))
        # first check existing tags ids to check for valid entries
        existing_org_ids = fetch_existing_ids('Users', 'Id')
        # filter up the chunk data based on valid existing ids; do not replace _id with OrganizationId as organizations relates to users
        valid_chunk_data = filter_and_replace_ids_chunk(chunk_data, existing_org_ids, 'FollowingUserId', False)
        # secondly check existing competetitions ids to check for valid entries against competetitions collection
        existing_user_ids = fetch_existing_ids('users', 'Id')
        # filter up the chunk data based on valid existing competitions ids; replace _id with CompetitionId 
        valid_chunk_data = filter_and_replace_ids_chunk(valid_chunk_data, existing_user_ids, 'UserId', True)
        # Insert valid data is not empty; then insert record
        if valid_chunk_data:
            bulk_update_users_with_followers(valid_chunk_data)

def bulk_update_users_with_achievement(chunk_data):
    """
    This method updates users collection with achievements insertion.
    
    :param chunk_data: the chunk of data containing achievements read from the user achievements input file.
    """
    # connect to the database
    db = connect()
    # get a reference to posts collection
    collection = db['users']
    # Create a list to hold all update operations
    bulk_operations = []
    # process each chunk as tuple pair of post id and comment data
    for user_id, user_achievement in chunk_data:
        # Update using the _id, which is already replaced in chunk data, Don't insert if post doesn't exist
        operation = UpdateOne(
            {'_id': user_id}, 
            {'$addToSet': {'Achievements': user_achievement}},
            upsert=False
        )
        bulk_operations.append(operation)
    try:
        # Execute all operations in a single bulk call
        collection.bulk_write(bulk_operations)
    # throw an error if there an an issue.
    except Exception as e:
        print(f"Error in bulk update: {e}")

def insert_user_achievements(input_file):
    """
    This method Insert User Achievements.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            user_achievment_data = {
                "UserId": elem[1], # will be removed after mapping 
                "AchievementType": elem[2],
                "Tier": elem[3],
                "TierAchievementDate": elem[4],
                "Points": elem[5],
                "CurrentRanking": elem[6],
                "HighestRanking": elem[7],
                "TotalGold": elem[8],
                "TotalSilver": elem[9],
                "TotalBronze": elem[10]
                }
            # collect comp id as key for each competition entry
            user_id = elem[1]
            chunk_data.append((user_id, user_achievment_data))
        # first check existing tags ids to check for valid entries
        existing_user_ids = fetch_existing_ids('Users', 'Id')
        # filter up the chunk data based on valid existing competitions ids; replace _id with CompetitionId 
        valid_chunk_data = filter_and_replace_ids_chunk(valid_chunk_data, existing_user_ids, 'UserId', True)
        # Insert valid data is not empty; then insert record
        if valid_chunk_data:
            bulk_update_users_with_achievement(valid_chunk_data)

def insert_datasets(input_file):
    """
    This method Insert Datasets
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # connect to the database
    db = connect()
    # get a reference to users collection
    collection = db['datasets']
    # process each chunk
    for chunk in chunks:
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            document = {
                "Id": elem[0],
                "CreatorUserId": elem[1],
                "ForumId": elem[2],
                "CreationDate": elem[3],
                "LastActivityDate": elem[4],
                "TotalViews": elem[5],
                "TotalDownloads": elem[6],
                "TotalVotes": elem[7],
                "TotalKernels": elem[8],
                "DatasetTags": []
            }
            chunk_data.append(document)
        # first check existing forums ids to check for valid entries
        existing_ids = fetch_existing_ids('forums', 'Id')
        # filter up the chunk data based on valid existing ids; replace _id with ForumId field
        valid_chunk_data = filter_and_replace_ids(chunk_data, existing_ids, 'ForumId')
        # first check existing forums ids to check for valid entries
        existing_user_ids = fetch_existing_ids('users', 'Id')
        # filter up the chunk data based on valid existing ids; replace _id with ForumId field
        valid_chunk_data = filter_and_replace_ids(valid_chunk_data, existing_user_ids, 'CreatorUserId')
        # Insert valid data is not empty; then insert record
        if valid_chunk_data:
            collection.insert_many(valid_chunk_data)

def bulk_update_datasets_with_tags(chunk_data):
    """
    This method updates dataset collection with tags insertion.
    
    :param chunk_data: the chunk of data containing tags read from the datasets tags input file.
    """
    # connect to the database
    db = connect()
    # get a reference to posts collection
    collection = db['datasets']
    # Create a list to hold all update operations
    bulk_operations = []
    # process each chunk as tuple pair of post id and comment data
    for user_id, dataset_tags in chunk_data:
        # Update using the _id, which is already replaced in chunk data, Don't insert if post doesn't exist
        operation = UpdateOne(
            {'_id': user_id}, 
            {'$addToSet': {'DatasetTags': dataset_tags}},
            upsert=False
        )
        bulk_operations.append(operation)
    try:
        # Execute all operations in a single bulk call
        collection.bulk_write(bulk_operations)
    # throw an error if there an an issue.
    except Exception as e:
        print(f"Error in bulk update: {e}")

def insert_user_achievements(input_file):
    """
    This method Insert User Achievements.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # process each chunk
    for chunk in chunks:
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            dataset_tags_data = {
                "DatasetId": elem[1], # will be removed after mapping 
                "TagId": elem[2],
                }
            # collect comp id as key for each competition entry
            dataset_id = elem[1]
            chunk_data.append((dataset_id, dataset_tags_data))
        # first check existing tags ids to check for valid entries
        # first check existing tags ids to check for valid entries
        existing_tag_ids = fetch_existing_ids('tags', 'Id')
        # filter up the chunk data based on valid existing user ids; do not replace _id with TagId as competetions relates to competetions
        valid_chunk_data = filter_and_replace_ids_chunk(chunk_data, existing_tag_ids, 'TagId', False)
        # secondly check existing competetitions ids to check for valid entries against competetitions collection
        existing_data_ids = fetch_existing_ids('datasets', 'Id')
        # filter up the chunk data based on valid existing competitions ids; replace _id with CompetitionId 
        valid_chunk_data = filter_and_replace_ids_chunk(valid_chunk_data, existing_data_ids, 'DatasetId', True)
        # Insert valid data is not empty; then insert record
        if valid_chunk_data:
            bulk_update_datasets_with_tags(valid_chunk_data)

def insert_teams(input_file):
    """
    This method Insert Teams.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # connect to the database
    db = connect()
    # get a reference to users collection
    collection = db['teams']
    # process each chunk
    for chunk in chunks:
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            document = {
                "Id": elem[0],
                "CompetitionId": elem[1],
                "TeamLeaderId": elem[2],
                "TeamName": elem[3],
            }
            chunk_data.append(document)
        # first check existing forums ids to check for valid entries
        existing_ids = fetch_existing_ids('competitions', 'Id')
        # filter up the chunk data based on valid existing ids; replace _id with ForumId field
        valid_chunk_data = filter_and_replace_ids(chunk_data, existing_ids, 'CompetitionId')
        # Insert valid data is not empty; then insert record
        if valid_chunk_data:
            collection.insert_many(valid_chunk_data)

def insert_submissions(input_file):
    """
    This method Insert Submissions.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # connect to the database
    db = connect()
    # get a reference to users collection
    collection = db['teams']
    # process each chunk
    for chunk in chunks:
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            document = {
                "Id": elem[0],
                "SubmittedUserId": elem[1],
                "TeamId": elem[2],
                "SourceKernelVersionId": elem[3],
                "SubmissionDate": elem[4],
                "ScoreDate": elem[5],
                "IsAfterDeadline": elem[6],
                "PublicScoreLeaderboardDisplay": elem[7],
                "PublicScoreFullPrecision": elem[8],
                "PrivateScoreLeaderboardDisplay": elem[9],
                "PrivateScoreFullPrecision": elem[10],
            }
            chunk_data.append(document)
        # first check existing forums ids to check for valid entries
        existing_ids = fetch_existing_ids('teams', 'Id')
        # filter up the chunk data based on valid existing ids; replace _id with ForumId field
        valid_chunk_data = filter_and_replace_ids(chunk_data, existing_ids, 'TeamId')
        # Insert valid data is not empty; then insert record
        if valid_chunk_data:
            collection.insert_many(valid_chunk_data)

def report_db_statistics():
    """
    This method reports the count of records inserted in collections.
    """
    # connect to the database
    db = connect()
    # get all collections
    collections = db.list_collection_names()
    # print the count of documents in each collection
    for collection_name in collections:
        count = db[collection_name].count_documents({})
        print(f"Collection: {collection_name}   Docuemnts: {count}")
        
        # If the collection is competitions, count the competitionstags
        if collection_name == "competitions": 
            print(f"Collection: {collection_name}   Docuemnts: {count}")
            total_badges = 0
            # Iterate through each document in the collection
            for document in db[collection_name].find():
                badges = document.get('CompetitionTags', [])
                total_badges += len(badges)
            print(f"Collection: {collection_name}   CompetitionTags: {total_badges}")
        # # If the collection is posts, count the comments
        # if collection_name == "posts":
        #     print(f"Collection: {collection_name}   Posts: {count}")
        #     total_comments = 0
        #     total_tags = 0
        #     # Iterate through each document in the collection
        #     for document in db[collection_name].find():
        #         tags = document.get('Tags', [])
        #         comments = document.get('Comments', [])
        #         total_tags += len(tags)
        #         total_comments += len(comments)
        #     print(f"Collection: {collection_name}   Comments: {total_comments}")
        #     print(f"Collection: {collection_name}   Tags: {total_tags}")

def list_indexes_all():
    """
    This method reports the list of indexes on users and posts collections.

    :returns a list of indexes.
    """
    # list indexes 
    indexes_list = []
    # connect to the database
    db = connect()
    # get the posts collection
    posts_collection = db['posts'] 
    # get posts indexes and append to the list
    for index in posts_collection.list_indexes():
        indexes_list.append(index)
    # get the users collection
    users_collection = db['users'] 
    # get users indexes and append to the list
    for index in users_collection.list_indexes():
        indexes_list.append(index)
    # return the combined list
    return indexes_list