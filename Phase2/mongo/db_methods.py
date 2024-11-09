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

def get_csv_chunker(csv_file, reduced_size=chunk_size):
    """
    Takes an input csv file and reads it into chunks.
    
    :param csv_file: Path to input csv file.
    :params reduced_size: Reduce the size if the database where failure expected. By default chunk_size is a global variable set in globals.py
    :return: pandas chunks to read data in chunks.
    """
    try:
        # read the chunks based on chunk size and return to function call.
        chunks = pd.read_csv(csv_file, chunksize=reduced_size)
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
            if swap_key and str(_id) in existing_ids:
                _id = existing_ids[str(_id)]
            # add it to valid data; keep the tuple formation
            valid_chunk_data.append((_id, sub_chunk_data))
    # return all valid chunk data by only having entries with valid (existing ids)
    return valid_chunk_data

def bulk_update_collection_with_subset(chunk_data, collection_name, subset_name):
    """
    This method updates the collection with sub-document insertion or update.
    
    :param chunk_data: the chunk of data containing subset documents read and mapped from the input file.
    :param collection_name: the name of the collection in the databse to update.
    :param subset_name: the name of sub-document (array) in the collection to update.
    """
    # connect to the database
    db = connect()
    # get a reference to the collection
    collection = db[collection_name]
    # Create a list to hold all update operations
    bulk_operations = []
    # process each chunk as tuple pair of main id (where to update) and sub data (what to update)
    for main_id, sub_data in chunk_data:
        # Update using the _id, which is already replaced in chunk data, Don't insert if the match doesn't exist
        operation = UpdateOne(
            {'_id': main_id}, 
            {'$addToSet': {subset_name: sub_data}},
            upsert=False
        )
        bulk_operations.append(operation)
    try:
        # Execute all operations in a single bulk call
        collection.bulk_write(bulk_operations)
    # throw an error if there an an issue.
    except Exception as e:
        print(f"Error in bulk update: {e}")

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
    This method Insert organizations.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # connect to the database
    db = connect()
    # get a reference to the collection
    collection = db['organizations']
    # get a counter just for output tracking
    counter = 0
    # process each chunk
    for chunk in chunks:
        # Increment and print the counter
        counter += 1
        print(f"\rProcessed Chunks: {counter} of 1", end="")
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
        # insert tuples into database
        collection.insert_many(chunk_data)

def insert_forums(input_file):
    """
    This method Insert Forums.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # connect to the database
    db = connect()
    # get a reference to the collection
    collection = db['forums']
    # get a counter just for output tracking
    counter = 0
    # process each chunk
    for chunk in chunks:
        # Increment and print the counter
        counter += 1
        print(f"\rProcessed Chunks: {counter} of 5", end="")
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            document = {
                "Id": elem[0],
                "ParentForumId": elem[1],
                "Title": elem[2]
            }
            chunk_data.append(document)
        # insert tuples into database
        collection.insert_many(chunk_data)

def insert_tags(input_file):
    """
    This method Insert Tags.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # connect to the database
    db = connect()
    # get a reference to the collection
    collection = db['tags']
    # get a counter just for output tracking
    counter = 0
    # process each chunk
    for chunk in chunks:
        # Increment and print the counter
        counter += 1
        print(f"\rProcessed Chunks: {counter} of 1", end="")
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
        # insert tuples into database
        collection.insert_many(chunk_data)

def insert_competitions(input_file):
    """
    This method Insert Competitions.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # connect to the database
    db = connect()
    # get a reference to the collection
    collection = db['competitions']
    # first check existing forums ids to check for valid entries
    existing_ids = fetch_existing_ids('forums', 'Id')
    # get a counter just for output tracking
    counter = 0
    # process each chunk
    for chunk in chunks:
        # Increment and print the counter
        counter += 1
        print(f"\rProcessed Chunks: {counter} of 1", end="")
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
                "EnabledDate": datetime.strptime(elem[4], '%m/%d/%Y %H:%M:%S'),
                "DeadlineDate": datetime.strptime(elem[5], '%m/%d/%Y %H:%M:%S'),
                "EvaluationAlgorithmName": elem[6],
                "MaxTeamSize": elem[7],
                "TotalTeams": elem[8],
                "TotalSubmissions": elem[9],
                "TotalCompetitors": elem[10],
                "TotalSubmissions": elem[11],
                "CompetitionTags": []
            }
            chunk_data.append(document)
        # filter up the chunk data based on valid existing ids; replace _id with ForumId field
        valid_chunk_data = filter_and_replace_ids(chunk_data, existing_ids, 'ForumId')
        # Insert valid data if not empty
        if valid_chunk_data:
            collection.insert_many(valid_chunk_data)

def insert_competition_tags(input_file):
    """
    This method Insert Competition Tags.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)    
    # first check existing tags ids to check for valid entries
    existing_tag_ids = fetch_existing_ids('tags', 'Id')
    # secondly check existing competetitions ids to check for valid entries against competetitions
    existing_comp_ids = fetch_existing_ids('competitions', 'Id')
    # get a counter just for output tracking
    counter = 0
    # process each chunk
    for chunk in chunks:
        # Increment and print the counter
        counter += 1
        print(f"\rProcessed Chunks: {counter} of 1", end="")
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            comp_tag_data = {
                "CompetitionId": elem[1], # can be removed after mapping 
                "TagId": elem[2]
                }
            # collect comp id as key for each competition entry
            comp_id = elem[1]
            chunk_data.append((comp_id, comp_tag_data))
        # filter up the chunk data based on valid existing_tag_ids; do not replace _id with TagId as competetions relates to competetions
        valid_chunk_data = filter_and_replace_ids_chunk(chunk_data, existing_tag_ids, 'TagId', False)
        # filter up the chunk data based on valid existing competitions ids; replace CompetitionId with _id
        valid_chunk_data = filter_and_replace_ids_chunk(valid_chunk_data, existing_comp_ids, 'CompetitionId', True)
        # Insert valid data if not empty;
        if valid_chunk_data:
            bulk_update_collection_with_subset(valid_chunk_data, 'competitions', 'CompetitionTags')

def insert_users(input_file):
    """
    This method Insert Users.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # connect to the database
    db = connect()
    # get a reference to the collection
    collection = db['users']
    # get a counter just for output tracking
    counter = 0
    # process each chunk
    for chunk in chunks:
        # Increment and print the counter
        counter += 1
        print(f"\rProcessed Chunks: {counter} of 205", end="")
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

def insert_user_organizations(input_file):
    """
    This method Insert User Organizations.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # first check existing existing_org_ids to check for valid entries
    existing_org_ids = fetch_existing_ids('organizations', 'Id')
    # secondly check existing competetitions ids to check for valid entries against users
    existing_user_ids = fetch_existing_ids('users', 'Id')
    # get a counter just for output tracking
    counter = 0
    # process each chunk
    for chunk in chunks:
        # Increment and print the counter
        counter += 1
        print(f"\rProcessed Chunks: {counter} of 1", end="")
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
                "JoinDate": datetime.strptime(elem[3], '%m/%d/%Y')
                }
            # collect user_id as key for each user entry
            user_id = elem[1]
            chunk_data.append((user_id, user_org_data))
        # filter up the chunk data based on valid existing_org_ids; do not replace _id with OrganizationId as organizations relates to users
        valid_chunk_data = filter_and_replace_ids_chunk(chunk_data, existing_org_ids, 'OrganizationId', False)
        # filter up the chunk data based on valid existing_user_ids; replace UserId with _id
        valid_chunk_data = filter_and_replace_ids_chunk(valid_chunk_data, existing_user_ids, 'UserId', True)
        # Insert valid data if not empty
        if valid_chunk_data:
            bulk_update_collection_with_subset(valid_chunk_data, 'users', 'Organizations')

def insert_user_followers(input_file):
    """
    This method Insert User Followers.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. Reducing the chunk size to 75K 
    chunks = get_csv_chunker(csv_file, 75000)
    # first check existing_user_ids to check for valid entries
    existing_user_ids = fetch_existing_ids('users', 'Id')
    # get a counter just for output tracking
    counter = 0
    # process each chunk
    for chunk in chunks:
        # Increment and print the counter
        counter += 1
        print(f"\rProcessed Chunks: {counter} of 21", end="")
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
                "CreationDate": datetime.strptime(elem[3], '%m/%d/%Y')
                }
            # collect user_id as key for each users entry
            user_id = elem[1]
            chunk_data.append((user_id, user_fol_data))
        # filter up the chunk data based on valid existing_user_ids; do not replace _id with FollowingUserId
        valid_chunk_data = filter_and_replace_ids_chunk(chunk_data, existing_user_ids, 'FollowingUserId', False)
        # filter up the chunk data based on valid existing_user_ids; replace UserId with _id 
        valid_chunk_data = filter_and_replace_ids_chunk(valid_chunk_data, existing_user_ids, 'UserId', True)
        # Insert valid data if not empty
        if valid_chunk_data:
            bulk_update_collection_with_subset(valid_chunk_data, 'users', 'Followers')

def insert_datasets(input_file):
    """
    This method Insert Datasets.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # connect to the database
    db = connect()
    # get a reference to the collection
    collection = db['datasets']
    # first check existing forums ids to check for valid entries
    existing_ids = fetch_existing_ids('forums', 'Id')
    # second check existing_user_ids to check for valid entries
    existing_user_ids = fetch_existing_ids('users', 'Id')
    # get a counter just for output tracking
    counter = 0
    # process each chunk
    for chunk in chunks:
        # Increment and print the counter
        counter += 1
        print(f"\rProcessed Chunks: {counter} of 4", end="")
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
                "CreationDate": datetime.strptime(elem[3], '%m/%d/%Y %H:%M:%S'),
                "LastActivityDate": datetime.strptime(elem[4], '%m/%d/%Y'),
                "TotalViews": elem[5],
                "TotalDownloads": elem[6],
                "TotalVotes": elem[7],
                "TotalKernels": elem[8],
                "DatasetTags": []
            }
            chunk_data.append(document)
        # filter up the chunk data based on valid existing forum ids
        valid_chunk_data = filter_and_replace_ids(chunk_data, existing_ids, 'ForumId')
        # filter up the chunk data based on valid existing_user_ids
        valid_chunk_data = filter_and_replace_ids(valid_chunk_data, existing_user_ids, 'CreatorUserId')
        # Insert valid data if not empty
        if valid_chunk_data:
            collection.insert_many(valid_chunk_data)

def insert_dataset_tags(input_file):
    """
    This method Insert Dataset Tags.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. 
    chunks = get_csv_chunker(csv_file)
    # first check existing tags ids to check for valid entries
    existing_tag_ids = fetch_existing_ids('tags', 'Id')
    # secondly check existing dataset ids to check for valid entries
    existing_data_ids = fetch_existing_ids('datasets', 'Id')
    # get a counter just for output tracking
    counter = 0
    # process each chunk
    for chunk in chunks:
        # Increment and print the counter
        counter += 1
        print(f"\rProcessed Chunks: {counter} of 4", end="")
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
            # collect dataset id as key for each dataset entry
            dataset_id = elem[1]
            chunk_data.append((dataset_id, dataset_tags_data))
        # filter up the chunk data based on valid existing_tag_ids; do not replace _id with TagId as datatsettags relates to datasets
        valid_chunk_data = filter_and_replace_ids_chunk(chunk_data, existing_tag_ids, 'TagId', False)
        # filter up the chunk data based on valid existing_data_ids; replace DatasetId with _id
        valid_chunk_data = filter_and_replace_ids_chunk(valid_chunk_data, existing_data_ids, 'DatasetId', True)
        # Insert valid data if not empty
        if valid_chunk_data:
            bulk_update_collection_with_subset(valid_chunk_data, 'datasets', 'DatasetTags')

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
    # get a reference to the collection
    collection = db['teams']
    # first check existing competitions ids to check for valid entries
    existing_ids = fetch_existing_ids('competitions', 'Id')
    # get a counter just for output tracking
    counter = 0
    # process each chunk
    for chunk in chunks:
        # Increment and print the counter
        counter += 1
        print(f"\rProcessed Chunks: {counter} of 77", end="")
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
                "Submissions": []
            }
            chunk_data.append(document)
        # filter up the chunk data based on valid existing competitions ids
        valid_chunk_data = filter_and_replace_ids(chunk_data, existing_ids, 'CompetitionId')
        # Insert valid data is not empty; then insert record
        if valid_chunk_data:
            collection.insert_many(valid_chunk_data)

def insert_submissions_in_teams(input_file):
    """
    This method Insert Submissions in Teams.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. Reducing the chunk size to 50K 
    chunks = get_csv_chunker(csv_file, 50000)
    # first check existing teams ids to check for valid entries
    existing_team_ids = fetch_existing_ids('teams', 'Id')
    # get a counter just for output tracking
    counter = 0
    # process each chunk
    for chunk in chunks:
        # Increment and print the counter
        counter += 1
        print(f"\rProcessed Chunks: {counter} of 297", end="")
        # Set chunk data for document
        chunk_data = []  
        # convert chunk into a list of tuples
        df_values = list(chunk.itertuples(index=False, name=None))
        # iterate over the df_values to create document
        for elem in df_values:
            # Collect data from element attributes as document
            sub_data = {
                "SubmittedUserId": elem[1],
                "TeamId": elem[2],
                "SubmissionDate": datetime.strptime(elem[3], '%m/%d/%Y'),
                "IsAfterDeadline": elem[4],
                "PublicScoreLeaderboardDisplay": elem[5],
                "PrivateScoreLeaderboardDisplay": elem[6],
            }
            team_id = elem[2]
            chunk_data.append((team_id, sub_data))
        # filter up the chunk data based on valid existing teams ids; replace TeamId with _id
        valid_chunk_data = filter_and_replace_ids_chunk(chunk_data, existing_team_ids, 'TeamId', True)
        # Insert valid data if not empty;
        if valid_chunk_data:
            bulk_update_collection_with_subset(valid_chunk_data, 'teams', 'Submissions')
            valid_chunk_data = []

def insert_user_achievements(input_file):
    """
    This method Insert User Achievements.
    
    :param input_file: Name of the CSV file.
    """
    # merge the file name with the data_directory provided in globals.py
    csv_file = data_directory + input_file
    # read the chunks of file by providing the path to file. Reducing the chunk size to 75K 
    chunks = get_csv_chunker(csv_file, 50000)
    # first check existing users ids to check for valid entries
    existing_user_ids = fetch_existing_ids('users', 'Id')
    # get a counter just for output tracking
    counter = 0
    # process each chunk
    for chunk in chunks:
        # Increment and print the counter
        counter += 1
        print(f"\rProcessed Chunks: {counter} of 1640", end="")
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
            # collect user id as key for each user entry
            user_id = elem[1]
            chunk_data.append((user_id, user_achievment_data))
        # filter up the chunk data based on valid existing_user_ids; replace UserId with _id 
        valid_chunk_data = filter_and_replace_ids_chunk(chunk_data, existing_user_ids, 'UserId', True)
        # Insert valid data if not empty
        if valid_chunk_data:
            bulk_update_collection_with_subset(valid_chunk_data, 'users', 'Achievements')
            valid_chunk_data = []

def remove_mapping_keys():
    """
    This method removes the keys used for mapping.
    """
    # connect to database
    db = connect()
    # get the list of all collections
    collections = db.list_collection_names()
    # iterate over each collection
    for collection_name in collections:
        # get the collection
        collection = db[collection_name]
        # Remove the 'Id' field from all each collection using filter to match all documents
        collection.update_many(
            {},  
            {'$unset': {'Id': ""}}
        )
        # for competitions, remove CompetitionId from CompetitionTags, as it was used for mapping only
        if collection_name == "competitions": 
            collection.update_many(
                {}, 
                {'$unset': {'CompetitionTags.$[].CompetitionId': ""}} 
            )
        # for users, remove UserId from Organizations, Followers, Achievements as it was used for mapping only
        elif collection_name == "users": 
            collection.update_many(
                {}, 
                {'$unset': {'Organizations.$[].UserId': ""}} 
            )
            collection.update_many(
                {}, 
                {'$unset': {'Followers.$[].UserId': ""}} 
            )
            collection.update_many(
                {}, 
                {'$unset': {'Achievements.$[].UserId': ""}} 
            )
        # for datasets, remove DatasetId from DatasetTags, as it was used for mapping only
        elif collection_name == "datasets": 
            collection.update_many(
                {}, 
                {'$unset': {'DatasetTags.$[].DatasetId': ""}} 
            )
        # for teams, remove TeamId from Submissions, as it was used for mapping only
        elif collection_name == "teams": 
            collection.update_many(
                {}, 
                {'$unset': {'Submissions.$[].TeamId': ""}} 
            )

def report_db_statistics():
    """
    This method reports the count of records inserted in collections.
    """
    # connect to the database
    db = connect()
    # get all collections
    collections = db.list_collection_names()
    # iterate over the collections
    for collection_name in collections:
        # print the count of documents in each collection
        count = db[collection_name].count_documents({})
        print(f"Collection: {collection_name}   Docuemnts: {count}")
        
        # If the collection is competitions, count the competitionstags
        if collection_name == "competitions": 
            total_comps = 0
            # Iterate through each document in the collection
            for document in db[collection_name].find():
                comps = document.get('CompetitionTags', [])
                total_comps += len(comps)
            print(f"Collection: {collection_name}   CompetitionTags: {total_comps}")
        # If the collection is users, count the Organizations, Followers, Achievements
        elif collection_name == "users": 
            total_orgs = 0
            # Iterate through each document in the collection
            for document in db[collection_name].find():
                orgs = document.get('Organizations', [])
                total_orgs += len(orgs)
            print(f"Collection: {collection_name}   Organizations: {total_orgs}")
            total_foll = 0
            # Iterate through each document in the collection
            for document in db[collection_name].find():
                folls = document.get('Followers', [])
                total_foll += len(folls)
            print(f"Collection: {collection_name}   Followers: {total_foll}")
            total_achi = 0
            # Iterate through each document in the collection
            for document in db[collection_name].find():
                achi = document.get('Achievements', [])
                total_achi += len(achi)
            print(f"Collection: {collection_name}   Achievements: {total_achi}")
        # If the collection is datasets, count the DatasetTags
        elif collection_name == "datasets": 
            dataset_tags = 0
            # Iterate through each document in the collection
            for document in db[collection_name].find():
                tags = document.get('DatasetTags', [])
                dataset_tags += len(tags)
            print(f"Collection: {collection_name}   DatasetTags: {dataset_tags}")
         # If the collection is teams, count the Organizations, Followers, Achievements
        elif collection_name == "teams": 
            totalsubs = 0
            # Iterate through each document in the collection
            for document in db[collection_name].find():
                subs = document.get('Submissions', [])
                totalsubs += len(subs)
            print(f"Collection: {collection_name}   Submissions: {totalsubs}")