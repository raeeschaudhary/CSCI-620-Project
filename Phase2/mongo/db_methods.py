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
        # create a mongodb connection using the db_config provided in globals.py
        client = MongoClient(host=mongo_db_config['host'], 
                         port=mongo_db_config['port'])
        return client[mongo_db_config['database']]
    except PyMongoError as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def get_csv_chunker(csv_file):
    """
    Takes a input csv file and reads it into chunks.
    
    :param csv_file: Path to input csv file.
    :return: pandas chunks to read data in chunks.
    """
    try:
        # chunk_size is a global variable set it globals.py
        # read the chunks based on chunk size and return to function call.
        chunks = pd.read_csv(csv_file, chunksize=chunk_size)
        return chunks
    except:
        # print the error if the file read fails. 
        print("=======================================================")
        print("Wrong file or file path; ", csv_file, " Does not Exists")
        print("=======================================================")
        return None

def fetch_existing_ids(collection, key):
    """
    fetches the existing keys (primary) from an exisitig collection. To get user ids; collection = 'users', key = 'Id'
    
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
    # process each entry in the chunk to replace keys; now chunk is key: document (userId: bagde)
    for _id, sub_chunk_data in chunk_data:
        # Get the key to replace (UserId) from chunk_data
        test_id = str(sub_chunk_data.get(key))
        # Ensure to consider test_id is present in chunk data (not none) and valid with existing_ids
        if test_id is not None and str(test_id) in existing_ids:
            # Find and update the key (e.g., UserId) with corresponding _id from the collection; mapped by provided dictionary
            sub_chunk_data[key] = existing_ids[str(test_id)]
            # replace the existing _id with object ID to match insertion; if swap_key==True
            if swap_key == True:
                _id = existing_ids[_id]
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
    Create a list of collections privded by names in the globals collections variable.
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

# This method inserts users in chunks to database.
# get the input file; make sure the data directory is correct and the input file is present in the data directory;
# otherwise the execution will fail, this is applicable for all insert methods
def insert_users(input_file):
    """
    This method Insert users.
    
    :param input_file: Name of the XML file to extract data from.
    """
    # merge the file name with the data_directory provided in globals.py
    input_file = data_directory + input_file
    # get the context for reading the file
    context = ET.iterparse(input_file, events=("start", "end"))
    # connect to the database
    db = connect()
    # get a reference to users collection
    collection = db['users']
    # an indicator to keep track of chunks; not necessary
    chunk_count = 0
    # an indicator to keep track of elements in the chunk 
    elements_in_chunk = 0
    # List to hold data to be inserted into the database
    chunk_data = []  
    # Iterate over the elements present in the input file
    for event, elem in context:
        if event == 'end' and elem.tag == 'row':
            if elem.get('Id') is not None:
                # Collect data from element attributes as document
                document = {
                    "Id": elem.get('Id'),
                    "DisplayName": elem.get('DisplayName'),
                    "CreationDate": datetime.fromisoformat(elem.get('CreationDate')),
                    "LastAccessDate": datetime.fromisoformat(elem.get('LastAccessDate')),
                    "Views": int(elem.get('Views')),
                    'Badges': []
                }
                chunk_data.append(document)
                elements_in_chunk += 1
            # If chunk is full, save the chunk and reset
            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                # from data analysis from previous assignments, the selected fields does not contain nulls, so cleaning is skipped based on that surety.
                # Insert the clean data into db;
                collection.insert_many(chunk_data)
                chunk_data = []  
                elements_in_chunk = 0
            # Clear the element to free up memory
            elem.clear()
        print(f"\rProcessed more than {chunk_count * chunk_size} elements. ", end="")
    
    # Processing the remaining data in final chunk
    if elements_in_chunk > 0:
        chunk_count += 1
        # from data analysis from previous assignments, the selected fields does not contain nulls, so cleaning is skipped based on that surety.
        # Insert the clean data into db;
        collection.insert_many(chunk_data)

def bulk_update_users_with_badges(chunk_data):
    """
    This method updates users collection with badges insertion.
    
    :param chunk_data: the chunk of data containing badges read from the badges input file.
    """
    # connect to the database
    db = connect()
    # get a reference to users collection
    collection = db['users']
    # Create a list to hold all update operations
    bulk_operations = []
    # process each chunk as tuple pair of user id and badge data
    for user_id, badge in chunk_data:
        # Update using the _id, which is already replaced in chunk data, Don't insert if user doesn't exist
        operation = UpdateOne(
            {'_id': user_id}, 
            {'$addToSet': {'Badges': badge}}, 
            upsert=False 
        )
        bulk_operations.append(operation)
    try:
        # Execute all operations in a single bulk call
        collection.bulk_write(bulk_operations)
    # throw an error if there an an issue
    except Exception as e:
        print(f"Error in bulk update: {e}")

def insert_badges(input_file):
    """
    This method insert badges in users collection.
    
    :param input_file: Name of the XML file to extract data from.
    """
    # merge the file name with the data_directory provided in globals.py
    input_file = data_directory + input_file
    # get the context for reading the file
    context = ET.iterparse(input_file, events=("start", "end"))
    # an indicator to keep track of chunks; not necessary
    chunk_count = 0
    # an indicator to keep track of elements in the chunk 
    elements_in_chunk = 0
    # List to hold data to be inserted into the database
    chunk_data = []  
    # Iterate over the elements present in the input file    
    for event, elem in context:
        if elem.tag == 'row':
            if elem.get('Id') is not None and elem.get('UserId') is not None:
                # Collect badge data from element attributes
                badge_data = {
                    "Name": elem.get('Name'),
                    "Date": datetime.fromisoformat(elem.get('Date')),
                    "UserId": elem.get('UserId') # will be removed after mapping
                }
                # collect user id as key for each badge entry
                user_id = elem.get('UserId')
                chunk_data.append((user_id, badge_data))
                elements_in_chunk += 1

            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                # from data analysis from previous assignments, the selected fields does not contain nulls, so cleaning is skipped based on that surety.
                # first check existing users ids to check for valid entries
                existing_ids = fetch_existing_ids('users', 'Id')
                # filter up the chunk data based on valid existing ids; replace _id with UserId field
                valid_chunk_data = filter_and_replace_ids_chunk(chunk_data, existing_ids, 'UserId', True)
                chunk_data = [] 
                elements_in_chunk = 0
                # Insert valid data is not empty; then insert record
                if valid_chunk_data:
                    bulk_update_users_with_badges(valid_chunk_data)
                    valid_chunk_data = []
            # clear up elements
            elem.clear()
        print(f"\rProcessed more than {chunk_count * chunk_size} elements. ", end="")

    # Process any remaining data
    if elements_in_chunk > 0: 
        chunk_count += 1
        # from data analysis from previous assignments, the selected fields does not contain nulls, so cleaning is skipped based on that surety.
        # first check existing users ids to check for valid entries
        existing_ids = fetch_existing_ids('users', 'Id')
        # filter up the chunk data based on valid existing ids; replace _id with UserId field
        valid_chunk_data = filter_and_replace_ids_chunk(chunk_data, existing_ids, 'UserId', True)
        # Insert valid data is not empty; then insert record
        if valid_chunk_data:
            bulk_update_users_with_badges(valid_chunk_data)

def insert_posts(input_file):
    """
    This method insert posts.
    
    :param input_file: Name of the XML file to extract data from.
    """
    # merge the file name with the data_directory provided in globals.py
    input_file = data_directory + input_file
    # get the context for reading the file
    context = ET.iterparse(input_file, events=("start", "end"))
    # connect to the database
    db = connect()
    # get a reference to users collection
    collection = db['posts']
    # an indicator to keep track of chunks; not necessary
    chunk_count = 0
    # an indicator to keep track of elements in the chunk 
    elements_in_chunk = 0
    # List to hold data to be inserted into the database
    chunk_data = []  
    # Iterate over the elements present in the input file
    for event, elem in context:
        if elem.tag == 'row':
            if elem.get('Id') is not None and elem.get('OwnerUserId') is not None: 
                # get tags data to and clean it to remove <, >
                tags_string = elem.get('Tags')
                # insert as tag list
                tag_list = []
                if tags_string:
                    # strip < and > from left and right
                    tags = [tag.strip('>') for tag in tags_string.split('<') if tag.strip()]
                    # create list
                    tag_list = [tag for tag in tags if tag]
                # Collect data from element attributes as document
                document = {
                "Id": elem.get('Id'),
                "OwnerUserId": elem.get('OwnerUserId'),
                "PostTypeId": int(elem.get('PostTypeId')),
                "CreationDate": datetime.fromisoformat(elem.get('CreationDate')),
                "Title": elem.get('Title'),
                "Body": elem.get('Body'),
                "ViewCount": elem.get('ViewCount'),
                "Score": int(elem.get('Score')),
                "Tags": tag_list,
                'Comments': []
                }
                chunk_data.append(document)
                elements_in_chunk += 1

            # If chunk is full, save the chunk and reset
            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                # clean the insertion data for all read keys
                valid_chunk_data = clean_chunk_data(chunk_data, ["Id", "OwnerUserId", "PostTypeId", "CreationDate", "Title", "Body", "ViewCount", "Score"])
                chunk_data = []  
                elements_in_chunk = 0
                # Insert the cleaned data into db;
                # first check existing users ids to check for valid entries
                existing_ids = fetch_existing_ids('users', 'Id')
                # filter up the chunk data based on valid existing ids; replace _id with OwnerUserId field
                valid_chunk_data = filter_and_replace_ids(valid_chunk_data, existing_ids, 'OwnerUserId')
                # Insert valid data is not empty; then insert record
                if valid_chunk_data:
                    collection.insert_many(valid_chunk_data)
            
            # Clear the element to free up memory
            elem.clear()
        print(f"\rProcessed more than {chunk_count * chunk_size} elements. ", end="")
    
    # Processing the remaining data in final chunk
    if elements_in_chunk > 0:
        chunk_count += 1
        # clean the insertion data for all read keys
        valid_chunk_data = clean_chunk_data(chunk_data, ["Id", "OwnerUserId", "PostTypeId", "CreationDate", "Title", "Body", "ViewCount", "Score"])
        # first check existing users ids to check for valid entries
        existing_ids = fetch_existing_ids('users', 'Id')
        # filter up the chunk data based on valid existing ids; replace _id with OwnerUserId field
        valid_chunk_data = filter_and_replace_ids(valid_chunk_data, existing_ids, 'OwnerUserId')
        # Insert valid data is not empty; then insert record
        if valid_chunk_data:
            collection.insert_many(valid_chunk_data)

def bulk_update_posts_with_comments(chunk_data):
    """
    This method updates posts collection with comments insertion.
    
    :param chunk_data: the chunk of data containing comments read from the comments input file.
    """
    # connect to the database
    db = connect()
    # get a reference to posts collection
    collection = db['posts']
    # Create a list to hold all update operations
    bulk_operations = []
    # process each chunk as tuple pair of post id and comment data
    for post_id, comment in chunk_data:
        # Update using the _id, which is already replaced in chunk data, Don't insert if post doesn't exist
        operation = UpdateOne(
            {'_id': post_id}, 
            {'$addToSet': {'Comments': comment}},
            upsert=False
        )
        bulk_operations.append(operation)
    try:
        # Execute all operations in a single bulk call
        collection.bulk_write(bulk_operations)
    # throw an error if there an an issue.
    except Exception as e:
        print(f"Error in bulk update: {e}")

def insert_comments(input_file):
    """
    This method insert comments in posts collection.
    
    :param input_file: Name of the XML file to extract data from.
    """
    # merge the file name with the data_directory provided in globals.py
    input_file = data_directory + input_file
    # get the context for reading the file
    context = ET.iterparse(input_file, events=("start", "end"))
    # an indicator to keep track of chunks; not necessary
    chunk_count = 0
    # an indicator to keep track of elements in the chunk 
    elements_in_chunk = 0
    # List to hold data to be inserted into the database
    chunk_data = []  
    # Iterate over the elements present in the input file 
    for event, elem in context:
        if elem.tag == 'row':
            if elem.get('Id') is not None and elem.get('PostId') is not None and elem.get('UserId') is not None:
                # Collect comment data from element attributes
                comment_data = {
                    "UserId": elem.get('UserId'),   
                    "Text": elem.get('Text'),
                    "CreationDate": datetime.fromisoformat(elem.get('CreationDate')),
                    "Score": int(elem.get('Score')),
                    "PostId": elem.get('PostId') # will be removed after mapping
                }
                # collect user id as key for each badge entry
                post_id = elem.get('PostId')
                chunk_data.append((post_id, comment_data))
                elements_in_chunk += 1

            # If chunk is full, save the chunk and reset
            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                # from data analysis from previous assignments, the selected fields does not contain nulls (excpet UserId), so cleaning is skipped based on that surety.
                # first check existing users ids to check for valid entries against users collection
                existing_users_ids = fetch_existing_ids('users', 'Id')
                # filter up the chunk data based on valid existing user ids; do not replace _id with UserId as comments relates to post
                valid_chunk_data = filter_and_replace_ids_chunk(chunk_data, existing_users_ids, 'UserId', False)
                chunk_data = [] 
                elements_in_chunk = 0
                # secondly check existing posts ids to check for valid entries against posts collection
                existing_post_ids = fetch_existing_ids('posts', 'Id')
                # filter up the chunk data based on valid existing post ids; replace _id with postId 
                valid_chunk_data = filter_and_replace_ids_chunk(valid_chunk_data, existing_post_ids, 'PostId', True)
                # Insert valid data is not empty; then insert record
                if valid_chunk_data:
                    bulk_update_posts_with_comments(valid_chunk_data)
                
            # clear up the element 
            elem.clear()
        print(f"\rProcessed more than {chunk_count * chunk_size} elements. ", end="")

    # Process any remaining data
    if elements_in_chunk > 0: 
        chunk_count += 1
        # from data analysis from previous assignments, the selected fields does not contain nulls (excpet UserId), so cleaning is skipped based on that surety.
        # first check existing users ids to check for valid entries against users collection
        existing_users_ids = fetch_existing_ids('users', 'Id')
        # filter up the chunk data based on valid existing user ids; do not replace _id with UserId as comments relates to post
        valid_chunk_data = filter_and_replace_ids_chunk(chunk_data, existing_users_ids, 'UserId', False)
        # secondly check existing posts ids to check for valid entries against posts collection
        existing_post_ids = fetch_existing_ids('posts', 'Id')
        # filter up the chunk data based on valid existing post ids; replace _id with postId 
        valid_chunk_data = filter_and_replace_ids_chunk(valid_chunk_data, existing_post_ids, 'PostId', True)
            # Insert valid data is not empty; then insert record
        if valid_chunk_data:
            bulk_update_posts_with_comments(valid_chunk_data)

def report_mapping_keys_users():
    """
    This method removes the keys used for mapping.
    """
    # connect to database
    db = connect()
    # take the users collection
    collection = db['users']
    # Remove the 'Id' field from all documents in the users collection using filter to match all documents
    collection.update_many(
        {},  
        {'$unset': {'Id': ""}}
    )
    # Remove the 'UserId' field from the Badges array in all documents
    collection.update_many(
        {}, 
        {'$unset': {'Badges.$[].UserId': ""}} 
    )

def report_mapping_keys_posts():
    """
    This method removes the keys used for mapping.
    """
    # connect to database
    db = connect()
    collection = db['posts']
    # Remove the 'Id' field from all documents in the users collection using filter to match all documents
    collection.update_many(
        {},  
        {'$unset': {'Id': ""}}
    )
    # Remove the 'PostId' field from the Badges array in all documents
    collection.update_many(
        {}, 
        {'$unset': {'Comments.$[].PostId': ""}} 
    )

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
        
        # If the collection is users, count the badges
        # if collection_name == "users": 
        #     print(f"Collection: {collection_name}   Users: {count}")
        #     total_badges = 0
        #     # Iterate through each document in the collection
        #     for document in db[collection_name].find():
        #         badges = document.get('Badges', [])
        #         total_badges += len(badges)
        #     print(f"Collection: {collection_name}   Badges: {total_badges}")
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