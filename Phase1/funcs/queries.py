# insert queries for each table are provided under. 

users_insert_query = """
    INSERT INTO Users (Id, UserName, DisplayName,RegisterDate,PerformanceTier,Country)
    VALUES %s
    """