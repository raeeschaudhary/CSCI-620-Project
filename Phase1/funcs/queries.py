# insert queries for each table are provided under. 

users_insert_query = """
    INSERT INTO users (Id, UserName, DisplayName, RegisterDate, PerformanceTier, Country)
    VALUES %s
    """

organizations_insert_query = """
    INSERT INTO organizations (Id, Name, Slug, CreationDate, Description)
    VALUES %s
    """

user_organizations_insert_query = """
    INSERT INTO UserOrganizations (Id, UserId, OrganizationId, JoinDate)
    VALUES %s
"""