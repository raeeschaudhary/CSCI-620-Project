# insert queries for each table are provided under. 
# users
users_insert_query = """
    INSERT INTO users (Id, UserName, DisplayName, RegisterDate, PerformanceTier, Country)
    VALUES %s
    """
# organizations
organizations_insert_query = """
    INSERT INTO organizations (Id, Name, Slug, CreationDate, Description)
    VALUES %s
    """
# UserOrganizations
user_organizations_insert_query = """
    INSERT INTO UserOrganizations (Id, UserId, OrganizationId, JoinDate)
    VALUES %s
"""
# UserFollowers
user_followers_insert_query = """
    INSERT INTO UserFollowers (Id, UserId, FollowingUserId, CreationDate)
    VALUES %s
"""
# UserAchievements
user_achievements_insert_query = """
    INSERT INTO UserAchievements (Id, UserId, AchievementType, Tier, TierAchievementDate, Points, CurrentRanking, 
    HighestRanking, TotalGold, TotalSilver, TotalBronze)
    VALUES %s
"""

# Competitions
cleaned_competitions_insert_query = """
    INSERT INTO Competitions (Id, Slug, Title, ForumId, EnabledDate, DeadlineDate, EvaluationAlgorithmName, 
    MaxTeamSize, NumPrizes, TotalTeams, TotalCompetitors, TotalSubmissions)
    VALUES %s
"""
# Tags
tags_insert_query = """
    INSERT INTO Tags (Id, ParentTagId, FullPath, Name, Slug, Description, DatasetCount, 
    CompetitionCount, KernelCount)
    VALUES %s
"""