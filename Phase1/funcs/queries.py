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

# Tags
competition_tags_insert_query = """
    INSERT INTO CompetitionTags (Id, CompetitionId, TagId)
    VALUES %s
"""
# CleanedDatasets
cleaned_dataset_insert_query = """
    INSERT INTO Datasets (Id, CreatorUserId, ForumId, CreationDate, LastActivityDate,
    TotalViews, TotalDownloads, TotalVotes, TotalKernels)
    VALUES %s
"""
# DatasetTags
dataset_tags_insert_query = """
    INSERT INTO DatasetTags (Id, DatasetId, TagId)
    VALUES %s
"""
# Forums
forums_insert_query = """
    INSERT INTO Forums (Id, ParentForumId, Title)
    VALUES %s
"""
# Teams
teams_insert_query = """
    INSERT INTO Teams (Id, CompetitionId, TeamLeaderId, TeamName)
    VALUES %s
"""
#  Submissions
submission_insert_query = """
    INSERT INTO Submissions(Id, SubmittedUserId, TeamId, SubmissionDate, IsAfterDeadline, PublicScoreLeaderboardDisplay,
    PrivateScoreLeaderboardDisplay)
    VALUES %s 
    """
