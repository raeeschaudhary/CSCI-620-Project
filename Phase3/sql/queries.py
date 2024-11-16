# The queries cover:
# - Top competition tags by user medals.
# - Top users by followers and achievements.
# - User achievements and dataset creation patterns by competition topic.
# - Competition tags with highest engagement by submissions.
# - Dataset tag engagement by high-achieving users.

all_queries = [
        {
            "name": "Top Competition Tags by User Medals",
            "columns": "(TagName, ActiveUsers, GoldMedals, BronzeMedals, TotalMedals)",
            "query": """
                SELECT 
                    T.Name AS TagName,
                    COUNT(DISTINCT UA.UserId) AS ActiveUsers,
                    SUM(UA.TotalGold) AS GoldMedals,
                    SUM(UA.TotalSilver) AS SilverMedals,
                    SUM(UA.TotalBronze) AS BronzeMedals,
                    SUM(UA.TotalGold + UA.TotalSilver + UA.TotalBronze) AS TotalMedals
                FROM competitiontags CT
                INNER JOIN tags T ON CT.TagId = T.Id
                INNER JOIN competitionscleaned C ON CT.CompetitionId = C.Id
                INNER JOIN userachievements UA ON UA.UserId = C.Id
                GROUP BY T.Name
                ORDER BY TotalMedals DESC, ActiveUsers DESC
                LIMIT 10;
            """
        },
        {
            "name": "Top Users by Followers and Achievements",
            "columns": "(UserName, FollowerCount, SilverMedals, BronzeMedals, TotalMedals)",
            "query": """
                WITH FollowerCounts AS (
                    SELECT 
                        UF.UserId,
                        COUNT(UF.FollowingUserId) AS FollowerCount
                    FROM userfollowers UF
                    GROUP BY UF.UserId
                    HAVING COUNT(UF.FollowingUserId) > 100
                )
                SELECT 
                    U.DisplayName AS UserName,
                    FC.FollowerCount,
                    COALESCE(SUM(UA.TotalGold), 0) AS GoldMedals,
                    COALESCE(SUM(UA.TotalSilver), 0) AS SilverMedals,
                    COALESCE(SUM(UA.TotalBronze), 0) AS BronzeMedals,
                    COALESCE(SUM(UA.TotalGold + UA.TotalSilver + UA.TotalBronze), 0) AS TotalMedals
                FROM FollowerCounts FC
                INNER JOIN users U ON FC.UserId = U.Id
                LEFT JOIN userachievements UA ON U.Id = UA.UserId
                GROUP BY U.DisplayName, FC.FollowerCount
                ORDER BY FC.FollowerCount DESC, TotalMedals DESC
                LIMIT 10;
            """
        },
        {
            "name": "User Achievements and Dataset Creation Patterns by Competition Topic",
            "columns": "(UserName, CompetitionsParticipated, DatasetsCreated, CompetitionTopic, TotalMedals, AvgDatasetViews, AvgDatasetDownloads, AvgDatasetVotes)",
            "query": """ 
                SELECT 
                    U.DisplayName AS UserName,
                    COUNT(DISTINCT C.Id) AS CompetitionsParticipated,
                    COUNT(DISTINCT DC.Id) AS DatasetsCreated,
                    T.Name AS CompetitionTopic,
                    SUM(UA.TotalGold + UA.TotalSilver + UA.TotalBronze) AS TotalMedals,
                    AVG(DC.TotalViews) AS AvgDatasetViews,
                    AVG(DC.TotalDownloads) AS AvgDatasetDownloads,
                    AVG(DC.TotalVotes) AS AvgDatasetVotes
                FROM users U
                INNER JOIN userachievements UA ON U.Id = UA.UserId
                INNER JOIN submissionscleaned S ON U.Id = S.SubmittedUserId
                INNER JOIN competitionscleaned C ON S.TeamId = C.Id
                INNER JOIN competitiontags CT ON C.Id = CT.CompetitionId
                INNER JOIN tags T ON CT.TagId = T.Id
                INNER JOIN datasetscleaned DC ON U.Id = DC.CreatorUserId
                GROUP BY U.DisplayName, T.Name
                HAVING SUM(UA.TotalGold + UA.TotalSilver + UA.TotalBronze) > 1  AND COUNT(DISTINCT DC.Id) > 1 
                ORDER BY TotalMedals DESC, AvgDatasetViews DESC;
            """
        },
        {
            "name": "Competition Tags with Highest Engagement by Submissions",
            "columns": "(TagName, NumberOfCompetitions, TotalSubmissions, AvgPublicScore, AvgPrivateScore)",
            "query": """
                SELECT 
                    T.Name AS TagName,
                    COUNT(DISTINCT C.Id) AS NumberOfCompetitions,
                    COUNT(SC.Id) AS TotalSubmissions,
                    ROUND(AVG(SC.PublicScoreLeaderboardDisplay)::numeric, 2) AS AvgPublicScore,
                    ROUND(AVG(SC.PrivateScoreLeaderboardDisplay)::numeric, 2) AS AvgPrivateScore
                FROM competitiontags CT
                INNER JOIN tags T ON CT.TagId = T.Id
                INNER JOIN competitionscleaned C ON CT.CompetitionId = C.Id
                INNER JOIN submissionscleaned SC ON C.Id = SC.TeamId
                GROUP BY T.Name
                HAVING COUNT(SC.Id) > 100
                ORDER BY TotalSubmissions DESC;
            """
        },
        {
            "name": "Dataset Tag Engagement by High-Achieving Users",
            "columns": "(DatasetTag, TotalDatasets, HighAchievingUsers, AvgViews, AvgDownloads, AvgVotes)",
            "query": """
            SELECT 
                T.Name AS DatasetTag,
                COUNT(DISTINCT DC.Id) AS TotalDatasets,
                COUNT(DISTINCT U.Id) AS HighAchievingUsers,
                AVG(DC.TotalViews) AS AvgViews,
                AVG(DC.TotalDownloads) AS AvgDownloads,
                AVG(DC.TotalVotes) AS AvgVotes
            FROM userachievements UA
            INNER JOIN users U ON UA.UserId = U.Id
            INNER JOIN datasetscleaned DC ON U.Id = DC.CreatorUserId
            INNER JOIN datasettags DT ON DC.Id = DT.DatasetId
            INNER JOIN tags T ON DT.TagId = T.Id
            WHERE UA.TotalGold + UA.TotalSilver + UA.TotalBronze >= 5  -- Only consider users with 5 or more medals
            GROUP BY T.Name
            HAVING COUNT(DISTINCT DC.Id) > 3  -- Only include tags with more than 3 datasets
            ORDER BY AvgViews DESC, AvgDownloads DESC, AvgVotes DESC;
            """
        }
    ]

# Indexes 
# - idx_competitiontags_tagid_competitionid: (TagId, CompetitionId) on competitiontags
# - idx_userachievements_userid_medals: (UserId, TotalGold, TotalSilver, TotalBronze) on userachievements
# - idx_userfollowers_userid_followingid: (UserId, FollowingUserId) on userfollowers
# - idx_datasetscleaned_creatoruserid: (CreatorUserId) on datasetscleaned
# - idx_submissionscleaned_submitteduserid_teamid: (SubmittedUserId, TeamId) on submissionscleaned
# - idx_submissionscleaned_teamid_scores: (TeamId, PublicScoreLeaderboardDisplay, PrivateScoreLeaderboardDisplay) on submissionscleaned
# - idx_userachievements_userid_highmedals: (UserId, TotalGold, TotalSilver, TotalBronze) on userachievements
# - idx_datasettags_datasetid_tagid: (DatasetId, TagId) on datasettags
index_queries = [
        # Indexes for Query 1
        "CREATE INDEX idx_competitiontags_tagid_competitionid ON competitiontags (TagId, CompetitionId);",
        "CREATE INDEX idx_userachievements_userid_medals ON userachievements (UserId, TotalGold, TotalSilver, TotalBronze);",

        # Indexes for Query 2
        "CREATE INDEX idx_userfollowers_userid_followingid ON userfollowers (UserId, FollowingUserId);",

        # Indexes for Query 3 
        # "CREATE INDEX idx_datasetscleaned_creatoruserid ON datasetscleaned (CreatorUserId);", # not adding value, hence not created after trial
        # "CREATE INDEX idx_submissionscleaned_submitteduserid_teamid ON submissionscleaned (SubmittedUserId, TeamId);", # not adding value, hence not created after trial

        # Indexes for Query 4
        "CREATE INDEX idx_submissionscleaned_teamid_scores ON submissionscleaned (TeamId, PublicScoreLeaderboardDisplay, PrivateScoreLeaderboardDisplay);",

        # Indexes for Query 4 
        # "CREATE INDEX idx_userachievements_userid_highmedals ON userachievements (UserId, TotalGold, TotalSilver, TotalBronze);", # not adding value, hence not created after trial
        "CREATE INDEX idx_datasettags_datasetid_tagid ON datasettags (DatasetId, TagId);"
    ]

drop_queries = [
        "DROP INDEX IF EXISTS idx_competitiontags_tagid_competitionid;",
        "DROP INDEX IF EXISTS idx_userachievements_userid_medals;",
        "DROP INDEX IF EXISTS idx_userfollowers_userid_followingid;",
        "DROP INDEX IF EXISTS idx_datasetscleaned_creatoruserid;",
        "DROP INDEX IF EXISTS idx_submissionscleaned_submitteduserid_teamid;",
        "DROP INDEX IF EXISTS idx_submissionscleaned_teamid_scores;",
        "DROP INDEX IF EXISTS idx_userachievements_userid_highmedals;",
        "DROP INDEX IF EXISTS idx_datasettags_datasetid_tagid;"
    ]