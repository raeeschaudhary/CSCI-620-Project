-- Schema creation goes here

-- users

-- DROP TABLE IF EXISTS users CASCADE;

-- CREATE TABLE users (
--     Id INTEGER PRIMARY KEY,
--     UserName VARCHAR(60),
--     DisplayName VARCHAR(260),
--     RegisterDate DATE NOT NULL,
--     PerformanceTier SMALLINT NOT NULL, 
--     Country VARCHAR(40)
-- )

-- organizations

-- DROP TABLE IF EXISTS organizations CASCADE;

-- CREATE TABLE organizations (
--     Id SMALLINT PRIMARY KEY,
--     Name VARCHAR(60) NOT NULL,
--     Slug VARCHAR(60) NOT NULL,
--     CreationDate DATE NOT NULL,
--     Description TEXT    
-- )

-- UserOrganizations

-- DROP TABLE IF EXISTS UserOrganizations CASCADE;

-- CREATE TABLE UserOrganizations (
--     Id SMALLINT PRIMARY KEY,
--     UserId INTEGER NOT NULL,
--     OrganizationId SMALLINT NOT NULL,
--     JoinDate DATE NOT NULL
-- )

-- UserFollowers

-- DROP TABLE IF EXISTS UserFollowers CASCADE;

-- CREATE TABLE UserFollowers (
--     Id INTEGER PRIMARY KEY,
--     UserId INTEGER NOT NULL,
--     FollowingUserId INTEGER NOT NULL,
--     CreationDate DATE NOT NULL
-- )

-- UserAchievements

-- DROP TABLE IF EXISTS UserAchievements CASCADE;

-- CREATE TABLE UserAchievements (
--     Id INTEGER PRIMARY KEY,
--     UserId INTEGER NOT NULL,
--     AchievementType VARCHAR(15) NOT NULL,
--     Tier SMALLINT NOT NULL,
--     TierAchievementDate DATE,
--     Points INTEGER NOT NULL,
--     CurrentRanking FLOAT,
--     HighestRanking FLOAT,
--     TotalGold SMALLINT NOT NULL,
--     TotalSilver SMALLINT NOT NULL,
--     TotalBronze SMALLINT NOT NULL
-- )


-- -- Competitions

-- DROP TABLE IF EXISTS Competitions CASCADE;

-- CREATE TABLE Competitions (
--     Id INTEGER PRIMARY KEY,
--     Slug VARCHAR(80) NOT NULL,
--     Title VARCHAR(95) NOT NULL,
--     ForumId INTEGER NOT NULL,
--     EnabledDate TIMESTAMP NOT NULL,
--     DeadlineDate TIMESTAMP NOT NULL,
--     EvaluationAlgorithmName VARCHAR(70),
--     MaxTeamSize SMALLINT NOT NULL,
--     NumPrizes SMALLINT NOT NULL,
--     TotalTeams SMALLINT NOT NULL,
--     TotalCompetitors SMALLINT NOT NULL,
--     TotalSubmissions INTEGER NOT NULL
-- )


-- Tags

DROP TABLE IF EXISTS Tags CASCADE;

CREATE TABLE Tags (
    Id INTEGER PRIMARY KEY,
    ParentTagId FLOAT,
    Name VARCHAR(50) NOT NULL,
    Slug VARCHAR(85) NOT NULL,
    FullPath VARCHAR(95) NOT NULL,
    Description VARCHAR(300),
    DatasetCount INTEGER NOT NULL,
    CompetitionCount INTEGER NOT NULL,
    KernelCount INTEGER NOT NULL
)
         

