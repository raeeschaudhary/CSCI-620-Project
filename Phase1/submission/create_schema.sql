--  -- Schema creation goes here; 
-- -- This file contains the SQL scripts to create tables 

-- -- Users

DROP TABLE IF EXISTS Users CASCADE;

CREATE TABLE Users (
    Id INTEGER PRIMARY KEY,
    UserName VARCHAR(60),
    DisplayName VARCHAR(260),
    RegisterDate DATE NOT NULL,
    PerformanceTier SMALLINT NOT NULL, 
    Country VARCHAR(40)
);

-- -- Tags

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
);

-- -- Forums

DROP TABLE IF EXISTS Forums CASCADE;

CREATE TABLE Forums (
    Id INTEGER PRIMARY KEY,
    ParentForumId FLOAT,
    Title VARCHAR(100)
);

-- -- Organizations

DROP TABLE IF EXISTS Organizations CASCADE;

CREATE TABLE Organizations (
    Id SMALLINT PRIMARY KEY,
    Name VARCHAR(60) NOT NULL,
    Slug VARCHAR(60) NOT NULL,
    CreationDate DATE NOT NULL,
    Description TEXT    
);

-- -- -- UserOrganizations

DROP TABLE IF EXISTS UserOrganizations CASCADE;

CREATE TABLE UserOrganizations (
    Id SMALLINT PRIMARY KEY,
    UserId INTEGER NOT NULL,
    FOREIGN KEY (UserId) REFERENCES Users (Id),
    OrganizationId SMALLINT NOT NULL,
    FOREIGN KEY (OrganizationId) REFERENCES Organizations (Id), 
    JoinDate DATE NOT NULL
);

-- -- UserFollowers

DROP TABLE IF EXISTS UserFollowers CASCADE;

CREATE TABLE UserFollowers (
    Id INTEGER PRIMARY KEY,
    UserId INTEGER NOT NULL,
    FOREIGN KEY (UserId) REFERENCES Users (Id),
    FollowingUserId INTEGER NOT NULL,
    FOREIGN KEY (FollowingUserId) REFERENCES Users (Id),
    CreationDate DATE NOT NULL
);

-- -- -- CleanedDatasets

DROP TABLE IF EXISTS DatasetsCleaned CASCADE;

CREATE TABLE DatasetsCleaned (
    Id INTEGER PRIMARY KEY,
    CreatorUserId INTEGER NOT NULL,
    FOREIGN KEY (CreatorUserId) REFERENCES Users (Id),
    ForumId INTEGER NOT NULL, 
    FOREIGN KEY (ForumId) REFERENCES Forums (Id),
    CreationDate TIMESTAMP NOT NULL,
    LastActivityDate TIMESTAMP NOT NULL,
    TotalViews INTEGER NOT NULL, 
    TotalDownloads INTEGER NOT NULL, 
    TotalVotes INTEGER NOT NULL, 
    TotalKernels SMALLINT NOT NULL
);

-- -- -- DatasetTags

DROP TABLE IF EXISTS DatasetTags CASCADE;

CREATE TABLE DatasetTags (
    Id INTEGER PRIMARY KEY,
    DatasetId INTEGER NOT NULL,
    FOREIGN KEY (DatasetId) REFERENCES DatasetsCleaned (Id),
    TagId INTEGER NOT NULL,
    FOREIGN KEY (TagId) REFERENCES Tags (Id)
);

-- -- -- CompetitionsCleaned

DROP TABLE IF EXISTS CompetitionsCleaned CASCADE;

CREATE TABLE CompetitionsCleaned (
    Id INTEGER PRIMARY KEY,
    Slug VARCHAR(80) NOT NULL,
    Title VARCHAR(95) NOT NULL,
    ForumId INTEGER NOT NULL,
    FOREIGN KEY (ForumId) REFERENCES Forums (Id),
    EnabledDate TIMESTAMP NOT NULL,
    DeadlineDate TIMESTAMP NOT NULL,
    EvaluationAlgorithmName VARCHAR(70),
    MaxTeamSize SMALLINT NOT NULL,
    NumPrizes SMALLINT NOT NULL,
    TotalTeams SMALLINT NOT NULL,
    TotalCompetitors SMALLINT NOT NULL,
    TotalSubmissions INTEGER NOT NULL
);

-- -- CompetitionTags

DROP TABLE IF EXISTS CompetitionTags CASCADE;

CREATE TABLE CompetitionTags (
    Id INTEGER PRIMARY KEY,
    CompetitionId INTEGER NOT NULL,
    FOREIGN KEY (CompetitionId) REFERENCES CompetitionsCleaned (Id),
    TagId INTEGER NOT NULL,
    FOREIGN KEY (TagId) REFERENCES Tags (Id)
);

-- -- -- CleanedTeams

DROP TABLE IF EXISTS TeamsCleaned CASCADE;

CREATE TABLE TeamsCleaned (
    Id INTEGER PRIMARY KEY,
    CompetitionId INTEGER NOT NULL,
    FOREIGN KEY (CompetitionId) REFERENCES CompetitionsCleaned (Id),
    TeamLeaderId FLOAT, -- need to clean from float to int in Phase 3
    TeamName VARCHAR(260)
);

-- -- CleanedSubmissions

DROP TABLE IF EXISTS SubmissionsCleaned CASCADE;

CREATE TABLE SubmissionsCleaned (
    Id INTEGER PRIMARY KEY,
    SubmittedUserId FLOAT, -- need to clean float to int Phase 3
    TeamId INTEGER NOT NULL,
    FOREIGN KEY (TeamId) REFERENCES TeamsCleaned (Id),
    SubmissionDate DATE NOT NULL,
    IsAfterDeadline BOOLEAN NOT NULL,
    PublicScoreLeaderboardDisplay FLOAT,
    PrivateScoreLeaderboardDisplay FLOAT
);

-- -- -- UserAchievements

DROP TABLE IF EXISTS UserAchievements CASCADE;

CREATE TABLE UserAchievements (
    Id INTEGER PRIMARY KEY,
    UserId INTEGER NOT NULL,
    FOREIGN KEY (UserId) REFERENCES Users (Id),
    AchievementType VARCHAR(15) NOT NULL,
    Tier SMALLINT NOT NULL,
    TierAchievementDate VARCHAR(30), --  this should be DATE but needs cleaning
    Points INTEGER NOT NULL,
    CurrentRanking FLOAT,
    HighestRanking FLOAT,
    TotalGold SMALLINT NOT NULL,
    TotalSilver SMALLINT NOT NULL,
    TotalBronze SMALLINT NOT NULL
);