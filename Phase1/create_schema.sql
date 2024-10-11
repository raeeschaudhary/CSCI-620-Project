-- Schema creation goes here

-- DROP TABLE IF EXISTS users CASCADE;

-- CREATE TABLE users (
--     Id INTEGER PRIMARY KEY,
--     UserName VARCHAR(60),
--     DisplayName VARCHAR(260),
--     RegisterDate DATE NOT NULL,
--     PerformanceTier SMALLINT NOT NULL, 
--     Country VARCHAR(40)
-- )

-- DROP TABLE IF EXISTS organizations CASCADE;

-- CREATE TABLE organizations (
--     Id SMALLINT PRIMARY KEY,
--     Name VARCHAR(60) NOT NULL,
--     Slug VARCHAR(60) NOT NULL,
--     CreationDate DATE NOT NULL,
--     Description TEXT    
-- )

DROP TABLE IF EXISTS UserOrganizations CASCADE;

CREATE TABLE UserOrganizations (
    Id SMALLINT PRIMARY KEY,
    UserId INTEGER NOT NULL,
    OrganizationId SMALLINT NOT NULL,
    JoinDate DATE NOT NULL
)


