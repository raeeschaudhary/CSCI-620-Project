-- Schema creation goes here

DROP TABLE IF EXISTS users CASCADE;

CREATE TABLE users (
    Id INTEGER PRIMARY KEY,
    UserName VARCHAR(50),
    DisplayName VARCHAR(50),
    RegisterDate DATE,
    PerformanceTier INTEGER, 
    Country VARCHAR(50)
)