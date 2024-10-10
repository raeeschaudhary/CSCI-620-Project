-- Schema creation goes here

DROP TABLE IF EXISTS users CASCADE;

CREATE TABLE users (
    Id INTEGER PRIMARY KEY,
    UserName VARCHAR(60),
    DisplayName VARCHAR(260),
    RegisterDate DATE NOT NULL,
    PerformanceTier SMALLINT NOT NULL, 
    Country VARCHAR(40)
)

-- organizations

-- user organizations