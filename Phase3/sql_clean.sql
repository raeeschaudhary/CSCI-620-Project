-- Update rows where username is 'NaN' with the value of displayname
UPDATE users
SET username = displayname
WHERE username = 'NaN';

-- Update rows where displayname is 'NaN' with the value of username
UPDATE users
SET displayname = username
WHERE displayname = 'NaN';