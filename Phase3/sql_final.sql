-- update rows where displayname is 'NaN' with the value of username
UPDATE users
SET displayname = username
WHERE displayname = 'NaN';

-- delete parenttagid where parent is not present except none (which do not have parent)
DELETE FROM Tags WHERE parenttagid NOT IN (SELECT id FROM tags);
-- alter table to create reference to parent table.
ALTER TABLE tags ADD CONSTRAINT fk_ParentTagId FOREIGN KEY (ParentTagId) REFERENCES Tags(Id) ON DELETE CASCADE; 