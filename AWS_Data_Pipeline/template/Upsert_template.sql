-- Start 
begin transaction; 

-- Create a staging table 
CREATE TABLE users_staging (LIKE users);

-- Load data into the staging table 
COPY users_staging (id, name, city) 
FROM 's3://.......' 
CREDENTIALS 'aws_access_key_id=xxxxxxx;aws_secret_access_key=xxxxxxx'
COMPUPDATE OFF STATUPDATE OFF;

-- Update records 
UPDATE users 
SET name = s.name, city = s.city 
FROM users_staging s 
WHERE users.id = s.id; 

-- Insert records 
INSERT INTO users 
SELECT s.* FROM users_staging s LEFT JOIN users 
ON s.id = users.id
WHERE users.id IS NULL;

-- Drop the staging table
DROP TABLE users_staging; 

-- End 
end transaction ;