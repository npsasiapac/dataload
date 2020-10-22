ALTER USER migration2 ACCOUNT UNLOCK;

UPDATE users
SET usr_current_ind = 'Y'
WHERE usr_username = 'MIGRATION2';