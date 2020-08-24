SET SERVEROUTPUT ON
SPOOL upd_NPS_users.lis
UPDATE users
SET    usr_current_ind = 'N'
/

UPDATE users
SET    usr_current_ind = 'Y'
WHERE  usr_username = 'NPS_SUPPORT'
/

