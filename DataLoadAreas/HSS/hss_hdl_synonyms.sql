--
-- hss_hdl_synonyms.sql
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 08-FEB-2019   1.0  6.18   AJ    Created For new SAHT loader
--                                 Support Services Referrals
--
-- ******************************************************************
--
set lines 200
--
DECLARE
--
CURSOR c1 IS
SELECT 'CREATE OR REPLACE PUBLIC SYNONYM '||object_name||' FOR '||object_name NEW_SYN
FROM user_objects uo
WHERE ((object_type = 'PACKAGE'
       AND object_name LIKE 'S_DL_HSS%')
      or
       (object_type = 'TABLE'
       AND object_name LIKE 'DL_HSS%')
      )
AND NOT EXISTS (SELECT NULL FROm all_synonyms asyn
                WHERE asyn.synonym_name = uo.object_name
                  AND asyn.owner        = 'PUBLIC'
                  AND table_owner       = user)
;
--
BEGIN
--
FOR p1 in c1 LOOP
--
dbms_output.put_line(p1.new_syn);
EXECUTE IMMEDIATE p1.new_syn;
--
END LOOP;
--
END;
/

set lines 80
