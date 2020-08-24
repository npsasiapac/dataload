--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 17-JUN-2007   1.0         PH    Originally created
-- 28-JAN-2012   2.0  6.1.1. PJD   Now Uses Execute Immediate
-- 31-JUL-2015   3.0  6.11   AJ    Version control added
-- 20-DEC-2017   3.1  6.15   AJ    Added 'or replace' came in at 10g
--                                 actually put back to v613 folders
--                                 this means there will be an error if
--                                 hits a problem where with just created
--                                 there will be no error massage
--
--
set lines 200
--
DECLARE
--
CURSOR c1 IS
SELECT 'CREATE OR REPLACE PUBLIC SYNONYM '||object_name||' FOR '||object_name NEW_SYN
FROM user_objects uo
WHERE ((object_type = 'PACKAGE'
       AND object_name LIKE 'S_DL_%')
      or
       (object_type = 'TABLE'
       AND object_name LIKE 'DL_%')
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