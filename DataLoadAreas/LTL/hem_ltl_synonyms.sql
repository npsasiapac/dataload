--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 17-JUN-2007   1.0         PH    Originally created
-- 28-JAN-2012   2.0  6.1.1. PJD   Now Uses Execute Immediate
-- 31-JUL-2015   3.0  6.11   AJ    Version control added
-- 18-MAR-2016   4.0  6.14   AJ    Bespoke version for Land Titles DL_%
--                                 all three start with S_DL_LTL_LAND....
--                                 or DL_LTL_LAND....
--
--
set lines 200
--
DECLARE
--
CURSOR c1 IS
SELECT 'CREATE PUBLIC SYNONYM '||object_name||' FOR '||object_name NEW_SYN
FROM user_objects uo
WHERE ((object_type = 'PACKAGE'
       AND object_name LIKE 'S_DL_LTL_LAND%')
      or
       (object_type = 'TABLE'
       AND object_name LIKE 'DL_LTL_LAND%')
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