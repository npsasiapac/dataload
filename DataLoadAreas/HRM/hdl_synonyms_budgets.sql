--------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
--
-- 10-AUG-2007   1.0  x.xx   xx    Initial Creation for Data Loads
-- 01-MAR-2016   1.1  6.13   AJ    Added Change Control and altered
--                                 object names so only looks for
--                                 DL_HRM_BUDGETS only
--
--------------------------------------------------------------------
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
       AND object_name LIKE 'S_DL_HRM_BUDGET%')
      or
       (object_type = 'TABLE'
       AND object_name LIKE 'DL_HRM_BUDGET%')
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
