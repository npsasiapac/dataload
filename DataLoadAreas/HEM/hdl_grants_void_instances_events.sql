--
-- Amended by Pete Davies 11-JUN-2007
-- Now just creates missing grants rather
-- than do all.
--
-- Date          Ver  DB Ver  Name  Amendment(s)
-- ----          ---  ------  ----  ------------
-- 16-APR-2008   2.0  5.13.0  PH    Added this Change control Section
--                                  and amended path of output to be
--                                  $PROD_HOME/dload
-- 23-APR-2008   2.1  5.13.0  PH    Added  $PROD_HOME/dload/ to execute
--                                  script
-- 28-JAN-2012   3.0  6.1.1.  PJD   Now Uses Execute Immediate
-- 18-NOV-2015   3.1  6.13.0. MJK   Changed grants to PUBLIC to HOU_FULL
-- 21-MAR-2016   3.2  6.13    AJ    Added FSC and made bespoke for void Events
-- 24-MAY-2016   3.3  6.13    MOK   Amended so to inc Void_instances
-- 08-FEB-2017   3.4  6.14/5  AJ    Amended added extra blank lines at bottom
--
set lines 200

DECLARE
--
CURSOR c1 IS
select 'GRANT ALL ON '||object_name||' TO HOU_FULL' NEW_GRANT
from  user_objects u1
where u1.object_type =    'PACKAGE'
  and u1.object_name like 'S_DL_HEM_VOID%'
  and not exists (select null from table_privileges a1
                  where owner         = user
                    and grantee       = 'HOU_FULL'
                    and a1.table_name = u1.object_name)
union
select 'GRANT ALL ON '||object_name||' TO HOU_FULL'
from user_objects u1
where u1.object_type =    'TABLE'
  and u1.object_name like 'DL_HEM_VOID%'
  and not exists (select null from all_tab_privs_recd a1
                  where owner = user
                    and grantee = 'HOU_FULL'
                    and privilege = 'UPDATE'
                    and a1.table_name = u1.object_name)
union
select 'GRANT ALL ON '||object_name||' TO FSC' NEW_GRANT
from  user_objects u1
where u1.object_type =    'PACKAGE'
  and u1.object_name like 'S_DL_HEM_VOID%'
  and not exists (select null from table_privileges a1
                  where owner         = user
                    and grantee       = 'FSC'
                    and a1.table_name = u1.object_name)
union
select 'GRANT ALL ON '||object_name||' TO FSC'
from user_objects u1
where u1.object_type =    'TABLE'
  and u1.object_name like 'DL_HEM_VOID%'
  and not exists (select null from all_tab_privs_recd a1
                  where owner = user
                    and grantee = 'FSC'
                    and privilege = 'UPDATE'
                    and a1.table_name = u1.object_name)
;
--
BEGIN
--
--
FOR p1 IN c1 LOOP
--
DBMS_OUTPUT.PUT_LINE(p1.new_grant);
EXECUTE IMMEDIATE p1.new_grant;
--
END LOOP;
--
END;
--
/

set lines 80

