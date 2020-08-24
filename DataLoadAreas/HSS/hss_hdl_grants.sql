--
-- hss_hdl_grants.sql
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 08-FEB-2019   1.0  6.18   AJ    Created For new SAHT loader
--                                 Support Services Referrals
--
-- ******************************************************************
--
set lines 200

DECLARE
--
CURSOR c1 IS
select 'GRANT ALL ON '||object_name||' TO HOU_FULL' NEW_GRANT
from  user_objects u1
where u1.object_type =    'PACKAGE'
  and u1.object_name like 'S_DL_HSS%'
  and not exists (select null from table_privileges a1
                  where owner         = user
                    and grantee       = 'HOU_FULL'
                    and a1.table_name = u1.object_name)
union
select 'GRANT ALL ON '||object_name||' TO HOU_FULL'
from user_objects u1
where u1.object_type =    'TABLE'
  and u1.object_name like 'DL_HSS%'
  and not exists (select null from all_tab_privs_recd a1
                  where owner = user
                    and grantee = 'HOU_FULL'
                    and privilege = 'UPDATE'
                    and a1.table_name = u1.object_name)
union
select 'GRANT ALL ON '||object_name||' TO FSC' NEW_GRANT
from  user_objects u1
where u1.object_type =    'PACKAGE'
  and u1.object_name like 'S_DL_HSS%'
  and not exists (select null from table_privileges a1
                  where owner         = user
                    and grantee       = 'FSC'
                    and a1.table_name = u1.object_name)
union
select 'GRANT ALL ON '||object_name||' TO FSC'
from user_objects u1
where u1.object_type =    'TABLE'
  and u1.object_name like 'DL_HSS%'
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
