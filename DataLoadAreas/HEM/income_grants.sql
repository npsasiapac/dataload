--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0               VS   2008         Initial Creation
--  1.0     6.11      AJ   22-Dec-2015  Added new functionality
--                                      released at v611 income liabilities
--                                      Amended Grants to FSC and HOU_FULL
--
--
--***********************************************************************
--
-- Bespoke Income Grants for Income Data Load NonICS v6.11
--
set lines 200

DECLARE
--
CURSOR c1 IS
select 'GRANT ALL ON '||object_name||' TO HOU_FULL' NEW_GRANT
from  user_objects u1
where u1.object_type =    'PACKAGE'
  and u1.object_name in ('S_DL_HEM_INCOME_HEADERS'
                        ,'S_DL_HEM_INCOME_HEADER_USAGES'
                        ,'S_DL_HEM_INCOME_DETAILS'
                        ,'S_DL_HEM_ASSETS'
                        ,'S_DL_HEM_INC_DET_DEDUCTIONS'
                        ,'S_DL_HEM_INCOME_LIABILITIES')
  and not exists (select null from table_privileges a1
                  where owner         = user
                    and grantee       = 'HOU_FULL'
                    and a1.table_name = u1.object_name)
union
select 'GRANT ALL ON '||object_name||' TO HOU_FULL'
from user_objects u1
where u1.object_type =    'TABLE'
  and u1.object_name in ('DL_HEM_INCOME_HEADERS'
                        ,'DL_HEM_INCOME_HEADER_USAGES'
                        ,'DL_HEM_INCOME_DETAILS'
                        ,'DL_HEM_ASSETS'
                        ,'DL_HEM_INC_DET_DEDUCTIONS'
                        ,'DL_HEM_INCOME_LIABILITIES')
  and not exists (select null from all_tab_privs_recd a1
                  where owner = user
                    and grantee = 'HOU_FULL'
                    and privilege = 'UPDATE'
                    and a1.table_name = u1.object_name)
union
select 'GRANT ALL ON '||object_name||' TO FSC' NEW_GRANT
from  user_objects u1
where u1.object_type =    'PACKAGE'
  and u1.object_name in ('S_DL_HEM_INCOME_HEADERS'
                        ,'S_DL_HEM_INCOME_HEADER_USAGES'
                        ,'S_DL_HEM_INCOME_DETAILS'
                        ,'S_DL_HEM_ASSETS'
                        ,'S_DL_HEM_INC_DET_DEDUCTIONS'
                        ,'S_DL_HEM_INCOME_LIABILITIES')
  and not exists (select null from table_privileges a1
                  where owner         = user
                    and grantee       = 'FSC'
                    and a1.table_name = u1.object_name)
union
select 'GRANT ALL ON '||object_name||' TO FSC'
from user_objects u1
where u1.object_type =    'TABLE'
  and u1.object_name in ('DL_HEM_INCOME_HEADERS'
                        ,'DL_HEM_INCOME_HEADER_USAGES'
                        ,'DL_HEM_INCOME_DETAILS'
                        ,'DL_HEM_ASSETS'
                        ,'DL_HEM_INC_DET_DEDUCTIONS'
                        ,'DL_HEM_INCOME_LIABILITIES')
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


