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
--                                      Amended to use standard scripting
--
--
--***********************************************************************
--
-- Bespoke Income Synonyms for Income Data Load NonICS v6.11
--
set lines 200
--
DECLARE
--
CURSOR c1 IS
SELECT 'CREATE PUBLIC SYNONYM '||object_name||' FOR '||object_name NEW_SYN
FROM user_objects uo
WHERE ((object_type = 'PACKAGE'
       AND object_name in ('S_DL_HEM_INCOME_HEADERS'
                          ,'S_DL_HEM_INCOME_HEADER_USAGES'
                          ,'S_DL_HEM_INCOME_DETAILS'
                          ,'S_DL_HEM_ASSETS'
                          ,'S_DL_HEM_INC_DET_DEDUCTIONS'
                          ,'S_DL_HEM_INCOME_LIABILITIES'))
      or
       (object_type = 'TABLE'
       AND object_name in ('DL_HEM_INCOME_HEADERS'
                          ,'DL_HEM_INCOME_HEADER_USAGES'
                          ,'DL_HEM_INCOME_DETAILS'
                          ,'DL_HEM_ASSETS'
                          ,'DL_HEM_INC_DET_DEDUCTIONS'
                          ,'DL_HEM_INCOME_LIABILITIES'))
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

