--
-- dl_hra_subsidy_tab_new.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to ensure the Rents Subsidy Dataload Tables have
-- the correct columns. This script does not drop the
-- dataload tables, just adds in any missing columns.
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver   Name  Amendment(s)
-- ----          ---  ------   ----  ------------
-- 05-JAN-2009   1.0  5.15.0   VS    Initial Creation.
--
-- 20-OCT-2009   2.0  5.16.0   VS    Addition of Group Subsidy Reviews Dataload
--                                   table.
--
--                                   Add LSURV_GRSR_USER_REFERENCE to Subsidy
--                                   Reviews dataload table.
--
-- 22-OCT-2009   2.1  5.16.0   VS    Changed the size of LSURV_GRSR_USER_REFERENCE
--                                   and LGRSR_USER_REFERENCE columns from
--                                   VARCHAR2(20) to VARCHAR2(30).
--
-- 03-DEC-2009   2.2  5.16.0   VS    Fix for Defect id 2703 - Rename
--                                   LARLI_TCY_ALT_REF to LARLI_REFERENCE for
--                                   ACCOUNT_RENT_LIMITS
--
-- 13-MAY-2010   2.3  5.16.0   VS    Fix for Defect id 4517 - Added Modified By/Date
--                                   to Subsidy Applications
--
-- 18-MAY-2011   2.4  5.16.0   MT    V16: Added suit_par_per_alt_ref to subsidty income items
--
-- 29-JUN-2011   2.5  5.16.0   PH    Added suap_ref to subsidy applications and
--     to                            changed legacy ref to be VARCHAR2(30).
-- 01-JUL-2011                       Added lsurv_refno to subsidy reviews and
--                                   changed legacy ref to be VARCHAR2(30).
--                                   Amended upd_col_def set l_nullable to null.
--                                   Amended subsidy_income_items surv legacy
--                                   ref to be varchar2(30).
--                                   Amended account_rent_limits surv legacy
--                                   ref to be varchar2(30).
--                                   Amended subsidy_grace_periods suap legacy
--                                   ref to be varchar2(30).
--                                   Amended subsidy_letters surv legacy
--                                   ref to be varchar2(30).
--                                   Added new fields for subsidy_applications
--                                   and subsidy_reviews
--                                   Added new table subsidy_debt_assessments
--                                   Amended account rent limits legacy
--                                   ref to be varchar2(30).
--
-- 01-NOV-2011   2.6  6.5.1   MT    V19 specification: Added LSURV_CAP_AMOUNT and TYPE
-- 04-OCT-2017   2.7  6.14    AJ    allowed for nullable being the same as current setting
--                                  in upd_col_def by removing l_nullable from statement
--                                  if p_nullable is passed as a null value as this is only
--                                  done if old and new nullable setting is the same value
--                                  otherwise its a Y or an N update might fail
-- 11-OCT-2017   2.8  6.9/14  AJ    Added column LSURV_PAY_MARKET_RENT_IND picked up from
--                                  review of different versions for MB
-- 12-OCT-2017   2.9  6.9/14  AJ    Added column LSUAP_AUN_CODE to subsidy applications
--
-------------------------------------------------------------------------------
--
set serverout on size 1000000
--
DECLARE
-- This script is designed to create / correct the dataload tables used
-- by the Housing Dataload
-- The plan is
--
-- Have variables of Table Name (of previous record)
--                  Statement so Far
--                  Status

-- Pass the row into the driving function
-- What is the status being passed in
-- If the status is B (Building a statement)
-- then 
--   If this record is the same table as the previous record
--   then 
--     add the column definition statement to the 'Statement so Far'
--   If the record relates to a different table 
--   then
--    execute the 'Statement so Far'
--     change the status to N 
--   end if
-- end if
--
-- If the status is N (New) then
--   If the table already exists
--   then 
--     check if the column exists
--     if the column exists 
--     then 
--       check if the definition is the same
--       if the definition is not the same
--       then
--         update the column definition
--       end if
--     if the column does not already exist
--     then
--       alter the table by adding this column
--     end if
--   if the table does not already exist
--  then 
--     start building up the create table statement
--     change the status to B
-- end if
--
--
l_debug      VARCHAR2(1) := 'N';
--
l_t          VARCHAR2(30);        -- the table name from the previous record
l_s          VARCHAR2(1) := 'N';  -- the status from the previous record
l_f          VARCHAR2(6000);      -- the table create statement built so far
--
PROCEDURE DM(p_message  VARCHAR2
            ,p_on       VARCHAR2) IS
--
BEGIN
--
--
IF p_on = 'Y' 
THEN
  DBMS_OUTPUT.PUT_LINE(p_message);
END IF;
--
END DM;
--
PROCEDURE ins_col_def (p_table_name VARCHAR2
                      ,p_column_name VARCHAR2
                      ,p_col_type    VARCHAR2
                      ,p_col_length  VARCHAR2
                      ,p_nullable    VARCHAR2) is
--
l_update_statement VARCHAR2(1000);
l_nullable VARCHAR2(20);
--
BEGIN
--
l_nullable := 'NULL';
--
IF nvl(p_nullable,'Y') = 'N'
THEN l_nullable := 'NOT NULL';
END IF;
--
BEGIN
  IF    p_col_type IN ('DATE','INTEGER')
  THEN  l_update_statement := 'ALTER TABLE '||p_table_name||
                              ' ADD      ( '||p_column_name||
                              ' '           ||p_col_type||
                              ' '           ||l_nullable||
                              ' )';
        DM('Issuing Alter Table Statement ADD DATE or INTEGER',l_debug);
        EXECUTE IMMEDIATE l_update_statement;
  ELSIF p_col_type IN ('VARCHAR2','NUMBER')
  THEN  l_update_statement := 'ALTER TABLE  '||p_table_name||
                              ' ADD       ( '||p_column_name||
                              ' '|| p_col_type||
                              ' ( '||p_col_length||
                              ' )           '||l_nullable  ||
                              ' )';
        DM('Issuing Alter Table Statement ADD VARCHAR2 or NUMBER ',l_debug);
       EXECUTE IMMEDIATE l_update_statement;  
  ELSE
    DM('Unknown Column type '||p_col_type,'Y');
  END IF;
EXCEPTION
  WHEN OTHERS
  THEN
  DM('Creating Column Definition','Y');        
  DM('Oracle Error on '||p_table_name||
                         '.'||p_column_name,'Y');
  DM('l_update_statement '||l_update_statement,'Y');
  DM('Error '||sqlerrm,'Y');
END;
--
END  ins_col_def;
--
PROCEDURE upd_col_def (p_table_name  VARCHAR2
                      ,p_column_name VARCHAR2
                      ,p_col_type    VARCHAR2
                      ,p_col_length  VARCHAR2
                      ,p_nullable    VARCHAR2) is
--
l_update_statement VARCHAR2(1000);
l_nullable VARCHAR2(20);
--
BEGIN
--
l_nullable := 'NULL';
--
IF nvl(p_nullable,'Y') = 'N'
THEN l_nullable := 'NOT NULL';
END IF;
--
BEGIN
--
-- when l_col_ch = 'X' p_nullable passed as null value as nullable has
-- not changed so remove from update 
--
IF nvl(p_nullable,'X') = 'X'
 THEN 
        DM('p_col_type '||p_col_type,l_debug);
  IF    p_col_type IN ('DATE','INTEGER')
  THEN  l_update_statement := 'ALTER TABLE '||p_table_name||
                              ' MODIFY   ( '||p_column_name||
                              ' '           ||p_col_type||
                              ' )';
        DM('Issuing Alter Table Statement MODIFY DATE or INTEGER',l_debug);
        EXECUTE IMMEDIATE l_update_statement;
  ELSIF p_col_type IN ('VARCHAR2','NUMBER')
  THEN  l_update_statement := 'ALTER TABLE   '||p_table_name||
                              ' MODIFY     ( '||p_column_name||
                              ' '             ||  p_col_type||
                              ' ( '           ||p_col_length||
                              ' ) )';
        DM('Issuing Alter Table Statement MODIFY VARCHAR2 or NUMBER ',l_debug);
       EXECUTE IMMEDIATE l_update_statement;  
  ELSE
    DM('Unknown Column type '||p_col_type,'Y');
  END IF;
END IF;
--
-- when l_col_ch = 'U' p_nullable passed as Y or N as nullable has
-- changed so l_nullable needs to be included
--
IF ((nvl(p_nullable,'X') = 'Y') OR  (nvl(p_nullable,'X') = 'N'))
 THEN 
        DM('p_col_type '||p_col_type,l_debug);
  IF    p_col_type IN ('DATE','INTEGER')
  THEN  l_update_statement := 'ALTER TABLE '||p_table_name||
                              ' MODIFY   ( '||p_column_name||
                              ' '           ||p_col_type||
                              ' '           ||l_nullable||
                              ' )';
        DM('Issuing Alter Table Statement MODIFY DATE or INTEGER',l_debug);
        EXECUTE IMMEDIATE l_update_statement;
  ELSIF p_col_type IN ('VARCHAR2','NUMBER')
  THEN  l_update_statement := 'ALTER TABLE   '||p_table_name||
                              ' MODIFY     ( '||p_column_name||
                              ' '             ||  p_col_type||
                              ' ( '           ||p_col_length||
                              ' )            '||l_nullable  ||
                              ' )';
        DM('Issuing Alter Table Statement MODIFY VARCHAR2 or NUMBER ',l_debug);
       EXECUTE IMMEDIATE l_update_statement;  
  ELSE
    DM('Unknown Column type '||p_col_type,'Y');
  END IF;
END IF;
--
EXCEPTION
  WHEN OTHERS
  THEN
  DM('Updating Column Definition','Y');        
  DM('Oracle Error on '||p_table_name||
                         '.'||p_column_name,'Y');
  DM('Error '||sqlerrm,'Y');
  DM('Statement was '||l_update_statement,'Y');
END;
--
END  upd_col_def;
--
PROCEDURE create_tab  (p_table_name  IN VARCHAR2
                      ,p_sql         IN VARCHAR2) IS
--
CURSOR c_tab_tablespace (p_table_name VARCHAR2) IS
SELECT tablespace_name, 
       initial_extent, 
       nvl(next_extent,initial_extent) next_extent,
       nvl(pct_increase,0),    
       pct_used, 
       nvl(pct_free,0) pct_free 
FROM   all_tables
WHERE  owner IN ('HOU','FSC')
and    table_name like p_table_name;
--
--
l_table_name varchar2(30);
l_tablespace_name varchar2(30);
l_initial_extent NUMBER;
l_next_extent    NUMBER;
l_pct_increase   NUMBER;
l_pct_used       NUMBER;
l_pct_free       NUMBER;
--
l_insert_statement varchar2(4000);
--
BEGIN
  --
  DM('In Create Tab procedure '||p_table_name,l_debug);
  --
  l_table_name := ltrim(p_table_name,'DL_');
  IF substr(l_table_name,4,1) = '_'
  THEN
    l_table_name := substr(l_table_name,5,25);
  END IF;
  --
  OPEN c_tab_tablespace(p_table_name);
  FETCH c_tab_tablespace INTO 
                  l_tablespace_name,             
                  l_initial_extent,             
                  l_next_extent,             
                  l_pct_increase,             
                  l_pct_used,             
                  l_pct_free;
  CLOSE c_tab_tablespace;
  --
  IF l_tablespace_name IS NULL
  THEN
  OPEN c_tab_tablespace('TENANCIES');
  FETCH c_tab_tablespace INTO 
                  l_tablespace_name,             
                  l_initial_extent,             
                  l_next_extent,             
                  l_pct_increase,             
                  l_pct_used,             
                  l_pct_free;
  CLOSE c_tab_tablespace;
  END IF;
  --
  l_insert_statement := p_sql;
  --
        l_insert_statement := l_insert_statement||
                              ')  TABLESPACE '|| l_tablespace_name;
        DM('Issuing Create Table Statement ',l_debug);
BEGIN
  --
        EXECUTE IMMEDIATE l_insert_statement;
  --
EXCEPTION
  WHEN OTHERS
  THEN
  DM('Creating New Table '||p_table_name,'Y');
  DM('Error '||sqlerrm,'Y');
  DM('Statement was ','Y');
    DM(substr(l_insert_statement,1,80),l_debug);
    DM(substr(l_insert_statement,81,80),l_debug);
    DM(substr(l_insert_statement,161,80),l_debug);
    DM(substr(l_insert_statement,241,80),l_debug);
    DM(substr(l_insert_statement,321,80),l_debug);
    DM(substr(l_insert_statement,401,80),l_debug);
    DM(substr(l_insert_statement,481,80),l_debug);
    DM(substr(l_insert_statement,561,80),l_debug);
    DM(substr(l_insert_statement,641,80),l_debug);
    DM(substr(l_insert_statement,721,80),l_debug);
    DM(substr(l_insert_statement,801,80),l_debug);
    DM(substr(l_insert_statement,881,80),l_debug);
    DM(substr(l_insert_statement,961,80),l_debug);
END;
--
END  create_tab;   
--

PROCEDURE chk_col_def (p_table_name  IN VARCHAR2
                      ,p_column_name IN VARCHAR2
                      ,p_col_type    IN VARCHAR2
                      ,p_col_length  IN VARCHAR2
                      ,p_nullable    IN VARCHAR2
                      ,p_res         IN OUT VARCHAR2) is
--
CURSOR c1 IS
SELECT  data_type
,       to_char(data_length) data_length
,       to_char(data_precision) data_precision
,       to_char(data_scale)     data_scale
,       nullable
FROM all_tab_columns
WHERE table_name  = p_table_name
AND   column_name = p_column_name;
--
l_nullable       VARCHAR2(20);
l_data_type      VARCHAR2(10);
l_data_length    VARCHAR2(10);
l_data_precision VARCHAR2(10);
l_data_scale     VARCHAR2(10);
l_res            VARCHAR2(1);
--
BEGIN
--
DM('Checking column definition ',l_debug);
--
l_nullable       := NULL;
l_data_type      := NULL;      
l_data_length    := NULL;      
l_data_precision := NULL;      
l_data_scale     := NULL;      
l_res            := 'O';
--
OPEN c1;
FETCH c1 INTO l_data_type,l_data_length,l_data_precision,
              l_data_scale,l_nullable;
CLOSE c1;
--
IF (l_data_type = 'NUMBER'
    OR l_data_precision IS NOT NULL)
THEN 
  l_data_length := l_data_precision;
  IF nvl(l_data_scale,0) > 0
  THEN
    l_data_length := l_data_length||','||l_data_scale;
  END IF;
END IF;
--
IF l_data_type IS NULL
THEN
  l_res := 'I';
  DM('l_res := I - i.e. no existing match ',l_debug);
ELSIF l_data_type != p_col_type
THEN 
  l_res := 'U';
  DM('l_res := U - i.e. definition needs updating ',l_debug);
ELSIF l_data_type != 'DATE'
THEN
  IF nvl(l_data_length,'0') != nvl(ltrim(p_col_length),'0')
  THEN 
    l_res := 'U';
    DM('l_res := U - i.e. definition needs updating ',l_debug);
  ELSIF l_nullable  != p_nullable 
  THEN 
    l_res := 'U';
    DM('l_res := U - i.e. definition needs updating ',l_debug);
  ELSE
    DM('Column Definition matches - no update needed ',l_debug);
  END IF;
ELSIF l_data_type = 'DATE'
THEN
  IF l_nullable  != p_nullable 
  THEN 
    l_res := 'U';
    DM('l_res := U - i.e. definition needs updating ',l_debug);
  ELSE
    DM('Column Definition matches - no update needed ',l_debug);
  END IF;
ELSE
  DM('Column Definition matches - no update needed ',l_debug);
END IF;
--
-- Check to see if it needs updating but also if the nullable
-- is correct as this can cause the SQL statement to fail
--
IF l_res = 'U'
 AND l_nullable = p_nullable
  THEN l_res := 'X';
END IF;
--
p_res := l_res; -- set the result to be returned to calling proc
--
END chk_col_def;
--
--
PROCEDURE PR (p_table_name    IN varchar2
             ,p_column_name   IN varchar2
             ,p_column_type   IN varchar2
             ,p_column_length IN varchar2
             ,p_column_null   IN varchar2
             ,p_t             IN OUT varchar2 
             ,p_s             IN OUT varchar2 
             ,p_f             IN OUT varchar2 ) IS
--
CURSOR c_chk_tab IS
SELECT 'Y'
FROM  user_tables
WHERE table_name = p_table_name;
--
l_col_st varchar2(200); -- definition for this column
l_col_ch varchar2(1);   -- does the column need updating
l_exists varchar2(1);
--
BEGIN
--
DM('IN PR Proc',l_debug);
DM('Table = '||p_table_name||' Column = '||p_column_name,l_debug);
--
IF p_s = 'B' -- we have already started building a table create statement
THEN
  DM('p_s = B - so we are building a table create statement ',l_debug);
  IF p_table_name = p_t
  THEN
    DM('..and we are still building it ',l_debug);
    l_col_st  := NULL;
    --
    l_col_st := ', '||p_column_name||' '||p_column_type;
    IF p_column_type IN ('DATE','INTEGER')
      THEN NULL;
    ELSIF p_column_type in ('VARCHAR2','NUMBER')
      THEN l_col_st := l_col_st||'('||nvl(rtrim(p_column_length),'10')||')';
    END IF;
    --
    IF p_column_null = 'N'
    THEN 
      l_col_st := l_col_st||' NOT NULL';
    END IF;
    p_f := p_f||l_col_st;
    --
  ELSE -- we need to go off and run the create table statement
    -- 
    DM('..time to issue the Create table command ',l_debug);
    DM(substr(p_f,1,80),l_debug);
    DM(substr(p_f,81,80),l_debug);
    DM(substr(p_f,161,80),l_debug);
    DM(substr(p_f,241,80),l_debug);
    DM(substr(p_f,321,80),l_debug);
    DM(substr(p_f,401,80),l_debug);
    DM(substr(p_f,481,80),l_debug);
    DM(substr(p_f,561,80),l_debug);
    DM(substr(p_f,641,80),l_debug);
    DM(substr(p_f,721,80),l_debug);
    DM(substr(p_f,801,80),l_debug);
    DM(substr(p_f,881,80),l_debug);
    DM('And then process this row ',l_debug);
    --
    create_tab(p_table_name,p_f); 
    --
    p_s := 'N';
    p_f := NULL;
  END IF;
END IF;
-- Is the status Now N 
IF p_s = 'N'
THEN -- we need to find out if we need to change the status to B
  l_exists := 'N';
  OPEN  c_chk_tab;
  FETCH c_chk_tab INTO l_exists;
  CLOSE c_chk_tab;
  IF nvl(l_exists,'N') = 'N'
  THEN -- we need to start building a create table statement
    DM('Starting to build a Create table statement ',l_debug);
    p_s := 'B';
    p_f := 'CREATE TABLE '||p_table_name||'(';
    l_col_st  := NULL;
    --
    l_col_st := p_column_name||' '||p_column_type;
    IF p_column_type IN ('DATE','INTEGER')
      THEN NULL;
    ELSIF p_column_type in ('VARCHAR2','NUMBER')
      THEN l_col_st := l_col_st||'('||nvl(rtrim(p_column_length),'10')||')';
    END IF;
    --
    IF p_column_null = 'N'
    THEN 
      l_col_st := l_col_st||' NOT NULL';
    END IF;
    p_f := p_f||l_col_st;
  ELSE
    p_s := 'N';
    DM('Table Exists - check column definition ',l_debug);
    chk_col_def(p_table_name,p_column_name,p_column_type,
                p_column_length,p_column_null,l_col_ch);
    IF l_col_ch = 'O' -- Means it is OK
    THEN
    DM('Column Definition Matches - no update required ',l_debug);
    ELSIF l_col_ch = 'I' -- need to add the column into the table
    THEN
      DM('Column does not exist - insert required ',l_debug);
      ins_col_def(p_table_name,p_column_name,p_column_type,
                  p_column_length,p_column_null);
    --
    ELSIF l_col_ch = 'U' -- need to update the column def in the table
    THEN
      DM('Column Def does not match - update required ',l_debug);
      upd_col_def(p_table_name,p_column_name,p_column_type,
                  p_column_length,p_column_null);
    ELSIF l_col_ch = 'X' -- need to update the column def but without not null
    THEN
      DM('Column Def does not match - update required ',l_debug);
      upd_col_def(p_table_name,p_column_name,p_column_type,
                  p_column_length,null);
    END IF;
  END IF;
--
END IF;
--
p_t := p_table_name;
--
END PR;
--
BEGIN
--
--
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_TCY_ALT_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_RECEIVED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_CHECKED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_CHECKED_BY','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_HRV_ASCA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_HRV_HSTR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_APP_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_NEXT_SCHED_REVIEW_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_APPLICATIONS','LSUAP_REFERENCE','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_SUAP_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_COMMENTS','VARCHAR2','4000','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_TOTAL_DEBT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_TOTAL_ACCRUED_DEBT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_TOTAL_NON_ACCRUED_DEBT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_CALCULATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_CALCULATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_ESTABLISHED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_ESTABLISHED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_SDAR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_SDWR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_DEBT_ASSMNTS','LSUDA_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_USER_REFERENCE','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_EFFECTIVE_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_ASSESSMENT_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_SCO_CODE','VARCHAR2','3','N',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_ISSUE_ICS_REQUESTS_IND','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_ISSUE_INCOME_CERTIF_IND','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_REMINDER_NUM_DAYS','NUMBER','38','Y',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_TERMINATE_NUM_DAYS','NUMBER','38','Y',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_LATEST_LAST_REVIEW_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_HRV_ASCA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_TTY_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_GENERATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_GROUP_SUBSIDY_REVIEWS','LGRSR_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_SUAP_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_TCY_APP_ALT_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_SUAP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_CLASS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_ASSESSMENT_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_DEN_ELIGIBLE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_SUBP_ASCA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_SUBP_SEQ','NUMBER','5','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_HSRR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_AUTHORISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_DEFAULT_PERCENTAGE','NUMBER','8,5','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_DEN_SUB_UNROUNDED_AMT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_DEN_HHOLD_ASSESS_INC','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_DEN_SUBSIDY_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_DEN_CALC_RENT_PAYABLE','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_DEN_TCY_MARKET_RENT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_ASSESSED_SELB_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_HSCR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_ACHO_LEGACY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_GRSR_USER_REFERENCE','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_HSRS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_HTY_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_REVIEW_INITIATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_DETAILS_RECEIVED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_SUDA_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_CAP_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_CAP_TYPE','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_PAY_MARKET_RENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_REVIEWS','LSURV_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_SURV_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_PAR_REFNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_PAR_PER_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_HSIT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_ELIGIBILITY_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_SUBSIDY_CALC_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_OVERRIDDEN_INCOME_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_RENT_PAYABLE_CONTRIB','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_SUAR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_PERCENTAGE','NUMBER','8,5','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_SUAR_SUBP_SEQ','NUMBER','5','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_SUAR_SUBP_ASCA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_INCOME_ITEMS','LSUIT_SIOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
--
PR('DL_HRA_ACCOUNT_RENT_LIMITS','LARLI_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_RENT_LIMITS','LARLI_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_RENT_LIMITS','LARLI_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_RENT_LIMITS','LARLI_SURV_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_RENT_LIMITS','LARLI_REFERENCE','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_RENT_LIMITS','LARLI_SURV_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_RENT_LIMITS','LARLI_RLTY_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_RENT_LIMITS','LARLI_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_RENT_LIMITS','LARLI_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_RENT_LIMITS','LARLI_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_RENT_LIMITS','LARLI_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_RENT_LIMITS','LARLI_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_RENT_LIMITS','LARLI_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HRA_SUBSIDY_GRACE_PERIODS','LSGPE_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_GRACE_PERIODS','LSGPE_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_GRACE_PERIODS','LSGPE_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_GRACE_PERIODS','LSGPE_SUAP_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_GRACE_PERIODS','LSGPE_SEQ','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_GRACE_PERIODS','LSGPE_HGPR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_GRACE_PERIODS','LSGPE_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_GRACE_PERIODS','LSGPE_RENT_PAYABLE','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_GRACE_PERIODS','LSGPE_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_GRACE_PERIODS','LSGPE_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_GRACE_PERIODS','LSGPE_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_GRACE_PERIODS','LSGPE_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_GRACE_PERIODS','LSGPE_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
--
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_SURV_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_SEQ','NUMBER','2','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_HSLT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_COMMENTS','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_PRINTED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_CANCELLED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_CANCELLED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_SUBSIDY_LETTERS','LSULE_HLCR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
--
-- This DL_DUMMY table must always be the last one as it doesn't get created
--
PR('DL_DUMMY','LDUMMY','VARCHAR2','1','N',l_t,l_s,l_f);
END;
/
