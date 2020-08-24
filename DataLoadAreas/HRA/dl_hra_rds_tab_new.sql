--
-- dl_hra_rds_tab_new.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to ensure the RDS Dataload Tables have
-- the correct columns. This script does not drop the
-- dataload tables, just adds in any missing columns.
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
--
-- 15-JAN-2009   1.0  5.15.0 PH    Initial Creation.
--
-- 04-JUN-2009   2.0  5.15.0 VS    Split Authorised Deductions and Account
--                                 Deductions
--
-- 11-JUN-2009   3.0  5.15.0 VS    Changed DL_HRA_RDS_TRANSMISSION_FILES to
--                                 DL_HRA_RDS_TRANS_FILES
--
-- 13-NOV-2009   4.0  5.15.0 VS    Changed DL_HRA_RDS_ACCOUNT_ALLOCS to include
--                                 new fields for defect id 2494 Fix
--
-- 09-DEC-2009   5.0  5.15.0 VS    Changed DL_HRA_RDS_ACCOUNT_ALLOCS to include
--                                 new fields for defect id 2847 Fix
--
-- 06-APR-2010   6.0  5.15.0 VS    Changed DL_HRA_RDS_PYI520 to include
--                                 new field LP520_RAUD_START_DATE
--
-- 10-APR-2010   7.0  5.15.0 VS    Changed PYI100, 110, 500,510,511,512,513,520 to include
--                                 new field RAUD_START_DATE. Defect id 4162
--
-- 28-JUN-2010   8.0  5.15.0 VS    Changed DL_HRA_RDS_AUTHORITIES and DL_HRA_RDS_ACC_DEDUCTIONS to include
--                                 new field modified by and Date for defect id 5159 fix.
-- 17-JAN-2018   8.1  6.15   AJ    allowed for nullable being the same as current setting
--                                 in upd_col_def by removing l_nullable from statement
--                                 if p_nullable is passed as a null value as this is only
--                                 done if old and new nullable setting is the same value
--                                 otherwise its a Y or an N
-- 06-FEB-2018   8.2  6.16   TG    Remove LRDAL_PAR_PER_ALT_REF as it is not used anywhere.
--                                 Added LRDSA_HRV_BSRC_CODE.
--
-- 17-DEC-2018   8.3  6.18   VRS   Updating for 6.18 Change for DVA to RDS_AUTHORITIES
--                                 Additional columns to RDS_ACCOUNT_DEDUCTIONS
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
                              ' '            || p_col_type||
                              ' (           '||p_col_length||
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
PROCEDURE upd_col_def (p_table_name VARCHAR2
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
                              ' (            '||p_col_length||
                              ' ))';
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
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_HA_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_PAR_PER_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_HRV_RPAG_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_PAY_AGENCY_CRN','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_PENDING_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_SUSPEND_FROM_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_SUSPEND_TO_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_ACTION_SENT_DATETIME','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_HRV_SUSR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_HRV_TERR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_HRV_BSRC_CODE','VARCHAR2','3','N',l_t,l_s,l_f);
-- 6.18 columns
PR('DL_HRA_RDS_AUTHORITIES','LRDSA_DVA_UIN','NUMBER','10','Y',l_t,l_s,l_f);
--
-- No Change
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_RDSA_HA_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_HRV_DEDT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_CURRENT_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_HRV_RBEG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_PENDING_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_SUSPEND_FROM_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_SUSPEND_TO_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_ACTION_SENT_DATETIME','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_NEXT_PAY_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_HRV_SUSR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_AUTH_DEDUCTIONS','LRAUD_HRV_TERR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
--
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_RDSA_HA_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_RAUD_HRV_DEDT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_RAUD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_RAUD_HRV_RBEG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_RADT_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_HRV_RBEG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_REQUESTED_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_FIXED_AMOUNT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_MINOR_ARR_VARY_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_CURRENT_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_PENDING_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_REQUESTED_PERCENTAGE','NUMBER','6,3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_SUSPENDED_FROM_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_SUSPENDED_TO_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_ACTION_SENT_DATETIME','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_HRV_SUSR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_HRV_TERR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_LAST_DEDUCTION_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_LAST_DEDUCTION_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_NEXT_DEDUCTION_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_NEXT_DEDUCTION_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_MINOR_ARR_VARY_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_NET_RENT_BASIS_DEDN','NUMBER','11,2','Y',l_t,l_s,l_f);
-- 6.18 columns
PR('DL_HRA_RDS_ACC_DEDUCTIONS','LRACD_NON_RELATED','VARCHAR2','1','Y',l_t,l_s,l_f);
--
-- ******************************************************************************************
--
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRACD_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_RDSA_HA_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_RDSA_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_HRV_DEDT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_CURRENT_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_HRV_RBEG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_PENDING_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_SUSPEND_FROM_DATE','DATE',' ','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_SUSPEND_TO_DATE','DATE',' ','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_ACTION_SENT_DATETIME','DATE',' ','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_NEXT_PAY_DATE','DATE',' ','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_HRV_SUSR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRAUD_HRV_TERR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRACD_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRACD_RADT_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRACD_REQUESTED_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRACD_FIXED_AMOUNT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRACD_MINOR_ARR_VARY_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRACD_REQUESTED_PERCENTAGE','NUMBER','6,3','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRACD_LAST_DEDUCTION_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRACD_LAST_DEDUCTION_DATE','DATE',' ','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRACD_NEXT_DEDUCTION_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRACD_NEXT_DEDUCTION_DATE','DATE',' ','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRACD_MINOR_ARR_VARY_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
--PR('DL_HRA_RDS_AUTH_ACC_DEDUCT','LRACD_NET_RENT_BASIS_DEDN','NUMBER','11,2','Y',l_t,l_s,l_f);
--
-- ******************************************************************************************
--
-- No Change
PR('DL_HRA_RDS_INSTRUCTIONS','LRDIN_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_INSTRUCTIONS','LRDIN_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_INSTRUCTIONS','LRDIN_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_INSTRUCTIONS','LRDIN_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_INSTRUCTIONS','LRDIN_RDSA_HA_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_INSTRUCTIONS','LRDIN_HRV_DEDT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_INSTRUCTIONS','LRDIN_HRV_RBEG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_INSTRUCTIONS','LRDIN_RAUD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_INSTRUCTIONS','LRDIN_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_INSTRUCTIONS','LRDIN_INSTRUCTION_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_INSTRUCTIONS','LRDIN_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_INSTRUCTIONS','LRDIN_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_INSTRUCTIONS','LRDIN_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
--
-- No Change
PR('DL_HRA_RDS_ALLOCATIONS','LRDAL_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_ALLOCATIONS','LRDAL_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_ALLOCATIONS','LRDAL_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_ALLOCATIONS','LRDAL_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ALLOCATIONS','LRDAL_RDSA_HA_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ALLOCATIONS','LRDAL_RAUD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ALLOCATIONS','LRDAL_HRV_DEDT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ALLOCATIONS','LRDAL_HRV_RBEG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ALLOCATIONS','LRDAL_RDIN_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ALLOCATIONS','LRDAL_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ALLOCATIONS','LRDAL_ALLOCATED_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ALLOCATIONS','LRDAL_DEDUCTION_ACTION_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ALLOCATIONS','LRDAL_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ALLOCATIONS','LRDAL_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
--
-- No Change
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_RDSA_HA_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_PAR_PER_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_RAUD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_HRV_DEDT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_HRV_RBEG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_RACD_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_RACD_START_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_RACD_RADT_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_RDAL_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_REQUESTED_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_FIXED_AMOUNT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_ALLOCATED_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_PRIORITY','VARCHAR2','2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_REQUESTED_PERCENTAGE','NUMBER','6,3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_RACD_REQUESTED_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_RDAL_DEDUCT_ACTION_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ACCOUNT_ALLOCS','LRAAL_RDIN_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
--
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_HRV_RPAG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_FILE_NUMBER','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_TIMESTAMP','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_SENDING_AGENCY','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_RECEIVING_AGENCY','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_PROCESSED_DATETIME','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_TRANSACTION_COUNT','NUMBER','9','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_REC_AUTHD_DATETIME','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_REC_AUTHD_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_TRANS_FILES','LRDTF_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HRA_RDS_ERRORS','LRERR_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_ERRORS','LRERR_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_ERRORS','LRERR_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_ERRORS','LRERR_RDTF_ALT_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ERRORS','LRERR_SOURCE_TRAN_REF','NUMBER','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ERRORS','LRERR_TRANS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ERRORS','LRERR_EDX_ERROR_CODE','VARCHAR2','6','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ERRORS','LRERR_TIMESTAMP','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ERRORS','LRERR_CRN','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ERRORS','LRERR_EXT_REF_ID','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ERRORS','LRERR_DATA_LENGTH','NUMBER','5','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ERRORS','LRERR_TRANSACTION_DATA','VARCHAR2','255','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_ERRORS','LRERR_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HRA_RDS_PYI100','LP100_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_RDTF_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_RDSA_HA_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_RAUD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_HRV_DEDT_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_HRV_RBEG_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_CUSTOMER_BIRTH_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_CUSTOMER_SURNAME','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_CUSTOMER_POSTCODE','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_TRANS_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_CRN','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_TIMESTAMP','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_REQUEST_ACTION_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_INSTRUCTION_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_TP_ID','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI100','LP100_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
--
PR('DL_HRA_RDS_PYI110','LP110_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI110','LP110_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI110','LP110_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI110','LP110_RDTF_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI110','LP110_RDSA_HA_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI110','LP110_RAUD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI110','LP110_HRV_DEDT_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI110','LP110_HRV_RBEG_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI110','LP110_TRANS_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI110','LP110_CRN','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI110','LP110_TIMESTAMP','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI110','LP110_REQUEST_ACTION_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI110','LP110_INSTRUCTION_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI110','LP110_TP_ID','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI110','LP110_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI110','LP110_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
--
PR('DL_HRA_RDS_PYI500','LP500_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_RDTF_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_RDSA_HA_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_RAUD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_HRV_DEDT_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_HRV_RBEG_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_CRN','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_EXT_REF_ID','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_SOURCE_TRAN_REF','NUMBER','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_TIMESTAMP','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_INSTRUCTION_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_ENVIRONMENT_ID','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_ERROR_CODE','VARCHAR2','6','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_ERROR_MESSAGE','VARCHAR2','45','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_DEDUCTION_ACTION','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI500','LP500_REFNO','NUMBER','20','Y',l_t,l_s,l_f);
--
PR('DL_HRA_RDS_PYI510','LP510_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_RDTF_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_RDSA_HA_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_RAUD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_HRV_DEDT_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_HRV_RBEG_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_CRN','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_EXT_REF_ID','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_SOURCE_TRAN_REF','NUMBER','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_TIMESTAMP','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_INSTRUCTION_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_ENVIRONMENT_ID','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_DEDUCTION_ACTION_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI510','LP510_REFNO','NUMBER','20','Y',l_t,l_s,l_f);
--
PR('DL_HRA_RDS_PYI512','LP512_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_RDTF_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_RDSA_HA_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_RAUD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_HRV_DEDT_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_HRV_RBEG_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_CRN','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_EXT_REF_ID','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_SOURCE_TRAN_REF','NUMBER','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_TIMESTAMP','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_INSTRUCTION_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_ENVIRONMENT_ID','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_DEDUCTION_ACTION_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_ALLOCATED_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_FUTURE_ACTION_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI512','LP512_REFNO','NUMBER','20','Y',l_t,l_s,l_f);
--
PR('DL_HRA_RDS_PYI513','LP513_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI513','LP513_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI513','LP513_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI513','LP513_RDTF_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI513','LP513_RDSA_HA_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI513','LP513_RAUD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI513','LP513_HRV_DEDT_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI513','LP513_HRV_RBEG_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI513','LP513_CRN','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI513','LP513_EXT_REF_ID','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI513','LP513_TIMESTAMP','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI513','LP513_ENVIRONMENT_ID','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI513','LP513_DEDUCTION_ACTION_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI513','LP513_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI513','LP513_REFNO','NUMBER','20','Y',l_t,l_s,l_f);
--
PR('DL_HRA_RDS_PYI520','LP520_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_RDTF_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_RDSA_HA_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_RAUD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_HRV_DEDT_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_HRV_RBEG_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_CRN','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_EXT_REF_ID','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_SOURCE_TRAN_REF','NUMBER','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_TIMESTAMP','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_INSTRUCTION_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_ENVIRONMENT_ID','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_DEDUCTION_ACTION_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_PAY_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_PAYMENT_STRIP','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_PAY_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_BSB_NUMBER','NUMBER','6','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_ACCOUNT_NUMBER','NUMBER','9','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_PAYMENT_STATUS_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_CUSTOMER_NAME','VARCHAR2','26','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_TRANS_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_LODGEMENT_PREFIX','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI520','LP520_REFNO','NUMBER','20','Y',l_t,l_s,l_f);
--
PR('DL_HRA_RDS_PYI530','LP530_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI530','LP530_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI530','LP530_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI530','LP530_RDTF_ALT_REF','VARCHAR2','20','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI530','LP530_HRV_RBEG_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI530','LP530_TIMESTAMP','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI530','LP530_ENVIRONMENT_ID','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI530','LP530_DEDUCTION_ACTION_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI530','LP530_COSTING_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI530','LP530_SERVICE_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI530','LP530_UNIT_COUNT','NUMBER','7','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI530','LP530_UNIT_COST','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI530','LP530_TOTAL_COST','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI530','LP530_REFNO','NUMBER','20','Y',l_t,l_s,l_f);
--
PR('DL_HRA_RDS_PYI540','LP540_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_RDTF_ALT_REF','VARCHAR2','20','N',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_TIMESTAMP','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_DEDUCTION_ACTION_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_ENVIRONMENT_ID','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_BENEFIT_GROUP','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_PAY_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_TOTAL_COUNT','NUMBER','7','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_TOTAL_COST','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_TOTAL_AMOUNT_PAID','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_BANK_FAX_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_FAX_INPUT_DATETIME','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_FAX_INPUT_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_REC_ORIDE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_REC_ORIDE_DATETIME','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_REC_ORIDE_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_RDS_PYI540','LP540_REFNO','NUMBER','20','Y',l_t,l_s,l_f);
--
--
-- This DL_DUMMY table must always be the last one as it doesn't get created
--
PR('DL_DUMMY','LDUMMY','VARCHAR2','1','N',l_t,l_s,l_f);
END;
/
