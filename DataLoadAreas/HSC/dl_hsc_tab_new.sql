--
-- dl_hsc_tab_new.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to ensure the Service Charge Dataload Tables have
-- the correct columns. This script does not drop the
-- dataload tables, just adds in any missing columns.
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 29-NOV-2006   1.0  5.9.0  PJD   Initial Creation.
-- 17-JUL-2007   2.0  5.12.0 PH    Introduced into standard dataload install. 
-- 10-AUG-2007   2.1  5.12.0 PH    Corrected update statement for adding
--                                 DATE and INTEGER Columns in ins_col_def
--                                 and upd_col_def. Amended to cater for where
--                                 update required and NOT NULL is correct as
--                                 this causes sql error
-- 21-AUG-2007   2.2  5.12.0 PH    Set any dl_seqno number values to 8 rather
--                                 than ''.
-- 05-FEB-2008   2.3  5.13.0 PH    Changed p_column_null = 'NOT NULL' to
--                                 'N'. Also added dummy table at the end as
--                                 new table will not get created if it's the
--                                 last one.
-- 14-MAR-2008   2.4  5.13.0 PH    Amended create table to exclude pct free
--                                 and storage options. Only uses tablespace.
-- 14-DEC-2009   2.5  5.15.1 PH    Amended l_nullable := NULL to be
--                                 l_nullable := 'NULL'
--
-- 06-DEC-2012   2.6  6.5.1  PJD    Amended PROCEDURE chk_col_def to include
--                                  section for DATE datatypes NULL/NOT NULL
-- 27-JAN-2013   3.0  6.1.1  PJD   Remove l_nullable = NULL from upd_col_def
-- 23-MAR-2016   4.0  6.13.0 JS    allowed for nullable being the same as current setting
--                                 in upd_col_def by removing l_nullable from statement
--                                 if p_nullable is passed as a null value as this is only
--                                 done if old and new nullable setting is the same value
--                                 otherwise its a Y or an N
--                                 LLEA_LEASE_RECORD_TYPE_IND set to be not nullable
--                                 LLEA_LCO_CODE set to be not nullable
--                                 Added LSCB_PRORATE_INITIAL_CHGS_IND
-- 23-MAR-2016   5.0  6.13   AJ    1)amended LCLIN_INVOICE_REF from (35) to (25)
--                                 2)amended LSSCI_SEQNO from (10) to (5)
-- 10-MAY-2016   6.0  6.13   AJ    1)amended all xxx_DLB_BATCH_ID xxx_DL_SEQNO
--                                 xxx_DL_LOAD_STATUS so all null-able indicators set to 'Y'
--                                 2) DL_HSC_CREDIT_ALLOCATIONS first 3 columns order corrected
--                                 3) LCIAA_NOP_TEXT increased from 2000 to 4000
-- 21-DEC-2017   6.1  6.15   AJ    defined length as missing for
--                                 'DL_HSC_ACTIVE_SCP_EST','LASE_AMOUNT','NUMBER','10,2'
--                                 'DL_HSC_INACTIVE_SCP_EST','LISE_DL_SEQNO','NUMBER','8'
--                                 'DL_HSC_INACTIVE_SCP_EST','LISE_AMOUNT','NUMBER','10,2'
--                                 'DL_HSC_SCP_ACTUALS','LSCA_ESTIMATED_AMOUNT','NUMBER','10,2'
--                                 'DL_HSC_SCP_ACTUALS','LSCA_ACTUAL_AMOUNT','NUMBER','10,2'
-- 09-AUG-2018   6.2  6:15   AJ    Added to a single version by PJD 12-Mar-2018 (6.1) and now
--                                 amalgamated to main version control copy now (6.2) is the
--                                 addition of DL_HSC_SUNDRY_INV_ITEMS data load is not in the
--                                 standard offering
-- 16-NOV-2018   6.3  6:17   PJD   Added Clin Comments
-- 20-NOV-2018   6.4  6:18   PJD   Change the LLAS_CORRESPOND_NAME to 250 chars
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
PR('DL_HSC_ACTIVE_SCP_EST','LASE_DLB_BATCH_ID','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_ACTIVE_SCP_EST','LASE_DL_SEQNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HSC_ACTIVE_SCP_EST','LASE_DL_LOAD_STATUS','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_ACTIVE_SCP_EST','LASE_PROREFNO_AUNCODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_ACTIVE_SCP_EST','LASE_PRO_AUN_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_ACTIVE_SCP_EST','LASE_SCB_SCP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_ACTIVE_SCP_EST','LASE_SCB_SCP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_ACTIVE_SCP_EST','LASE_SCB_SVC_ATT_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_ACTIVE_SCP_EST','LASE_SCB_SVC_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_ACTIVE_SCP_EST','LASE_AMOUNT','NUMBER','10,2','Y',l_t,l_s,l_f);
PR('DL_HSC_ACTIVE_SCP_EST','LASE_ORIDE_WEIGHTING_TOT','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HSC_ACTIVE_SCP_EST','LASE_VOID_LOSS_PERCENTAGE','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HSC_ACTIVE_SCP_EST','LASE_RECONCILED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_ACTUALS','LCOA_DLB_BATCH_ID','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_ACTUALS','LCOA_DL_SEQNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_ACTUALS','LCOA_DL_LOAD_STATUS','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_ACTUALS','LCOA_SCR_PROREFNO_AUNCODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_ACTUALS','LCOA_SCR_PRO_AUN_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_ACTUALS','LCOA_SCR_SCB_SCP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_ACTUALS','LCOA_SCR_SCB_SCP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_ACTUALS','LCOA_SCR_SCB_SVC_ATT_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_ACTUALS','LCOA_SCR_SCB_SVC_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_ACTUALS','LCOA_ACT_EST_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_ACTUALS','LCOA_AMOUNT','NUMBER','10,2','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_ACTUALS','LCOA_DESCRIPTION','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_ACTUALS','LCOA_EXT_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_ACTUALS','LCOA_DATE_INCURRED','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_EST','LCOE_DLB_BATCH_ID','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_EST','LCOE_DL_SEQNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_EST','LCOE_DL_LOAD_STATUS','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_EST','LCOE_SCR_PROREFNO_AUNCODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_EST','LCOE_SCR_PRO_AUN_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_EST','LCOE_SCR_SCB_SCP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_EST','LCOE_SCR_SCB_SCP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_EST','LCOE_SCR_SCB_SVC_ATT_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_EST','LCOE_SCR_SCB_SVC_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_EST','LCOE_ACT_EST_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_EST','LCOE_AMOUNT','NUMBER','10,2','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_EST','LCOE_DESCRIPTION','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_EST','LCOE_EXT_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_COMP_EST','LCOE_DATE_INCURRED','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_ALLOCATIONS','LCRAL_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_ALLOCATIONS','LCRAL_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_ALLOCATIONS','LCRAL_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_ALLOCATIONS','LCRAL_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_ALLOCATIONS','LCRAL_ALLOCATED_AMOUNT','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_ALLOCATIONS','LCRAL_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_ALLOCATIONS','LCRAL_TRA_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_ALLOCATIONS','LCRAL_TRA_CR','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_ALLOCATIONS','LCRAL_TRA_EXTERNAL_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_ALLOCATIONS','LCRAL_CCME_REFNO_CREDIT_FROM','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_ALLOCATIONS','LCRAL_CLIN_REFNO_CREDIT_FROM','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_ALLOCATIONS','LCRAL_TRA_REFNO_CREDIT_TO','NUMBER','12','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_ALLOCATIONS','LCRAL_CLIN_REFNO_CREDIT_TO','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_MEMO_BALANCES','LCMBA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_MEMO_BALANCES','LCMBA_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_MEMO_BALANCES','LCMBA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_MEMO_BALANCES','LCMBA_CCME_CREDIT_MEMO_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_MEMO_BALANCES','LCMBA_SEQNO','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_MEMO_BALANCES','LCMBA_BALANCE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_MEMO_BALANCES','LCMBA_TOTAL_BALANCE','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_MEMO_BALANCES','LCMBA_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_MEMO_BALANCES','LCMBA_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_MEMO_BALANCES','LCMBA_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HSC_CREDIT_MEMO_BALANCES','LCMBA_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_CREDIT_MEMOS','LCCME_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_CREDIT_MEMOS','LCCME_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_CREDIT_MEMOS','LCCME_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_CREDIT_MEMOS','LCCME_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_CREDIT_MEMOS','LCCME_CREDIT_MEMO_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_CREDIT_MEMOS','LCCME_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_CREDIT_MEMOS','LCCME_CLIN_INVOICE_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_CREDIT_MEMOS','LCCME_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_CREDIT_MEMOS','LCCME_AUTHORISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_CREDIT_MEMOS','LCCME_ISSUED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_CREDIT_MEMOS','LCCME_ISSUED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_CREDIT_MEMOS','LCCME_LEVEL2_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_CREDIT_MEMOS','LCCME_LEVEL2_AUTHORISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_CREDIT_MEMOS','LCCME_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_CLASS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_INCA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_INVOICE_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_PRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_INSC_SCIC_SCP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_INSC_SCIC_SCP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_INPO_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_AUTHORISE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_ISSUED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_ISSUE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_PAYMENT_DUE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_TRANSACTION_RAISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_INVOICED_PERIOD_START','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_INVOICED_PERIOD_END','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_LEVEL2_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_LEVEL2_AUTHORISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_PREV_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_SOURCE_INVOICE_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_ARREARS_POSSIBLE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_MWP_REFERENCE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_PAR_PER_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_RECONCILE_ONLY_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_CUSTOMER_INVOICES','LCLIN_COMMENTS','VARCHAR2','4000','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_INVOICE_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_ARA_CODE','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_STATUS','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_TOTAL_INVOICE_BALANCE','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_UNDISPUTED_BALANCE','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_CREATION_TYPE','VARCHAR2','6','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_EAC_EPO_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_REMAINING_INSTAL_AMT','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_NEXT_ACTION_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_EXPIRY_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_REVIEW_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_AUTHORISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_DELETED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_DELETED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_HRV_ADL_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_PRINTED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_PRINT_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_CUST_INV_ARREARS_ACT','LCIAA_NOP_TEXT','VARCHAR2','4000','Y',l_t,l_s,l_f);
PR('DL_HSC_INACTIVE_SCP_EST','LISE_DLB_BATCH_ID','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_INACTIVE_SCP_EST','LISE_DL_SEQNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HSC_INACTIVE_SCP_EST','LISE_DL_LOAD_STATUS','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_INACTIVE_SCP_EST','LISE_PROREFNO_AUNCODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_INACTIVE_SCP_EST','LISE_PRO_AUN_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_INACTIVE_SCP_EST','LISE_SCB_SCP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_INACTIVE_SCP_EST','LISE_SCB_SCP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_INACTIVE_SCP_EST','LISE_SCB_SVC_ATT_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_INACTIVE_SCP_EST','LISE_SCB_SVC_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_INACTIVE_SCP_EST','LISE_AMOUNT','NUMBER','10,2','Y',l_t,l_s,l_f);
PR('DL_HSC_INACTIVE_SCP_EST','LISE_ORIDE_WEIGHTING_TOT','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HSC_INACTIVE_SCP_EST','LISE_RECONCILED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_BALANCES','LINBA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_BALANCES','LINBA_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_BALANCES','LINBA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_BALANCES','LINBA_CLIN_INVOICE_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_BALANCES','LINBA_SEQNO','NUMBER','5','Y',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_BALANCES','LINBA_BALANCE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_BALANCES','LINBA_TOTAL_BALANCE','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_BALANCES','LINBA_UNDISPUTED_BALANCE','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_BALANCES','LINBA_INTEREST_CHARGE_TO_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_PARTIES','LSCIP_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_PARTIES','LSCIP_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_PARTIES','LSCIP_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_PARTIES','LSCIP_PAR_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_PARTIES','LSCIP_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_PARTIES','LSCIP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_INVOICE_PARTIES','LSCIP_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_PRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_LEASE_RECORD_TYPE_IND','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_LCO_CODE','VARCHAR2','8','N',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_IPP_SHORTNAME','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_RTB_REFERENCE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_S125_OFFER_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_INITIAL_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_ACTUAL_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_LEASE_DURATION','NUMBER','5','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_REF_PERIOD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_HRV_LIR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_HRV_FLT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_HRV_SCH_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_RTB_DISCOUNT','NUMBER','10,2','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_RTB_APPLICATION_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASES','LLEA_RTB_PURCHASE_PRICE','NUMBER','10,2','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASE_ASSIGNMENTS','LLAS_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_LEASE_ASSIGNMENTS','LLAS_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_LEASE_ASSIGNMENTS','LLAS_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_LEASE_ASSIGNMENTS','LLAS_LEA_PRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASE_ASSIGNMENTS','LLAS_LEA_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASE_ASSIGNMENTS','LLAS_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASE_ASSIGNMENTS','LLAS_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASE_ASSIGNMENTS','LLAS_CORRESPOND_NAME','VARCHAR2','250','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASE_PARTIES','LLPT_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_LEASE_PARTIES','LLPT_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_LEASE_PARTIES','LLPT_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_LEASE_PARTIES','LLPT_LAS_LEA_PRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASE_PARTIES','LLPT_LAS_LEA_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASE_PARTIES','LLPT_LAS_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASE_PARTIES','LLPT_PAR_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASE_PARTIES','LLPT_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASE_PARTIES','LLPT_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASE_SUMMARIES','LLES_DLB_BATCH_ID','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASE_SUMMARIES','LLES_DL_LOAD_STATUS','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASE_SUMMARIES','LLES_DL_SEQNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HSC_LEASE_SUMMARIES','LLES_PROCESS','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HSC_PAYMENT_BALANCES','LPABA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_PAYMENT_BALANCES','LPABA_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_PAYMENT_BALANCES','LPABA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_PAYMENT_BALANCES','LPABA_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_PAYMENT_BALANCES','LPABA_TRA_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_PAYMENT_BALANCES','LPABA_TRA_CR','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HSC_PAYMENT_BALANCES','LPABA_TRA_EXTERNAL_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_PAYMENT_BALANCES','LPABA_TRA_REFNO','NUMBER','12','Y',l_t,l_s,l_f);
PR('DL_HSC_PAYMENT_BALANCES','LPABA_SEQNO','NUMBER','5','Y',l_t,l_s,l_f);
PR('DL_HSC_PAYMENT_BALANCES','LPABA_BALANCE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_PAYMENT_BALANCES','LPABA_TOTAL_BALANCE','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_CLASS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_SSCI_CLIN_INVOICE_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_SSCI_SEQNO','NUMBER','5','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_AMOUNT','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_INVOICEABLE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_PREV_ESTIMATED_COST','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_NEW_ESTIMATED_COST','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_TAX_AMOUNT','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_PREV_CAPPED_COST','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_NEW_CAPPED_COST','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_INVOICE_ADJUSTMENTS','LSCIA_NEW_ACTUAL_COST','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_SEQNO','NUMBER','5','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_CLIN_INVOICE_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_CLASS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_ESTIMATED_AMOUNT','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_INVOICED_AMOUNT','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_ADDED_POST_ISSUE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_INVOICED_TAX_AMOUNT','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_ESTIMATED_WEIGHT_VALUE','NUMBER','11,5','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_ACTUAL_AMOUNT','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_ACTUAL_WEIGHT_VALUE','NUMBER','11,5','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_CAPPED_AMOUNT','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_SCR_PROREFNO_AUNCODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_SCR_PRO_AUN_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_SCR_SCB_SCP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_SCR_SCB_SCP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_SCR_SCB_SVC_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_SCR_SCB_SVC_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_SUS_PRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_SUS_SVC_ATT_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_SUS_SVC_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_SUS_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_MCG_SCP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_MCG_SCP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_MCG_SVC_ATT_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_MCG_SVC_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCI_SERVICE_CHG_ITEMS','LSSCI_DDE_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCP_ACTUALS','LSCA_DLB_BATCH_ID','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCP_ACTUALS','LSCA_DL_SEQNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HSC_SCP_ACTUALS','LSCA_DL_LOAD_STATUS','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SCP_ACTUALS','LSCA_PROREFNO_AUNCODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_SCP_ACTUALS','LSCA_PRO_AUN_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SCP_ACTUALS','LSCA_SCB_SCP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCP_ACTUALS','LSCA_SCB_SCP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_SCP_ACTUALS','LSCA_SCB_SVC_ATT_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCP_ACTUALS','LSCA_SCB_SVC_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCP_ACTUALS','LSCA_ESTIMATED_AMOUNT','NUMBER','10,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SCP_ACTUALS','LSCA_ACTUAL_AMOUNT','NUMBER','10,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SCP_ACTUALS','LSCA_ORIDE_WEIGHTING_TOT','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SCP_ACTUALS','LSCA_VOID_LOSS_PERCENTAGE','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HSC_SCP_ACTUALS','LSCA_RECONCILED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_ASSIGNMENTS','LSEA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_ASSIGNMENTS','LSEA_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_ASSIGNMENTS','LSEA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_ASSIGNMENTS','LSEA_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_ASSIGNMENTS','LSEA_SVC_ATT_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_ASSIGNMENTS','LSEA_SVC_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_ASSIGNMENTS','LSEA_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_ASSIGNMENTS','LSEA_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_ASSIGNMENTS','LSEA_SEA_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_ASSIGNMENTS','LSEA_SEA_SVC_ATT_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_ASSIGNMENTS','LSEA_SEA_SVC_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_ASSIGNMENTS','LSEA_SEA_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_SCP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_SCP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_SVC_ATT_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_SVC_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_DESCRIPTION','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_COST_BASIS','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_APPORTION_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_APPLY_CAP_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_INCREASE_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_TAX_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_CHARGE_APPLICABLE_TO','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_COMPLETE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_REBATEABLE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_ADJUSTMENT_METHOD','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_NOM_ADMIN_PERIOD','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_VCA_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_REAPPORTION_ACTUALS_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_SCIC_INCA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_DEFAULT_PERCENT_INCREASE','NUMBER','4,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_EXTRACT_FROM_REPAIRS_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_INCLUDE_PROP_REPAIRS_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_COMPONENT_LEVEL','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_AUY_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_BASES','LSCB_PRORATE_INITIAL_CHGS_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_PROPREF_AUNCODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_PRO_AUN_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_SCB_SCP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_SCB_SCP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_SCB_SVC_ATT_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_SCB_SVC_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_ESTIMATED_AMOUNT','NUMBER','10,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_ACTUAL_AMOUNT','NUMBER','10,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_ORIDE_WEIGHTING_TOT','NUMBER','10,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_VOID_LOSS_PERCENTAGE','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_RECONCILED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_ACTUAL_CALC_WEIGHT_TOT','NUMBER','12,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_CHARGE_RATES','LSCR_ACTUAL_ORIDE_WEIGHT_TOT','NUMBER','12,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_USAGES','LSUS_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_USAGES','LSUS_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_USAGES','LSUS_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_USAGES','LSUS_PROPREF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_USAGES','LSUS_SVC_ATT_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_USAGES','LSUS_SVC_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_USAGES','LSUS_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_USAGES','LSUS_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_USAGES','LSUS_SEA_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_USAGES','LSUS_SEA_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_USAGES','LSUS_ORIGINE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SERVICE_USAGES','LSUS_CHARGEABLE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HSC_SUNDRY_INV_ITEMS','LSUII_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HSC_SUNDRY_INV_ITEMS','LSUII_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HSC_SUNDRY_INV_ITEMS','LSUII_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HSC_SUNDRY_INV_ITEMS','LSUII_CLIN_INVOICE_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HSC_SUNDRY_INV_ITEMS','LSUII_SEQNO','NUMBER','5','Y',l_t,l_s,l_f);
PR('DL_HSC_SUNDRY_INV_ITEMS','LSUII_INVOICED_AMOUNT','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SUNDRY_INV_ITEMS','LSUII_INVOICED_TAX_AMOUNT','NUMBER','13,2','Y',l_t,l_s,l_f);
PR('DL_HSC_SUNDRY_INV_ITEMS','LSUII_ADTK_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
--
-- This DL_DUMMY table must always be the last one as it doesn't get created
--
PR('DL_DUMMY','LDUMMY','VARCHAR2','1','N',l_t,l_s,l_f);
END;
/
