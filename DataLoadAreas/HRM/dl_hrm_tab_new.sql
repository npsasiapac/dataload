--
-- dl_hrm_tab_new.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to ensure the Repairs Dataload Tables have
-- the correct columns. This script does not drop the
-- dataload tables, just adds in any missing columns.
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 29-NOV-2006   1.0  5.9.0  PJD   Initial Creation.
-- 17-JUL-2007   2.0  5.12.0 PH    Removed any Repairs Contractors tables
--                                 Introduced into standard dataload install.
-- 10-AUG-2007   2.1  5.12.0 PH    Corrected update statement for adding
--                                 DATE and INTEGER Columns in ins_col_def 
--                                 and upd_col_def. Amended to cater for where
--                                 update required and NOT NULL is correct as
--                                 this causes sql error
-- 05-FEB-2008   2.2  5.13.0 PH    Added new field for Jobs.
-- 05-FEB-2008   2.3  5.13.0 PH    Changed p_column_null = 'NOT NULL' to
--                                 'N'. Also added dummy table at the end as
--                                 new table will not get created if it's the
--                                 last one.
-- 14-MAR-2008   2.4  5.13.0 PH    Amended create table to exclude pct free
--                                 and storage options. Only uses tablespace.
-- 14-DEC-2009   2.5  5.15.1 PH    Amended l_nullable := NULL to be
--                                 l_nullable := 'NULL'
-- 23-APR-2012   2.6  6.1.1  MB    Made lsrq_comments 2000 char as per system table
-- 06-DEC-2012   2.7  6.5.1  PJD   Amended PROCEDURE chk_col_def to include
--                                 section for DATE datatypes NULL/NOT NULL
-- 27-JAN-2013   3.0  6.1.1  PJD   Remove l_nullable = NULL from upd_col_def
-- 26-MAY-2015   3.1  6.9    PJD   Add some extra date columns as used at RBG
-- 14-SEP-2015   3.2  6.11   AJ    Added LWDC_CODE_MLANG and LWDC_DESCRIPTION_MLANG
--                                 to DL_HRM_WORK_DESCRIPTIONS table
--                                 Added LSOR_CODE_MLANG,LSOR_DESCRIPTION_MLANG and
--                                 LSOR_KEYWORDS_MLANG to DL_HRM_SCHEDULE_OF_RATES table
-- 01-MAR-2016   3.5  6.13   AJ    1) allowed for nullable being the same as current setting
--                                 in upd_col_def by removing l_nullable from statement
--                                 if p_nullable is passed as a null value as this is only
--                                 done if old and new nullable setting is the same value
--                                 otherwise its a Y or an N
--                                 2)amended DL_HRM_SCHEDULE_OF_RATES field LSOR_QUANTITY
--                                 from (8) to (6,2) to match Doc and DBase table.
--                                 3) amended DL_HRM_SCHEDULE_OF_RATES field LSOR_HRV_VCA_CODE
--                                 from (8) to (10) to match Doc and Dbase table
-- 11-MAR-2016   3.6  6.13   AJ    1)amended DL_HRM_SERVICE_REQUESTS field LSRQ_ALTERNATIVE_REFNO
--                                 from NUM (8) to VARCHAR2 (20) to match guide and DBase table
--                                 2)amended DL_HRM_SERVICE_REQUESTS field LSRQ_REPORTED_BY
--                                 from (40) to (60) to match DBase table
-- 14-MAR-2016   3.7  6.13   AJ    1)amended DL_HRM_WORKS_ORDERS field LWOV_ESTIMATED_COST and
--                                 LWOV_INVOICED_COST from NUM(8,2) to NUM(9,2) to match guide and DBase table
--                                 2)amended DL_HRM_WORKS_ORDER_VERSIONS field LWOV_ESTIMATED_COST and
--                                 LWOV_INVOICED_COST from NUM(8,2) to NUM(9,2) to match guide and DBase table
--                                 3)Removed DL_HRM_PP_CON_SITES and DL_HRM_CONTRACTORS
--                                 as installed using dl_hrm_cont_tab_new.sql
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
PR('DL_HRM_CON_SITE_PRICES','LCSPG_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRM_CON_SITE_PRICES','LCSPG_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRM_CON_SITE_PRICES','LCSPG_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRM_CON_SITE_PRICES','LCSPG_PPC_PPP_PPG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SITE_PRICES','LCSPG_PPC_PPP_WPR_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SITE_PRICES','LCSPG_PPC_PPP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SITE_PRICES','LCSPG_PPC_COS_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SITE_PRICES','LCSPG_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SITE_PRICES','LCSPG_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SITE_PRICES','LCSP_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SITE_PRICES','LCSP_PRICE','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SITE_PRICES','LCSP_PREFERRED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_SRQ_LEGACY_REFNO','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_LEGACY_REFNO','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_SEQNO','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_RAISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_TARGET_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_PRINTED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_HRV_ITY_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_HRV_IRN_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_PRI_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_PRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_DESCRIPTION','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_ISSUED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_COMPLETED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LINS_ALTERNATIVE_REFNO','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LIVI_SHORTNAME','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LIVI_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LIVI_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LIVI_VISIT_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LIVI_VISIT_DESCRIPTION','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LIVI_IRE_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRM_INSPECTIONS','LIVI_RESULT_DESCRIPTION','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_SRQ_LEGACY_REFNO','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_LWOR_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_ORDER_SEQNO','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_HRV_LIA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_HRV_TRD_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_QUANTITY','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_BHE_CODE','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_ESTIMATED_COST','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_ESTIMATED_TAX_AMOUNT','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_HRV_UOM_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_PRI_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_HRV_LOC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_DESCRIPTION','VARCHAR2','4000','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_LOCATION_NOTES','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_REPORTED_COMP_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_SYSTEM_COMP_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_TARGET_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_VCA_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_INVOICED_COST','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_INVOICED_TAX_AMOUNT','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_INVOICED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_HRV_JCL_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_COMMENTS','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_LIABILITY_TYPE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_COVERAGE_AMOUNT','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_REFNO','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_BUDGET_YEAR','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_WOV_VERSION_NO','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_JOBS','LJOB_DTS_DTY_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_DESCRIPTION','VARCHAR2','4000','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_PRE_INSPECT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_POST_INSPECT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_HRV_ITT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_REORDER_PERIOD_NO','NUMBER','2','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_REORDER_PERIOD_UNIT','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_WARRANTY_PERIOD_NO','NUMBER','2','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_WARRANTY_PERIOD_UNIT','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_KEYWORDS','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_PRI_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_HRV_VCA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_HRV_TRD_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_HRV_LIA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_ARC_SYS_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_HRV_UOM_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_WDC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_LIABILITY_TYPE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOP_PRICE','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOP_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_COVERAGE_AMOUNT','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_HRV_JCL_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_QUANTITY','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_HRV_LOC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_ARC_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_REPEAT_UNIT','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_REPEAT_PERIOD_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_HRM_ELEMENT_UPDATE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_HPM_ELEMENT_UPDATE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_ALLOW_BREAK_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_CODE_MLANG','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_DESCRIPTION_MLANG','VARCHAR2','4000','Y',l_t,l_s,l_f);
PR('DL_HRM_SCHEDULE_OF_RATES','LSOR_KEYWORDS_MLANG','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_LEGACY_REFNO','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_DESCRIPTION','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_SOURCE','VARCHAR2','7','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_RTR_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_RECHARGEABLE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_INSPECTION_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_WORKS_ORDER_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_PRINTED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_HRV_LOC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_PRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_TARGET_DATETIME','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_ACCESS_NOTES','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_LOCATION_NOTES','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_REPORTED_BY','VARCHAR2','60','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_REPORTED_DATETIME','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_PRI_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_ACCESS_AM','VARCHAR2','7','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_ACCESS_PM','VARCHAR2','7','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_ALTERNATIVE_REFNO','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_HRV_RBR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_HRV_RMT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_HRV_ACC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_HRV_CBY_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SERVICE_REQUESTS','LSRQ_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_SRQ_LEGACY_REFNO','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_SEQNO','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_PRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_PPC_PPP_PPG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_PPC_PPP_WPR_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_PREV_STATUS_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_PREV_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_CONFIRMATION_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_ALTERNATIVE_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_PPC_COS_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_PRINT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_TENANT_TICKET_PRINT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_REASSIGN_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_DEF_CONTR_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_AUTHORISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_ISSUED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_REPORTED_COMP_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_SYSTEM_COMP_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOR_HRV_CBY_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_PRI_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_HRV_VRE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_ACCESS_AM','VARCHAR2','7','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_ACCESS_PM','VARCHAR2','7','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_HRV_ACC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_ACCESS_NOTES','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_HRV_LOC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_LOCATION_NOTES','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_RTR_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_HELD_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_AUTHORISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_INVOICED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_ESTIMATED_COST','NUMBER','9,2','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_ESTIMATED_TAX_AMOUNT','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_INVOICED_COST','NUMBER','9,2','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_INVOICED_TAX_AMOUNT','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_RAISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_TARGET_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_SPR_PRINTER_NAME','VARCHAR2','80','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_CONTRACTOR_EXTRACT_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_FINANCIALS_EXTRACT_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_HRV_UST_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_DESCRIPTION','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_VERSION_NO','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDERS','LWOV_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_WOR_SRQ_LEGACY_REFNO','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_WOR_SEQNO','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_WOR_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_PRI_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_HRV_VRE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_ACCESS_AM','VARCHAR2','7','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_ACCESS_PM','VARCHAR2','7','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_HRV_ACC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_ACCESS_NOTES','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_HRV_LOC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_LOCATION_NOTES','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_RTR_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_HELD_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_AUTHORISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_INVOICED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_ESTIMATED_COST','NUMBER','9,2','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_ESTIMATED_TAX_AMOUNT','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_INVOICED_COST','NUMBER','9,2','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_INVOICED_TAX_AMOUNT','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_RAISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_TARGET_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_SPR_PRINTER_NAME','VARCHAR2','80','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_CONTRACTOR_EXTRACT_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_FINANCIALS_EXTRACT_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_HRV_UST_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_DESCRIPTION','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_VERSION_NO','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HRM_WORKS_ORDER_VERSIONS','LWOV_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRM_WORK_DESCRIPTIONS','LWDC_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRM_WORK_DESCRIPTIONS','LWDC_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRM_WORK_DESCRIPTIONS','LWDC_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRM_WORK_DESCRIPTIONS','LWDC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_WORK_DESCRIPTIONS','LWDC_DESCRIPTION','VARCHAR2','4000','Y',l_t,l_s,l_f);
PR('DL_HRM_WORK_DESCRIPTIONS','LWDC_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_WORK_DESCRIPTIONS','LWDC_CODE_MLANG','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_WORK_DESCRIPTIONS','LWDC_DESCRIPTION_MLANG','VARCHAR2','4000','Y',l_t,l_s,l_f);
--
-- This DL_DUMMY table must always be the last one as it doesn't get created
--
PR('DL_DUMMY','LDUMMY','VARCHAR2','1','N',l_t,l_s,l_f);
END;
/
