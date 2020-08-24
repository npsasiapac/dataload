--
-- dl_hpm_tab_new_Qaus_HPM_bespoke.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to ensure the Planned Maintenance Dataload Tables have
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
-- 05-FEB-2008   2.2  5.13.0 PH    Changed p_column_null = 'NOT NULL' to
--                                 'N'. Also added dummy table at the end as
--                                 new table will not get created if it's the
--                                 last one.
-- 14-MAR-2008   2.3  5.13.0 PH    Amended create table to exclude pct free
--                                 and storage options. Only uses tablespace.
-- 10-MAR-2010   3.0  6.1.1  PH    Added 8 additional dataload areas 
-- 06-DEC-2012   3.1  6.5.1  PJD   Amended PROCEDURE chk_col_def to include
--                                 section for DATE datatypes NULL/NOT NULL
-- 27-JAN-2013   3.2  6.5.1  PJD   Remove l_nullable = NULL from upd_col_def
-- 11-MAR-2016   3.3  6.13   MJK   Allowed for nullable being the same as current setting
--                                 in upd_col_def by removing l_nullable from statement
--                                 if p_nullable is passed as a null value as this is only
--                                 done if old and new nullable setting is the same value
--                                 otherwise its a Y or an N
-- 21-APR-2014   3.4  6.13.0 MJK   Added columns LCNT_AUTHORISED_BY, 
--                                 LDLV_DLV_AUTHORISED_BY and LDLV_DLV_ACTUAL_END_DATE
-- 19-OCT-2016   3.5  6.13   AJ    Added ltsk_refno to dl_hpm_tasks table
-- 22-FEB-2017   3.6  6.15   AJ    Added dl_hpm_task_payments for Queensland CR462
-- 23-FEB_2017   3.7  6.15   AJ    Further changes made to dl_hpm_task_payments added
--                                 added ltpy_tpm_bud_refno and ltpy_tpm_seqno to aid
--                                 delete
-- 27-FEB_2017   3.8  6.15   AJ    Further changes added ltpy_tsk_stk_code to dl_hpm_task_payments
-- 28-FEB-2017   3.9  6.15   AJ    Added dl_hpm_deliverable_val for Queensland CR462
-- 01-MAR-2017   4.0  6.15   AJ    Added the following to dl_hpm_deliverable_val
--                                 ldvl_amount, ldvl_comments, ldvl_refno, ldvl_bud_refno
--                                 ldvl_dlv_dcp_refno, ldvl_dlv_bhe_code, ldvl_bud_bca_year
-- 02-MAR-2017   4.1  6.15   AJ    1)Errors would not create deliverable_val table eventually
--                                 traced to ldvl_valued_datetime..amended to ldvl_valued_date
--                                 now will run and create table
--                                 2)Deliverables fields added to dl_hpm_deliverable_val table
-- 03-MAR-2017   4.2  6.15   AJ    Removed LDVL_DVE_HRV_LOC_CODE from dl_hpm_deliverable_val
--                                 as not required
-- 06-MAR-2017   4.3  6.15   AJ    Added QAus...tag to script name as bespoke
-- 12-APR-2017   4.4  6.15   AJ    Added LDVL_HRV_VTY_CODE (Deliverable valuation type)
--                                 to dl_hpm_deliverable_val table
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
l_nullable := NULL;
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
PR('DL_HPM_SURVEY_ADDRESSES','LSUD_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_ADDRESSES','LSUD_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_ADDRESSES','LSUD_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_ADDRESSES','LSUD_SCS_REFERENCE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_ADDRESSES','LSUD_PRO_AUN_CODE','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_ADDRESSES','LSUD_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_ADDRESSES','LSUD_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_ADDRESSES','LSUD_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_ADDRESSES','LSUD_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_ADDRESSES','LSUD_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_ADDRESSES','LSUD_TEXT','VARCHAR2','255','Y',l_t,l_s,l_f);
--
PR('DL_HPM_SURVEY_RESULTS','LSRT_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_SUD_SCS_REFERENCE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_SUD_PRO_AUN_CODE','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_SUD_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_HANDHELD_CREATED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_COPIED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_ASSESSMENT_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_CMPT_DISPLAY_SEQ','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_SUB_CMPT_DISPLAY_SEQ','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_MATERIAL_DISPLAY_SEQ','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_HRV_LOC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_FAT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_COMMENTS','VARCHAR2','255','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_QUANTITY','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_DATE_VALUE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_NUMERIC_VALUE','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_ESTIMATED_COST','NUMBER','12,2','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_UPLOAD_IPP_SHORTNAME','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_HRV_RET_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_HRV_PMU_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_HRV_SYA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_HRV_RUR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_SURVEY_RESULTS','LSRT_HRV_RCO_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
--
PR('DL_HPM_TASKS','LTSK_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTSK_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTSK_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTSK_TKG_SRC_REFERENCE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTSK_TKG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTSK_TKG_SRC_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTSK_TYPE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTSK_STK_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTSK_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTSK_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTSK_ALT_REFERENCE','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTSK_ACTUAL_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTSK_ID','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTVE_VERSION_NUMBER','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTVE_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTVE_DISPLAY_SEQUENCE','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTVE_HRV_TUS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTVE_BCA_YEAR','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTVE_VCA_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTVE_PLANNED_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTVE_NET_AMOUNT','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTVE_TAX_AMOUNT','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTVE_RETENTION_PERCENT','NUMBER','5,2','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTVE_RETENTION_PERIOD','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTVE_RETENTION_PERIOD_UNITS','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTVE_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTBA_BHE_CODE','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTBA_BCA_YEAR','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTBA_NET_AMOUNT','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTBA_TAX_AMOUNT','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_TASKS','LTSK_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HPM_TASK_GROUPS','LTKG_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HPM_TASK_GROUPS','LTKG_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HPM_TASK_GROUPS','LTKG_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HPM_TASK_GROUPS','LTKG_SRC_REFERENCE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_GROUPS','LTKG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_GROUPS','LTKG_SRC_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_GROUPS','LTKG_DESCRIPTION','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_GROUPS','LTKG_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_GROUPS','LTKG_GROUP_TYPE','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_GROUPS','LTKG_STM_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
--
PR('DL_HPM_CONTRACT_ADDRESSES','LCAD_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_ADDRESSES','LCAD_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_ADDRESSES','LCAD_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_ADDRESSES','LCAD_CAD_CNT_REFERENCE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_ADDRESSES','LCAD_CAD_PRO_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_ADDRESSES','LCAD_CAD_TYPE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_ADDRESSES','LCAD_CAI_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_ADDRESSES','LCAD_CAI_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_ADDRESSES','LCAD_CAI_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_ADDRESSES','LCAD_CAI_CSE_SECTION_NUMBER','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_ADDRESSES','LCAD_CAI_HRV_CAA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_ADDRESSES','LCAD_CAI_HRV_CAT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
--
PR('DL_HPM_CONTRACTS','LCNT_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCNT_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCNT_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCNT_REFERENCE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCNT_PRJ_REFERENCE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCNT_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCNT_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCNT_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCNT_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCNT_WARN_HRM_USERS_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCNT_DRAWINGS_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCNT_COS_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCNT_FILE_REF','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCNT_ALTERNATIVE_REFERENCE','VARCHAR2','60','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCNT_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCNT_RESCHEDULE_ALLOWED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_VERSION_NUMBER','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_DESCRIPTION','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_RPT_IN_PLANNED_WORK_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_BCA_YEAR','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_BHE_CODE','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_CNT_REF_ASSOCIATED_WITH','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_HRV_PYR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_ESTIMATED_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_ESTIMATED_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_PROJECTED_COST','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_PROJECTED_COST_TAX','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_CONTRACT_VALUE','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_MAX_VARIATION_AMOUNT','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_MAX_VARIATION_TAX_AMT','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_NON_COMP_DAMAGES_AMT','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_NON_COMP_DAMAGES_UNIT','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_LIABILITY_PERIOD','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_PENULT_RETENTION_PCT','NUMBER','5,2','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_INTERIM_RETENTION_PCT','NUMBER','5,2','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_INTERIM_PYMNT_INTERVAL','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_INTERIM_PYMNT_INT_UNIT','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_FINAL_MEASURE_PERIOD','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_MAX_NO_OF_REPEATS','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_REPEAT_PERIOD','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_REPEAT_PERIOD_UNIT','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_RETENTIONS_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVE_FINAL_MEASURE_PERIOD_UNIT','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVS_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVS_REPEAT_UNIT','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCVS_REPEAT_PERIOD_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCTT_HRV_CTP_CODE1','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCTT_HRV_CTP_CODE2','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCTT_HRV_CTP_CODE3','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACTS','LCTT_HRV_CTP_CODE4','VARCHAR2','10','Y',l_t,l_s,l_f);
--
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_TVE_TSK_TKG_SRC_REF','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_TVE_TSK_TKG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_TVE_TSK_TKG_SRC_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_TVE_VERSION_NUMBER','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_BCA_YEAR','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_BHE_CODE','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_DISPLAY_SEQUENCE','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_NET_AMOUNT','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_TAX_AMOUNT','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_TSK_ID','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTGC_HGL_SEGMENT1','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTGC_HGL_SEGMENT2','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTGC_HGL_SEGMENT3','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTGC_HGL_SEGMENT4','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTGC_HGL_SEGMENT5','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTGC_HGL_SEGMENT6','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTGC_HGL_SEGMENT7','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTGC_HGL_SEGMENT8','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTGC_HGL_SEGMENT9','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTGC_HGL_SEGMENT10','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_TSK_STK_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTGC_HGF_SEQNO','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_PAYMENT_TASK_DETS','LTBA_TVE_DISPLAY_SEQUENCE','NUMBER','3','Y',l_t,l_s,l_f);
--
PR('DL_HPM_DELIVERABLES','LDLV_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DLV_CNT_REFERENCE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DLV_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DLV_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DLV_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DLV_ACTUAL_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DLV_CAD_PRO_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DLV_CAD_TYPE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DVE_DISPLAY_SEQUENCE','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DVE_STD_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DVE_PLANNED_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DVE_ESTIMATED_COST','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DVE_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_BHE_CODE','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_BCA_YEAR','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DVE_VCA_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DVE_DESCRIPTION','VARCHAR2','4000','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DVE_QUANTITY','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DVE_HRV_PMU_CODE_QUANTITY','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DVE_UNIT_COST','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DVE_PROJECTED_COST','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DVE_HRV_LOC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLES','LDLV_DLV_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HPM_CONTRACT_SORS','LCVS_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_SORS','LCVS_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_SORS','LCVS_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_SORS','LCVS_CNT_REFERENCE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_SORS','LCVS_CVE_VERSION_NUMBER','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_SORS','LCVS_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_SORS','LCVS_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_SORS','LCVS_REPEAT_UNIT','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_SORS','LCVS_REPEAT_PERIOD_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_SORS','LCPC_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_SORS','LCPC_PRICE','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HPM_CONTRACT_SORS','LCPC_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
--
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_REFNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_CNT_REFERENCE','VARCHAR2','15','N',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_CAD_PRO_AUN_CODE','VARCHAR2','20','N',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_CAD_TYPE_IND','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DVE_STD_CODE','VARCHAR2','10','N',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_SCO_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_STATUS_DATE','DATE','  ','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_ACTUAL_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_AUTHORISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_PREV_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_PREV_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_VERSION_NUMBER','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_DISPLAY_SEQUENCE','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_REUSABLE_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_BHE_CODE','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_BCA_YEAR','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_VCA_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_DESCRIPTION','VARCHAR2','4000','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_QUANTITY','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_HRV_PMU_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_UNIT_COST','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_ESTIMATED_COST','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_PLANNED_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_PROJECTED_COST','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_CMPTS','LDCP_DCV_HRV_LOC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
--
PR('DL_HPM_TASK_PAYMENTS','LTPY_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_TSK_ALT_REFERENCE','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_TSK_TKG_SRC_REFERENCE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_TSK_TKG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_TSK_STK_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_TSK_TKG_SRC_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_TSK_ID','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_PAY_TYPE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_DUE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_TASK_NET_AMOUNT','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_TASK_TAX_AMOUNT','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_CREATED_BY','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_PAID_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_PAYMENT_ID','NUMBER','15','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_PAYMENT_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_TPM_BUD_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HPM_TASK_PAYMENTS','LTPY_TPM_SEQNO','NUMBER','4','Y',l_t,l_s,l_f);
--
PR('DL_HPM_DELIVERABLE_VAL','LDVL_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_TSK_TKG_SRC_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_TPY_PAY_TYPE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_TSK_ALT_REFERENCE','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_TSK_TKG_SRC_REFERENCE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_TSK_TKG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_TSK_STK_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_TPY_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_VALUED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_TPY_TASK_NET_AMOUNT','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_TPY_TASK_TAX_AMOUNT','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_CAD_PRO_AUN_CODE','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_CAD_TYPE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_DVE_DISPLAY_SEQUENCE','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_DVE_STD_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_DVE_ESTIMATED_COST','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_DLV_BHE_CODE','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_BUD_BCA_YEAR','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_DVE_QUANTITY','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_DVE_HRV_PMU_CODE_QUANTITY','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_DVE_UNIT_COST','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_DVE_PROJECTED_COST','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_HRV_VTY_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_TSK_ID','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_CREATED_BY','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_BUD_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_DLV_DCP_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_AMOUNT','NUMBER','14,2','Y',l_t,l_s,l_f);
PR('DL_HPM_DELIVERABLE_VAL','LDVL_PRO_REFNO_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
--
-- This DL_DUMMY table must always be the last one as it doesn't get created
--
PR('DL_DUMMY','LDUMMY','VARCHAR2','1','N',l_t,l_s,l_f);
--
END;
/
