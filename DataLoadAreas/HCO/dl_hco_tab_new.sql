--
-- dl_hco_tab_new.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to ensure the Contractors and Repairs Contractors
-- Dataload Tables have the correct columns. This script does
-- not drop the dataload tables, just adds in any missing columns.
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 29-NOV-2006   1.0  5.9.0  PJD   Initial Creation.
-- 17-JUL-2007   2.0  5.12.0 PH    Added in any Repairs Contractors Tables.
--                                 Introduced into standard dataload install.
-- 20-JUL-2007   2.1  5.12.0 PH    New dataload table for Con SOR Products
-- 10-AUG-2007   2.2  5.12.0 PH    Corrected update statement for adding
--                                 DATE and INTEGER Columns in ins_col_def 
--                                 and upd_col_def. Amended to cater for where
--                                 update required and NOT NULL is correct as
--                                 this causes sql error
-- 05-FEB-2008   2.3  5.13.0 PH    Changed p_column_null = 'NOT NULL' to
--                                 'N'. Also added dummy table at the end as
--                                 new table will not get created if it's the
--                                 last one.
-- 04-MAR-2008   2.4  5.13.0 PH    New field for Operative Skills (preference)
-- 14-MAR-2008   2.5  5.13.0 PH    Amended create table to exclude pct free
--                                 and storage options. Only uses tablespace.
-- 13-JUN-2008   2.6  5.13.0 PH    Amended all dl_seqno to 10 from 8, also 
--                                 chanhed nullable flag for LSCSP_REFNO from
--                                 N to Y
-- 14-DEC-2009   2.7  5.15.1 PH    Amended l_nullable := NULL to be
--                                 l_nullable := 'NULL'
-- 06-DEC-2012   2.8  6.5.1  PJD   Amended PROCEDURE chk_col_def to include
--                                 section for DATE datatypes NULL/NOT NULL
-- 27-JAN-2013   3.0  6.5.1  PJD   Remove l_nullable = NULL from upd_col_def
-- 17-AUG-2015   4.1  6.11   AJ    added lprod_code_mlang, lprod_description_mlang and
--                                 lprod_short_desc_mlang fields to dl_hco_products table
-- 18-AUG-2015   4.2  6.11   AJ    added ldep_code_mlang and ldep_description_mlang to
--                                 the dl_hco_depots table
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
-- l_nullable := 'NULL';
--
IF nvl(p_nullable,'Y') = 'N'
THEN l_nullable := 'NOT NULL';
END IF;
--
BEGIN
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
                              ' '||  p_col_type||
                              ' ( '||p_col_length||
                              ' )            '||l_nullable  ||
                              ' )';
        DM('Issuing Alter Table Statement MODIFY VARCHAR2 or NUMBER ',l_debug);
       EXECUTE IMMEDIATE l_update_statement;  
  ELSE
    DM('Unknown Column type '||p_col_type,'Y');
  END IF;
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
PR('DL_HCO_COS_DEPOTS','LCDEP_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_COS_DEPOTS','LCDEP_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_COS_DEPOTS','LCDEP_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_COS_DEPOTS','LCDEP_COS_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HCO_COS_DEPOTS','LCDEP_DEP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_COS_DEPOTS','LCDEP_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_DEPOTS','LDEP_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_DEPOTS','LDEP_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_DEPOTS','LDEP_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_DEPOTS','LDEP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_DEPOTS','LDEP_DESCRIPTION','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HCO_DEPOTS','LDEP_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_DEPOTS','LDEP_DEP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_DEPOTS','LDEP_CODE_MLANG','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_DEPOTS','LDEP_DESCRIPTION_MLANG','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_IPP_SHORTNAME','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_OTGR_IPT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_OTGR_HRV_GRA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_DEFAULT_START_LOCN_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_DEFAULT_END_LOCN_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_MAX_WKLY_STD_WORKING_TIM','VARCHAR2','5','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_MAX_WKLY_OVERTIME','VARCHAR2','5','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_START_UNPAID_TRAV_TIME','VARCHAR2','5','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_END_UNPAID_TRAV_TIME','VARCHAR2','5','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_HOURLY_RATE','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_OVERTIME_RATE','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_MAX_TRAVEL_TIME','VARCHAR2','5','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_COST_PER_KM','NUMBER','8,3','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_DETAILS','LODET_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_SKILLS','LOSKL_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_SKILLS','LOSKL_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_SKILLS','LOSKL_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_SKILLS','LOSKL_IPP_SHORTNAME','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_SKILLS','LOSKL_IPT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_SKILLS','LOSKL_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_SKILLS','LOSKL_PROFICIENCY_PCT','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_SKILLS','LOSKL_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_SKILLS','LOSKL_PREFERENCE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_TYPE_GRADES','LOTGR_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_TYPE_GRADES','LOTGR_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_TYPE_GRADES','LOTGR_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_TYPE_GRADES','LOTGR_IPT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_TYPE_GRADES','LOTGR_HRV_GRA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_TYPE_GRADES','LOTGR_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_TYPE_GRADES','LOTGR_MAX_WKLY_STD_WORKING_TIM','VARCHAR2','5','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_TYPE_GRADES','LOTGR_MAX_WKLY_OVERTIME','VARCHAR2','5','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_TYPE_GRADES','LOTGR_DEFAULT_HOURLY_RATE','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HCO_OPERATIVE_TYPE_GRADES','LOTGR_DEFAULT_OVERTIME_RATE','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_DESCRIPTION','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_SHORT_DESC','VARCHAR2','100','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_TYPE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_REUSABLE_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_PROD_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_PDG_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_PDG_HRV_FPG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_PDG_HRV_FPSG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_HRV_MANU_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_HRV_UOM_BOUGHT_IN','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_HRV_UOM_ISSUED_IN','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_HAZARDOUS_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_MANUFACTURED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_SUPERCEDED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_QUALITY_REGNO','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_RECYCLABLE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_INSPECTION_REQD_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_SERVICE_RQD_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_SAFE_CLOTH_REQD_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_SPECIAL_INSTRUCTION','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_ABC_RANK','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_LAST_INVOICED_COST','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_CONTRACT_PRICE','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_HANDLING_OHEAD_AMT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_HANDLING_PERCENTAGE','NUMBER','5,2','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_ESTIMATED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_FRACTION_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_STOCKED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_HRV_TUN_LEAD_TIME','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_MINIMUM_LEAD_TIME','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_HRV_TUN_SHELF_LIFE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_SHELF_LIFE','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_CODE_MLANG','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_DESCRIPTION_MLANG','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HCO_PRODUCTS','LPROD_SHORT_DESC_MLANG','VARCHAR2','100','Y',l_t,l_s,l_f);
PR('DL_HCO_SOR_PRDT_SPECIFICATN','LSPSN_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_SOR_PRDT_SPECIFICATN','LSPSN_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_SOR_PRDT_SPECIFICATN','LSPSN_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_SOR_PRDT_SPECIFICATN','LSPSN_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_SOR_PRDT_SPECIFICATN','LSPSN_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_SOR_PRDT_SPECIFICATN','LSPSN_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_SOR_PRDT_SPECIFICATN','LSPRO_PROD_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_SOR_PRDT_SPECIFICATN','LSPRO_DEFAULT_QUANTITY','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HCO_SOR_PRDT_SPECIFICATN','LSPRO_HRV_UOM_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_SOR_PRDT_SPECIFICATN','LSPSN_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HCO_CON_SOR_PRODUCTS','LCSPH_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_CON_SOR_PRODUCTS','LCSPH_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_CON_SOR_PRODUCTS','LCSPH_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_CON_SOR_PRODUCTS','LCSPH_COS_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HCO_CON_SOR_PRODUCTS','LCSPH_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_CON_SOR_PRODUCTS','LCSPS_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_CON_SOR_PRODUCTS','LCSPS_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_CON_SOR_PRODUCTS','LCSPR_PROD_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_CON_SOR_PRODUCTS','LCSPR_DEFAULT_QUANTITY','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HCO_CON_SOR_PRODUCTS','LCSPR_HRV_UOM_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_LOCATION','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_DESCRIPTION','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_STRT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_CDEP_DEP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_CDEP_COS_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_COS_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_VEHICLE_REG','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_HRV_FFTY_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_HRV_VEHI_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_HRV_VMM_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_TAX_DUE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_INSURANCE_DUE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_INSURANCE_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_FIRST_REGISTERED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_CC','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_MOT_DUE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_SERVICE_DUE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_STORES','LSTO_SUSPENDED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_STORE_STOCK_ITEMS','LSSI_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_STORE_STOCK_ITEMS','LSSI_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_STORE_STOCK_ITEMS','LSSI_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_STORE_STOCK_ITEMS','LSSI_STO_LOCATION','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_STORE_STOCK_ITEMS','LSSI_PROD_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_STORE_STOCK_ITEMS','LSSI_QUANTITY','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HCO_STORE_STOCK_ITEMS','LSSI_REORDER_LEVEL','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HCO_STORE_STOCK_ITEMS','LSSI_REORDER_QUANTITY','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HCO_STORE_STOCK_ITEMS','LSSI_ON_ORDER_QUANTITY','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HCO_STORE_STOCK_ITEMS','LSSI_MINIMUM_QUANTITY','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HCO_STORE_STOCK_ITEMS','LSSI_MAXIMUM_QUANTITY','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HCO_STORE_STOCK_ITEMS','LSSI_IDEAL_QUANTITY','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HCO_STORE_STOCK_ITEMS','LSSI_RESERVED_QUANTITY','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HCO_STORE_STOCK_ITEMS','LSSI_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HCO_TEAMS','LTEA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_TEAMS','LTEA_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_TEAMS','LTEA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_TEAMS','LTEA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_TEAMS','LTEA_NAME','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HCO_TEAMS','LTEA_TYPE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_TEAMS','LTEA_LEVEL_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_TEAMS','LTEA_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_TEAMS','LTEA_CDEP_COS_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HCO_TEAMS','LTEA_CDEP_DEP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_TEAMS','LTEA_TEA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_TEAMS','LTEA_DEFAULT_UTILISATION_PCT','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HCO_TEAMS','LTEA_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HCO_VEHICLE_OPERATIVES','LVOP_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_VEHICLE_OPERATIVES','LVOP_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_VEHICLE_OPERATIVES','LVOP_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_VEHICLE_OPERATIVES','LVOP_STO_LOCATION','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_VEHICLE_OPERATIVES','LVOP_IPP_SHORTNAME','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_VEHICLE_OPERATIVES','LVOP_IPT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_VEHICLE_OPERATIVES','LVOP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_VEHICLE_OPERATIVES','LVOP_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HCO_VEHICLE_OPERATIVES','LVOP_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HCO_VEHICLE_OPERATIVES','LVOP_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_PPC_PPP_WPR_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_PPC_PPP_PPG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_PPC_PPP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_PPC_COS_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_CSP_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_ESTIMATED_EFFORT','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_ESTIMATED_EFFORT_UNIT','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_EFFORT_DRIVEN_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_MAX_OPERATIVES','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_NEXT_JOB_DELAY_TIME','NUMBER','11','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_NXT_JOB_DEL_TIME_UNIT','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_MIN_OPERATIVES','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_EFFORT','LCSEF_CSPG_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_EFFORT','LSEFF_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRM_SOR_EFFORT','LSEFF_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRM_SOR_EFFORT','LSEFF_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRM_SOR_EFFORT','LSEFF_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_EFFORT','LSEFF_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_EFFORT','LSEFF_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_EFFORT','LSEFF_ESTIMATED_EFFORT','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_EFFORT','LSEFF_ESTIMATED_EFFORT_UNIT','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_EFFORT','LSEFF_EFFORT_DRIVEN_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_EFFORT','LSEFF_MIN_OPERATIVES','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_EFFORT','LSEFF_MAX_OPERATIVES','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_EFFORT','LSEFF_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_EFFORT','LSEFF_NEXT_JOB_DELAY_TIME','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_EFFORT','LSEFF_NXT_JOB_DEL_TIME_UNIT','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_CMPT_SPECS','LSCSP_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRM_SOR_CMPT_SPECS','LSCSP_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRM_SOR_CMPT_SPECS','LSCSP_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRM_SOR_CMPT_SPECS','LSCSP_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_CMPT_SPECS','LSCSP_START_DATE','DATE',' ' ,'Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_CMPT_SPECS','LSCSP_END_DATE','DATE',' ' ,'Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_CMPT_SPECS','LSCSP_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_COMPONENTS','LSCMP_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRM_SOR_COMPONENTS','LSCMP_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRM_SOR_COMPONENTS','LSCMP_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRM_SOR_COMPONENTS','LSCMP_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_COMPONENTS','LSCMP_START_DATE','DATE',' ' ,'Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_COMPONENTS','LSCMP_SCMT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_COMPONENTS','LSCMP_COST','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRM_SOR_COMPONENTS','LSCMP_PERCENTAGE_IND','VARCHAR2','10','Y',l_t,l_s,l_f); 
PR('DL_HRM_CON_SOR_CMPT_SPECS','LCSCS_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_CMPT_SPECS','LCSCS_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_CMPT_SPECS','LCSCS_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_CMPT_SPECS','LCSPG_PPC_COS_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_CMPT_SPECS','LCSPG_PPC_PPP_PPG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_CMPT_SPECS','LCSPG_PPC_PPP_WPR_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_CMPT_SPECS','LCSPG_PPC_PPP_START_DATE','DATE',' ' ,'Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_CMPT_SPECS','LCSPG_START_DATE','DATE',' ' ,'Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_CMPT_SPECS','LCSCS_CSP_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_CMPT_SPECS','LCSCS_START_DATE','DATE',' ' ,'Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_CMPT_SPECS','LCSCS_END_DATE','DATE',' ' ,'Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_CMPT_SPECS','LCSCS_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_COMPONENTS','LCSCO_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_COMPONENTS','LCSCO_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_COMPONENTS','LCSCO_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_COMPONENTS','LCSPG_PPC_COS_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_COMPONENTS','LCSPG_PPC_PPP_PPG_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_COMPONENTS','LCSPG_PPC_PPP_WPR_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_COMPONENTS','LCSPG_PPC_PPP_START_DATE','DATE',' ' ,'Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_COMPONENTS','LCSPG_START_DATE','DATE',' ' ,'Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_COMPONENTS','LCSCS_CSP_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_COMPONENTS','LCSCS_START_DATE','DATE',' ' ,'Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_COMPONENTS','LCSCO_SCMT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_COMPONENTS','LCSCO_COST','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRM_CON_SOR_COMPONENTS','LCSCO_PERCENTAGE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_NON_ACCESS_EVENTS','LNAE_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_NON_ACCESS_EVENTS','LNAE_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_NON_ACCESS_EVENTS','LNAE_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_NON_ACCESS_EVENTS','LNAE_WOR_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HCO_NON_ACCESS_EVENTS','LNAE_APPOINTMENT_DATETIME','DATE',' ' ,'Y',l_t,l_s,l_f);
PR('DL_HCO_NON_ACCESS_EVENTS','LNAE_HRV_NAR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_NON_ACCESS_EVENTS','LNAE_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HCO_NON_ACCESS_EVENTS','LNAE_NOTIFY_OCCUPANT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_NON_ACCESS_EVENTS','LNAE_NOTIFIED_DATE','DATE',' ' ,'Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPES','LDTY_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPES','LDTY_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPES','LDTY_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPES','LDTY_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPES','LDTY_DESCRIPTION','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPES','LDTY_RTR_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPES','LDTY_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPES','LDTY_RECHARGEABLE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPES','LDTY_PRI_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPES','LDTY_REPEAT_PERIOD_UNIT','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPES','LDTY_REPEAT_PERIOD_NO','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPES','LDTY_WARRANTY_PERIOD_UNIT','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPES','LDTY_WARRANTY_PERIOD_NO','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPES','LDTY_REPEAT_WARNING_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPE_SORS','LDTS_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPE_SORS','LDTS_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPE_SORS','LDTS_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPE_SORS','LDTS_DTY_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPE_SORS','LDTS_SOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPE_SORS','LDTS_START_DATE','DATE',' ' ,'Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPE_SORS','LDTS_END_DATE','DATE',' ' ,'Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPE_SORS','LDTS_DEFAULT_ORDER_SEQNO','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HCO_DEFECT_TYPE_SORS','LDTS_DEFAULT_QUANTITY','VARCHAR2','9','Y',l_t,l_s,l_f);
PR('DL_HCO_LBG_GSC_LETTERS','LLGL_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HCO_LBG_GSC_LETTERS','LLGL_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HCO_LBG_GSC_LETTERS','LLGL_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HCO_LBG_GSC_LETTERS','LLGL_WOR_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HCO_LBG_GSC_LETTERS','LLGL_LETTER_NAME','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HCO_LBG_GSC_LETTERS','LLGL_RUNDATE','DATE',' ' ,'Y',l_t,l_s,l_f);
PR('DL_HCO_LBG_GSC_LETTERS','LLGL_USER','VARCHAR2','30','Y',l_t,l_s,l_f);

--
-- This DL_DUMMY table must always be the last one as it doesn't get created
--
PR('DL_DUMMY','LDUMMY','VARCHAR2','1','N',l_t,l_s,l_f);
END;
/