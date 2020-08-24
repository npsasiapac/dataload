--
-- dl_hat_tab_new.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to ensure the Allocations Dataload Tables have
-- the correct columns. This script does not drop the
-- dataload tables, just adds in any missing columns.
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 29-NOV-2006   1.0  5.9.0  PJD   Initial Creation.
-- 17-JUL-2007   2.0  5.12.0 PH    Added in new fields for Applications and
--                                 Homeless Applications. Introduced into
--                                 standard dataload install.
-- 10-AUG-2007   2.1  5.12.0 PH    Corrected update statement for adding
--                                 DATE and INTEGER Columns in ins_col_def
--                                 and upd_col_def. Amended to cater for where
--                                 update required and NOT NULL is correct as
--                                 this causes sql error
-- 22-AUG-2007   2.2  5.12.0 PH    Corrected dl_seqno to be 8 where null.
-- 05-FEB-2008   2.3  5.13.0 PH    Changed p_column_null = 'NOT NULL' to
--                                 'N'. Also added dummy table at the end as
--                                 new table will not get created if it's the
--                                 last one.
-- 14-MAR-2008   2.4  5.13.0 PH    Amended create table to exclude pct free
--                                 and storage options. Only uses tablespace.
--  03-DEC-2009  2.5  5.15.1 MB    Added homeless instances, answers and stages
-- 14-DEC-2009   2.6  5.15.1 PH    Amended l_nullable := NULL to be
--                                 l_nullable := 'NULL'
-- 27-JUL-2010   2.7  6.1.1  MB    Addition of new fields to Involved Parties
-- 06-DEC-2012   2.8  6.5.1  PJD   Amended PROCEDURE chk_col_def to include
--                                 section for DATE datatypes NULL/NOT NULL
--                                 and removed l_nullable := 'NULL' clause
--                                 from upd_col_def procedure.
-- 23-Feb-2016   2.9  6.13   AJ    allowed for nullable being the same as current setting
--                                 in upd_col_def by removing l_nullable from statement
--                                 if p_nullable is passed as a null value as this is only
--                                 done if old and new nullable setting is the same value
--                                 otherwise its a Y or an N
-- 26-Feb-2016   3.0  6.13   AJ    Amended DL_HAT_APPLICATIONS fields LAPP_OFFER_FLAG,
--                                 LAPP_NOMINATION_FLAG, LAPP_RECEIVED_DATE, LAPP_CORR_NAME 
--                                 LAPP_SCO_CODE, LAPP_STATUS_DATE, LAPP_LEGACY_REF 
--                                 to make nullable as check moved
--                                 to validate in s_dl_hat_applications
--    **** GNB Bespoke for Allocations Migration *********
-- 05-Sept-2017  3.1  6.14   AJ    1)Added Change control as Bespoke fields for Applications
--                                 List Entries and List Entries History for GNB Bespoke
--                                 Migration changes done by MOK on the 23rd and 30th August 2017
--                                 2)Updated previous changes in dl_applic_list_entries table
-- 06-Sept-2017  3.2  6.14   AJ    LALE_DEFINED_HTY_CODE removed as calculated by HAT004
-- 19-Sept-2017  3.3  6.14   AJ    Added tables DL_HAT_APP_LEGACY_REF and DL_HAT_ALE_LEGACY_REF
-- 26-Sept-2017  3.4  6.14   AJ    1)In list Entry History LLEH_CATEGORY_SYS_GENERATED_IND altered
--                                 to LLEH_CATEGORY_SYS_GEN_IND as too long
-- 26-Sept-2017  3.5  6.14   AJ    Changed DL_HAT_APPLIC_LIST_ENTRY_HISTO to DL_HAT_APPLIC_LIST_ENT_HIST
-- 02-Oct-2017   3.6  6.14   AJ    Added LALR_DEL_LEGACY_REF to dl_hat_app_legacy_ref table
-- 02-Oct-2017   3.7  6.14   AJ    1)Added LALR_APP_TYPE to allow for
--                                 Other legacy refs other than Applications
--                                 2)Dropped table DL_HAT_ALE_LEGACY_REF as using the Applications
--                                 Legacy Table for both added fields for PK of list entries
-- 03-Oct-2017   3.8  6.14   AJ    1)Added LALR_UPDATED to dl_hat_app_legacy_ref table
-- 05-Oct-2017   3.9  6.14   AJ    1)Amended LALR_UPDATED to LALR_UPDATE_REQ
-- 25-Oct-2017   4.0  6.14   AJ    Amended dl_hat_general_answers added modified by and date
--                                 and made moved ques_no created by date and legacy ref fields check
--                                 if supplied from load process into validate froM
-- 02-Nov-2017   4.1  6.14   AJ    Amended dl_hat_lettings_area_answers added modified by and date
--                                 and made moved ques_no created by date and legacy ref fields check
--                                 if supplied from load process into validate from
-- 03-Jan-2018   4.2  6.14   AJ    Added dl_hat_answer_history
-- 04-Jan-2018   4.3  6.14   AJ    1) Removed the following as not required
--                                 lahs_ipa_refno  lahs_mrf_assessment_refno and 
--                                 lahs_hia_hin_instance_refno
-- 05-Jan-2018   4.4  6.14   AJ    Added modified data and BY and field to hold ipa_refno for the 
--                                 record that is created to DL_HAT_INVOLVED_PARTIES table
--                                 putting the mandatory check in data load package allowed null
--                                 records
-- 15+16/01/2018 4.5  6.14   AJ    Added DL_HAT_INVOLVED_PARTY_HIST bespoke for HNB migration
-- 23-Jan-2018   4.6  6.14   AJ    Added LIPA_LEGACY_REF, LIPA_DEL_PAR_REFNO, LIPA_HEAD_HHOLD_IND
--                                 and LIPA_HHOLD_GROUP_NO, LIPA_DEL_APP_REFNO, LIPA_DEL_CDE_REFNO
--                                 to Involved parties
-- 20-NOV-2018   4.7  6.18   PJD   Changed LAPP_CORR_NAME to length 250 
--                                 (in DL_HAT_APPLICATIONS and DL_HAT_HML_APPLICATIONS)
--                                 and LPAR_PER_FORENAME and LPAR_PER_SURNAME to--                                 length 50 in DL_HAT_INVOLVED_PARTIES
-- 19-DEC-2018   4.8  6.18   PJD   Made various column defs nullable in 
--                                 dl_hat_hml_applications as proper validation
--                                 now added to the dataload package.
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
PR('DL_HAT_APPLICATIONS','LAPP_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LAPP_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LAPP_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LAPP_OFFER_FLAG','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LAPP_NOMINATION_FLAG','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LAPP_RECEIVED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LAPP_CORR_NAME','VARCHAR2','250','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LAPP_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LAPP_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LAPP_RENT_ACCOUNT_DETAILS','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LAPP_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LTCY_ALT_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LAPP_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LAPP_HRV_FSSA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LAPP_ACAS_ALT_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LAPP_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLICATIONS','LAPP_BECAME_ACTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
--
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_RLI_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_LST_CODE','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_ALS_ACTIVE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_REGISTERED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_STATUS_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_STATUS_REVIEW_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_REREG_BY_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_BECAME_ACTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_REFUSALS_COUNT','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_ALA_HRV_APC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_HRV_LRQ_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_HRV_APS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_CURRENT_NOMINATION_COUNT','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_CURRENT_OFFER_COUNT','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_CATEGORY_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_APP_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_CHANGED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENTRIES','LALE_AMENDED_ONLINE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
--
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_RLI_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_TYPE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_LST_CODE','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_MODIFED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_ACTION_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_ALS_ACTIVE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_REGISTERED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_HTY_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_MODEL_HTY_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_STATUS_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_STATUS_REVIEW_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_REREG_BY_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_CPR_PRI','NUMBER','2','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_BECAME_ACTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_APPLICATION_CATEGORY','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_LIST_REASON_QUALIFICATION','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_APPLICATION_STATUS_REASON','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_APP_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_CATEGORY_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_ENT_HIST','LLEH_CATEGORY_SYS_GEN_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
--
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LAPP_LEGACY_REF','VARCHAR2','20','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_ALE_RLI_CODE','VARCHAR2','10','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_RLS_CODE','VARCHAR2','8','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_SCO_CODE','VARCHAR2','3','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_STATUS_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_CREATED_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_CREATED_BY','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_DECISION_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_DECISION_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_AUTHORISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_AUTH_STATUS_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_AUTH_STATUS_REVIEW_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_HRV_APS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_HRV_SDR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_RSD_HRV_LSD_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_LIST_STAGE_DECIS','LALS_PROVISIONAL_LST_CODE','VARCHAR2','4','Y',l_t,l_s,l_f);
--
PR('DL_HAT_APPLIC_STATUSES','LALE_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_STATUSES','LALE_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_STATUSES','LALE_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_STATUSES','LALE_RLI_CODE','VARCHAR2','10','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_STATUSES','LALE_LST_CODE','VARCHAR2','4','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_STATUSES','LALE_ALS_ACTIVE_IND','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_STATUSES','LALE_STATUS_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_STATUSES','LALE_STATUS_REVIEW_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_STATUSES','LALE_HRV_APS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APPLIC_STATUSES','LAPP_LEGACY_REF','VARCHAR2','20','N',l_t,l_s,l_f);
--
PR('DL_HAT_ED_APP_LIST_ENTRIES','LEAE_DLB_BATCH_ID','VARCHAR2','10','N',l_t,l_s,l_f);
PR('DL_HAT_ED_APP_LIST_ENTRIES','LEAE_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_ED_APP_LIST_ENTRIES','LEAE_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_ED_APP_LIST_ENTRIES','LEAE_HRV_IAS_CODE','VARCHAR2','10','N',l_t,l_s,l_f);
PR('DL_HAT_ED_APP_LIST_ENTRIES','LEAE_VEI_VED_REFERENCE','VARCHAR2','10','N',l_t,l_s,l_f);
PR('DL_HAT_ED_APP_LIST_ENTRIES','LEAE_VEI_VIN_REFNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_ED_APP_LIST_ENTRIES','LEAE_ALE_APP_REFNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAT_ED_APP_LIST_ENTRIES','LEAE_CREATED_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HAT_ED_APP_LIST_ENTRIES','LEAE_CREATED_BY','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_ED_APP_LIST_ENTRIES','LEAE_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAT_ED_APP_LIST_ENTRIES','LEAE_SHOP_REF','VARCHAR2','9','Y',l_t,l_s,l_f);
PR('DL_HAT_ED_APP_LIST_ENTRIES','LEAE_FORM_NAME','VARCHAR2','40','Y',l_t,l_s,l_f);
--
PR('DL_HAT_GENERAL_ANSWERS','LGAN_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_GENERAL_ANSWERS','LGAN_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_GENERAL_ANSWERS','LGAN_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_GENERAL_ANSWERS','LGAN_QUE_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAT_GENERAL_ANSWERS','LGAN_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_GENERAL_ANSWERS','LGAN_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_GENERAL_ANSWERS','LGAN_DATE_VALUE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_GENERAL_ANSWERS','LGAN_NUMBER_VALUE','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HAT_GENERAL_ANSWERS','LGAN_CHAR_VALUE','VARCHAR2','11','Y',l_t,l_s,l_f);
PR('DL_HAT_GENERAL_ANSWERS','LGAN_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_GENERAL_ANSWERS','LGAN_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_GENERAL_ANSWERS','LGAN_QOR_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HAT_GENERAL_ANSWERS','LGAN_OTHER_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HAT_GENERAL_ANSWERS','LGAN_OTHER_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_GENERAL_ANSWERS','LGAN_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAT_GENERAL_ANSWERS','LAPP_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
--
PR('DL_HAT_HML_APPLICATIONS','LAPP_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_OFFER_FLAG','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_NOMINATION_FLAG','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_RECEIVED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_CORR_NAME','VARCHAR2','250','Y',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_EXPECTED_HLESS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LIPT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_HRV_HCR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_HRV_HOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_RENT_ACCOUNT_DETAILS','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LTCY_ALT_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_HRV_FSSA_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_HML_APPLICATIONS','LAPP_ACAS_ALT_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
--
PR('DL_HAT_INVOLVED_PARTIES','LIPA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_JOINT_APPL_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_LIVING_APART_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_REHOUSE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_MAIN_APPLICANT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_GROUPNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_ACT_ROOMNO','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_HRV_HPER_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_HRV_REL_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_TYPE','VARCHAR2','5','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_SURNAME','VARCHAR2','50','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_FORENAME','VARCHAR2','50','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_INITIALS','VARCHAR2','5','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_TITLE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_DATE_OF_BIRTH','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_HOU_HRV_HMS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_HOU_HRV_HGO_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_UPD_INS_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_HOU_OAP_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_HOU_DISABLED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_FRV_FGE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PHONE_NO','VARCHAR2','100','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_FRV_FEO_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_FRV_FNL_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_NI_NO','VARCHAR2','9','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_HOU_SURNAME_PREFIX','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LAPP_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_HOU_AT_RISK_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_HOU_HRV_NTLY_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_HOU_HRV_SEXO_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_HOU_HRV_RLGN_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LPAR_PER_HOU_HRV_ECST_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_LEGACY_REF','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_HEAD_HHOLD_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_HHOLD_GROUP_NO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_DEL_IPA_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_DEL_PAR_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_DEL_APP_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTIES','LIPA_DEL_CDE_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HAT_INVOLVED_PARTY_ANSWERS','LIPN_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_ANSWERS','LIPN_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_ANSWERS','LIPN_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_ANSWERS','LPAR_PER_ALT_REF','VARCHAR2','20','N',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_ANSWERS','LAPP_LEGACY_REF','VARCHAR2','20','N',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_ANSWERS','LIPN_QUE_REFNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_ANSWERS','LIPN_CREATED_BY','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_ANSWERS','LIPN_CREATED_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_ANSWERS','LIPN_DATE_VALUE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_ANSWERS','LIPN_NUMBER_VALUE','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_ANSWERS','LIPN_CHAR_VALUE','VARCHAR2','11','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_ANSWERS','LIPN_OTHER_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_ANSWERS','LIPN_OTHER_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_ANSWERS','LIPN_QOR_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_ANSWERS','LIPN_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
--
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_LAR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LAPP_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_QUE_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_DATE_VALUE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_NUMBER_VALUE','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_CHAR_VALUE','VARCHAR2','11','Y',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_QOR_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_OTHER_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_OTHER_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAT_LETTINGS_AREA_ANSWERS','LLAA_APP_REFNO','NUMBER','8','Y',l_t,l_s,l_f);
--
PR('DL_HAT_MEDICAL_ANSWERS','LMAN_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_ANSWERS','LMAN_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_ANSWERS','LMAN_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_ANSWERS','LMAN_QUE_REFNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_ANSWERS','LMAN_CREATED_BY','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_ANSWERS','LMAN_CREATED_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_ANSWERS','LMAN_DATE_VALUE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_ANSWERS','LMAN_NUMBER_VALUE','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_ANSWERS','LMAN_CHAR_VALUE','VARCHAR2','11','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_ANSWERS','LMAN_QOR_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_ANSWERS','LMAN_OTHER_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_ANSWERS','LMAN_OTHER_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_ANSWERS','LMAN_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_ANSWERS','LAPP_LEGACY_REF','VARCHAR2','20','N',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_ANSWERS','LMRF_REFERRAL_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_ANSWERS','LMAN_MRF_ASSESSMENT_REFNO','VARCHAR2','10','Y',l_t,l_s,l_f);
--
PR('DL_HAT_MEDICAL_REFERRALS','LMRF_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_REFERRALS','LMRF_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_REFERRALS','LMRF_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_REFERRALS','LPAR_PER_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_REFERRALS','LMRF_REFERRAL_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_REFERRALS','LMRF_STATUS_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_REFERRALS','LMRF_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_REFERRALS','LMRF_ASSESSMENT_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_REFERRALS','LMRF_AWARD_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_REFERRALS','LMRF_COMMENTS','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_REFERRALS','LMRF_HRV_MRR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_REFERRALS','LAPP_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_MEDICAL_REFERRALS','LMRF_ASSESMENT_LEGACY_REF','VARCHAR2','10','Y',l_t,l_s,l_f);
--
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHIN_INSTANCE_REFNO','VARCHAR2','15','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_RLS_CODE','VARCHAR2','8','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_SCO_CODE','VARCHAR2','3','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_STATUS_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_CREATED_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_CREATED_BY','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_DECISION_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_DECISION_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_AUTHORISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_AUTH_STATUS_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_AUTH_STATUS_REVIEW_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_HRV_APS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_HRV_SDR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_RSD_HRV_LSD_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_STAGE_DECIS','LHID_PROVISIONAL_LST_CODE','VARCHAR2','4','Y',l_t,l_s,l_f);
--
PR('DL_HAT_HLESS_INS_ANSWERS','LHIA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_ANSWERS','LHIA_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_ANSWERS','LHIA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_ANSWERS','LHIN_INSTANCE_REFNO','VARCHAR2','15','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_ANSWERS','LHIA_QUE_REFNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_ANSWERS','LHIA_CREATED_BY','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_ANSWERS','LHIA_CREATED_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_ANSWERS','LHIA_DATE_VALUE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_ANSWERS','LHIA_NUMBER_VALUE','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_ANSWERS','LHIA_CHAR_VALUE','VARCHAR2','11','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_ANSWERS','LHIA_QOR_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_ANSWERS','LHIA_OTHER_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_ANSWERS','LHIA_OTHER_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INS_ANSWERS','LHIA_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
--
PR('DL_HAT_HLESS_INSTANCES','LHIN_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INSTANCES','LHIN_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INSTANCES','LHIN_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INSTANCES','LAPP_LEGACY_REF','VARCHAR2','20','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INSTANCES','LHIN_ALE_RLI_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INSTANCES','LHIN_INSTANCE_REFNO','VARCHAR2','15','N',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INSTANCES','LHIN_EXPECTED_HLESS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INSTANCES','LHIN_PRESENTED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INSTANCES','LHIN_ACCEPTED_HLESS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INSTANCES','LIPT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INSTANCES','LHIN_HRV_HCR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INSTANCES','LHIN_HRV_HOR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INSTANCES','LHIN_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INSTANCES','LHIN_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_HLESS_INSTANCES','LHIN_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
--
PR('DL_HAT_APP_LEGACY_REF','LALR_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_APP_LEGACY_REF','LALR_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_APP_LEGACY_REF','LALR_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_APP_LEGACY_REF','LALR_UPDATE_REQ','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_APP_LEGACY_REF','LALR_APP_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAT_APP_LEGACY_REF','LALR_APP_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APP_LEGACY_REF','LALR_APP_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_APP_LEGACY_REF','LALR_ALE_RLI_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_APP_LEGACY_REF','LALR_ALE_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_APP_LEGACY_REF','LALR_DEL_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_APP_LEGACY_REF','LALR_DEL_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
--
PR('DL_HAT_ANSWER_HISTORY','LAHS_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_REC_TYPE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_APP_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_QUE_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_LAR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_ACTION_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_DATE_VALUE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_NUMBER_VALUE','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_CHAR_VALUE','VARCHAR2','11','Y',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_QOR_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_OTHER_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_OTHER_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_ANSWER_HISTORY','LAHS_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
--
--
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_APP_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_PAR_PER_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_IPA_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_IPA_LEGACY_REF','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_ACTION_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_JOINT_APPL_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_LIVING_APART_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_REHOUSE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_MAIN_APPLICANT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_GROUPNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_ACT_ROOMNO','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_FRV_END_REASON','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_FRV_RELATION','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_IPA_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_APP_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAT_INVOLVED_PARTY_HIST','LIPH_PAR_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
-- This DL_DUMMY table must always be the last one as it doesn't get created
--
PR('DL_DUMMY','LDUMMY','VARCHAR2','1','N',l_t,l_s,l_f);
END;
/
