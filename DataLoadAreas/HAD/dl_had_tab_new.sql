--
-- dl_had_tab_new.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to ensure the Housing Advice Dataload Tables have
-- the correct columns. This script does not drop the
-- dataload tables, just adds in any missing columns.
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 23-OCT-2008   1.0  5.15.0 PH    Initial Creation.
--
-- 30-Nov-2009   2.0  5.15.0 VS    Change to ADVICE_CASE_HOUSING_OPTIONS table
--                                 to add revenue accounts payment reference. 
--                                 Required for defect id 2751 fix.
--
-- 11-Feb-2010   3.0  5.15.0 VS    Change to ADVICE_CASE_HOUSING_OPTIONS table
--                                 to add lacho_start_date and lacho_next_pay_period_start. 
--                                 Required for defect id 3501 fix.
--
-- 17-Feb-2010   4.0  5.15.0 VS    Change to REGISTERED_ADDRESS_LETTINGS table
--                                 to add lreal_ipt_code, required for defect id 3526 fix.
--
--                                 Change to REGISTERED_ADDRESSES table
--                                 to add lrega_ipt_code, to derive the ipp_refno correctly.
--
-- 08-Aug-2013   5.0  6.6    MK    REGISTERED_ADDRESSES and REGISTERED_ADDRESS_LETTINGS 
--                                 substnatially altered.
-- 29-Aug-2013   5.1  6.8    PJD   Remove IPP fields from Registered Addresses and 
--                                 Registered Address Lettings
-- 14-Jan-2014   5.2  6.9    PJD   Reinstated one of the fields previously commented out
--
-- 18-Dec-2015   5.3  6.11   AJ    added LACRS_ARSS_HRV_ARST_CODE to DL_HAD_ADVICE_CASES
--                                 and DL_HAD_ADVICE_CASE_REASONS which is the stage the
--                                 advice case reason is at
-- 23-Feb-2016   5.4  6.13   AJ    1) added LHOP_HRV_HPSR_CODE and LHOP_HRV_HPER_CODE to
--                                 DL_HAD_HOUSEHOLD_PEOPLE
--                                 2) allowed for nullable being the same as current setting
--                                 in upd_col_def by removing l_nullable from statement
--                                 if p_nullable is passed as a null value as this is only
--                                 done if old and new nullable setting is the same value
--                                 otherwise its a Y or an N
-- 24-Feb-2016   5.5  6.13   AJ    1) amended lhou_refno on dl_had_households to (8) from (10)
--                                 to match field length in households table
--                                 2) amended larce_acho_reference on dl_had_adv_rsn_casewrk_evts
--                                 to (30) from (20) to match field length in advice_case_housing_options
--                                 table as represents acho_alternative_reference
--                                 3) amended lbnd_acho_legacy_ref on dl_had_bonds to (30) from (20)
--                                 to match field length in advice_case_housing_options table as
--                                 represents acho_alternative_reference
--                                 4) amended lppyt_acho_legacy_ref on DL_HAD_PREVENTION_PAYMENTS
--                                 to (30) from (25) to match field length in advice_case_housing_options
--                                 table as represents acho_alternative_reference
--                                 5) amended LPPYT_PAR_PER_ALT_IND and LPPYT_LAND_PAR_PER_ALT_IND
--                                 to DL_HAD_PREVENTION_PAYMENTS
-- 25-FEB-2016   5.6  6.13   AJ    1) NOT NULL attribute removed from LREGA_INS_STREET_INDEX_CODE and
--                                 LREGA_AEL_STREET_INDEX_CODE in dl_had_registered_addresses table
--                                 2) LREGA_ADR_UPRN amended from NUMBER (20) to VARCHAR2 (12) to match
--                                 the addresses table where its loaded
--                                 3) amended LHOU_ACHO_ALTERNATE_REF on DL_HAD_HOUSEHOLDS
--                                 amended LACHO_LEGACY_REF on DL_HAD_ADVICE_CASE_HSG_OPTN
--                                 amended LACHH_ACHO_LEGACY_REF on DL_HAD_ADV_CASE_HSG_OPTN_HIS
--                                 amended LREAL_ACHO_LEGACY_REF on DL_HAD_REG_ADDRESS_LETTINGS
--                                 amended LACQR_ACHO_LEGACY_REF on DL_HAD_ADV_CASE_QUESTN_RESP   
--                                 to (30) from (25) to match field length in advice_case_housing_options
--                                 table as represents acho_alternative_reference
--                                 4) amended LACQR_NUMBER_VALUE on DL_HAD_ADV_CASE_QUESTN_RESP
--                                 to (11,2) from (8) to match field length in advice_case_questn_responses
-- 14-NOV-2017   5.7  6.14   MJK   Added 'LACPE_HEAD_HHOLD_IND'and 'LACPE_HHOLD_GROUP_NO' LHOP_HEAD_HHOLD_IND
-- 05-DEC-2017   5.8  6.14   MJK   Added 'LHOP_HEAD_HHOLD_IND'and 'LHOP_HHOLD_GROUP_NO' (Note added AJ 12Dec)
-- 09-NOV-2018   5.9  6.16   PJD   Made more columns non_mandatory on Household persons and household DL tables
-- 09-NOV-2018   6.0  6.16   PJD   Make LACAS_CORRESPONDENCE_NAME 250 chars in length
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
-- 
PR('DL_HAD_ADVICE_CASES','LACAS_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_ALTERNATE_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_APPROACH_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_CORRESPONDENCE_NAME','VARCHAR2','250','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_HOMELESS_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_HRV_ACAM_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_HRV_CWTP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_HRV_ACSP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_EXPECTED_HOMELESS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_START_TIME_AT_RECEPTION','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_END_TIME_AT_RECEPTION','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_CASE_OPENED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_PREV_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_PREV_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACAS_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACRS_ARSN_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACRS_MAIN_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACRS_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACRS_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACRS_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACRS_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACRS_OUTCOME_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACRS_PREV_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACRS_PREV_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACRS_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACRS_AUTHORISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASES','LACRS_ARSS_HRV_ARST_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
--
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_ACAS_ALTERNATE_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_ARSN_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_MAIN_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_OUTCOME_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_PREV_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_PREV_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_AUTHORISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LAROC_OUTC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LAROC_PRIMARY_OUTCOME_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LAROC_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LAROC_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LAROC_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LAROC_SEQNO','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_REASONS','LACRS_ARSS_HRV_ARST_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
--
PR('DL_HAD_ADVICE_CASE_PEOPLE','LACPE_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_PEOPLE','LACPE_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_PEOPLE','LACPE_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_PEOPLE','LACPE_ACAS_ALTERNATE_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_PEOPLE','LACPE_PAR_PER_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_PEOPLE','LACPE_CLIENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_PEOPLE','LACPE_JOINT_CLIENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_PEOPLE','LACPE_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_PEOPLE','LACPE_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_PEOPLE','LACPE_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_PEOPLE','LACPE_HRV_FRL_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_PEOPLE','LACPE_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_PEOPLE','LACPE_COMMENT','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_PEOPLE','LACPE_HEAD_HHOLD_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_PEOPLE','LACPE_HHOLD_GROUP_NO','NUMBER','4','Y',l_t,l_s,l_f);
--
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_ACAS_ALTERNATE_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_ARCS_ARSN_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_HOOP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_HODS_HRV_DEST_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_HOAU_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_SAS_IND','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_RAC_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_NEXT_PAY_PERIOD_START','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADVICE_CASE_HSG_OPTN','LACHO_REFERENCE','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_ACAS_ALTERNATE_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_ACRS_ARSN_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_ACET_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_EVENT_DATETIME','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_TEXT','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_EVENT_DIRECTION_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_CLIENT_INVOLVEMENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_DIRECT_INTERVENTION_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_DURATION','VARCHAR2','5','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_REVIEW_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_ACHO_REFERENCE','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_RSN_CASEWRK_EVTS','LARCE_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HAD_ADV_CASE_HSG_OPTN_HIS','LACHH_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_HSG_OPTN_HIS','LACHH_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_HSG_OPTN_HIS','LACHH_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_HSG_OPTN_HIS','LACHH_ACHO_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_HSG_OPTN_HIS','LACHH_HOAU_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_HSG_OPTN_HIS','LACHH_HODS_HRV_DEST_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_HSG_OPTN_HIS','LACHH_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_HSG_OPTN_HIS','LACHH_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_HSG_OPTN_HIS','LACHH_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_HSG_OPTN_HIS','LACHH_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HAD_BONDS','LBND_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_ACHO_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_REFERENCE','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_BOND_LODGEMENT_NUMBER','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_BOND_AMOUNT','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_CONTRIBUTION_AMOUNT','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_HRV_BOVR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_OVERRIDE_AMOUNT','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_OVERRIDE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_OVERRIDE_USERNAME','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_REFUND_AMOUNT','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_IPP_SHORTNAME','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_HRV_BCR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_CLAIM_AMOUNT','NUMBER','6,2','Y',l_t,l_s,l_f);
PR('DL_HAD_BONDS','LBND_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_LEGACY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_ADRE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_HRV_RAE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_PROPOSED_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
-- PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_IPP_SHORTNAME','VARCHAR2','10','Y',l_t,l_s,l_f);
-- PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_IPP_IPT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_ADR_FLAT','VARCHAR2','100','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_ADR_BUILDING','VARCHAR2','100','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_ADR_STREET_NUMBER','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_AEL_STREET_INDEX_CODE','VARCHAR2','12','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_AEL_STREET','VARCHAR2','100','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_AEL_AREA','VARCHAR2','50','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_AEL_TOWN','VARCHAR2','50','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_AEL_COUNTY','VARCHAR2','50','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_AEL_COUNTRY','VARCHAR2','50','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_AEL_POSTCODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_AEL_LOCAL_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_AEL_ABROAD_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_ADR_EASTINGS','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_ADR_NORTHINGS','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_ADR_UPRN','VARCHAR2','12','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_INS_STREET_INDEX_CODE','VARCHAR2','12','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_INS_ADR_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAD_REGISTERED_ADDRESSES','LREGA_INS_REGA_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_REGA_LEGACY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_REGA_ADRE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_REGA_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_REFERENCE','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_ACAS_ALTERNATE_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_PROPOSED_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_VISIT_DATETIME','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_ACHO_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
-- PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_IPP_SHORTNAME','VARCHAR2','10','Y',l_t,l_s,l_f);
-- PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_IPT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f); 
PR('DL_HAD_REG_ADDRESS_LETTINGS','LREAL_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_ACAS_ALTERNATE_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_CAQU_REFERENCE','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_CQRS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_BOOLEAN_VALUE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_TEXT_VALUE','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_DATE_VALUE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_NUMBER_VALUE','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_ADDITIONAL_RESPONSE','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_ACHO_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_ADV_CASE_QUESTN_RESP','LACQR_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
--
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_ACAS_ALTERNATE_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_PAYMENT_AMOUNT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_PAYMENT_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_PPTP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_PAYEE_TYPE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_HRV_HHPF_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_HRV_HPPM_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_SCO_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_STATUS_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_PAR_PER_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_PAR_ORG_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_ACPE_PAR_PER_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_LAND_PAR_PER_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_ALTERNATIVE_REFERENCE','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_TRA_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_TRA_TRT_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_TRA_TRS_CODE','VARCHAR2','5','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_AUTHORISED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_AUTHORISED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_HRV_HPCR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_REVIEW_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_ACHO_LEGACY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_IPP_SHORTNAME','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_PAR_PER_ALT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_PREVENTION_PAYMENTS','LPPYT_LAND_PAR_PER_ALT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
--
PR('DL_HAD_HOUSEHOLD_PEOPLE','LHOP_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLD_PEOPLE','LHOP_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLD_PEOPLE','LHOP_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLD_PEOPLE','LHOP_ACAS_ALTERNATE_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLD_PEOPLE','LHOP_ACHO_ALTERNATE_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLD_PEOPLE','LHOP_PAR_PER_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLD_PEOPLE','LHOP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLD_PEOPLE','LHOP_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLD_PEOPLE','LHOP_HRV_FRL_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLD_PEOPLE','LHOP_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLD_PEOPLE','LHOP_HRV_HPSR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLD_PEOPLE','LHOP_HRV_HPER_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLD_PEOPLE','LHOP_HEAD_HHOLD_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLD_PEOPLE','LHOP_HHOLD_GROUP_NO','VARCHAR2','8','Y',l_t,l_s,l_f);
--
PR('DL_HAD_HOUSEHOLDS','LHOU_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLDS','LHOU_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLDS','LHOU_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLDS','LHOU_ACAS_ALTERNATE_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLDS','LHOU_ACHO_ALTERNATE_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HAD_HOUSEHOLDS','LHOU_REFNO','NUMBER','8','Y',l_t,l_s,l_f);
--
-- This DL_DUMMY table must always be the last one as it doesn't get created
--
PR('DL_DUMMY','LDUMMY','VARCHAR2','1','N',l_t,l_s,l_f);
END;
/
