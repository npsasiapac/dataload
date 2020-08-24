--
-- dl_hem_tab_new.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to ensure the Estates Dataload Tables have
-- the correct columns. This script does not drop the
-- dataload tables, just adds in any missing columns.
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 29-NOV-2006   1.0  5.9.0  PJD   Initial Creation.
-- 17-JUL-2007   2.0  5.12.0 PH    Added in new fields for People.
--                                 Introduced into standard dataload install. 
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
-- 08-MAY-2008   2.4  5.13.1 PH    Added LADR_UPRN for Addresses dataload
-- 06-APR-2009   2.5  5.15.1 PH    Added new fields for Landlord information
--                                 to addresses table.
-- 14-DEC-2009   2.6  5.15.1 PH    Amended l_nullable := NULL to be
--                                 l_nullable := 'NULL'
-- 08-JAn-2010   2.7  5.15.1 PH    New fields for Property Elements
--                                 (lpel_quantity)
-- 29-SEP-2011   2.8  6.1.1      MB    Addition of Property Landlords to the standard
--                                                                      dataload fold
-- 23-NOV-2012   2.9  6.1.1  MB    correction of nullable fields in Property Landlords
-- 06-DEC-2012   2.6  6.5.1  PJD   Amended PROCEDURE chk_col_def to include
--                                 section for DATE datatypes NULL/NOT NULL
-- 27-JAN-2013   3.0  6.1.1  PJD   Remove l_nullable = NULL from upd_col_def
-- 10-MAR-2014   3.1  6.9.0  AJ    Remove creation of dl_hem_interested_party_usages
--                                 table as bespoke dataload only
-- 17-JUN-2015   3.2  6.11   AJ    admin_units and bank_details mlang fields added
--                                 to DL_HEM_ADMIN_UNITS table
-- 08-FEB-2016   3.3  6.13   AJ    LPAR_ORG_CURRENT_IND added to dl_hem_people table
-- 08-FEB-2016   3.4  6.13   AJ    allowed for nullable being the same as current setting
--                                 in upd_col_def by removing l_nullable from statement
--                                 if p_nullable is passed as a null value as this is only
--                                 done if old and new nullable setting is the same value
--                                 otherwise its a Y or an N
-- 07-NOV-2017   3.5  6.14   AJ    Added to dl_hem_people LPAR_HOP_HEAD_HHOLD_IND and
--                                 LPAR_HOP_HHOLD_GROUP_NO new at this release and spaces
--                                 between tables to make it easier to read
-- 10-NOV-2017   3.6  6.14   AJ    Added to dl_hem_people LPAR_C_PAR_REFNO this will hold the
--                                 par_refno that was either created or updated to aid checking
-- 06-DEC-2017   3.7  6.14  AJ     Added the following to DL_HEM_PEOPLE
--                                 LPAR_CREATED_DATE      LPAR_CREATED_BY
--                                 LPAR_MODIFIED_DATE     LPAR_MODIFIED_BY
--                                 LPAR_PER_HOU_END_DATE  LPAR_PER_HOU_HRV_HPE_CODE
-- 20-NOV-2018   3.8  6.18  PJD    Increased Name field lengths for Forename, Surname and Other Name on People 
--                                 and on Correspondence Name on Tenancies
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
-- PR('DL_HEM_PROPERTIES','LPRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LADD_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LADD_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LADD_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_LEGACY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_AUT_FAO_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_AUT_FAR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LADR_FLAT','VARCHAR2','100','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LADR_BUILDING','VARCHAR2','100','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LADR_STREET_NUMBER','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAEL_STREET','VARCHAR2','100','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAEL_SUB_STREET1','VARCHAR2','50','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAEL_SUB_STREET2','VARCHAR2','50','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAEL_SUB_STREET3','VARCHAR2','50','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAEL_AREA','VARCHAR2','50','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAEL_TOWN','VARCHAR2','50','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAEL_COUNTY','VARCHAR2','50','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAEL_COUNTRY','VARCHAR2','50','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAEL_POSTCODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAEL_LOCAL_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAEL_ABROAD_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LADD_ADDL1','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LADD_ADDL2','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LADD_ADDL3','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAEL_STREET_INDEX_CODE','VARCHAR2','12','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_CONTACT_NAME','VARCHAR2','60','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LADR_EASTINGS','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LADR_NORTHINGS','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LADR_UPRN','VARCHAR2','12','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_LANDLORD_PAR_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_HRV_LLT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_HRV_AAT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_PTY_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_PROPERTY_SIZE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_FLOOR_LEVEL','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_HRV_ALR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_TENANCY_LEAVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_ARREARS_AMOUNT','NUMBER','6','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_STORAGE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_STORAGE_UNIT_COST','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HEM_ADDRESSES','LAUS_STORAGE_COST','NUMBER','6','Y',l_t,l_s,l_f);
--
PR('DL_HEM_ADMIN_GROUPINGS','LAGR_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_GROUPINGS','LAGR_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_GROUPINGS','LAGR_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_GROUPINGS','LAGR_AUN_CODE_PARENT','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_GROUPINGS','LAGR_AUN_CODE_CHILD','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_PROPERTIES','LAPR_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_PROPERTIES','LAPR_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_PROPERTIES','LAPR_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_PROPERTIES','LAPR_PRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_PROPERTIES','LAPR_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
--
PR('DL_HEM_ADMIN_UNITS','LAUN_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_NAME','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_AUY_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_TENANCY_WK_START','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_HB_PERIOD','NUMBER','2','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_ALT_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_COMMENTS','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_BDE_BANK_NAME','VARCHAR2','35','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_BDE_BRANCH_NAME','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_BAD_ACCOUNT_NO','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_BAD_ACCOUNT_NAME','VARCHAR2','35','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_BAD_SORT_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_BAD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_CODE_MLANG','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_NAME_MLANG','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_BDE_BANK_NAME_MLANG','VARCHAR2','35','Y',l_t,l_s,l_f);
PR('DL_HEM_ADMIN_UNITS','LAUN_BDE_BRANCH_NAME_MLANG','VARCHAR2','30','Y',l_t,l_s,l_f);
--
PR('DL_HEM_LINK_TENANCIES','LLTE_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_LINK_TENANCIES','LLTE_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_LINK_TENANCIES','LLTE_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_LINK_TENANCIES','LLTE_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_LINK_TENANCIES','LLTE_TCY_ALT_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HEM_LINK_TENANCIES','LLTE_TCY_ALT_REF_IS_FOR','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HEM_LINK_TENANCIES','LLTE_HRV_FTLR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_LINK_TENANCIES','LLTE_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
--
PR('DL_HEM_PEOPLE','LPAR_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_HOP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_SURNAME','VARCHAR2','50','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_TCY_ALT_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_FORENAME','VARCHAR2','50','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_HOP_HPSR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_TITLE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_INITIALS','VARCHAR2','5','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_DATE_OF_BIRTH','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_HOU_DISABLED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_HOU_OAP_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_FRV_FGE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_HOP_HRV_REL_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_HOU_EMPLOYER','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_HOU_HRV_HMS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PHONE','VARCHAR2','100','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_HOP_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_HOP_HPER_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_TCY_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_TIN_MAIN_TENANT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_TIN_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_TIN_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_TIN_HRV_TIR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_TIN_STAT_SUCCESSOR_IND','VARCHAR2','2','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_FRV_FEO_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_NI_NO','VARCHAR2','9','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_FRV_HGO_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_FRV_FNL_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_OTHER_NAME','VARCHAR2','100','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_HOU_SURNAME_PREFIX','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_HOU_LEGACY_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_IPP_SHORTNAME','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_IPP_PLACEMENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_IPP_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_IPP_IPT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_IPP_USR_USERNAME','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_IPP_SPR_PRINTER_NAME','VARCHAR2','80','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_IPP_COMMENTS','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_IPP_VCA_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_IPU_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_IPP_STAFF_ID','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_IPP_COS_CODE','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_IPP_HRV_FIT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_TYPE','VARCHAR2','5','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_ORG_SORT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_ORG_NAME','VARCHAR2','60','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_ORG_SHORT_NAME','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_ORG_FRV_OTY_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_HOU_AT_RISK_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_HOU_HRV_NTLY_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_HOU_HRV_SEXO_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_HOU_HRV_RLGN_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_HOU_HRV_ECST_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_ORG_CURRENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_HOP_HEAD_HHOLD_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_HOP_HHOLD_GROUP_NO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_HOU_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_PER_HOU_HRV_HPE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PEOPLE','LPAR_C_PAR_REFNO','NUMBER','8','Y',l_t,l_s,l_f);
--
PR('DL_HEM_PROPERTIES','LPRO_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_FRB','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_SCO_CODE','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_ORGANISATION_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_HRV_HOT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_HRV_HRS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_HRV_HBU_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_HRV_HLT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_PARENT_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_SALE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_SERVICE_PROP_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_ACQUIRED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_DEFECTS_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_ALT_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_LEASE_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_LEASE_REVIEW_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_CONSTRUCTION_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_PTV_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_HRV_PST_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_HRV_HMT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_MANAGEMENT_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_FREE_REFNO','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_FREE_NAME','VARCHAR2','40','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_PROP_STATUS','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_STATUS_START','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_ALLOW_PLACEMENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_DEBIT_TO_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_ON_DEBIT_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_REFNO','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_HOU_RESIDENTIAL_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_PHONE','VARCHAR2','100','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_AGENT_PAR_REFNO','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTIES','LPRO_PLD_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
--
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_PRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_ETY_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_START','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_ATTR_TYPE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_ATTY_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_VALUE','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_HRV_REPCAT','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_END','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_HRV_ELO_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_FAT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_TEXT','VARCHAR2','255','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_AUN_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_HRV_RCO_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_QUANTITY','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_TEXT_VALUE','VARCHAR2','255','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_REMAINING_LIFE','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_INSTALL_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_REPAIR_COST','NUMBER','9,2','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_ELEMENTS','LPEL_REPAIR_YEAR','NUMBER','4','Y',l_t,l_s,l_f);
--
PR('DL_HEM_PROPERTY_STATUSES','LHPS_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_STATUSES','LHPS_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_STATUSES','LHPS_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_STATUSES','LHPS_PRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_STATUSES','LHPS_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
--
PR('DL_HEM_TENANCIES','LTCY_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_ALT_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_TTY_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_ACT_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_CORRESPOND_NAME','VARCHAR2','250','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_HRV_TTYP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_HRV_TSO_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_ACT_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_NOTICE_GIVEN_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_NOTICE_REC_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_EXPECTED_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_RTB_RECEIVED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_RTB_ADMITTED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_RTB_HELD_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_RTB_WITHDRAWN_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_RTB_APP_EXPECTED_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_HRV_TST_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_HRV_TTR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_HRV_TNR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_HRV_RHR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_HRV_RWR_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_RTB_APP_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_PROPREF1','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_START_DATE1','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_END_DATE1','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_HRV_TTR_CODE1','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_PROPREF2','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_START_DATE2','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_END_DATE2','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_HRV_TTR_CODE2','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_PROPREF3','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_START_DATE3','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_END_DATE3','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_HRV_TTR_CODE3','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_PROPREF4','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_START_DATE4','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_END_DATE4','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_HRV_TTR_CODE4','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_PROPREF5','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_START_DATE5','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_END_DATE5','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_HRV_TTR_CODE5','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_PROPREF6','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_START_DATE6','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_END_DATE6','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_THO_HRV_TTR_CODE6','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_PHONE','VARCHAR2','100','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_REVIEW_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_HRV_TPT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_HRV_TSC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_HRV_TSE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_HRV_FTC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_PROPOSED_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_TENANCIES','LTCY_PERM_TEMP_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
--
PR('DL_HEM_PROPERTY_LANDLORDS','LPLD_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_LANDLORDS','LPLD_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_LANDLORDS','LPLD_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_LANDLORDS','LPLD_PRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_LANDLORDS','LPLD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_LANDLORDS','LPLD_PAR_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_LANDLORDS','LPLD_ALT_PAR_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_LANDLORDS','LPLD_PAY_LANDLORD_DIRECTLY_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_LANDLORDS','LPLD_LIVING_ABROAD_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_LANDLORDS','LPLD_AGENT_PAR_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_LANDLORDS','LPLD_AGENT_ALT_PAR_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_LANDLORDS','LPLD_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_LANDLORDS','LPLD_ALTERNATIVE_REFERENCE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_LANDLORDS','LPLD_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HEM_PROPERTY_LANDLORDS','LPLD_REFNO','NUMBER','10','N',l_t,l_s,l_f);
--
-- This DL_DUMMY table must always be the last one as it doesn't get created
--
PR('DL_DUMMY','LDUMMY','VARCHAR2','1','N',l_t,l_s,l_f);
END;
/
