--
-- dl_hra_tab_new.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to ensure the Rents Dataload Tables have
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
-- 22-AUG-2007   2.2  5.12.0 PH    Added NCT AUN Links table.
-- 05-FEB-2008   2.3  5.13.0 PH    Changed p_column_null = 'NOT NULL' to
--                                 'N'. Also added dummy table at the end as
--                                 new table will not get created if it's the
--                                 last one.
-- 14-MAR-2008   2.4  5.13.0 PH    Amended create table to exclude pct free
--                                 and storage options. Only uses tablespace.
-- 14-DEC-2009   2.5  5.15.1 PH    Amended l_nullable := NULL to be
--                                 l_nullable := 'NULL'
-- 06-SEP-2011   2.6  6.1.0  MB    Added LTRA_EXT_DESCRIPTION to Transactions
-- 02-FEB-2012   2.7  6.1.0  MB    COrrection of field lengths for bank accounts
-- 06-DEC-2012   2.8  6.5.1  PJD   Amended PROCEDURE chk_col_def to include
--                                 section for DATE datatypes NULL/NOT NULL
-- 27-JAN-2013   3.0  6.1.1  PJD   Remove l_nullable = NULL from upd_col_def
-- 26-SEP-2013   3.1  6.7    PJD   Removed Arrears Dispute Fields 
-- 11-APR-2014   3.2  6.9    PJD   Includes improvement made to other similar
--                                 scripts when a field moves from mandatory
--                                 to non-mandatory
-- 27-JUL-2015   3.3  6.11   AJ    Added 2 x Multi Language fields to dl_hra_revenue_accounts 
-- 28-JUL-2015   3.4  6.11   AJ    Added 2 x Multi Language fields to dl_hra_payment_methods 
-- 23-FEB-2016   3.5  6.13   AJ    1) allowed for nullable being the same as current setting
--                                 in upd_col_def by removing l_nullable from statement
--                                 if p_nullable is passed as a null value as this is only
--                                 done if old and new nullable setting is the same value
--                                 otherwise its a Y or an N
--                                 2) removed cursor c1 from initial processing
-- 01-MAR-2016   3.6  6.13   AJ     table DL_HRA_PAYMENT_METHODS amended LPME_PCT_AMOUNT now (10,2)
-- 26-APR-2016   3.7  6.13   AJ    1) amended DL_HRA_ACCOUNT_ARREARS_ACTIONS field LACA_NOP_TEXT
--                                 from 2000 to 4000 to match notepad DB table
--                                 2) DL_HRA_PAYMENT_CONTRACTS field LPCT_PAY_REF amended
--                                 from number(10) to varchar(25) match revenue_accounts table
-- 09-MAY-2016   3.8  6.13   AJ    Added to DL_HRA_TRANSACTIONS fields LTRA_CLIN_INVOICE_REF
--                                 and LTRA_ALLOCATE_TO_CLIN
--
-------------------------------------------------------------------------------
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
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_BALANCE','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_RAC_ACCNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_TYPE','VARCHAR2','6','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_ARREARS_DISPUTE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_ARA_CODE','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_STATUS','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_HRV_ADL_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_EAC_EPO_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_CREATED_MODULE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_EXPIRY_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_NEXT_ACTION_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_AUTH_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_AUTH_USERNAME','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_PRINT_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_DEL_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_DEL_USERNAME','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_PRINT_USERNAME','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_NOP_TEXT','VARCHAR2','4000','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_ARREARS_ACTIONS','LACA_PAY_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_BALANCES','LABA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_BALANCES','LABA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_BALANCES','LABA_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_BALANCES','LABA_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_BALANCES','LABA_BALANCE','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_BALANCES','LABA_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_ACCOUNT_BALANCES','LABA_SUMMARISE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_ACC_PROP_LINKS','LAPL_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_ACC_PROP_LINKS','LAPL_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_ACC_PROP_LINKS','LAPL_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRA_ACC_PROP_LINKS','LAPL_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_ACC_PROP_LINKS','LAPL_TCY_ALT_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_ACC_PROP_LINKS','LAPL_PRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_BREAKDOWNS','LDBR_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_BREAKDOWNS','LDBR_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_BREAKDOWNS','LDBR_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_BREAKDOWNS','LDBR_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_BREAKDOWNS','LDBR_PRO_REFNO','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_BREAKDOWNS','LDBR_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_BREAKDOWNS','LDBR_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_BREAKDOWNS','LDBR_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_BREAKDOWNS','LDBR_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_BREAKDOWNS','LDBR_ELE_VALUE','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_BREAKDOWNS','LDBR_VCA_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_BREAKDOWNS','LDBR_PAR_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_DLB_BATCH_ID','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_DL_SEQNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_DL_LOAD_STATUS','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_TRA_REFNO','NUMBER','12','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_ELE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_AMOUNT','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_VCA_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_VAT_AMOUNT','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_ATT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_PRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_STR_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_SCP_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_SCP_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_TRA_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_TRT_CODE','VARCHAR2','3','Y',l_t,l_s,l_f);
PR('DL_HRA_DEBIT_DETAILS','LDDE_TRA_HDE_CLAIM_NO','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_NCT_AUN_LINKS','LNAL_DLB_BATCH_ID','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_NCT_AUN_LINKS','LNAL_DL_SEQNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HRA_NCT_AUN_LINKS','LNAL_DL_LOAD_STATUS','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_NCT_AUN_LINKS','LNAL_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_NCT_AUN_LINKS','LNAL_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_PARALLEL_RENTS','LPRE_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_PARALLEL_RENTS','LPRE_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_PARALLEL_RENTS','LPRE_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRA_PARALLEL_RENTS','LPRE_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_PARALLEL_RENTS','LPRE_GROSS_RENT','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_PARALLEL_RENTS','LPRE_BALANCE','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_PARALLEL_RENTS','LPRE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_CONTRACTS','LPCT_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_CONTRACTS','LPCT_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_CONTRACTS','LPCT_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_CONTRACTS','LPCT_REFNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_CONTRACTS','LPCT_PAY_REF','VARCHAR2','25','N',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_CONTRACTS','LPCT_PAR_PER_ALT_REF','VARCHAR2','25','N',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_CONTRACTS','LPCT_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_CONTRACTS','LPCT_STATUS','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_CONTRACTS','LPCT_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_CONTRACTS','LPCT_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_CONTRACTS','LPCT_AMOUNT','NUMBER','10,2','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_CONTRACTS','LPCT_PERCENTAGE','NUMBER','5,2','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_CONTRACTS','LPCT_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_CONTRACTS','LPCT_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_CONTRACTS','LPCT_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_PME_PMY_CODE','VARCHAR2','2','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_PME_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_PME_HRV_PPC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_PME_FIRST_DD_TAKEN_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_BDE_BANK_NAME','VARCHAR2','35','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_BAD_ACCOUNT_NO','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_BAD_ACCOUNT_NAME','VARCHAR2','35','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_BAD_SORT_CODE','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_BDE_BRANCH_NAME','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_BAD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_AUN_BAD_ACCOUNT_NO','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_SOURCE_ACC_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_BAD_PAR_PER_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_PCT_AMOUNT','NUMBER','10,2','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_PCT_PERCENTAGE','NUMBER','5,2','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_BDE_BTY_CODE','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_PME_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_PCT_PAR_PER_ALT_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_PCT_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_PCT_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_BDE_BANK_NAME_MLANG','VARCHAR2','35','Y',l_t,l_s,l_f);
PR('DL_HRA_PAYMENT_METHODS','LPME_BDE_BRANCH_NAME_MLANG','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_ACCNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_HRV_ATE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_CLASS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_RECOVERABLE_IND','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_VAT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_ARCHIVED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_DISPUTE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_VERIFY_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_RES_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_SUSPEND_STATEMENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_HB_NEW_ACC_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_MODEL_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_TCY_REFNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_CREATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_CREATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_REUSABLE_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_REPORT_PRO_REFNO','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_HRV_ADE_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_PAR_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_TCY_NEW_REFNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_CHECK_DIGIT','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_DEBIT_TO_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_REBATE','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_STATEMENT_TO_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_STATEMENT_BAL','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_ALT_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_END_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_HB_CLAIM_NO','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_TEXT','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_IPP_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_BHE_CODE','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_BUDGET_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_WOR_ORDNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_SRC_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_HDE_CLAIM_NO','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_PRO_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_REVIEW_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_VERIFY_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_VERIFY_COUNT','NUMBER','2','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_VERIFY_TEXT','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_MODEL_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_MODEL_COUNT','NUMBER','2','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_MODEL_TEXT','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_CREDIT_BUDGET_AUN','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_BAH_SEQNO_HBO','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_NEXT_BAL_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_TERMINATED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_REVIEW_CODE','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_ARREARS_TEXT','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_TERMINATED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_DCD_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_LAST_ABA_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_LAST_ABA_BALANCE','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_LCO_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_RTB_REF','VARCHAR2','15','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_S125_OFFER_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_LEASE_YEARS','NUMBER','5','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_INITIAL_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_HRV_INITRSN','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_REFERENCE_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_HRV_REFRSN','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_PREV_REPORT_PRO_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_EPO_CODE','VARCHAR2','8','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_PME_PMY_CODE','VARCHAR2','2','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_PME_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_PME_HRV_PPC_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_PME_FIRST_DD_TAKEN_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_BDE_BANK_NAME','VARCHAR2','35','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_BDE_BRANCH_NAME','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_BAD_ACCOUNT_NO','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_BAD_ACCOUNT_NAME','VARCHAR2','35','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_BAD_SORT_CODE','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_BAD_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_TCY_ALT_REF','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_AUN_BAD_ACCOUNT_NO','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_PAR_ORG_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_BAD_PAR_PER_ALT_REF','VARCHAR2','20','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_PCT_AMOUNT','NUMBER','8,2','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_PCT_PERCENTAGE','NUMBER','5,2','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_BDE_BTY_CODE','VARCHAR2','4','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_DAY_DUE','NUMBER','2','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_LAS_LEA_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_LAS_START_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_LAS_LEA_PRO_PROPREF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_BDE_BANK_NAME_MLANG','VARCHAR2','35','Y',l_t,l_s,l_f);
PR('DL_HRA_REVENUE_ACCOUNTS','LRAC_BDE_BRANCH_NAME_MLANG','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_REFNO','NUMBER','8','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_STATEMENT_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_TRT_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_TRS_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_PMY_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_BALANCE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_DR','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_CR','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_VAT_DR','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_VAT_CR','NUMBER','11,2','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_PAYMENT_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_HDE_CLAIM_NO','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_EXTERNAL_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_TEXT','VARCHAR2','240','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_BALANCE_YEAR','NUMBER','4','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_BALANCE_PERIOD','VARCHAR2','2','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_SUMMARISE_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_DEBIT_EFFECTIVE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_SUSP_PAY_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_EXT_DESCRIPTION','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_CLIN_INVOICE_REF','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HRA_TRANSACTIONS','LTRA_ALLOCATE_TO_CLIN','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HRA_VOID_SUMMARIES','LVOS_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HRA_VOID_SUMMARIES','LVOS_DL_LOAD_STATUS','VARCHAR2','10','N',l_t,l_s,l_f);
PR('DL_HRA_VOID_SUMMARIES','LVOS_DL_SEQNO','NUMBER','8','N',l_t,l_s,l_f);
PR('DL_HRA_VOID_SUMMARIES','LVOS_PROCESS','VARCHAR2','30','Y',l_t,l_s,l_f);
--
-- This DL_DUMMY table must always be the last one as it doesn't get created
--
PR('DL_DUMMY','LDUMMY','VARCHAR2','1','N',l_t,l_s,l_f);
END;
/

