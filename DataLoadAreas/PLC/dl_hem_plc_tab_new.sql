--
-- dl_hra_sas_tab_new.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to ensure the Rents SAS Dataload Tables have
-- the correct columns. This script does not drop the
-- dataload tables, just adds in any missing columns.
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 10-JUL-2009   1.0  5.16.0 IR    Initial Creation.
-- 
-- 20-NOV-2009   2.0  5.16.0 VS    Change definition for DL_HEM_PLC_REQ_DATA_ITEM
--                                 to fix defect 2611. LPRDI_PRO_SEQ_REFERENCE
--                                 and LPRDI_PENDING_PROP_IND nolonger mandatory
--
-- 24-NOV-2009   3.0  5.16.0 VS    Add definition for DL_HEM_PLC_REQ_PROP_LINKS
--
-- 02-DEC-2009   4.0  5.16.0 VS    Defect 2784. This script does not handle the
--                                 setting of NULL/NOT NULL properly
--
-- 02-MAR-2010   5.0  5.16.0 VS    Change definition for DL_HEM_PLC_PROP_ACT_HIST
--                                 Remove column LPAH_HRV_PLMI_CODE.
--
-- 09-MAR-2010   6.0  5.16.0 VS    Change definition for DL_HEM_PLC_PROP_ACT_HIST
--                                 Remove column LPAH_PLRP_REFNO and add
--                                 LPAH_PLPR_REFNO, LPAH_PRO_SEQ_REFERENCE,
--                                 LPAH_PENDING_PROP_IND instead to derive the 
--                                 plrp_refno
--
-- 02-SEP-2015   7.0  6.12.0 AJ    DL_HEM_PLC_PROP_REQ_HIST commented out as no longer
--                                 available
--
-- 03-NOV-2015   7.1  6.12.0 VS    Added LPLPR_AUN_CODE to DL_HEM_PLC_PROP_REQUESTS
--
-- 22-MAR-2016   7.2  6.13.0 PAH   allowed for nullable being the same as current setting
--                                 in upd_col_def by removing l_nullable from statement
--                                 if p_nullable is passed as a null value as this is only
--                                 done if old and new nullable setting is the same value
--                                 otherwise its a Y or an N
-------------------------------------------------------------------------------
--
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
--
BEGIN
--
-- 
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_REFNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_CREATE_IND','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_PPRT_CODE','VARCHAR2','10','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_STATUS_CODE','VARCHAR2','3','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_STATUS_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_CREATED_BY','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_CREATED_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_PROJECT_NUMBER','VARCHAR2','24','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_TOTAL_BEDROOMS','NUMBER','3','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_TOTAL_PROPERTIES','NUMBER','6','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_HRV_PLMI_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_MILESTONE_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_PLPR_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_COMMENTS','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_REQUESTS','LPLPR_AUN_CODE','VARCHAR2','20','Y',l_t,l_s,l_f);
--
PR('DL_HEM_PLC_REQUEST_PROPS','LPLRP_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQUEST_PROPS','LPLRP_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQUEST_PROPS','LPLRP_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQUEST_PROPS','LPLRP_PLPR_REFNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQUEST_PROPS','LPLRP_PENDING_PROP_IND','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQUEST_PROPS','LPLRP_CREATED_BY','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQUEST_PROPS','LPLRP_CREATED_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQUEST_PROPS','LPLRP_SEQUENCE','NUMBER','6','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQUEST_PROPS','LPLRP_PROP_REFERENCE','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQUEST_PROPS','LPLRP_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQUEST_PROPS','LPLRP_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQUEST_PROPS','LPLRP_REFNO','NUMBER','10','N',l_t,l_s,l_f);
--
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_TYPE','VARCHAR2','3','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_PDAI_REFNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_CREATED_BY','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_CREATED_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_PLPR_REFNO','NUMBER','10','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_PRO_SEQ_REFERENCE','VARCHAR2','25','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_PENDING_PROP_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_CHAR_VALUE','VARCHAR2','2000','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_NUMBER_VALUE','NUMBER','20,5','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_DATE_VALUE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_ESTATES_UPDATED_IND','VARCHAR2','1','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_MODIFIED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_MODIFIED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_DATA_ITEMS','LPRDI_REFNO','NUMBER','10','N',l_t,l_s,l_f);
--
--PR('DL_HEM_PLC_PROP_REQ_HIST','LPPRH_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
--PR('DL_HEM_PLC_PROP_REQ_HIST','LPPRH_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
--PR('DL_HEM_PLC_PROP_REQ_HIST','LPPRH_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
--PR('DL_HEM_PLC_PROP_REQ_HIST','LPPRH_PLPR_REFNO','NUMBER','10','N',l_t,l_s,l_f);
--PR('DL_HEM_PLC_PROP_REQ_HIST','LPPRH_CREATED_BY','VARCHAR2','30','N',l_t,l_s,l_f);
--PR('DL_HEM_PLC_PROP_REQ_HIST','LPPRH_CREATED_DATE','DATE',' ','N',l_t,l_s,l_f);
--PR('DL_HEM_PLC_PROP_REQ_HIST','LPPRH_TOTAL_BEDROOMS','NUMBER','6','Y',l_t,l_s,l_f);
--PR('DL_HEM_PLC_PROP_REQ_HIST','LPPRH_TOTAL_PROPERTIES','NUMBER','6','Y',l_t,l_s,l_f);
--PR('DL_HEM_PLC_PROP_REQ_HIST','LPPRH_PROJECT_NUMBER','VARCHAR2','24','Y',l_t,l_s,l_f);
--PR('DL_HEM_PLC_PROP_REQ_HIST','LPPRH_HRV_PLMI_CODE','VARCHAR2','10','Y',l_t,l_s,l_f);
--PR('DL_HEM_PLC_PROP_REQ_HIST','LPPRH_MILESTONE_DATE','DATE',' ','Y',l_t,l_s,l_f);
--
PR('DL_HEM_PLC_PROP_ACT_HIST','LPAH_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_ACT_HIST','LPAH_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_ACT_HIST','LPAH_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_ACT_HIST','LPAH_PLPR_REFNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_ACT_HIST','LPAH_PRO_SEQ_REFERENCE','VARCHAR2','25','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_ACT_HIST','LPAH_PENDING_PROP_IND','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_ACT_HIST','LPAH_PLAC_CODE','VARCHAR2','8','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_ACT_HIST','LPAH_COMPLETED_IND','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_ACT_HIST','LPAH_CREATED_BY','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_ACT_HIST','LPAH_CREATED_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_ACT_HIST','LPAH_COMPLETED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_PROP_ACT_HIST','LPAH_COMPLETED_DATE','DATE',' ','Y',l_t,l_s,l_f);
--
PR('DL_HEM_PLC_REQ_ACT_HIST','LPRAH_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_ACT_HIST','LPRAH_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_ACT_HIST','LPRAH_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_ACT_HIST','LPRAH_PLPR_REFNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_ACT_HIST','LPRAH_PLAC_CODE','VARCHAR2','8','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_ACT_HIST','LPRAH_COMPLETED_IND','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_ACT_HIST','LPRAH_SCO_CODE_FROM','VARCHAR2','3','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_ACT_HIST','LPRAH_CREATED_BY','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_ACT_HIST','LPRAH_CREATED_DATE','DATE',' ','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_ACT_HIST','LPRAH_COMPLETED_BY','VARCHAR2','30','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_ACT_HIST','LPRAH_COMPLETED_DATE','DATE',' ','Y',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_ACT_HIST','LPRAH_SCO_CODE_TO','VARCHAR2','3','Y',l_t,l_s,l_f);
--
PR('DL_HEM_PLC_REQ_PROP_LINKS','LPRPL_DLB_BATCH_ID','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_PROP_LINKS','LPRPL_DL_SEQNO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_PROP_LINKS','LPRPL_DL_LOAD_STATUS','VARCHAR2','1','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_PROP_LINKS','LPRPL_PRO_PROPREF_FROM','VARCHAR2','25','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_PROP_LINKS','LPRPL_PLPR_REFNO_FROM','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_PROP_LINKS','LPRPL_PRO_PROPREF_TO','VARCHAR2','25','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_PROP_LINKS','LPRPL_PLPR_REFNO_TO','NUMBER','10','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_PROP_LINKS','LPRPL_CREATED_BY','VARCHAR2','30','N',l_t,l_s,l_f);
PR('DL_HEM_PLC_REQ_PROP_LINKS','LPRPL_CREATED_DATE','DATE',' ','N',l_t,l_s,l_f);
--
-- This DL_DUMMY table must always be the last one as it doesn't get created
--
PR('DL_DUMMY','LDUMMY','VARCHAR2','1','N',l_t,l_s,l_f);
END;
/