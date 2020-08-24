--
-- hem_ltl_indexes.sql  **BESPOKE FOR LAND TITLES DL**
--
--------------------------------- Comment -------------------------------------
--
-- Script to create new PK2 Indexes on Dataload Tables
-- Most table will have PK1 but on batch_id then seqno
-- These are created on seqno then batch_id
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
--
-- 05-MAR-2008   1.0  5.13.0 PH    Initial Creation
-- 16-APR-2008   1.1  5.13.0 PH    Amended path of output to be
--                                 $PROD_HOME/dload
-- 23-APR-2008   1.2  5.13.0 PH    Corrected spool filename
-- 28-JAN-2013   1.3  6.1.1  PJD   Now uses Execute Immediate
-- 18-MAR-2016   1.4  6.14   AJ    Bespoke version for Land Titles Data Load
--                                 all three start with DL_LTL_LAND....
-------------------------------------------------------------------------------
--
--
set lines 200
--
SET SERVEROUT ON SIZE 1000000
DECLARE
--
CURSOR c_index_tablespace IS
SELECT distinct tablespace_name
FROM   user_indexes
WHERE  table_name = 'TENANCIES'
AND    tablespace_name IS NOT NULL;
--
CURSOR c_tab IS
SELECT t1.table_name     table_name,
       t1.column_name    seqno_col,
       t2.column_name    batch_col
FROM   user_tab_columns t1,
       user_tab_columns t2
WHERE  t1.table_name     = t2.table_name
AND    t1.table_name  like 'DL_LTL_LAND%'
AND    t1.column_name like '%DL_SEQNO'
AND    t2.column_name like '%DLB_BATCH_ID'
MINUS
SELECT i1.table_name,
       i1.column_name,
       i2.column_name
FROM   user_ind_columns i1,
       user_ind_columns i2
WHERE  i1.index_name      = i2.index_name
AND    i1.table_name      = i2.table_name
AND    i1.index_name   like 'PK%'
AND    i1.table_name   like 'DL_LTL_LAND%'
AND    i1.column_name  like '%DL_SEQNO'
AND    i2.column_name  like '%DLB_BATCH_ID'
AND    i1.column_position = 1
AND    i2.column_position = 2;
--
l_ind_tablespace  VARCHAR2(30);
l_string          VARCHAR2(500);
--
BEGIN
--
l_ind_tablespace  :=NULL;
--
  OPEN c_index_tablespace;
   FETCH c_index_tablespace INTO l_ind_tablespace;
  CLOSE c_index_tablespace ;
--
  for p1 in c_tab loop
--
l_string:=NULL;
--
l_string := 'CREATE UNIQUE INDEX PK2_'
               ||substr(p1.table_name,1,26)
               ||' ON '
               ||p1.table_name
               ||'('
               ||p1.seqno_col
               ||', '
               ||p1.batch_col
               ||') '
               ||' TABLESPACE '
               ||l_ind_tablespace;
--                ||';';
--
dbms_output.put_line(l_string);
EXECUTE IMMEDIATE l_string;
NULL;
--
END LOOP;
--
end;
/
set lines 80
