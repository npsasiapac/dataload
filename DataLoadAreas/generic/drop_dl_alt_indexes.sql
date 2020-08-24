--
-- dl_indexes.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to create new Indexes on Dataload Tables for the alt ref
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
--
-- 28-MAR-2018   1.0  6.16.1 DLB   Initial Creation
--
--
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
AND    t1.table_name  like 'DL%'
AND    t1.column_name like '%DLB_BATCH_ID'
AND    t2.column_name like '%ALT_REF'
--MINUS
--SELECT i1.table_name,
--       i1.column_name,
--       i2.column_name
--FROM   user_ind_columns i1,
--       user_ind_columns i2
--WHERE  i1.index_name      = i2.index_name
--AND    i1.table_name      = i2.table_name
--AND    i1.index_name   like 'ALT%'
--AND    i1.table_name   like 'DL%'
--AND    i1.column_name  like '%DLB_BATCH_ID'
--AND    i2.column_name  like '%ALT_REF'
--AND    i1.column_position = 1
--AND    i2.column_position = 2
ORDER BY 1;

--
l_ind_tablespace  VARCHAR2(30);
l_string          VARCHAR2(500);
l_ind             VARCHAR2(40);
l_i               NUMBER :=0;
l_table           VARCHAR2(40);
--
BEGIN
--
l_ind_tablespace  :=NULL;
l_table  :='xxx';
--
  OPEN c_index_tablespace;
   FETCH c_index_tablespace INTO l_ind_tablespace;
  CLOSE c_index_tablespace ;
--
  for p1 in c_tab loop
--
--IF l_table != p1.table_name
--THEN
--   l_i := 1;
--ELSE
--   l_i := l_i + 1;
--END IF;

l_string:=NULL;
l_ind   := 'ALT'||'_'||substr(p1.table_name,1,26);
--
l_string := 'DROP INDEX '||l_ind;
--
dbms_output.put_line(l_string);
BEGIN
EXECUTE IMMEDIATE l_string;
dbms_output.put_line('INDEX DROPPED');
EXCEPTION
WHEN OTHERS
THEN
  DBMS_OUTPUT.PUT_LINE('No Index '||l_ind);
END;

l_ind   := 'ALT1'||'_'||substr(p1.table_name,1,26);
--
l_string := 'DROP INDEX '||l_ind;
--
dbms_output.put_line(l_string);
BEGIN
EXECUTE IMMEDIATE l_string;
dbms_output.put_line('INDEX DROPPED');
EXCEPTION
WHEN OTHERS
THEN
  DBMS_OUTPUT.PUT_LINE('No Index '||l_ind);
END;
l_ind   := 'ALT2'||'_'||substr(p1.table_name,1,26);
--
l_string := 'DROP INDEX '||l_ind;
--
dbms_output.put_line(l_string);
BEGIN
EXECUTE IMMEDIATE l_string;
dbms_output.put_line('INDEX DROPPED');
EXCEPTION
WHEN OTHERS
THEN
  DBMS_OUTPUT.PUT_LINE('No Index '||l_ind);
END;

l_ind   := 'ALT3'||'_'||substr(p1.table_name,1,26);
--
l_string := 'DROP INDEX '||l_ind;
--
dbms_output.put_line(l_string);
BEGIN
EXECUTE IMMEDIATE l_string;
dbms_output.put_line('INDEX DROPPED');
EXCEPTION
WHEN OTHERS
THEN
  DBMS_OUTPUT.PUT_LINE('No Index '||l_ind);
END;
--
--l_table := p1.table_name;

END LOOP;
--
end;
/
set lines 80
