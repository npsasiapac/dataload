set pages 0
set feedback off
set lines 120
set trimspool on
spool hdl_invalid2.sql
select 'alter package '||object_name||' compile body;'
from user_objects
where (object_name like 'S_DL%'
       or object_name like 'DL%')
  and object_type = 'PACKAGE BODY'
  and status = 'INVALID'
/
spool off
set feedback on
@hdl_invalid2