column owner format a15
column object_name format a30
column object_type format a20
set pagesize 50

select owner, object_name, object_type
from dba_objects
where status='INVALID'
order by owner, object_type, object_name
/
