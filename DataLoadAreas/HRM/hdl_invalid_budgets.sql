--------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
--
-- 04-MAR-2008   1.0  6.10   AJ    Initial Creation for Data loads
-- 01-MAR-2016   1.1  6.13   AJ    Added Change Control and altered
--                                 object names so only looks for
--                                 HRM_BUDGETS data loads
--
--------------------------------------------------------------------
--
--
set pages 0
set feedback off
set lines 120
set trimspool on
spool hdl_invalid2.sql
select 'alter package '||object_name||' compile body;'
from user_objects
where (object_name like 'S_DL_HRM_BUDGET%'
       or object_name like 'DL_HRM_BUDGET%')
  and object_type = 'PACKAGE BODY'
  and status = 'INVALID'
/
spool off
set feedback on
@hdl_invalid2