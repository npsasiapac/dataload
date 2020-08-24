SET HEADING OFF
SET PAGESIZE 0
SET FEEDBACK OFF
SET TERMOUT OFF

SELECT 'ALTER TRIGGER '||owner||'.'||trigger_name||' DISABLE;'
FROM   all_triggers
WHERE  status       = 'ENABLED'
AND    table_name   = 'ADMIN_UNIT_ELEMENTS'
AND    trigger_name != 'AUE_BR_I'

spool disable_aue_triggers.sql
/
spool off

SELECT 'ALTER TRIGGER '||owner||'.'||trigger_name||' ENABLE;'
FROM   all_triggers
WHERE  status       = 'ENABLED'
AND    table_name   = 'ADMIN_UNIT_ELEMENTS'
AND    trigger_name != 'AUE_BR_I'

spool enable_aue_triggers.sql
/
spool off
