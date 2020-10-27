SET HEADING OFF
SET PAGESIZE 0
SET LINES 200
SET FEEDBACK OFF
SET TERMOUT OFF

SELECT 'PROMPT DISABLE trigger '||owner||'.'||trigger_name||' on table '||table_name||chr(10)||'ALTER TRIGGER '||owner||'.'||trigger_name||' DISABLE;'
FROM   all_triggers
WHERE  status     = 'ENABLED'
AND    table_name = 'PARTIES'
AND    owner = 'FSC'
ORDER BY trigger_name

spool disable_fsc_par_triggers.sql
PROMPT SET FEEDBACK ON TERMOUT ON
/
spool off

SELECT 'PROMPT DISABLE trigger '||owner||'.'||trigger_name||' on table '||table_name||chr(10)||'ALTER TRIGGER '||owner||'.'||trigger_name||' DISABLE;'
FROM   all_triggers
WHERE  status     = 'ENABLED'
AND    table_name = 'PARTIES'
AND    owner = 'HOU'
ORDER BY trigger_name

PROMPT DISABLE trigger HOU.LOAS_BR_U
spool disable_hou_par_triggers.sql
PROMPT SET FEEDBACK ON TERMOUT ON
/
spool off

SELECT 'PROMPT ENABLE trigger '||owner||'.'||trigger_name||' on table '||table_name||chr(10)||'ALTER TRIGGER '||owner||'.'||trigger_name||' ENABLE;'
FROM   all_triggers
WHERE  status     = 'ENABLED'
AND    table_name = 'PARTIES'
AND    owner = 'FSC'
ORDER BY trigger_name

spool enable_fsc_par_triggers.sql
PROMPT SET FEEDBACK ON TERMOUT ON
/
spool off

SELECT 'PROMPT ENABLE trigger '||owner||'.'||trigger_name||' on table '||table_name||chr(10)||'ALTER TRIGGER '||owner||'.'||trigger_name||' ENABLE;'
FROM   all_triggers
WHERE  status     = 'ENABLED'
AND    table_name = 'PARTIES'
AND    owner = 'HOU'
ORDER BY trigger_name

spool enable_hou_par_triggers.sql
PROMPT SET FEEDBACK ON TERMOUT ON
/
spool off

