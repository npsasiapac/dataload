set echo off verify off showmode off feedback off;
whenever sqlerror exit sql.sqlcode

begin
  if user != 'SYS' then
    raise_application_error(-20101, 'Must be logged in as the SYS user to run this script.');
  end if;
end;
/

@@?/rdbms/admin/spdrop

DECLARE

CURSOR c_job IS
SELECT job
FROM   dba_jobs
WHERE  schema_user = 'PERFSTAT';

p_job c_job%ROWTYPE;

BEGIN

OPEN c_job;
FETCH c_job INTO p_job;
WHILE c_job%FOUND LOOP

   dbms_job.remove(p_job.job);

   COMMIT;

   FETCH c_job INTO p_job;
END LOOP;
CLOSE c_job;

END;
/

DECLARE

CURSOR c_job IS
SELECT job
FROM   dba_jobs
WHERE  what        = 'gather_stats;';

p_job c_job%ROWTYPE;

BEGIN

OPEN c_job;
FETCH c_job INTO p_job;
WHILE c_job%FOUND LOOP

   dbms_job.remove(p_job.job);

   COMMIT;

   FETCH c_job INTO p_job;
END LOOP;
CLOSE c_job;

END;
/

DROP PROCEDURE gather_stats
/


