--set echo off verify off showmode off feedback off;
SET SERVEROUTPUT ON
whenever sqlerror exit sql.sqlcode

begin
  if user != 'SYS' then
    raise_application_error(-20101, 'Must be logged in as the SYS user to run this script.');
  end if;
end;
/

DECLARE

CURSOR c_job IS
SELECT job
FROM   dba_jobs
WHERE  what        LIKE '%fwf_timed_events.generate_events%';

p_job c_job%ROWTYPE;

BEGIN

OPEN c_job;
FETCH c_job INTO p_job;
WHILE c_job%FOUND LOOP

   dbms_ijob.remove(p_job.job);
   SYS.DBMS_OUTPUT.PUT_LINE('Removed Job Number for generate_events is: ' || to_char(p_job.job));

   COMMIT;

   FETCH c_job INTO p_job;
END LOOP;
CLOSE c_job;

END;
/

DECLARE
   job_num number;
   nlsvar VARCHAR2(4000);
   envvar RAW(32);

BEGIN

   SELECT MAX(job) + 1
   INTO   job_num
   FROM   dba_jobs;

   SELECT nls_env
   ,      misc_env
   INTO   nlsvar
   ,      envvar
   FROM   dba_jobs
   WHERE  ROWNUM < 2
   AND    nls_env IS NOT NULL
   AND    misc_env IS NOT NULL;

   SYS.DBMS_IJOB.SUBMIT
   ( job       => job_num
    ,luser     => 'FDW'
    ,puser     => 'FSC'
    ,cuser     => 'FSC'
    ,next_date => TRUNC(SYSDATE+1/24,'HH') -- on the hour
    ,interval  => 'SYSDATE+(5/(24*60))'   -- run every 5 minutes
    ,broken    => FALSE
    ,what      => 'begin fwf_timed_events.generate_events; end; /* Timed Events */'
    ,nlsenv    => nlsvar
    ,env       => envvar
   );

   SYS.DBMS_OUTPUT.PUT_LINE('Created Job Number for generate_events is: ' || to_char(job_num));
   COMMIT;
END;
/

DECLARE

CURSOR c_job IS
SELECT job
FROM   dba_jobs
WHERE  what        LIKE '%begin fwf_dip.process_events%';

p_job c_job%ROWTYPE;

BEGIN

OPEN c_job;
FETCH c_job INTO p_job;
WHILE c_job%FOUND LOOP

   dbms_ijob.remove(p_job.job);
   SYS.DBMS_OUTPUT.PUT_LINE('Removed Job Number for process_events is: ' || to_char(p_job.job));

   COMMIT;

   FETCH c_job INTO p_job;
END LOOP;
CLOSE c_job;

END;
/

DECLARE
   job_num number;
   nlsvar VARCHAR2(4000);
   envvar RAW(32);

BEGIN

   SELECT MAX(job) + 1
   INTO   job_num
   FROM   dba_jobs;

   SELECT nls_env
   ,      misc_env
   INTO   nlsvar
   ,      envvar
   FROM   dba_jobs
   WHERE  ROWNUM < 2
   AND    nls_env IS NOT NULL
   AND    misc_env IS NOT NULL;

   SYS.DBMS_IJOB.SUBMIT
   ( job       => job_num
    ,luser     => 'FDW'
    ,puser     => 'FSC'
    ,cuser     => 'FSC'
    ,next_date => TRUNC(SYSDATE+1/24,'HH')+(10/(24*60)) -- 10 minutes past the hour
    ,interval  => 'SYSDATE+(5/(24*60))'   -- run every 5 minutes
    ,broken    => FALSE
    ,what      => 'begin fwf_dip.process_events; end; /* Event Processing */'
    ,nlsenv    => nlsvar
    ,env       => envvar
   );

   SYS.DBMS_OUTPUT.PUT_LINE('Created Job Number for generate_events is: ' || to_char(job_num));
   COMMIT;
END;
/

