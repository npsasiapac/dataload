set echo off verify off showmode off feedback off;
whenever sqlerror exit sql.sqlcode

begin
  if user != 'SYS' then
    raise_application_error(-20101, 'Must be logged in as the SYS user to run this script.');
  end if;
end;
/

define perfstat_password="perfstat"
define default_tablespace="SYSAUX"
define temporary_tablespace="TEMP"

SET SERVEROUTPUT ON
CREATE OR REPLACE PROCEDURE gather_stats
IS
BEGIN
   dbms_stats.gather_schema_stats(OWNNAME=>'HOU',OPTIONS=>'GATHER STALE',CASCADE=>TRUE,NO_INVALIDATE=>FALSE);
   dbms_stats.gather_schema_stats(OWNNAME=>'FSC',OPTIONS=>'GATHER STALE',CASCADE=>TRUE,NO_INVALIDATE=>FALSE);
END;
/

DECLARE
  X NUMBER;
BEGIN
  SYS.DBMS_JOB.SUBMIT
  ( job       => X
   ,what      => 'gather_stats;'
   ,next_date => SYSDATE+(5/(24*60))   -- 5 minutes
   ,interval  => 'SYSDATE+(5/(24*60))' -- 5 minutes
   ,no_parse  => FALSE
  );
  SYS.DBMS_OUTPUT.PUT_LINE('Job Number is: ' || to_char(x));
COMMIT;
END;
/

@?/rdbms/admin/spcreate

DECLARE
  X NUMBER;
BEGIN
  SYS.DBMS_JOB.SUBMIT
  ( job       => X
   ,what      => 'statspack.snap(i_snap_level=>7);'
   ,next_date => trunc(sysdate+1/24,'HH')
   ,interval  => 'SYSDATE+(15/(24*60))' -- 15 minutes
   ,no_parse  => TRUE
  );
  SYS.DBMS_OUTPUT.PUT_LINE('Job Number is: ' || to_char(x));
COMMIT;
END;
/

DECLARE
  X NUMBER;
BEGIN
  SYS.DBMS_JOB.SUBMIT
  ( job       => X
   ,what      => 'statspack.purge(i_num_days=>3,i_extended_purge=>TRUE);'
   ,next_date => trunc(sysdate+1)
   ,interval  => 'trunc(sysdate+1)'
   ,no_parse  => TRUE
  );
  SYS.DBMS_OUTPUT.PUT_LINE('Job Number is: ' || to_char(x));
COMMIT;
END;
/


