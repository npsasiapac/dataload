CREATE OR REPLACE PACKAGE BODY s_dl_hsc_lease_assignments
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER  WHO  WHEN         WHY
--      1.0  5.2.0   PH   10-JAN-2003  Initial Creation
--      1.1  5.4.0   PH   06-SEP-2003  Made sure Package Compiles okay
--      1.2  5.6.0   PJD  28-DEC-2004  Delete section now leaves record at status of C
--                                     if exception found.
--      2.0  5.13.0  PH   06-FEB-2008 Now includes its own 
--                                    set_record_status_flag procedure.
--      2.1  6.10.0  AJ   04-SEP-2015 Minor reformatting changes and
--                                    added las_reusable_refno on create
--
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hsc_lease_assignments
  SET llas_dl_load_status  = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_lease_assignments');
     RAISE;
  --
END set_record_status_flag;

--      
-- ***********************************************************************     
--
--  declare package variables and constants
--
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,llas_dlb_batch_id
,llas_dl_seqno
,llas_dl_load_status
,llas_lea_pro_propref
,llas_lea_start_date
,llas_start_date
,llas_end_date
,llas_correspond_name
FROM dl_hsc_lease_assignments
WHERE llas_dlb_batch_id    = p_batch_id
AND   llas_dl_load_status  = 'V';
--
-- *********************************
--
CURSOR c_pro_refno(p_propref varchar2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
-- *********************************
--
CURSOR get_reusable_refno(p_l_pro_refno    number
                         ,p_lea_start_date date
                         ,p_las_start_date date  ) IS
SELECT reusable_refno_seq.NEXTVAL
FROM dual
WHERE not exists(SELECT 'X'
                 FROM lease_assignments
                WHERE las_lea_pro_refno  = p_l_pro_refno
                  AND las_lea_start_date = p_lea_start_date
                  AND las_start_date     = p_las_start_date);
--
-- *********************************
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HSC_LEASE_ASSIGNMENTS';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
l_pro_refno number;
l_reusable_refno number;
i           integer := 0;
--
-- *********************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_lease_assignments.dataload_create');
fsc_utils.debug_message( 's_dl_hsc_lease_assignments.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.llas_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- get the pro_refno
--
l_pro_refno := null;
 --
OPEN  c_pro_refno(p1.llas_lea_pro_propref);
FETCH c_pro_refno INTO l_pro_refno;
CLOSE c_pro_refno;
--
-- get the reusable_refno
--
l_reusable_refno := null;
 --
OPEN  get_reusable_refno(l_pro_refno
                        ,p1.llas_lea_start_date
                        ,p1.llas_start_date);
FETCH get_reusable_refno INTO l_reusable_refno;
CLOSE get_reusable_refno;
--
--
      INSERT INTO lease_assignments
         (las_lea_pro_refno
         ,las_lea_start_date
         ,las_start_date
         ,las_created_by
         ,las_created_date
         ,las_end_date
         ,las_modified_by
         ,las_modified_date
         ,las_correspond_name
         ,las_reusable_refno
         )
      VALUES
         (l_pro_refno
         ,p1.llas_lea_start_date
         ,p1.llas_start_date
         ,'DATALOAD'
         ,trunc(sysdate)
         ,p1.llas_end_date
         ,null
         ,null
         ,p1.llas_correspond_name
         ,l_reusable_refno
         );
--
-- keep a count of the rows processed and commit after every 5000
--
i := i+1; IF MOD(i,5000)=0 THEN COMMIT; END If;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'C');
--
 EXCEPTION
   WHEN OTHERS THEN
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
 END;
--
END LOOP;
COMMIT;
--
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('lease_assignments');
--
fsc_utils.proc_end;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
     RAISE;
--
END dataload_create;
--
-- ***********************************************************************     
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,llas_dlb_batch_id
,llas_dl_seqno
,llas_dl_load_status
,llas_lea_pro_propref
,llas_lea_start_date
,llas_start_date
,llas_end_date
,llas_correspond_name
FROM dl_hsc_lease_assignments
WHERE llas_dlb_batch_id    = p_batch_id
AND   llas_dl_load_status in ('L','F','O');
--
-- *********************************
--
CURSOR c_pro_refno(p_propref varchar2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
-- *********************************
--
CURSOR c_lease(p_pro_refno  varchar2
              ,p_start_date date)    IS
SELECT 'X'
FROM   leases
WHERE  lea_pro_refno  = p_pro_refno
AND    lea_start_date = p_start_date;
--
-- *********************************
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HSC_LEASE_ASSIGNMENTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
-- *********************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_lease_assignments.dataload_validate');
fsc_utils.debug_message( 's_dl_hsc_lease_assignments.dataload_validate',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.llas_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- get the pro_refno to be used in later processing
--
l_pro_refno := NULL;
OPEN  c_pro_refno(p1.llas_lea_pro_propref);
FETCH c_pro_refno INTO l_pro_refno;
CLOSE c_pro_refno;
--
-- Check the Links to Other Tables
--
-- Check the property exists on properties
--
  if (not s_dl_hem_utils.exists_propref(p1.llas_lea_pro_propref))
  then
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
  end if;
--
-- Check there is an entry in leases for the Property and Date
--
  OPEN c_lease(l_pro_refno, p1.llas_lea_start_date);
   FETCH c_lease INTO l_exists;
    IF c_lease%NOTFOUND
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',579);
    END IF;
  CLOSE c_lease;
--
-- Check the other mandatory fields
-- Lease Start Date
--
  IF (p1.llas_lea_start_date is null)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',576);
  END IF;
--
-- Lease Assignment Start Date
--
  IF (p1.llas_start_date is null)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',580);
  END IF;
--
-- Check that if the end date is supplied it's after start date
--
  IF p1.llas_end_date is not null
   THEN
    IF (p1.llas_end_date <= p1.llas_start_date)
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',581);
    END IF;
  END IF;
--
-- Check the assignment start date is not before the lease start
--
    IF (p1.llas_start_date < p1.llas_lea_start_date)
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',582);
    END IF;
--
--
--
-- Now UPDATE the record count AND error code
IF l_errors = 'F' THEN
  l_error_ind := 'Y';
ELSE
  l_error_ind := 'N';
END IF;
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
set_record_status_flag(l_id,l_errors);
--
   EXCEPTION
      WHEN OTHERS THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
END;
--
END LOOP;
COMMIT;
--
fsc_utils.proc_END;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
-- ***********************************************************************     
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,llas_dlb_batch_id
,llas_dl_seqno
,llas_dl_load_status
,llas_lea_pro_propref
,llas_lea_start_date
,llas_start_date
,llas_end_date
,llas_correspond_name
FROM dl_hsc_lease_assignments
WHERE llas_dlb_batch_id    = p_batch_id
AND   llas_dl_load_status  = 'C';
--
-- *********************************
--
CURSOR c_pro_refno(p_propref varchar2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
-- *********************************
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HSC_LEASE_ASSIGNMENTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i integer := 0;
l_pro_refno number;
--
-- *********************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_lease_assignments.dataload_delete');
fsc_utils.debug_message( 's_dl_hsc_lease_assignments.dataload_delete',3 );
--
cb := p_batch_id;
cd := p_date;
--
-- s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.llas_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- get the pro_refno
--
l_pro_refno := null;
 --
  OPEN  c_pro_refno(p1.llas_lea_pro_propref);
   FETCH c_pro_refno INTO l_pro_refno;
  CLOSE c_pro_refno;
--
     DELETE FROM lease_assignments
     WHERE  las_lea_pro_refno  = l_pro_refno
     AND    las_lea_start_date = p1.llas_lea_start_date
     AND    las_start_date     = p1.llas_start_date;
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'V');
--
EXCEPTION
   WHEN OTHERS THEN
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'C');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
--
END LOOP;
--
COMMIT;
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('lease_assignments');
--
fsc_utils.proc_end;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_delete;
--
--
END s_dl_hsc_lease_assignments;
/

