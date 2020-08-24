CREATE OR REPLACE PACKAGE BODY s_dl_hsc_lease_parties
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER   WHO  WHEN         WHY
--      1.0  5.2.0    PH   10-JAN-2003  Initial Creation
--      1.1  5.4.0    PH   06-SEP-2003  Made sure Package Compiles okay
--      1.2  5.10.0   PH   11-AUG-2006  Added Batch Question for using
--                                      par_refno or par_per_alt_ref.
--                                      Amended Create, Validate and Delete.
--      2.0  5.13.0   PH   06-FEB-2008  Now includes its own 
--                                      set_record_status_flag procedure.
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
  UPDATE dl_hsc_lease_parties
  SET llpt_dl_load_status  = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_lease_parties');
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
,llpt_dlb_batch_id
,llpt_dl_seqno
,llpt_dl_load_status
,llpt_las_lea_pro_propref
,llpt_las_lea_start_date
,llpt_las_start_date
,llpt_par_alt_ref
,llpt_start_date
,llpt_end_date
FROM dl_hsc_lease_parties
WHERE llpt_dlb_batch_id    = p_batch_id
AND   llpt_dl_load_status  = 'V';
--
CURSOR c_pro_refno(p_propref varchar2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
CURSOR c_par_refno(p_par_alt_ref varchar2) IS
SELECT par_refno
FROM   parties
WHERE  par_per_alt_ref = p_par_alt_ref;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HSC_LEASE_PARTIES';
cs       INTEGER;
ce	 VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
l_pro_refno number;
l_par_refno number;
i           integer := 0;
l_answer     VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_lease_parties.dataload_create');
fsc_utils.debug_message( 's_dl_hsc_lease_parties.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the 'Using Par Refno in place of Alt Ref?'
--
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.llpt_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- get the pro_refno
--
l_pro_refno := null;
 --
  OPEN  c_pro_refno(p1.llpt_las_lea_pro_propref);
   FETCH c_pro_refno INTO l_pro_refno;
  CLOSE c_pro_refno;
--
-- get the par_refno Depending on Batch Answer
--
l_par_refno := null;
--
  IF l_answer = 'Y'
   THEN l_par_refno := to_number(p1.llpt_par_alt_ref);
  ELSE
--
  OPEN  c_par_refno(p1.llpt_par_alt_ref);
   FETCH c_par_refno INTO l_par_refno;
  CLOSE c_par_refno;
  END IF;
--
      INSERT INTO lease_parties
         (lpt_las_lea_pro_refno
         ,lpt_las_lea_start_date
         ,lpt_las_start_date
         ,lpt_par_refno
         ,lpt_start_date
         ,lpt_end_date
         )
      VALUES
         (l_pro_refno
         ,p1.llpt_las_lea_start_date
         ,p1.llpt_las_start_date
         ,l_par_refno
         ,p1.llpt_start_date
         ,p1.llpt_end_date
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('lease_parties');
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
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,llpt_dlb_batch_id
,llpt_dl_seqno
,llpt_dl_load_status
,llpt_las_lea_pro_propref
,llpt_las_lea_start_date
,llpt_las_start_date
,llpt_par_alt_ref
,llpt_start_date
,llpt_end_date
FROM dl_hsc_lease_parties
WHERE llpt_dlb_batch_id    = p_batch_id
AND   llpt_dl_load_status in ('L','F','O');
--
CURSOR c_pro_refno(p_propref varchar2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
CURSOR c_lease_assign(p_pro_refno      varchar2
                     ,p_lea_start_date date
                     ,p_las_start_date date)    IS
SELECT 'X'
FROM   lease_assignments
WHERE  las_lea_pro_refno  = p_pro_refno
AND    las_lea_start_date = p_lea_start_date
AND    las_start_date     = p_las_start_date;
--
CURSOR c_par_refno(p_par_alt_ref varchar2)    IS
SELECT 'X'
FROM   parties
WHERE  par_per_alt_ref = p_par_alt_ref;
--
CURSOR c_par_refno2(p_par_refno varchar2) IS
SELECT 'X'
FROM   parties
WHERE  par_refno       = to_number(p_par_refno);
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HSC_LEASE_PARTIES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_answer         VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_lease_parties.dataload_validate');
fsc_utils.debug_message( 's_dl_hsc_lease_parties.dataload_validate',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the 'Using Par Refno in place of Alt Ref?'
--
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.llpt_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- get the pro_refno to be used in later processing
--
l_pro_refno := NULL;
--
  OPEN  c_pro_refno(p1.llpt_las_lea_pro_propref);
   FETCH c_pro_refno INTO l_pro_refno;
  CLOSE c_pro_refno;
--
-- Check the Links to Other Tables
--
-- Check the property exists on properties
--
  if (not s_dl_hem_utils.exists_propref(p1.llpt_las_lea_pro_propref))
  then
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
  end if;
--
-- Check there is an entry in lease assignments for the Property and Dates
--
  OPEN c_lease_assign(l_pro_refno, p1.llpt_las_lea_start_date, p1.llpt_las_start_date);
   FETCH c_lease_assign INTO l_exists;
    IF c_lease_assign%NOTFOUND
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',583);
    END IF;
  CLOSE c_lease_assign;
--
-- Check the Person Exists on Parties
--
  IF l_answer = 'Y'
   THEN
    OPEN c_par_refno2(p1.llpt_par_alt_ref);
     FETCH c_par_refno2 into l_exists;
      IF c_par_refno2%NOTFOUND
       THEN 
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',510);
      END IF;
     CLOSE c_par_refno2;
  ELSE
    OPEN c_par_refno(p1.llpt_par_alt_ref);
     FETCH c_par_refno INTO l_exists;
      IF c_par_refno%NOTFOUND
       THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',510);
      END IF;
    CLOSE c_par_refno;
  END IF;
--
-- Check the other mandatory fields
--
-- Lease Start Date
--
  IF (p1.llpt_las_lea_start_date is null)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',576);
  END IF;
--
-- Lease Assignment Start Date
--
  IF (p1.llpt_las_start_date is null)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',580);
  END IF;
--
-- Party Start Date
--
  IF (p1.llpt_start_date is null)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',584);
  END IF;
--
-- Check the start relationship between the dates
--
-- Person End Date and start date
--
  IF p1.llpt_end_date is not null
   THEN
    IF (p1.llpt_end_date <= p1.llpt_start_date)
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',585);
    END IF;
  END IF;
--
-- Check the assignment start date is not before the lease start
--
    IF (p1.llpt_las_start_date < p1.llpt_las_lea_start_date)
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',582);
    END IF;
--
-- Check the Party Start Date is not before the lease assignment
--
    IF (p1.llpt_start_date < p1.llpt_las_start_date)
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',586);
    END IF;
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
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,llpt_dlb_batch_id
,llpt_dl_seqno
,llpt_dl_load_status
,llpt_las_lea_pro_propref
,llpt_las_lea_start_date
,llpt_las_start_date
,llpt_par_alt_ref
,llpt_start_date
,llpt_end_date
FROM dl_hsc_lease_parties
WHERE llpt_dlb_batch_id    = p_batch_id
AND   llpt_dl_load_status  = 'C';
--
CURSOR c_pro_refno(p_propref varchar2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
CURSOR c_par_refno(p_par_alt_ref varchar2) IS
SELECT par_refno
FROM   parties
WHERE  par_per_alt_ref = p_par_alt_ref;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HSC_LEASE_PARTIES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i integer := 0;
l_pro_refno number;
l_par_refno number;
l_answer     VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_lease_parties.dataload_delete');
fsc_utils.debug_message( 's_dl_hsc_lease_parties.dataload_delete',3 );
--
cb := p_batch_id;
cd := p_date;
--
-- s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the 'Using Par Refno in place of Alt Ref?'
--
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.llpt_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- get the pro_refno
--
l_pro_refno := null;
--
  OPEN  c_pro_refno(p1.llpt_las_lea_pro_propref);
   FETCH c_pro_refno INTO l_pro_refno;
  CLOSE c_pro_refno;
--
-- Use the answer to get the person record
--
l_par_refno := null;
--
  IF l_answer = 'Y'
   THEN l_par_refno := to_number(p1.llpt_par_alt_ref);
  ELSE
  OPEN  c_par_refno(p1.llpt_par_alt_ref);
   FETCH c_par_refno INTO l_par_refno;
  CLOSE c_par_refno;
  END IF;
--
   DELETE FROM lease_parties
   WHERE lpt_las_lea_pro_refno   = l_pro_refno
   AND   lpt_las_lea_start_date  = p1.llpt_las_lea_start_date
   AND   lpt_las_start_date      = p1.llpt_las_start_date
   AND   lpt_par_refno           = l_par_refno
   AND   lpt_start_date          = p1.llpt_start_date;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('lease_parties');
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
END s_dl_hsc_lease_parties;
/

