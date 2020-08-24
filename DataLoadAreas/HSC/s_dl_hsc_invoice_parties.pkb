CREATE OR REPLACE PACKAGE BODY s_dl_hsc_invoice_parties
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION  DB VER   WHO  WHEN         WHY
--
--  1.0      5.9.0   PH   12-MAY-2006  Standard Dataload - Initial Creation.
--
--  2.0      5.9.0   PH   24-JUL-2006  Correct error 542 Start/end date wrong way round.
--  3.0     5.13.0   PH   06-FEB-2008 Now includes its own 
--                                    set_record_status_flag procedure.
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
  UPDATE dl_hsc_invoice_parties
  SET lscip_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hrm_con_site_prices');
     RAISE;
  --
END set_record_status_flag;
--
--
--  declare package variables AND constants
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2
,p_date              IN DATE    )
AS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,lscip_dlb_batch_id
,lscip_dl_seqno
,lscip_par_alt_ref
,lscip_pay_ref
,lscip_start_date
,lscip_end_date
FROM  dl_hsc_invoice_parties
WHERE lscip_dlb_batch_id   = p_batch_id
AND   lscip_dl_load_status = 'V';
--
-- Other Cursors
--
CURSOR c_par_refno  (p_par_alt_ref VARCHAR2) IS
SELECT par_refno
FROM parties
WHERE par_per_alt_ref = p_par_alt_ref;
--
--
-- Constants FOR process_summary
--
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'CREATE';
ct          VARCHAR2(30) := 'DL_HSC_INVOICE_PARTIES';
cs          INTEGER;
ce          VARCHAR2(200);
l_id     ROWID;
l_an_tab    VARCHAR2(1);
--
-- Other variables
--
i                 INTEGER:=0;
l_exists          VARCHAR2(1);
l_reusable_refno  INTEGER;
l_par_refno       NUMBER(10);
l_rac_accno       NUMBER(10);
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_invoice_parties.dataload_create');
fsc_utils.debug_message('s_dl_hsc_invoice_parties.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lscip_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
l_par_refno     := null;
l_rac_accno     := null;
--
--
-- Get the Party Refno
--
  OPEN c_par_refno(p1.lscip_par_alt_ref);
   FETCH c_par_refno INTO l_par_refno;
  CLOSE c_par_refno;
--
-- Get the Revenue Account number
--
   l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref
                  ( p1.lscip_pay_ref );
--
-- Do the insert into Invoice Parties
--
     INSERT INTO sc_invoice_parties
    (scip_start_date
    ,scip_end_date
    ,scip_modified_by
    ,scip_created_by
    ,scip_created_date
    ,scip_modified_date
    ,scip_par_refno
    ,scip_rac_accno
    )
    VALUES
    (p1.lscip_start_date
    ,p1.lscip_end_date
    ,null
    ,'DATALOAD'
    ,sysdate
    ,null
    ,l_par_refno
    ,l_rac_accno
    );
--
--
-- Set the dataload statuses
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'C');
--
-- keep a count of the rows processed AND COMMIT after every 5000
--
i := i+1; IF MOD(i,5000)=0 THEN COMMIT; END IF;
--
 EXCEPTION
   WHEN OTHERS THEN
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
 END;
--
--
 END LOOP;
--
COMMIT;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SC_INVOICE_PARTIES');
--
fsc_utils.proc_END;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
     RAISE;
--
END dataload_create;
--
--
--
PROCEDURE dataload_validate
(p_batch_id          IN VARCHAR2
,p_date              IN DATE    )
AS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,lscip_dlb_batch_id
,lscip_dl_seqno
,lscip_par_alt_ref
,lscip_pay_ref
,lscip_start_date
,lscip_end_date
FROM  dl_hsc_invoice_parties
WHERE lscip_dlb_batch_id   = p_batch_id
AND   lscip_dl_load_status IN ('L','F','O');
--
-- Other Cursors
--
CURSOR c_par_refno  (p_par_alt_ref VARCHAR2) IS
SELECT par_refno
FROM   parties
WHERE  par_per_alt_ref = p_par_alt_ref;
--
--
--
CURSOR c_rac_accno  (p_pay_ref VARCHAR2) IS
SELECT rac_accno, rac_start_date
FROM   revenue_accounts
WHERE  rac_pay_ref = p_pay_ref;
--
--
--
CURSOR c_scip_exists (p_start_date   DATE,
                      p_par_refno    NUMBER,
                      p_rac_accno    NUMBER)  IS
SELECT 'X'
FROM   sc_invoice_parties
WHERE  scip_start_date  = p_start_date
AND    scip_par_refno   = p_par_refno
AND    scip_rac_accno   = p_rac_accno;
--
--
--
CURSOR c_scip_overlaps (p_start_date   DATE,
                        p_end_date     DATE,
                        p_par_refno    NUMBER,
                        p_rac_accno    NUMBER)  IS
SELECT 'X'
FROM   sc_invoice_parties
WHERE  scip_par_refno = p_par_refno
AND    scip_rac_accno = p_rac_accno
AND    scip_start_date <= NVL(p_end_date, scip_start_date)
AND    NVL(scip_end_date, p_start_date) >= p_start_date;
--
--
--
CURSOR c_rac_end (p_rac_accno   NUMBER) IS
SELECT rac_end_date
FROM   revenue_accounts
WHERE  rac_accno = p_rac_accno;
--
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HSC_INVOICE_PARTIES';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- Other Variables
--
l_exists             VARCHAR2(1);
i                    INTEGER:=0;
l_errors             VARCHAR2(10);
l_error_ind          VARCHAR2(10);
l_rac_accno          NUMBER(10);
l_par_refno          NUMBER(10);
l_rac_start          DATE;
l_rac_end            DATE;    
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_invoice_parties.dataload_validate');
fsc_utils.debug_message('s_dl_hsc_invoice_parties.dataload_validate',3);
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
cs := p1.lscip_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors     := 'V';
l_error_ind  := 'N';
l_exists     := null;
l_par_refno  := null;
l_rac_accno  := null;
l_rac_start  := null;
l_rac_end    := null;
--
-- Check the Person Exists
--
  OPEN c_par_refno(p1.lscip_par_alt_ref);
   FETCH c_par_refno INTO l_par_refno;
    IF c_par_refno%NOTFOUND
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',510);
    END IF;
  CLOSE c_par_refno;
--
-- Check the Revenue Account Exists. Get the account
-- start date too as used later.
--
  OPEN c_rac_accno(p1.lscip_pay_ref);
   FETCH c_rac_accno INTO l_rac_accno, l_rac_start;
    IF c_rac_accno%NOTFOUND
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',117);
    END IF;
  CLOSE c_rac_accno;
--
--
-- Check the record doesn't already exist for same Account/Person/Start Date
--
  OPEN c_scip_exists(p1.lscip_start_date, l_par_refno, l_rac_accno);
   FETCH c_scip_exists INTO l_exists;
    IF c_scip_exists%FOUND
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',107);
    END IF;
  CLOSE c_scip_exists;
--
-- Check for Overlaps
--
  OPEN c_scip_overlaps(p1.lscip_start_date, p1.lscip_end_date, 
                       l_par_refno, l_rac_accno);
   FETCH c_scip_overlaps INTO l_exists;
    IF c_scip_overlaps%FOUND
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',108);
    END IF;
  CLOSE c_scip_overlaps;
--
-- Check the Start Date is not before the Account Start Date
--
  IF p1.lscip_start_date < l_rac_start
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',109);
  END IF;
--
-- Make sure the end date, if supplied, is not after 
-- the Account End Date
--
  IF p1.lscip_end_date IS NOT NUll
   THEN
    OPEN c_rac_end(l_rac_accno);
     FETCH c_rac_end INTO l_rac_end;
      IF p1.lscip_end_date > nvl(l_rac_end, p1.lscip_end_date)
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',110);
      END IF;
    CLOSE c_rac_end;
  END IF;
--
-- Check the other mandatory field
--
  IF p1.lscip_start_date IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',541);
  END IF;
--
-- Check that if the End Date is Supplied it's after
-- the Start Date
--
  IF p1.lscip_end_date IS NOT NUll
   THEN
    IF p1.lscip_end_date <= p1.lscip_start_date
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',542);
    END IF;
  END IF;
--
--
-- Now UPDATE the record count and error code
IF l_errors = 'F' THEN
  l_error_ind := 'Y';
ELSE
  l_error_ind := 'N';
END IF;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
set_record_status_flag(l_id,l_errors);
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
--
   EXCEPTION
      WHEN OTHERS THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
 END;
--
END LOOP;
--
COMMIT;
--
fsc_utils.proc_END;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_validate;
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) AS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,lscip_dlb_batch_id
,lscip_dl_seqno
,lscip_par_alt_ref
,lscip_pay_ref
,lscip_start_date
,lscip_end_date
FROM  dl_hsc_invoice_parties
WHERE lscip_dlb_batch_id   = p_batch_id
AND   lscip_dl_load_status = 'C';
--
-- Other Cursors
--
CURSOR c_par_refno  (p_par_alt_ref VARCHAR2) IS
SELECT par_refno
FROM parties
WHERE par_per_alt_ref = p_par_alt_ref;
--
--
i            INTEGER := 0;
l_an_tab     VARCHAR2(1);
l_par_refno  NUMBER(10);
l_rac_accno  NUMBER(10);
--
-- Constants FOR process_summary
--
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'DELETE';
ct          VARCHAR2(30) := 'DL_HSC_INVOICE_PARTIES';
cs          INTEGER;
ce          VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_invoice_parties.dataload_DELETE');
fsc_utils.debug_message( 's_dl_hsc_invoice_parties.dataload_DELETE',3 );
--
cb := p_batch_id;
cd := p_DATE;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
  cs := p1.lscip_dl_seqno;
  l_id := p1.rec_rowid;
--
  SAVEPOINT SP1;  
--
l_par_refno     := null;
l_rac_accno     := null;
--
-- Get the Par Refno
--
   OPEN c_par_refno (p1.lscip_par_alt_ref);
      FETCH c_par_refno INTO l_par_refno;
     CLOSE c_par_refno;
--
-- Get the Revenue Account number
--
   l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref
                  ( p1.lscip_pay_ref );
--
--
-- Now Perform the Delete
--
    DELETE from sc_invoice_parties
    WHERE  scip_par_refno  = l_par_refno
    AND    scip_rac_accno  = l_rac_accno
    AND    scip_start_date = p1.lscip_start_date;
--
-- Update Record and Procesed Count
--
  s_dl_process_summary.UPDATE_processed_count(cb,cp,cd,'N');
  set_record_status_flag(l_id,'V');
--
i := i +1; IF mod(i,1000) = 0 THEN commit; END IF;
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
--
COMMIT;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SC_INVOICE_PARTIES');
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
END s_dl_hsc_invoice_parties;
/



