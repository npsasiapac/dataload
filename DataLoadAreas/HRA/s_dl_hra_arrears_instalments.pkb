CREATE OR REPLACE PACKAGE BODY s_dl_hra_arrears_instalments
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      1.3  5.5.0     PJD               Bespoke for Prime Focus 
--      3.0  5.15.1    MB 09-JUN-2009    Bespoked for Midland Heart, inclusion 
--                                       of set_record_status_flag procedure
--      4.0  6.9.0     AJ  11-NOV-2013   Check out for issue at 6.9.0 and error codes
--                                       amended as HD1 errors file however the numbers all 
--                                       HDL errors so not affected by change to updated 
--                                       HD1(ONE) file plus added validate check on the
--                                       DD Instalment Processed date 
--      5.0  6.13.0    JS  23-MAR-2016   Exclude validation test for DD instalments extracted
--                                       before arrangement start as this could legitimately 
--                                       occur
-- ***********************************************************************
--
--  declare package variables and constants
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hra_arrears_instalments
  SET lain_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_arrears_instalments');
     RAISE;
  --
END set_record_status_flag;
--
-- **************************************************************************************************
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lain_dlb_batch_id
,lain_dl_seqno
,lain_dl_load_status
,lain_pay_ref
,lain_start_date
,lain_ara_code
,lain_seqno
,lain_amount
,lain_due_date
,lain_amend_ind
,lain_ext_processed_date
,lain_comments
FROM dl_hra_arrears_instalments
WHERE lain_dlb_batch_id    = p_batch_id
AND   lain_dl_load_status = 'V';
--
-- ***********************************************************************
--
CURSOR c_arr_refno (p_rac_Accno VARCHAR2, p_start_date DATE, p_ara_code VARCHAR2) IS
SELECT arr_refno
FROM account_arrears_arrangements
WHERE arr_rac_accno  = p_rac_accno
AND   arr_start_date = p_start_date
AND   arr_ara_code   = p_ara_code;
--
-- ***********************************************************************
--
-- Constants for process_summary
cb         VARCHAR2(30);
cd         DATE;
cp         VARCHAR2(30) := 'CREATE';
ct         VARCHAR2(30) := 'DL_HRA_ARREARS_INSTALMENTS';
cs         INTEGER;
ce	       VARCHAR2(200);
l_an_tab   VARCHAR2(1);
l_id       ROWID;
--
-- Other variables
--
l_pro_refno       NUMBER;
i                 INTEGER := 0;
l_reusable_refno  INTEGER;
l_rac_accno       INTEGER;
l_arr_refno       INTEGER;
--
-- ***********************************************************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_arrears_instalments.dataload_create');
fsc_utils.debug_message( 's_dl_hra_arrears_instalments.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lain_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- ***********************************************************************
--
-- Get the new RAC_ACCNO
  l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref( p1.lain_pay_ref);
--
-- Get the ARR REFNO
--
  l_arr_refno := NULL;
  OPEN  c_arr_refno(l_rac_accno, p1.lain_start_date, p1.lain_ara_code);
  FETCH c_arr_refno INTO l_arr_refno;
  CLOSE c_arr_refno;
--
--
    INSERT INTO arrears_installments
         (ain_arr_refno
         ,ain_seqno
         ,ain_amount
         ,ain_due_date              
         ,ain_amend_ind
         ,ain_text
         ,ain_ext_processed_date
         ,ain_int_processed_date
         )
      VALUES
         (l_arr_refno
         ,p1.lain_seqno
         ,p1.lain_amount
         ,p1.lain_due_date              
         ,p1.lain_amend_ind
         ,p1.lain_comments
         ,p1.lain_ext_processed_date
         ,p1.lain_ext_processed_date
          );
--
-- ***********************************************************************
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ARREARS_INSTALLMENTS');
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
-- **************************************************************************************************
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lain_dlb_batch_id
,lain_dl_seqno
,lain_dl_load_status
,lain_pay_ref
,lain_start_date
,lain_ara_code
,lain_seqno
,lain_amount
,lain_due_date
,lain_amend_ind
,lain_ext_processed_date
,lain_comments
FROM dl_hra_arrears_instalments
WHERE lain_dlb_batch_id    = p_batch_id
AND   lain_dl_load_status   in ('L','F','O');
--
-- ***********************************************************************
--
CURSOR c_arr_refno (p_rac_Accno VARCHAR2, p_start_date DATE, p_ara_code VARCHAR2) IS
SELECT arr_refno
FROM account_arrears_arrangements
WHERE arr_rac_accno  = p_rac_accno
AND   arr_start_date = p_start_date
AND   arr_ara_code   = p_ara_code;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_ARREARS_INSTALMENTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other Constants
--

l_exists         VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_arr_refno      INTEGER;
l_rac_accno      NUMBER(10);
--
-- ***********************************************************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_arrears_instalments.dataload_validate');
fsc_utils.debug_message( 's_dl_hra_arrears_instalments.dataload_validate',3);
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
cs := p1.lain_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- ***********************************************************************
--
-- Check the Links to Other Tables
--
-- Check the payment reference exists on REVENUE_ACCOUNTS
--
l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref( p1.lain_pay_ref );
IF l_rac_accno IS NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',117);
END IF;
--
-- ***********************************************************************
--
-- Check the arrangement instalments
--
  l_arr_refno := NULL;
  OPEN  c_arr_refno(l_rac_accno, p1.lain_start_date, p1.lain_ara_code);
  FETCH c_arr_refno INTO l_arr_refno;
  CLOSE c_arr_refno;
--
IF l_arr_refno IS NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',957);
END IF;
--
-- ***********************************************************************
--
-- Validate the reference value fields
--
-- None
--
-- ***********************************************************************
--
-- Check the Y/N fields
--
-- Manually Amended Ind       
--            
  IF ( p1.lain_amend_ind NOT IN ('Y','N') )        
  THEN          
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',958);       
  END IF;     --
--
-- ***********************************************************************
--
-- Check the other mandatory fields
--
-- Check the Seqno
--
IF p1.lain_seqno IS NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',959);
END IF;
--
-- ***********************************************************************
--
-- Check the Amount
--
IF p1.lain_amount IS NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',960);
END IF;
--
-- ***********************************************************************
--
-- Check the Due Date
--
IF p1.lain_due_date IS NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',961);
END IF;
--
-- ***********************************************************************
--
-- Any other checks for consistency between fields etc.
--

IF ( p1.lain_ext_processed_date is NOT NULL AND p1.lain_ext_processed_date > TRUNC(SYSDATE)) THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',980);
END IF;

-- THIS IS NOW ALL EXCLUDED 5.0
-- Check the DD Instalment Processed date is after the instalment start date
--
-- IF p1.lain_ext_processed_date IS NOT NULL) THEN
--
--  IF (p1.lain_ext_processed_date < NVL(p1.lain_start_date, p1.lain_ext_processed_date)) THEN
--    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',678);
--
--  ELSIF (p1.lain_ext_processed_date > TRUNC(SYSDATE)) THEN
--       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',980);
--  
--  END IF;
--
-- END IF;
--
--
-- ***********************************************************************
--
-- Now UPDATE the record count AND error code
--
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
-- **************************************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lain_dlb_batch_id
,lain_dl_seqno
,lain_dl_load_status
,lain_pay_ref
,lain_start_date
,lain_ara_code
,lain_seqno
,lain_amount
,lain_due_date
,lain_amend_ind
,lain_ext_processed_date
,lain_comments
FROM dl_hra_arrears_instalments
WHERE lain_dlb_batch_id     = p_batch_id
  AND lain_dl_load_status   = 'C';
--
-- ***********************************************************************
--
CURSOR c_arr_refno (p_rac_Accno VARCHAR2, p_start_date DATE, p_ara_code VARCHAR2) IS
SELECT arr_refno
FROM account_arrears_arrangements
WHERE arr_rac_accno  = p_rac_accno
AND   arr_start_date = p_start_date
AND   arr_ara_code   = p_ara_code;
--
-- ***********************************************************************
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRA_ARREARS_INSTALMENTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
l_id     ROWID;
--
-- Other Constants
--
l_rac_accno INTEGER;
l_arr_refno INTEGER;
i integer := 0;
--
-- ***********************************************************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_arrears_instalments.dataload_delete');
fsc_utils.debug_message( 's_dl_hra_arrears_instalments.dataload_delete',3 );
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
cs := p1.lain_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- ***********************************************************************
--
-- Get the new RAC_ACCNO
  l_rac_accno := NULL;
  l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref( p1.lain_pay_ref);
--
-- Get the ARR REFNO
--
  l_arr_refno := NULL;
  OPEN  c_arr_refno(l_rac_accno, p1.lain_start_date, p1.lain_ara_code);
  FETCH c_arr_refno INTO l_arr_refno;
  CLOSE c_arr_refno;
--
  DELETE FROM arrears_installments
  WHERE ain_arr_refno   = l_arr_refno
  AND   ain_due_date    = p1.lain_due_date;
--
-- ***********************************************************************
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ARREARS_INSTALLMENTS');
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
END s_dl_hra_arrears_instalments;
/

