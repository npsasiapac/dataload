CREATE OR REPLACE PACKAGE BODY s_dl_hra_debit_details
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      1.0  5.1.3.1   PJD  13-JUN-2002  Bespoke Dataload
--      1.1  5.1.6     PJD  01-Jul-2002  Minor Corrections
--      2.0  5.2.0     PJD  27-AUG-2002  Corrections and commenting out of tra date
--                                       in 2 cursors 
--      2.1  5.6.0     PH   12-AUG-2004  Now part of Standard Rents Dataloads
--                                       added new field ldde_tra_hde_claim_no
--      2.2  5.8.0     PH   08-AUG-2005  Added delete from debit details for 
--                                       corresponding UNID record.
--      2.3  5.10.0    PH   02-JAN-2007  Reinstated tra_date in cursors
--      2.4  5.12.0    PH   22-OCT-2007  Amended code to get rac_acno and use
--                                       this in tra_refno cursor rather than
--                                       payment reference. Also added same
--                                       code from transactions to process
--                                       in chunks of 30000 in an attempt
--                                       to speed things up.
--      3.0  5.13.0    PH   06-FEB-2008  Now includes its own 
--                                       set_record_status_flag procedure.
--                                       Amended validate on propref, only
--                                       check for non CL elements
--      3.1  5.15.1    PH   09-SEP-2009  Commented out initial check on Prop ref
--                                       as only required for non CL elements
--                                       and performed later on
--
-- ***********************************************************************
--
--  declare package variables and constants
--
--
----------------------------------------------------------------------------
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hra_debit_details
  SET ldde_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_debit_details');
     RAISE;
  --
END set_record_status_flag;
--
--------------------------------------------------------------------------
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c_max(p_batch_id VARCHAR2) is
SELECT MIN(ldde_dl_seqno) min_seqno
,      MAX(ldde_dl_seqno+1) max_seqno
FROM   dl_hra_debit_details
WHERE  ldde_dlb_batch_id    = p_batch_id
AND    ldde_dl_load_status  = 'V';
--
CURSOR c1(p_batch_id VARCHAR2,p_min_seqno INTEGER, p_curr_seqno INTEGER) IS
SELECT
rowid rec_rowid
,ldde_dlb_batch_id
,ldde_dl_seqno
,ldde_dl_load_status
,ldde_pay_ref
,ldde_pro_propref
,ldde_effective_date
,ldde_ele_code
,ldde_att_code
,ldde_amount
,ldde_vca_code
,ldde_vat_amount
,nvl(ldde_trt_code,'DRS') ldde_trt_code
,ldde_tra_date
,ldde_tra_hde_claim_no
FROM dl_hra_debit_details
WHERE ldde_dlb_batch_id    = p_batch_id
AND   ldde_dl_seqno        BETWEEN   p_min_seqno AND p_curr_seqno
AND   ldde_dl_load_status = 'V';
--
CURSOR c_pro_refno(p_propref varchar2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
CURSOR c_tra_refno(p_rac_accno NUMBER,    p_trt_code VARCHAR2,
                   p_effective_date DATE, p_tra_date DATE    ) IS
SELECT tra_refno
FROM   transactions
WHERE  tra_rac_accno      = p_rac_accno
AND    tra_trt_code       = p_trt_code
AND    tra_effective_date = p_effective_date
AND    trunc(tra_date)    = nvl(p_tra_date,trunc(tra_date));
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HRA_DEBIT_DETAILS';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
l_pro_refno number;
i           integer := 0;
l_tra_refno   number;
l_rac_accno   NUMBER;
l_min_seqno         PLS_INTEGER := 0;
l_max_seqno         PLS_INTEGER := 0;
l_curr_seqno        PLS_INTEGER := 0;
l_dde_count         PLS_INTEGER := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_debit_details.dataload_create');
fsc_utils.debug_message( 's_dl_hra_debit_details.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
OPEN c_max(p_batch_id);
FETCH c_max INTO l_min_seqno, l_max_seqno;
CLOSE c_max;
--
l_curr_seqno := l_min_seqno + 30000;
--
WHILE l_min_seqno < l_max_seqno LOOP
--
  FOR p1 IN c1(p_batch_id,l_min_seqno,l_curr_seqno) LOOP
--
BEGIN
--
cs := p1.ldde_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- Get the tra_refno using Get Revenue Account number
--
l_rac_accno := NULL;
--
l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref
                    (p1.ldde_pay_ref);
l_tra_refno := NULL;
--
OPEN  c_tra_refno(l_rac_accno,p1.ldde_trt_code,p1.ldde_effective_date,p1.ldde_tra_date);
FETCH c_tra_refno INTO l_tra_refno;
CLOSE c_tra_refno;
--
-- get the pro_refno
--
l_pro_refno := NULL;
--
IF p1.ldde_pro_propref IS NOT NULL
THEN
  OPEN  c_pro_refno(p1.ldde_pro_propref);
  FETCH c_pro_refno INTO l_pro_refno;
  CLOSE c_pro_refno;
END IF;
--
      INSERT INTO debit_details
         (
             dde_tra_refno
            ,dde_ele_code
            ,dde_amount
            ,dde_vca_code
            ,dde_vat_amount
            ,dde_att_code
            ,dde_pro_refno
         )
      VALUES
         (
             l_tra_refno         ,
             p1.ldde_ele_code    ,
             p1.ldde_amount      ,
             p1.ldde_vca_code    ,
             p1.ldde_vat_amount  ,
             p1.ldde_att_code     ,
             l_pro_refno
         );
--
--   Now Delete UNID record created by transactions process
--
       DELETE from debit_details
        WHERE dde_tra_refno = l_tra_refno
          AND dde_ele_code  = 'UNID';
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
l_min_seqno := l_curr_seqno +1;
l_curr_seqno := l_curr_seqno + 30000;
END LOOP;
COMMIT;
--
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DEBIT_DETAILS');
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
CURSOR c_max(p_batch_id VARCHAR2) is
SELECT MIN(ldde_dl_seqno) min_seqno
,      MAX(ldde_dl_seqno+1) max_seqno
FROM   dl_hra_debit_details
WHERE  ldde_dlb_batch_id    = p_batch_id
AND    ldde_dl_load_status   in ('L','F','O');
--
CURSOR c1(p_batch_id VARCHAR2,p_min_seqno INTEGER, p_curr_seqno INTEGER) IS
SELECT
rowid rec_rowid
,ldde_dlb_batch_id
,ldde_dl_seqno
,ldde_dl_load_status
,ldde_pay_ref
,ldde_pro_propref
,ldde_effective_date
,ldde_ele_code
,ldde_att_code
,ldde_amount
,ldde_vca_code
,ldde_vat_amount
,nvl(ldde_trt_code,'DRS') ldde_trt_code
,ldde_tra_date
,ldde_tra_hde_claim_no
FROM  dl_hra_debit_details
WHERE ldde_dlb_batch_id    = p_batch_id
AND   ldde_dl_seqno          BETWEEN   p_min_seqno AND p_curr_seqno
AND   ldde_dl_load_status in ('L','F','O');
--
CURSOR c_pro_refno(p_propref varchar2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
CURSOR c_tra_refno(p_rac_accno      NUMBER
                  ,p_trt_code       VARCHAR2
                  ,p_effective_date DATE
                  ,p_tra_date       DATE) IS
SELECT tra_refno
FROM   transactions
WHERE  tra_rac_accno      = p_rac_accno
AND    tra_trt_code       = p_trt_code
AND    tra_effective_date = p_effective_date
AND    trunc(tra_date)    = nvl(p_tra_date,trunc(tra_date));
--
CURSOR c_att(p_ele_code VARCHAR2, p_att_code VARCHAR2) IS
SELECT 'X'
FROM   attributes
WHERE  att_ele_code = p_ele_code
AND    att_code     = p_att_code;
--
CURSOR c_ele(p_ele_code VARCHAR2) is
SELECT ele_type,ele_value_type
FROM   elements
WHERE  ele_code = p_ele_code;

-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_DEBIT_DETAILS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_pro_refno      INTEGER;
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_tra_refno      INTEGER;
l_ele_type       VARCHAR2(2);
l_ele_value_type VARCHAR2(1);
l_ele_attr_type  VARCHAR2(1);
l_ele_att_date   VARCHAR2(1);
l_rac_accno      NUMBER;
l_min_seqno      PLS_INTEGER := 0;
l_max_seqno      PLS_INTEGER := 0;
l_curr_seqno     PLS_INTEGER := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_debit_details.dataload_validate');
fsc_utils.debug_message( 's_dl_hra_debit_details.dataload_validate',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
OPEN c_max(p_batch_id);
FETCH c_max INTO l_min_seqno, l_max_seqno;
CLOSE c_max;
--
l_curr_seqno := l_min_seqno + 30000;
--
WHILE l_min_seqno < l_max_seqno LOOP
--
  FOR p1 IN c1(p_batch_id,l_min_seqno,l_curr_seqno) LOOP
--
BEGIN
--
cs := p1.ldde_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- Check the Links to Other Tables
--
-- Check the property exists on properties
-- commented this check out as performed later on
-- and cannot have a record failing twice with the
-- same error
--
--IF (not s_dl_hem_utils.exists_propref(p1.ldde_pro_propref))
--THEN
--  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
--END IF;
--
-- Check that a matching transaction exists
--
l_rac_accno := NULL;
--
l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref
                    (p1.ldde_pay_ref);
l_tra_refno := NULL;
--
OPEN  c_tra_refno(l_rac_accno,p1.ldde_trt_code,p1.ldde_effective_date,p1.ldde_tra_date);
FETCH c_tra_refno INTO l_tra_refno;
CLOSE c_tra_refno;
--
IF l_tra_refno IS NULL
THEN
 l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',350);
END IF;
--
-- Check the element code exists on ELEMENTS
--
l_ele_type       := NULL;
l_ele_value_type := NULL;
--
OPEN  c_ele(p1.ldde_ele_code);
FETCH c_ele into l_ele_type, l_ele_value_type;
CLOSE c_ele;
--
IF l_ele_type IS NOT NULL
THEN
--
  IF l_ele_type != 'CL'
   THEN
     IF (not s_dl_hem_utils.exists_propref(p1.ldde_pro_propref))
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
     END IF;
  END IF;  -- l_ele_type != 'CL'
--
-- If an attribute code has been supplied, check that the atty/ety
-- code combination exists on ATTRIBUTE_TYPES
  IF (l_ele_value_type = 'C')
  THEN
--
    OPEN c_att(p1.ldde_ele_code, p1.ldde_att_code);
    FETCH c_att into l_exists;
    IF c_att%notfound
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',041);
    END IF;
    CLOSE c_att;
  END IF;
--
-- Check that an element value is supplied for a numeric element
--
  IF (l_ele_value_type = 'N' AND p1.ldde_amount IS NULL)
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',035);
  END IF;
--
-- Check that an attribute code is supplied for a coded element
--
  IF (l_ele_value_type = 'C' AND p1.ldde_att_code IS NULL)
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',033);
  END IF;
ELSE
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',031);
END IF;
--
-- Validate the reference value fields
--
-- Check that if a VAT Code has been supplied it is valid and has a
-- current rate on VAT_RATES
--
IF (p1.ldde_vca_code IS NOT NULL)
THEN
  IF ( s_vat_rates.get_vat_rate(p1.ldde_vca_code) ) IS NULL
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',123);
  END IF;
END IF;
--
-- Check the Y/N fields - none for this dataload
--
-- Check the other mandatory fields
--
IF p1.ldde_amount IS NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',960);
END IF;
--
-- Any other checks for consistancy between fields etc. - none in this dataload
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
l_min_seqno := l_curr_seqno +1;
l_curr_seqno := l_curr_seqno + 30000;
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
,ldde_dlb_batch_id
,ldde_dl_seqno
,ldde_dl_load_status
,ldde_pay_ref
,ldde_pro_propref
,ldde_effective_date
,ldde_ele_code
,ldde_att_code
,ldde_amount
,ldde_vca_code
,ldde_vat_amount
,nvl(ldde_trt_code,'DRS') ldde_trt_code
,ldde_tra_date
FROM dl_hra_debit_details
WHERE ldde_dlb_batch_id    = p_batch_id
AND   ldde_dl_load_status  = 'C';
--
CURSOR c_pro_refno(p_propref varchar2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
CURSOR c_tra_refno(p_rac_accno NUMBER, p_trt_code VARCHAR2,
                   p_effective_date DATE, p_tra_date DATE) IS
SELECT tra_refno
FROM   transactions
WHERE  tra_rac_accno      = p_rac_accno
AND    tra_trt_code       = p_trt_code
AND    tra_effective_date = p_effective_date
AND    tra_date           = nvl(p_tra_date,tra_date);
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRA_DEBIT_DETAILS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i           INTEGER := 0;
l_tra_refno INTEGER;
l_pro_refno INTEGER;
l_rac_accno NUMBER;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_debit_details.dataload_delete');
fsc_utils.debug_message( 's_dl_hra_debit_details.dataload_delete',3 );
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
cs := p1.ldde_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- Get the tra_refno
--
l_rac_accno := NULL;
--
l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref
                    (p1.ldde_pay_ref);
l_tra_refno := NULL;
--
OPEN  c_tra_refno(l_rac_accno,p1.ldde_trt_code,p1.ldde_effective_date,p1.ldde_tra_date);
FETCH c_tra_refno INTO l_tra_refno;
CLOSE c_tra_refno;
--
-- get the pro_refno
--
l_pro_refno := NULL;
--
IF p1.ldde_pro_propref IS NOT NULL
THEN
  OPEN  c_pro_refno(p1.ldde_pro_propref);
  FETCH c_pro_refno INTO l_pro_refno;
  CLOSE c_pro_refno;
END IF;
--
DELETE FROM debit_details
WHERE dde_tra_refno   = l_tra_refno
AND   nvl(dde_pro_refno,0) = nvl(l_pro_refno,0)
AND   dde_ele_code         = p1.ldde_ele_code;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DEBIT_DETAILS');
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
END s_dl_hra_debit_details;
/




