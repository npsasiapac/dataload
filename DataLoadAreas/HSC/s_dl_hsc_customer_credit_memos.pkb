CREATE OR REPLACE PACKAGE BODY s_dl_hsc_customer_credit_memos
AS
-- ***********************************************************************

  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION     WHO  WHEN       WHY
  --      1.0     MTR  23/11/01   Dataload
  --      2.0     PH   05/06/06   Amended lcme_rac_accno to lcme_pay_ref
  --                              and proper delete process.
  --      2.1     PH   19/07/06   Removed Created/Modified By and Dates,
  --                              corrected compilation errors.
--
--   3.0 5.13.0   PH   06-FEB-2008 Now includes its own 
--                                 set_record_status_flag procedure.
--   3.1 5.13.0   PH   04-MAR-2008 Moved exception handler in delete to
--                                 within loop
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
  UPDATE dl_hsc_customer_credit_memos
  SET lccme_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_customer_credit_memos');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
  --
  --  declare package variables AND constants


PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) is
SELECT
  rowid rec_rowid,
  lccme_dlb_batch_id,
  lccme_dl_seqno,
  lccme_dl_load_status,
  lccme_refno,
  lccme_credit_memo_ref,
  lccme_sco_code,
  lccme_clin_invoice_ref,
  lccme_authorised_by,
  lccme_authorised_date,
  lccme_issued_by,
  lccme_issued_date,
  lccme_level2_authorised_by,
  lccme_level2_authorised_date,
  lccme_pay_ref
FROM dl_hsc_customer_credit_memos
WHERE lccme_dlb_batch_id    = p_batch_id
AND   lccme_dl_load_status = 'V';
--
CURSOR c_rac_accno (p_pay_ref VARCHAR2) IS
SELECT rac_accno
FROM   revenue_accounts
WHERE  rac_pay_ref = p_pay_ref;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HSC_CUSTOMER_CREDIT_MEMOS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
i           INTEGER := 0;
r_customer_credit_memos     customer_credit_memos%ROWTYPE;
l_rac_accno                     number(10);
--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_customer_credit_memos.dataload_create');
  fsc_utils.debug_message( 's_dl_hsc_customer_credit_memos.dataload_create',3);
  --
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  for p1 in c1(p_batch_id) loop
    --
    BEGIN
      --
      cs := p1.lccme_refno;
      l_id := p1.rec_rowid;
--
l_rac_accno := null;
--
--
-- Get the rac_accno
--
   IF p1.lccme_pay_ref is not null
    THEN
     OPEN c_rac_accno(p1.lccme_pay_ref);
      FETCH c_rac_accno INTO l_rac_accno;
     CLOSE c_rac_accno;
   END IF;
--
     -- 
      INSERT INTO customer_credit_memos (
             ccme_refno,
             ccme_credit_memo_ref,
             ccme_sco_code,
             ccme_created_by,
             ccme_created_date,
             ccme_clin_refno,
             ccme_authorised_by,
             ccme_authorised_date,
             ccme_issued_by,
             ccme_issued_date,
             ccme_modified_by,
             ccme_modified_date,
             ccme_level2_authorised_by,
             ccme_level2_authorised_date,
             ccme_rac_accno)
      VALUES
            (p1.lccme_refno,
             p1.lccme_credit_memo_ref,
             p1.lccme_sco_code,
             'DATALOAD',
             trunc(sysdate),
             s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lccme_clin_invoice_ref),
             p1.lccme_authorised_by,
             p1.lccme_authorised_date,
             p1.lccme_issued_by,
             p1.lccme_issued_date,
             null,
             null,
             p1.lccme_level2_authorised_by,
             p1.lccme_level2_authorised_date,
             l_rac_accno);              

      --
      -- keep a count of the rows processed and commit after every 1000
      --
      i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
      --
      s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
      set_record_status_flag(l_id,'C');
      --
      EXCEPTION
        WHEN OTHERS THEN
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
        set_record_status_flag(l_id,'O');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
    END LOOP;
  --
  -- Section to anayze the table(s) populated by this dataload
  --
  -- l_an_tab:=s_dl_hem_utils.dl_comp_stats('CUSTOMER_CREDIT_MEMOS');
  --
  fsc_utils.proc_end;
  commit;
  --
EXCEPTION
  WHEN OTHERS THEN
  set_record_status_flag(l_id,'O');
  s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
--
END dataload_create;
--
--
-- As defined in FUNCTION H400.60.10.40.10.20
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT
  rowid rec_rowid,
  lccme_dlb_batch_id,
  lccme_dl_seqno,
  lccme_dl_load_status,
  lccme_refno,
  lccme_credit_memo_ref,
  lccme_sco_code,
  lccme_clin_invoice_ref,
  lccme_authorised_by,
  lccme_authorised_date,
  lccme_issued_by,
  lccme_issued_date,
  lccme_level2_authorised_by,
  lccme_level2_authorised_date,
  lccme_pay_ref
FROM dl_hsc_customer_credit_memos
WHERE lccme_dlb_batch_id    = p_batch_id
AND   lccme_dl_load_status       in ('L','F','O');
--
CURSOR c_val_rac (p_pay_ref  VARCHAR2) IS
SELECT rac_accno
  FROM revenue_accounts
 WHERE rac_pay_ref    = p_pay_ref
   AND rac_class_code = 'LIA';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HSC_CUSTOMER_CREDIT_MEMOS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_link1          VARCHAR2(1);
l_link2          VARCHAR2(1);
l_parent_type    VARCHAR2(1);
l_grandchild     VARCHAR2(1);

--
-- Other variables
--
l_dummy             VARCHAR2(10);
l_is_inactive       BOOLEAN DEFAULT FALSE;
l_rac_accno         NUMBER(10);                     
--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_customer_credit_memos.dataload_validate');
  fsc_utils.debug_message( 's_dl_hsc_customer_credit_memos.dataload_validate',3);
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
    cs := p1.lccme_dl_seqno;
    l_id := p1.rec_rowid;
    --
    l_errors := 'V';
    l_error_ind := 'N';
    --
    -- Validate CCME_CREDIT_MEMO_REF 
    IF (p1.lccme_credit_memo_ref IS NOT NULL)
    THEN
      IF s_customer_credit_memos.get_ccme_refno_for_memo_ref(p1.lccme_credit_memo_ref)
      IS NOT NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',132);
      END IF;
    END IF;        
    -- Check Constraint for CCME_SCO_CODE 
    IF p1.lccme_sco_code IS NOT NULL
    THEN
      IF p1.lccme_sco_code NOT IN ('RAI', 'ALL', 'AUT', 'AU1')
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',115);
      END IF;
    END IF;         
    -- Validate CCME_CLIN_REFNO
    IF (p1.lccme_clin_invoice_ref IS NOT NULL)
    THEN
      IF s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lccme_clin_invoice_ref)
      IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',102);
      END IF;
    END IF;  
    --
    -- Validate CCME_PAY_REF
    --
    l_rac_accno := null;
    --
    IF (p1.lccme_pay_ref IS NOT NULL)
     THEN
      OPEN c_val_rac(p1.lccme_pay_ref);
       FETCH c_val_rac INTO l_rac_accno;
        IF (c_val_rac%NOTFOUND)
         THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',116);
        END IF;
      CLOSE c_val_rac;
    END IF;  
    --            
    -- Now UPDATE the record count and error code
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
    --
    s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
    set_record_status_flag(l_id,l_errors);
    -- 
    EXCEPTION
      WHEN OTHERS THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
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
--
END dataload_validate;
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 is
SELECT
  rowid rec_rowid
  ,lccme_dlb_batch_id
  ,lccme_dl_seqno
  ,lccme_dl_load_status
  ,lccme_refno
  ,lccme_credit_memo_ref
FROM  dl_hsc_customer_credit_memos
WHERE lccme_dlb_batch_id   = p_batch_id
AND   lccme_dl_load_status = 'C';

-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HSC_CUSTOMER_CREDIT_MEMOS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i        INTEGER := 0;

BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_customer_credit_memos.dataload_delete');
  fsc_utils.debug_message( 's_dl_hsc_customer_credit_memos.dataload_delete',3 );
  --
  cp := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  FOR p1 IN c1 LOOP
--
  BEGIN
    --
    cs := p1.lccme_dl_seqno;
    l_id := p1.rec_rowid;
    i := i +1;
    --
    SAVEPOINT SP1;
    --
    DELETE FROM customer_credit_memos
    WHERE  ccme_refno    = p1.lccme_refno
    AND    ccme_credit_memo_ref = p1.lccme_credit_memo_ref;
    --
    --
    -- keep a count of the rows processed and commit after every 1000
    --
    i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
    --
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
-- Section to analyze the table(s) populated by this dataload
--
    l_an_tab := s_dl_hem_utils.dl_comp_stats('CUSTOMER_CREDIT_MEMOS');
--
    COMMIT;
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
END s_dl_hsc_customer_credit_memos;
/
