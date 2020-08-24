CREATE OR REPLACE PACKAGE BODY s_dl_hsc_payment_balances
AS
-- ***********************************************************************

  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION  DB Vers  WHO  WHEN       WHY
  --      1.0           MTR  23/11/01   Dataload
  --      2.0           PH   12/06/06   Added Delete Process
  --      2.1  5.10.0   PH   11/07/06   Added db version to change control.
  --                                    Amended code allowing sites to supply
  --                                    transaction details rather than
  --                                    tra_refno. Removed Created/Modify
  --                                    by and date fields
  --      3.0 5.13.0   PH   06-FEB-2008 Now includes its own 
--                                      set_record_status_flag procedure.
--        3.1 5.13.0   PH   04-MAR-2008 Moved exception handler in delete
--                                      to within loop
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
  UPDATE dl_hsc_payment_balances
  SET lpaba_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_payment_balances');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************

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
  lpaba_dlb_batch_id,
  lpaba_dl_seqno,
  lpaba_dl_load_status,
  lpaba_pay_ref,
  lpaba_tra_effective_date,
  lpaba_tra_cr,
  lpaba_tra_external_ref,
  lpaba_seqno,
  lpaba_balance_date,
  lpaba_total_balance
FROM dl_hsc_payment_balances
WHERE lpaba_dlb_batch_id    = p_batch_id
AND   lpaba_dl_load_status = 'V';
--
CURSOR c_get_tra_refno (p_rac_accno    number,
                        p_effect_date  date,
                        p_tra_cr       number,
                        p_external_ref varchar2) IS
SELECT  tra_refno
FROM    transactions
WHERE   tra_rac_accno                  = p_rac_accno
AND     tra_effective_date             = p_effect_date
AND     tra_cr                         = p_tra_cr
AND     tra_trt_code                   = 'PAY'
AND     nvl(tra_external_ref, '~XYZ~') = nvl(p_external_ref, '~XYZ~');

--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HSC_PAYMENT_BALANCES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
i            INTEGER := 0;
l_clin_refno NUMBER(10);
l_rac_accno  NUMBER(8);
l_tra_refno  NUMBER(12);

--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_payment_balances.dataload_create');
  fsc_utils.debug_message( 's_dl_hsc_payment_balances.dataload_create',3);
  --
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  for p1 in c1(p_batch_id) loop
    --
    BEGIN
      --      
      cs := p1.lpaba_dl_seqno;
      l_id := p1.rec_rowid;
      --
     l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref(p1.lpaba_pay_ref);
     l_tra_refno := null;
-- 
-- Get the Transaction
--
  OPEN c_get_tra_refno(l_rac_accno, p1.lpaba_tra_effective_date,
                 p1.lpaba_tra_cr, p1.lpaba_tra_external_ref);
   FETCH c_get_tra_refno INTO l_tra_refno;
  CLOSE c_get_tra_refno;
--                
      -- Create sci_invoice_adjustments record              
      INSERT INTO payment_balances (
           paba_tra_refno,
           paba_seqno,
           paba_balance_date,
           paba_total_balance)
      VALUES
          (l_tra_refno,
           p1.lpaba_seqno,
           p1.lpaba_balance_date,
           p1.lpaba_total_balance
           );                                    

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
  -- l_an_tab:=s_dl_hem_utils.dl_comp_stats('SCI_SERVICE_CHARGE_ITEMS');
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
  lpaba_dlb_batch_id,
  lpaba_dl_seqno,
  lpaba_dl_load_status,
  lpaba_pay_ref,
  lpaba_tra_effective_date,
  lpaba_tra_cr,
  lpaba_tra_external_ref,
  lpaba_seqno,
  lpaba_balance_date,
  lpaba_total_balance
FROM dl_hsc_payment_balances
WHERE lpaba_dlb_batch_id    = p_batch_id
AND   lpaba_dl_load_status       in ('L','F','O');
--
CURSOR c_get_tra_refno (p_rac_accno    number,
                        p_effect_date  date,
                        p_tra_cr       number,
                        p_external_ref varchar2) IS
SELECT  'X'
FROM    transactions
WHERE   tra_rac_accno                  = p_rac_accno
AND     tra_effective_date             = p_effect_date
AND     tra_cr                         = p_tra_cr
AND     tra_trt_code                   = 'PAY'
AND     nvl(tra_external_ref, '~XYZ~') = nvl(p_external_ref, '~XYZ~');
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HSC_PAYMENT_BALANCES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
-- Other variables
--
l_dummy             VARCHAR2(10);
l_is_inactive       BOOLEAN DEFAULT FALSE; 
l_rac_accno         NUMBER(8);
l_exists            VARCHAR2(1);
--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_payment_balances.dataload_validate');
  fsc_utils.debug_message( 's_dl_hsc_payment_balances.dataload_validate',3);
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
    cs := p1.lpaba_dl_seqno;
    l_id := p1.rec_rowid;
    --
    l_errors := 'V';
    l_error_ind := 'N';
    --
    
    -- Val related Transactions  -- Commented out as not tra_refno supplied.
    --IF p1.lpaba_tra_refno IS NOT NULL
    --THEN
    --  IF NOT s_transactions2.tra_refno_exists( p1.lpaba_tra_refno )                   
    --  THEN
    --    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',100);
    --  END IF;
    --END IF;     
    --
    -- Proper validation starts here
    --
    -- Validate Account/Transaction exist
    --         
    l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref(p1.lpaba_pay_ref);
    --
      OPEN c_get_tra_refno(l_rac_accno, p1.lpaba_tra_effective_date,
                     p1.lpaba_tra_cr, p1.lpaba_tra_external_ref);
       FETCH c_get_tra_refno INTO l_exists;
        IF c_get_tra_refno%notfound
         THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',100);
        END IF;
      CLOSE c_get_tra_refno;
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
  rowid rec_rowid,
  lpaba_dlb_batch_id,
  lpaba_dl_seqno,
  lpaba_dl_load_status,
  lpaba_pay_ref,
  lpaba_tra_effective_date,
  lpaba_tra_cr,
  lpaba_tra_external_ref,
  lpaba_seqno
FROM  dl_hsc_payment_balances
WHERE lpaba_dlb_batch_id   = p_batch_id
AND   lpaba_dl_load_status = 'C';
--
CURSOR c_get_tra_refno (p_rac_accno    number,
                        p_effect_date  date,
                        p_tra_cr       number,
                        p_external_ref varchar2) IS
SELECT  tra_refno
FROM    transactions
WHERE   tra_rac_accno                  = p_rac_accno
AND     tra_effective_date             = p_effect_date
AND     tra_cr                         = p_tra_cr
AND     tra_trt_code                   = 'PAY'
AND     nvl(tra_external_ref, '~XYZ~') = nvl(p_external_ref, '~XYZ~');
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HSC_PAYMENT_BALANCES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i        INTEGER := 0;
l_rac_accno  NUMBER(8);
l_tra_refno  NUMBER(12);

BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_payment_balances.dataload_delete');
  fsc_utils.debug_message( 's_dl_hsc_payment_balances.dataload_delete',3 );
  --
  cp := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  FOR p1 IN c1 LOOP
    --
    BEGIN
    --
    cs := p1.lpaba_dl_seqno;
    l_id := p1.rec_rowid;
    i := i +1;
    l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref(p1.lpaba_pay_ref);
    l_tra_refno := null;
    --
    SAVEPOINT SP1;
-- 
-- Get the Transaction
--
  OPEN c_get_tra_refno(l_rac_accno, p1.lpaba_tra_effective_date,
                 p1.lpaba_tra_cr, p1.lpaba_tra_external_ref);
   FETCH c_get_tra_refno INTO l_tra_refno;
  CLOSE c_get_tra_refno;
    --
    DELETE FROM payment_balances
    WHERE paba_tra_refno    = l_tra_refno
    AND   paba_seqno        = p1.lpaba_seqno;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PAYMENT_BALANCES');
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
END s_dl_hsc_payment_balances;
/
