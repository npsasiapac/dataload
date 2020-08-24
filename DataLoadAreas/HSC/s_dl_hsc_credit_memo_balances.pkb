CREATE OR REPLACE PACKAGE BODY s_dl_hsc_credit_memo_balances
AS
-- ***********************************************************************

  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION     WHO  WHEN       WHY
  --      1.0     MTR  23/11/01   Dataload
  --      2.0     PH   12/06/06   Added Delete Process
--
--      3.0 5.13.0   PH   06-FEB-2008 Now includes its own 
--                                    set_record_status_flag procedure.
--      3.1 5.13.0   PH   04-MAR-2008 Moved exception handler in delete
--                                    to within the loop.
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
  UPDATE dl_hsc_credit_memo_balances
  SET lcmba_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_credit_memo_balances');
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
  lcmba_dlb_batch_id,
  lcmba_dl_seqno,
  lcmba_dl_load_status,
  lcmba_ccme_credit_memo_ref,
  lcmba_seqno,
  lcmba_balance_date,
  lcmba_total_balance,
  lcmba_created_by,
  lcmba_created_date,
  lcmba_modified_by,
  lcmba_modified_date
FROM dl_hsc_credit_memo_balances
WHERE lcmba_dlb_batch_id    = p_batch_id
AND   lcmba_dl_load_status = 'V';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HSC_CREDIT_MEMO_BALANCES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
i            INTEGER := 0;
l_clin_refno NUMBER(10);

--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_credit_memo_balances.dataload_create');
  fsc_utils.debug_message( 's_dl_hsc_credit_memo_balances.dataload_create',3);
  --
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  for p1 in c1(p_batch_id) loop
    --
    BEGIN
      --      
      cs := p1.lcmba_dl_seqno;
      l_id := p1.rec_rowid;
     --    
                
      -- Create sci_invoice_adjustments record              
      INSERT INTO credit_memo_balances (
          cmba_ccme_refno,
          cmba_seqno,
          cmba_balance_date,
          cmba_total_balance,
          cmba_created_by,
          cmba_created_date,
          cmba_modified_by,
          cmba_modified_date)
      VALUES
          (s_customer_credit_memos.get_ccme_refno_for_memo_ref(p1.lcmba_ccme_credit_memo_ref),
           p1.lcmba_seqno,
           p1.lcmba_balance_date,
           p1.lcmba_total_balance,
           p1.lcmba_created_by,
           p1.lcmba_created_date,
           p1.lcmba_modified_by,
           p1.lcmba_modified_date);                                    

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
  lcmba_dlb_batch_id,
  lcmba_dl_seqno,
  lcmba_dl_load_status,
  lcmba_ccme_credit_memo_ref,
  lcmba_seqno,
  lcmba_balance_date,
  lcmba_total_balance,
  lcmba_created_by,
  lcmba_created_date,
  lcmba_modified_by,
  lcmba_modified_date
FROM dl_hsc_credit_memo_balances
WHERE lcmba_dlb_batch_id    = p_batch_id
AND   lcmba_dl_load_status       in ('L','F','O');
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HSC_CREDIT_MEMO_BALANCES';
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
--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_credit_memo_balances.dataload_validate');
  fsc_utils.debug_message( 's_dl_hsc_credit_memo_balances.dataload_validate',3);
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
    cs := p1.lcmba_dl_seqno;
    l_id := p1.rec_rowid;
    --
    l_errors := 'V';
    l_error_ind := 'N';
    --
   
    -- Val related Customer Credit Memos  
    IF p1.lcmba_ccme_credit_memo_ref IS NOT NULL
    THEN
      IF NOT s_customer_credit_memos.ccme_refno_exists( s_customer_credit_memos.get_ccme_refno_for_memo_ref(p1.lcmba_ccme_credit_memo_ref) ) 
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',101);
      END IF;
    END IF;        
         
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
  lcmba_dlb_batch_id,
  lcmba_dl_seqno,
  lcmba_dl_load_status,
  lcmba_ccme_credit_memo_ref,
  lcmba_seqno,
  lcmba_balance_date,
  lcmba_total_balance
FROM  dl_hsc_credit_memo_balances
WHERE lcmba_dlb_batch_id   = p_batch_id
AND   lcmba_dl_load_status = 'C';

-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HSC_CREDIT_MEMO_BALANCES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i        INTEGER := 0;
l_ccme_refno number(10);
--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_credit_memo_balances.dataload_delete');
  fsc_utils.debug_message( 's_dl_hsc_credit_memo_balances.dataload_delete',3 );
  --
  cp := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  FOR p1 IN c1 LOOP
--
  BEGIN
--
    --
    cs := p1.lcmba_dl_seqno;
    l_id := p1.rec_rowid;
    i := i +1;
    l_ccme_refno := null;
    --
    l_ccme_refno := s_customer_credit_memos.get_ccme_refno_for_memo_ref(p1.lcmba_ccme_credit_memo_ref);

    DELETE FROM credit_memo_balances
    WHERE  cmba_ccme_refno    = l_ccme_refno
    AND    cmba_seqno         = p1.lcmba_seqno
    AND    cmba_balance_date  = p1.lcmba_balance_date;
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
   -- set_record_status_flag(l_id,'C'); not needed as already C anyway 
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');  
--
END; 
--
  END LOOP;
  --
--       
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CREDIT_MEMO_BALANCES');
--
fsc_utils.proc_end;
commit;
--
EXCEPTION
  WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');     
      RAISE;  
--
END dataload_delete;
--
--
END s_dl_hsc_credit_memo_balances;
/
