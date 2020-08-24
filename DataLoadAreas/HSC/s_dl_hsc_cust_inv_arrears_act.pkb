CREATE OR REPLACE PACKAGE BODY s_dl_hsc_cust_inv_arrears_act
AS
-- ***********************************************************************

  --  DESCRIPTION:
  --
--  CHANGE CONTROL
--  VERSION  DB VER  WHO  WHEN       WHY
--      1.0  5.9.0   PH  22-MAY-06   Dataload
--      2.0 5.13.0   PH  06-FEB-2008 Now includes its own 
--                                   set_record_status_flag procedure.
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
  UPDATE dl_hsc_cust_inv_arrears_act
  SET lciaa_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_cust_inv_arrears_act');
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
rowid rec_rowid
,lciaa_dlb_batch_id
,lciaa_dl_load_status
,lciaa_dl_seqno
,lciaa_refno
,lciaa_invoice_ref
,lciaa_ara_code
,lciaa_effective_date
,lciaa_status
,lciaa_total_invoice_balance
,lciaa_undisputed_balance
,lciaa_creation_type
,lciaa_created_by
,lciaa_created_date
,lciaa_eac_epo_code
,lciaa_remaining_instal_amt
,lciaa_next_action_date
,lciaa_expiry_date
,lciaa_review_date
,lciaa_authorised_by
,lciaa_authorised_date
,lciaa_deleted_by
,lciaa_deleted_date
,lciaa_hrv_adl_code
,lciaa_printed_by
,lciaa_print_date
,lciaa_nop_text
FROM dl_hsc_cust_inv_arrears_act
WHERE lciaa_dlb_batch_id    = p_batch_id
AND   lciaa_dl_load_status  = 'V'
ORDER BY lciaa_dl_seqno;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HSC_CUST_INV_ARREARS_ACT';
cs       INTEGER;
ce	 VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
i                 INTEGER := 0;
l_reusable_refno  cust_invoice_arrears_actions.ciaa_reusable_refno%TYPE;
l_rac_accno       revenue_accounts.rac_accno%TYPE;
l_an_tab          VARCHAR2(1);
l_clin_refno      NUMBER(10);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_cust_inv_arrears_act.dataload_create');
fsc_utils.debug_message( 's_dl_hsc_cust_inv_arrears_act.dataload_create',3);
--
cb := p_batch_id;
cd := p_DATE;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
for p1 in c1(p_batch_id) loop
--
BEGIN
  --
  cs := p1.lciaa_dl_seqno;
  l_id := p1.rec_rowid;
  --
  -- Get the Invoice Refno
  --
     l_clin_refno := NULL;
  --  
     l_clin_refno := s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lciaa_invoice_ref);
  --
  --
  -- Get the REUSABLE_REFNO
  l_reusable_refno := fsc_utils.f_dynamic_value('reusable_refno_seq.NEXTVAL');
  --
  INSERT INTO cust_invoice_arrears_actions
  (ciaa_refno,
   ciaa_clin_refno,
   ciaa_ara_code,
   ciaa_effective_date,
   ciaa_status,
   ciaa_total_invoice_balance,
   ciaa_undisputed_balance,
   ciaa_reusable_refno,
   ciaa_creation_type,
   ciaa_created_by,
   ciaa_created_date,
   ciaa_created_module,
   ciaa_eac_epo_code,
   ciaa_remaining_instal_amt,
   ciaa_next_action_date,
   ciaa_expiry_date,
   ciaa_review_date,
   ciaa_authorised_by,
   ciaa_authorised_date,
   ciaa_deleted_by,
   ciaa_deleted_date,
   ciaa_hrv_adl_code,
   ciaa_printed_by,
   ciaa_printed_date,
   ciaa_modified_by,
   ciaa_modified_date
  )
   VALUES
  (p1.lciaa_refno
  ,l_clin_refno
  ,p1.lciaa_ara_code
  ,p1.lciaa_effective_date
  ,p1.lciaa_status
  ,p1.lciaa_total_invoice_balance
  ,p1.lciaa_undisputed_balance
  ,l_reusable_refno
  ,p1.lciaa_creation_type
  ,p1.lciaa_created_by
  ,p1.lciaa_created_date
  ,'DATALOAD'
  ,p1.lciaa_eac_epo_code
  ,p1.lciaa_remaining_instal_amt
  ,p1.lciaa_next_action_date
  ,p1.lciaa_expiry_date
  ,p1.lciaa_review_date
  ,p1.lciaa_authorised_by
  ,p1.lciaa_authorised_date
  ,p1.lciaa_deleted_by
  ,p1.lciaa_deleted_date
  ,p1.lciaa_hrv_adl_code
  ,p1.lciaa_printed_by
  ,p1.lciaa_print_date
  ,null
  ,null
  );
  --
  -- Now update the record to set the correct values for
  -- created_by and created_date
  --
  UPDATE cust_invoice_arrears_actions
     SET ciaa_created_by     = p1.lciaa_created_by,
         ciaa_created_date   = p1.lciaa_created_date
   WHERE ciaa_refno          = p1.lciaa_refno
     AND ciaa_reusable_refno = l_reusable_refno;
  --
  --
  -- IF there is a lciaa_nop_text THEN INSERT a row into NOTEPADS
  --
  IF p1.lciaa_nop_text IS NOT NULL
  THEN
    INSERT into notepads
   (nop_reusable_refno,
    nop_type,
    nop_created_date,
    nop_created_by,
    nop_current_ind,
    nop_highlight_ind,
    nop_text,
    nop_modified_date,
    nop_modified_by
   )
   VALUES
   (l_reusable_refno,
    'CIAA',
    p1.lciaa_created_date,
    'DATALOAD',
    'Y',
    'N',
    p1.lciaa_nop_text,
    NULL,
    NULL );
  END IF;
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
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
 END;
END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CUST_INVOICE_ARREARS_ACTIONS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('NOTEPADS');
--
fsc_utils.proc_end;
commit;
--
   EXCEPTION
      WHEN OTHERS THEN
      set_record_status_flag(l_id,'O');
      s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
--
--
END dataload_create;
--
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,lciaa_dlb_batch_id
,lciaa_dl_load_status
,lciaa_dl_seqno
,lciaa_refno
,lciaa_invoice_ref
,lciaa_ara_code
,lciaa_effective_date
,lciaa_status
,lciaa_total_invoice_balance
,lciaa_undisputed_balance
,lciaa_creation_type
,lciaa_created_by
,lciaa_created_date
,lciaa_eac_epo_code
,lciaa_remaining_instal_amt
,lciaa_next_action_date
,lciaa_expiry_date
,lciaa_review_date
,lciaa_authorised_by
,lciaa_authorised_date
,lciaa_deleted_by
,lciaa_deleted_date
,lciaa_hrv_adl_code
,lciaa_printed_by
,lciaa_print_date
,lciaa_nop_text
FROM dl_hsc_cust_inv_arrears_act
WHERE lciaa_dlb_batch_id    = p_batch_id
AND   lciaa_dl_load_status in ('L','F','O');
--
CURSOR c_ara(p_ara_code VARCHAR2) IS
SELECT ara_type from arrears_actions
WHERE  ara_code = p_ara_code;
--
CURSOR c_epo_code (p_epo_code VARCHAR2) IS
SELECT 'X'
FROM   escalation_policies
WHERE  epo_code = p_epo_code;
--
CURSOR c_eac_code (p_epo_code VARCHAR2, p_ara_code VARCHAR2) IS
SELECT 'X'
FROM escalation_policy_actions
WHERE eac_epo_code = p_epo_code
AND   eac_ara_code  = p_ara_code;

-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HSC_CUST_INV_ARREARS_ACT';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- Other variables
l_dummy          escalation_policies.epo_description%TYPE;
l_exists         VARCHAR2(1);
l_ara_exists     VARCHAR2(1);
l_ara_type       VARCHAR2(8);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_cust_inv_arrears_act.dataload_validate');
  fsc_utils.debug_message( 's_dl_hsc_cust_inv_arrears_act.dataload_validate',3);
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
  cs := p1.lciaa_dl_seqno;
  l_id := p1.rec_rowid;
  --
  l_errors := 'V';
  l_error_ind := 'N';
  --
  -- Check Customer Liability Invoices Reference exist's
  --
     IF (p1.lciaa_invoice_ref IS NOT NULL)
      THEN
       IF (s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lciaa_invoice_ref) IS NULL)
        THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',102);
       END IF;
  --
     END IF;
  --
  --
  -- Check for action type
  --
     l_ara_type := null;
  --
    OPEN  c_ara(p1.lciaa_ara_code);
     FETCH c_ara INTO l_ara_type;
    CLOSE c_ara;
     IF l_ara_type IS NULL
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',148);
     END IF;
  --
  -- Check the Creation Type
  --
     IF (p1.lciaa_creation_type NOT IN ('MANUAL','AUTO'))
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',400);
     END IF;
  --
  -- Check the Epo Code is valid
  --
     IF (p1.lciaa_eac_epo_code IS NOT NULL)
      THEN
        OPEN c_epo_code(p1.lciaa_eac_epo_code);
         FETCH c_epo_code into l_exists;
          IF    c_epo_code%notfound 
           THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',403);
          END IF;
        CLOSE c_epo_code;
  --
  -- Check the Action is assigned to the Policy
  --
     OPEN c_eac_code(p1.lciaa_eac_epo_code,p1.lciaa_ara_code);
      FETCH c_eac_code into l_exists;
       IF    c_eac_code%notfound 
        THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',416);
       END IF;
     CLOSE c_eac_code;
  --
     END IF;
  --
  -- Check Status
  --
     IF (nvl(p1.lciaa_status, 'XYZ') NOT IN ('AUTH', 'PEND', 'CLRD', 'DEL', 'PRNT'))
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',401);
     END IF;
  --
  -- Check Mandatory Fields
  --
  -- Invoice Ref
  --
     IF (p1.lciaa_invoice_ref IS NULL) 
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',370);
     END IF;
  --
  -- Effective Date
  --
     IF (p1.lciaa_effective_date IS NULL) THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1', 112);
     END IF;
  --
  -- Total Invoice Balance
  --
     IF (p1.lciaa_total_invoice_balance IS NULL)
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',113);
     END IF;
  --
  -- Undisputed Balance
  --
     IF (p1.lciaa_undisputed_balance IS NULL)
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',114);
     END IF;
  --
  -- Created By and Created Date
  --
     IF (p1.lciaa_created_by is null)
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',953);
     END IF;
  --
     IF (p1.lciaa_created_date is null)
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',952);
     END IF;
  --
  --
  -- Now UPDATE the record count AND error code
  --
  IF l_errors = 'F' THEN
    l_error_ind := 'Y';
  ELSE
    l_error_ind := 'N';
  END IF;
  --
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
--
END dataload_validate;
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,lciaa_dlb_batch_id
,lciaa_dl_load_status
,lciaa_dl_seqno
,lciaa_refno
,lciaa_nop_text
FROM dl_hsc_cust_inv_arrears_act
WHERE lciaa_dlb_batch_id    = p_batch_id
AND   lciaa_dl_load_status  = 'C';
--
-- Constants for process_summary
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'DELETE';
ct          VARCHAR2(30) := 'DL_HSC_CUST_INV_ARREARS_ACT';
cs          INTEGER;
ce          VARCHAR2(200);
l_id     ROWID;
--
i           INTEGER := 0;
l_an_tab    VARCHAR2(1);
--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_cust_inv_arrears_act.dataload_delete');
  fsc_utils.debug_message( 's_dl_hsc_cust_inv_arrears_act.dataload_delete',3 );
  --
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  FOR p1 IN c1 LOOP
  --
  BEGIN
  --
  cs := p1.lciaa_dl_seqno;
  l_id := p1.rec_rowid;
  --
  i := i +1;
  --
  IF p1.lciaa_nop_text IS NOT NULL
     THEN
      --
      -- Code to work around the notepad trigger that
      -- doesn't allow you to delete it if you haven't
      -- created it and within the last hour
      --
      UPDATE notepads
         SET nop_created_by     =  user,
             nop_created_date   = sysdate
       WHERE nop_type           = 'CIAA'
         AND nop_reusable_refno = (SELECT ciaa_reusable_refno
                                     FROM cust_invoice_arrears_actions
                                    WHERE ciaa_refno = p1.lciaa_refno);
      --
      DELETE FROM notepads
      WHERE  nop_type = 'CIAA'
        AND  nop_reusable_refno = (SELECT ciaa_reusable_refno
                                   FROM   cust_invoice_arrears_actions
                                   WHERE  ciaa_refno = p1.lciaa_refno);
  END IF;
  --
  DELETE FROM cust_invoice_arrears_actions
  WHERE ciaa_refno = p1.lciaa_refno;
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
  --
  EXCEPTION
  WHEN OTHERS THEN
  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
  set_record_status_flag(l_id,'C');
  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
  --
END;
--
END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CUST_INVOICE_ARREARS_ACTIONS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('NOTEPADS');
--
fsc_utils.proc_end;
commit;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_delete;
--
--
END s_dl_hsc_cust_inv_arrears_act;

/

