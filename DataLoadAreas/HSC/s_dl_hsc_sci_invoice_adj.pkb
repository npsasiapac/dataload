CREATE OR REPLACE PACKAGE BODY s_dl_hsc_sci_invoice_adj
AS
-- ***********************************************************************
  --
  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION DB Vers  WHO  WHEN       WHY
  --      1.0          MTR  23/11/01   Dataload
  --      2.0          PH   12/06/06   Added Delete Process
  --      2.1 5.10.0   PH   25/09/06   Added Database version, removed
  --                                   created and modified date/by. Also
  --                                   added new field new_actual_cost.
  --      3.0 5.13.0   PH   06-FEB-2008 Now includes its own 
  --                                    set_record_status_flag procedure.
  --                                    corrected cs variable in create
  --      3.1 5.13.0   PH   04-MAR-2008 Moved exception handler in delete
  --                                    to within loop
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
  UPDATE dl_hsc_sci_invoice_adjustments
  SET lscia_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_sci_invoice_adj');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
  --
  --  declare package variables AND constants
  --
  --
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) is
SELECT
  rowid rec_rowid,
  lscia_dlb_batch_id,
  lscia_dl_seqno,
  lscia_dl_load_status,
  lscia_refno,
  lscia_class_code,
  lscia_ssci_clin_invoice_ref,
  lscia_ssci_seqno,
  lscia_amount,
  lscia_invoiceable_ind,
  lscia_prev_estimated_cost,
  lscia_new_estimated_cost,
  lscia_tax_amount,
  lscia_prev_capped_cost,
  lscia_new_capped_cost,
  lscia_new_actual_cost
FROM dl_hsc_sci_invoice_adjustments
WHERE lscia_dlb_batch_id    = p_batch_id
AND   lscia_dl_load_status = 'V';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HSC_SCI_INVOICE_ADJUSTMENTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
i            INTEGER := 0;
l_clin_refno NUMBER(10);
l_scae_refno NUMBER(10);

--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_sci_invoice_adj.dataload_create');
  fsc_utils.debug_message( 's_dl_hsc_sci_invoice_adj.dataload_create',3);
  --
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  for p1 in c1(p_batch_id) loop
    --
    BEGIN
      --      
      l_clin_refno := s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lscia_ssci_clin_invoice_ref);
      cs := p1.lscia_dl_seqno;
      l_id := p1.rec_rowid;
      --    
     
      -- Create default Adjustment Event (all other columns are null 
      -- because this is a default 'DATALOAD' event.
      l_scae_refno := s_sc_adjustment_events.create_record(p_class_code => 'DATALOAD' );
                
      -- Create SCI_INVOICE_ADJUSTMENTS record              
      INSERT INTO sci_invoice_adjustments (
           scia_refno,
           scia_class_code,
           scia_ssci_clin_refno,
           scia_ssci_seqno,
           scia_scae_refno,
           scia_amount,
           scia_invoiceable_ind,
           scia_prev_estimated_cost,
           scia_new_estimated_cost,
           scia_created_by,
           scia_created_date,
           scia_tax_amount,
           scia_prev_capped_cost,
           scia_new_capped_cost,
           scia_new_actual_cost)
      VALUES
          (p1.lscia_refno,
           p1.lscia_class_code,
           l_clin_refno,
           p1.lscia_ssci_seqno,
           l_scae_refno,           -- default created above 
           p1.lscia_amount,
           p1.lscia_invoiceable_ind,
           p1.lscia_prev_estimated_cost,
           p1.lscia_new_estimated_cost,
           'DATALOAD',
           sysdate,
           p1.lscia_tax_amount,
           p1.lscia_prev_capped_cost,
           p1.lscia_new_capped_cost,
           p1.lscia_new_actual_cost);
                                                  
      -- Create associated Adjustment Item             
      s_sci_adjustment_items.create_adjustment( 
          p_clin_refno                => l_clin_refno
        , p_scia_refno                => p1.lscia_refno);
                                          
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
  lscia_dlb_batch_id,
  lscia_dl_seqno,
  lscia_dl_load_status,
  lscia_refno,
  lscia_class_code,
  lscia_ssci_clin_invoice_ref,
  lscia_ssci_seqno,
  lscia_amount,
  lscia_invoiceable_ind,
  lscia_prev_estimated_cost,
  lscia_new_estimated_cost,
  lscia_tax_amount,
  lscia_prev_capped_cost,
  lscia_new_capped_cost
FROM dl_hsc_sci_invoice_adjustments
WHERE lscia_dlb_batch_id    = p_batch_id
AND   lscia_dl_load_status       in ('L','F','O');

CURSOR c_val_serv_ssci(cp_ssci_clin_refno  IN sci_service_charge_items.ssci_clin_refno%TYPE,
                       cp_ssci_seqno       IN sci_service_charge_items.ssci_seqno%TYPE)  IS
SELECT ssci_clin_refno,
       ssci_seqno
  FROM sci_service_charge_items
 WHERE ssci_clin_refno = cp_ssci_clin_refno
   AND ssci_seqno      = cp_ssci_seqno
   AND ssci_class_code = 'SERV';

CURSOR c_val_mgmt_ssci(cp_ssci_clin_refno  IN sci_service_charge_items.ssci_clin_refno%TYPE,
                       cp_ssci_seqno       IN sci_service_charge_items.ssci_seqno%TYPE)  IS
SELECT ssci_clin_refno,
       ssci_seqno
  FROM sci_service_charge_items
 WHERE ssci_clin_refno = cp_ssci_clin_refno
   AND ssci_seqno      = cp_ssci_seqno
   AND ssci_class_code = 'MGMT';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HSC_SCI_INVOICE_ADJUSTMENTS';
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
l_clin_refno        NUMBER;
l_serv_ssci_refno   sci_service_charge_items.ssci_clin_refno%TYPE;
l_serv_ssci_seqno   sci_service_charge_items.ssci_seqno%TYPE;
l_mgmt_ssci_refno   sci_service_charge_items.ssci_clin_refno%TYPE;
l_mgmt_ssci_seqno   sci_service_charge_items.ssci_seqno%TYPE;
--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_sci_invoice_adj.dataload_validate');
  fsc_utils.debug_message( 's_dl_hsc_sci_invoice_adj.dataload_validate',3);
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
    cs := p1.lscia_dl_seqno;
    l_id := p1.rec_rowid;
    --
    l_errors := 'V';
    l_error_ind := 'N';
    --
    -- Check Constraint for CLIN_CLASS_CODE 
    IF p1.lscia_class_code IS NOT NULL
    THEN
      IF p1.lscia_class_code NOT IN ('MGMT','SERVICE')
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',108);
      END IF;
    END IF;
    --    
    -- Val related sci_service_charge_items exist 
    IF p1.lscia_ssci_clin_invoice_ref IS NOT NULL
    THEN
      l_clin_refno := s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lscia_ssci_clin_invoice_ref);
      IF l_clin_refno IS NOT NULL
      THEN
        IF s_sci_service_charge_items.chk_exists(l_clin_refno, p1.lscia_ssci_seqno) = 'FALSE'
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',133);
        END IF;   
      END IF;
    END IF; 
    --
    -- Val SCIA_INVOICEABLE_IND
    IF (NOT s_dl_hem_utils.yornornull(p1.lscia_invoiceable_ind))
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',109);
    END IF;  
    
    -- Reflect trigger validation 
    --
    --If a SERVICE subtype then the associated SCI SERVICE
    --CHARGE ITEM must be a SERV subtype.
    IF p1.lscia_class_code = 'SERVICE'
    THEN
       OPEN c_val_serv_ssci(l_clin_refno ,p1.lscia_ssci_seqno);
       FETCH c_val_serv_ssci INTO l_serv_ssci_refno,l_serv_ssci_seqno;
       IF c_val_serv_ssci%NOTFOUND
       THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',134);       
       END IF;
       CLOSE c_val_serv_ssci;
     END IF;  
  
    --If a MGMT subtype then the associated SCI SERVICE
    --CHARGE ITEM must be a MGMT subtype.
    IF p1.lscia_class_code = 'MGMT'
    THEN
      OPEN c_val_mgmt_ssci(l_clin_refno ,p1.lscia_ssci_seqno);
      FETCH c_val_mgmt_ssci INTO l_mgmt_ssci_refno,l_mgmt_ssci_seqno;
      IF c_val_mgmt_ssci%NOTFOUND
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',135);       
      END IF;
      CLOSE c_val_mgmt_ssci;
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
  rowid rec_rowid
  ,lscia_dlb_batch_id
  ,lscia_dl_seqno
  ,lscia_dl_load_status
  ,lscia_refno
FROM  dl_hsc_sci_invoice_adjustments
WHERE lscia_dlb_batch_id   = p_batch_id
AND   lscia_dl_load_status = 'C';

-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HSC_SCI_INVOICE_ADJUSTMENTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i        INTEGER := 0;

BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_sci_invoice_adj.dataload_delete');
  fsc_utils.debug_message( 's_dl_hsc_sci_invoice_adj.dataload_delete',3 );
  --
  cp := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  FOR p1 IN c1 LOOP
    --
    BEGIN
    --
    cs := p1.lscia_dl_seqno;
    l_id := p1.rec_rowid;
    i := i +1;
    --
    SAVEPOINT SP1;
    --
    --
    DELETE FROM sci_invoice_adjustments
    WHERE scia_refno    = p1.lscia_refno;
    --
    --
    -- keep a count of the rows processed and commit after every 1000
    --
    i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
    --
    --
    s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
    set_record_status_flag(l_id,'V');
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
    l_an_tab := s_dl_hem_utils.dl_comp_stats('SCI_INVOICE_ADJUSTMENTS');
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
END s_dl_hsc_sci_invoice_adj;
/
