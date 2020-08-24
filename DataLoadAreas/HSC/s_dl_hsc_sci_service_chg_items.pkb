CREATE OR REPLACE PACKAGE BODY s_dl_hsc_sci_service_chg_items
AS
-- *****************************************************************************
  --
  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION  DB Vers  WHO  WHEN       WHY
  --      1.0           MTR  23/11/01   Dataload
  --
  --      1.1           VRS  07/02/2006 CREATE Bug and Finish of the DELETE 
  --                                    Process
  --
  --      1.2           PH   13/06/2006 Additional validate, can only be associated
  --                                    with a SCI_SCHED subtype Invoice.
  --
  --      1.3           VRS  19/06/2006 Changed all ISG-DLO created error codes to HD1
  --      1.4  5.10.0   PH   19/07/2006 Corrected Compilation errors. Added in
  --                                    DB Version to change control
  --      1.5  5.10.0   PJD  15/11/2006 Pro_refno2 wasn't getting set - code corrected
  --                                    so that now getting set.
  --      1.6  5.10.0   PH   04/07/07   Amended s_service_usages.service_usages_exists
  --                                    validate to only do for type 'P'.
  --
  --      1.7  5.10.0   VRS  05/07/07   Service Usages Prop ref wasnt being validated
  --                                     correctly.
  --      2.0 5.13.0   PH   06-FEB-2008 Now includes its own 
  --                                    set_record_status_flag procedure.
  --      2.1 5.13.0   PH   04-MAR-2008 Moved exception handler in delete
  --                                    to within loop
  --      2.2 5.13.0   PJD  02-JUL-2008 Changed Validation on Service Usages to use
  --                                    c_chk_sus cursor
  --                                    Also slightly changed use of l_chk2
  --                                    and added in SQLERRM to Exception handlers. 
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
  UPDATE dl_hsc_sci_service_chg_items
  SET lssci_dl_load_status  = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_sci_service_chg_items');
     RAISE;
  --
END set_record_status_flag;
-- *****************************************************************************
--
--  declare package variables AND constants
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) is
SELECT rowid rec_rowid,
       lssci_dlb_batch_id,
       lssci_dl_seqno,
       lssci_dl_load_status,
       lssci_seqno,
       lssci_clin_invoice_ref,
       lssci_class_code,
       lssci_estimated_amount,
       lssci_invoiced_amount,
       lssci_added_post_issue_ind,
       lssci_invoiced_tax_amount,
       lssci_estimated_weight_value,
       lssci_actual_amount,
       lssci_actual_weight_value,
       lssci_capped_amount,
       lssci_scr_prorefno_auncode,
       lssci_scr_pro_aun_type,
       lssci_scr_scb_scp_code,
       lssci_scr_scb_scp_start_date,
       lssci_scr_scb_svc_ele_code,
       lssci_scr_scb_svc_att_code,
       lssci_sus_pro_propref,
       lssci_sus_svc_att_ele_code,
       lssci_sus_svc_att_code,
       lssci_sus_start_date,
       lssci_mcg_scp_code,
       lssci_mcg_scp_start_date,
       lssci_mcg_svc_att_ele_code,
       lssci_mcg_svc_att_code,
       lssci_dde_refno
  FROM dl_hsc_sci_service_chg_items
 WHERE lssci_dlb_batch_id    = p_batch_id
   AND lssci_dl_load_status  = 'V';
--
-- *************************************************************************
--
-- Constants for process_summary
--
cb       			VARCHAR2(30);
cd       			DATE;
cp       			VARCHAR2(30) := 'CREATE';
ct       			VARCHAR2(40) := 'DL_HSC_SCI_SERVICE_CHG_ITEMS';
cs       			INTEGER;
ce       			VARCHAR2(200);
l_id     ROWID;
l_an_tab 			VARCHAR2(1);
--
-- Other variables
--
i           			INTEGER := 0;
r_sci_service_charge_items     	sci_service_charge_items%ROWTYPE;
l_clin_refno 			NUMBER(10);
l_pro_aun_code     		VARCHAR2(20);
l_pro_refno                     NUMBER(10);
l_pro_refno2       		NUMBER(10);
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hsc_sci_service_chg_items.dataload_create');
    fsc_utils.debug_message('s_dl_hsc_sci_service_chg_items.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1(p_batch_id) LOOP
--
      BEGIN
--
          cs := p1.lssci_dl_seqno; 
          l_id := p1.rec_rowid;
-- 
-- VRS 07/02/2006 Pass back clin_refno into variable
--
          l_clin_refno := NULL;
--
          l_clin_refno := s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lssci_clin_invoice_ref);
--
-- If the lssci_scr_prorefno_auncode is not null
-- and the type is P then get the pro_refno
--
          IF (p1.lssci_scr_prorefno_auncode is not null) THEN
--
           l_pro_aun_code := p1.lssci_scr_prorefno_auncode;
--
           IF (p1.lssci_scr_pro_aun_type = 'P') THEN
--
            l_pro_refno := null;
--
            l_pro_refno := s_properties.get_refno_for_propref(p1.lssci_scr_prorefno_auncode);
            l_pro_aun_code := TO_CHAR(l_pro_refno);
--
           END IF;
--
          END IF;
--
-- If the lssci_sus_pro_propref is supplied get the pro_refno
--
          IF (p1.lssci_sus_pro_propref is not null) THEN
--
           l_pro_refno2 := null;
           l_pro_refno2 := s_properties.get_refno_for_propref(p1.lssci_sus_pro_propref);
--
          END IF;
--
          INSERT INTO sci_service_charge_items (ssci_seqno,
           				        ssci_clin_refno,
           				        ssci_class_code,
          				        ssci_estimated_amount,
                                                ssci_invoiced_amount,
                                                ssci_added_post_issue_ind,
           				        ssci_invoiced_tax_amount,
           				        ssci_estimated_weight_value,
           				        ssci_actual_amount,
           				        ssci_actual_weight_value,
           				        ssci_capped_amount,
           				        ssci_scr_prorefno_auncode,
           				        ssci_scr_pro_aun_type,
           			 	        ssci_scr_scb_scp_code,
           				        ssci_scr_scb_scp_start_date,
           				        ssci_scr_scb_svc_ele_code,
           				        ssci_scr_scb_svc_att_code,
           				        ssci_sus_pro_refno,
           				        ssci_sus_svc_att_ele_code,
           				        ssci_sus_svc_att_code,
           				        ssci_sus_start_date,
           				        ssci_mcg_scp_code,
           				        ssci_mcg_scp_start_date,
            				        ssci_mcg_svc_att_ele_code,
           				        ssci_mcg_svc_att_code,
           				        ssci_dde_refno)
--
       				        VALUES (p1.lssci_seqno,
           				        l_clin_refno,
           				        p1.lssci_class_code,
           				        p1.lssci_estimated_amount,
           				        p1.lssci_invoiced_amount,
           				        p1.lssci_added_post_issue_ind,
           				        p1.lssci_invoiced_tax_amount,
           				        p1.lssci_estimated_weight_value,
           				        p1.lssci_actual_amount,
           				        p1.lssci_actual_weight_value,
           				        p1.lssci_capped_amount,
           				        l_pro_aun_code,
           				        p1.lssci_scr_pro_aun_type,
           				        p1.lssci_scr_scb_scp_code,
           				        p1.lssci_scr_scb_scp_start_date,
           				        p1.lssci_scr_scb_svc_ele_code,
           				        p1.lssci_scr_scb_svc_att_code,
           				        l_pro_refno2,
           				        p1.lssci_sus_svc_att_ele_code,
           				        p1.lssci_sus_svc_att_code,
           				        p1.lssci_sus_start_date,
           				        p1.lssci_mcg_scp_code,
           				        p1.lssci_mcg_scp_start_date,
           				        p1.lssci_mcg_svc_att_ele_code,
           				        p1.lssci_mcg_svc_att_code,
           				        p1.lssci_dde_refno);

--
-- keep a count of the rows processed and commit after every 1000
--
          i := i+1; 
--
          IF MOD(i,1000) = 0 THEN 
           COMMIT; 
          END IF;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('SCI_SERVICE_CHARGE_ITEMS');
--
    fsc_utils.proc_end;
--
    commit;
--
    EXCEPTION
         WHEN OTHERS THEN
         set_record_status_flag(l_id,'O');
         s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
--
END dataload_create;
--
-- ********************************************************************************************
--
-- As defined in FUNCTION H400.60.10.40.10.20
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT rowid rec_rowid,
       lssci_dlb_batch_id,
       lssci_dl_seqno,
       lssci_dl_load_status,
       lssci_seqno,
       lssci_clin_invoice_ref,
       lssci_class_code,
       lssci_estimated_amount,
       lssci_invoiced_amount,
       lssci_added_post_issue_ind,
       lssci_invoiced_tax_amount,
       lssci_estimated_weight_value,
       lssci_actual_amount,
       lssci_actual_weight_value,
       lssci_capped_amount,
       lssci_scr_prorefno_auncode,
       lssci_scr_pro_aun_type,
       lssci_scr_scb_scp_code,
       lssci_scr_scb_scp_start_date,
       lssci_scr_scb_svc_ele_code,
       lssci_scr_scb_svc_att_code,
       lssci_sus_pro_propref,
       lssci_sus_svc_att_ele_code,
       lssci_sus_svc_att_code,
       lssci_sus_start_date,
       lssci_mcg_scp_code,
       lssci_mcg_scp_start_date,
       lssci_mcg_svc_att_ele_code,
       lssci_mcg_svc_att_code,
       lssci_dde_refno
  FROM dl_hsc_sci_service_chg_items
 WHERE lssci_dlb_batch_id    = p_batch_id
   AND lssci_dl_load_status IN ('L','F','O');
--
-- *************************************************************************
--
CURSOR c_val_clin (p_clin_invoice_ref VARCHAR2) IS
SELECT clin_refno
  FROM customer_liability_invoices
 WHERE clin_invoice_ref = p_clin_invoice_ref
   AND clin_class_code  = 'SCI_SCHED';
--
-- *************************************************************************
CURSOR c_check_sus(p_sus_pro_refno NUMBER
                  ,p_sus_svc_att_code VARCHAR2
                  ,p_sus_svc_att_ele_code VARCHAR2
                  ,p_sus_start_date DATE)
     IS
    SELECT 'x'
    FROM service_usages
     WHERE sus_pro_refno        = p_sus_pro_refno
       AND sus_svc_att_code     = p_sus_svc_att_code
       AND sus_svc_att_ele_code = p_sus_svc_att_ele_code
       AND sus_start_date       = p_sus_start_date ;                                                                                

-- *************************************************************************
--
-- Constants for process_summary
--
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'VALIDATE';
ct       		VARCHAR2(30) := 'DL_HSC_SCI_SERVICE_CHG_ITEMS';
cs       		INTEGER;
ce       		VARCHAR2(200);
l_id     ROWID;
--
l_errors         	VARCHAR2(10);
l_error_ind      	VARCHAR2(10);
i                	INTEGER :=0;
l_link1          	VARCHAR2(1);
l_link2          	VARCHAR2(1);
l_parent_type    	VARCHAR2(1);
l_grandchild     	VARCHAR2(1);
--
-- Other variables
--
l_exists                VARCHAR2(1);
l_dummy             	VARCHAR2(10);
l_is_inactive       	BOOLEAN DEFAULT FALSE;
l_serv_chk1	    	VARCHAR2(1);
l_serv_chk2	    	VARCHAR2(1);
l_mgmt_chk1	    	VARCHAR2(1);
l_pro_refno             NUMBER(10);
l_sus_pro_refno         NUMBER(10);
l_pro_aun_code 		VARCHAR2(20);
l_clin                  NUMBER(10);
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hsc_sci_service_chg_items.dataload_validate');
    fsc_utils.debug_message( 's_dl_hsc_sci_service_chg_items.dataload_validate',3);
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
          cs := p1.lssci_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
-- *************************************************************************
--
-- Validate SSCI_CLIN_REFNO
--
         IF (p1.lssci_clin_invoice_ref IS NOT NULL) THEN
--
          IF (s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lssci_clin_invoice_ref) IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',102);
          END IF;
--
         END IF;
--
-- *************************************************************************
--
         IF (p1.lssci_clin_invoice_ref IS NULL) THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',102);
         END IF;
--
-- *************************************************************************
--
-- Check Constraint for SSCI_CLASS_CODE
--
         IF (p1.lssci_class_code IS NOT NULL) THEN
--
         IF (p1.lssci_class_code NOT IN ('SERV','MGMT')) THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',110);
         END IF;
--
        END IF;
--
-- *************************************************************************
--
-- Validate SSCI_ADDED_POST_ISSUE_IND
--
        IF (NOT s_dl_hem_utils.yornornull(p1.lssci_added_post_issue_ind)) THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',111);
        END IF;
--
-- *************************************************************************
--
-- VRS 07/02/2006 Validate rules for trigger SSCI_AR_IU
--
-- 
        l_serv_chk1 := NULL;
        l_serv_chk2 := 'Y';
--
        IF (p1.lssci_class_code = 'SERV') THEN
--
         IF (   p1.lssci_scr_prorefno_auncode   IS NULL
             OR p1.lssci_scr_pro_aun_type       IS NULL
             OR p1.lssci_scr_scb_scp_code       IS NULL
             OR p1.lssci_scr_scb_scp_start_date IS NULL
             OR p1.lssci_scr_scb_svc_att_code   IS NULL
             OR p1.lssci_scr_scb_svc_ele_code   IS NULL) THEN
--
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',115);
          l_serv_chk1 := 'Y';
--
         END IF;
--  
         IF (   p1.lssci_sus_pro_propref      IS NULL
             OR p1.lssci_sus_svc_att_code     IS NULL
             OR p1.lssci_sus_svc_att_ele_code IS NULL
             OR p1.lssci_sus_start_date       IS NULL) THEN
--
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',116);
          l_serv_chk2 := 'N';
--
         END IF;
--
         IF (   p1.lssci_scr_scb_svc_ele_code != p1.lssci_sus_svc_att_ele_code
             OR p1.lssci_scr_scb_svc_att_code != p1.lssci_sus_svc_att_code) THEN
--
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',117);
--
         END IF;
--
         IF (    p1.lssci_scr_pro_aun_type      = 'P'
             AND p1.lssci_scr_prorefno_auncode != p1.lssci_sus_pro_propref) THEN
--
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',118);
--
         END IF;
--
        ELSE
--
           IF (   p1.lssci_scr_prorefno_auncode   IS NOT NULL
               OR p1.lssci_scr_pro_aun_type       IS NOT NULL
               OR p1.lssci_scr_scb_scp_code       IS NOT NULL
               OR p1.lssci_scr_scb_scp_start_date IS NOT NULL
               OR p1.lssci_scr_scb_svc_att_code   IS NOT NULL
               OR p1.lssci_scr_scb_svc_ele_code   IS NOT NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',119);
--
           END IF;
--
           IF (   p1.lssci_sus_pro_propref      IS NOT NULL
               OR p1.lssci_sus_svc_att_code     IS NOT NULL
               OR p1.lssci_sus_svc_att_ele_code IS NOT NULL
               OR p1.lssci_sus_start_date       IS NOT NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',120);
--
           END IF;
--
           IF (   p1.lssci_estimated_weight_value IS NOT NULL
               OR p1.lssci_actual_weight_value    IS NOT NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',121);
--
           END IF;
--
        END IF; -- IF SERV
--
-- *************************************************************************
--
        l_mgmt_chk1 := NULL;
--
        IF (p1.lssci_class_code = 'MGMT') THEN
--
         IF (   p1.lssci_mcg_scp_code	      IS NULL
             OR p1.lssci_mcg_scp_start_date   IS NULL
             OR p1.lssci_mcg_svc_att_ele_code IS NULL
             OR p1.lssci_mcg_svc_att_code     IS NULL) THEN
--
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',122);
          l_mgmt_chk1 := 'Y';
--
         END IF;
--
        ELSE
--
           IF (   p1.lssci_mcg_scp_code	        IS NOT NULL
               OR p1.lssci_mcg_scp_start_date   IS NOT NULL
               OR p1.lssci_mcg_svc_att_ele_code IS NOT NULL
               OR p1.lssci_mcg_svc_att_code     IS NOT NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',123);
--
           END IF;
--
        END IF; -- IF MGMT
--
-- *************************************************************************
--
-- VRS 07/02/2006 Changed this to only perform SERVICE_CHARGE_RATE check if 
-- all the values required are provided (l_serv_chk1)
-- PH 05/06/06 Changed to get the pro_refno as lssci_scr_prorefno_auncode
-- will be the pro_ref.
--
-- Validate related SERVICE_CHARGE_RATE
--
        IF (    p1.lssci_class_code = 'SERV'
            AND l_serv_chk1         IS NULL) THEN
--
         l_pro_aun_code := p1.lssci_scr_prorefno_auncode;
--
         IF (p1.lssci_scr_pro_aun_type = 'P') THEN
--
          l_pro_refno := null;
--
          l_pro_refno := s_properties.get_refno_for_propref(p1.lssci_scr_prorefno_auncode);
          l_pro_aun_code := TO_CHAR(l_pro_refno);
--
         END IF;
--
         IF NOT s_service_charge_rates.service_charge_rate_exists(l_pro_aun_code,
          							  p1.lssci_scr_pro_aun_type,
         							  p1.lssci_scr_scb_scp_code,
         							  p1.lssci_scr_scb_scp_start_date,
         							  p1.lssci_scr_scb_svc_att_code,
         							  p1.lssci_scr_scb_svc_ele_code) THEN
--
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',112);
--
         END IF;
--
        END IF;
--
-- *************************************************************************
--
-- VRS 07/02/2006 Changed this to only perform SERVICE_USAGES check if 
-- all the values required are provided (l_serv_chk2)
--
-- Validate related SERVICE_USAGES
--
        IF (    p1.lssci_class_code = 'SERV'
            AND l_serv_chk2         = 'Y') THEN
--
         l_sus_pro_refno := null;
--
         l_sus_pro_refno := s_properties.get_refno_for_propref(p1.lssci_sus_pro_propref);
--
         IF(l_sus_pro_refno IS NULL) THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
         ELSE
           l_exists := NULL;
           OPEN c_check_sus(l_sus_pro_refno
                  ,p1.lssci_sus_svc_att_code
                  ,p1.lssci_sus_svc_att_ele_code
                  ,p1.lssci_sus_start_date);
           FETCH c_check_sus INTO l_exists;
           CLOSE c_check_sus;
--
           IF l_exists IS NULL 
           THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',113);
           END IF;
--
         END IF;
--
        END IF;
--
-- *************************************************************************
--
--
-- VRS 07/02/2006 Changed this to only perform MANAGEMENT_COST_GROUPS check if 
-- all the values required are provided (l_mgmt_chk1)
--
-- Validate MANAGEMENT_COST_GROUPS
--
        IF (    p1.lssci_class_code = 'MGMT'
            AND l_mgmt_chk1         IS NULL) THEN
--
         IF NOT s_management_cost_groups.cost_group_exists(p1.lssci_mcg_svc_att_ele_code,
         						   p1.lssci_mcg_svc_att_code,
         						   p1.lssci_mcg_scp_code,
         						   p1.lssci_mcg_scp_start_date) THEN
--
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',114);
--
         END IF;
--
        END IF;
--
-- *************************************************************************
--
-- Additional validate for trigger ssci_ar_iu. The associated 
-- Customer Liability Invoice must be a SCI_SCHED subtype.
--
        l_clin := null;
--
         OPEN c_val_clin (p1.lssci_clin_invoice_ref);
        FETCH c_val_clin INTO l_clin;
--
        IF (c_val_clin%NOTFOUND) THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',124);
        END IF;
--
        CLOSE c_val_clin;
--
-- *************************************************************************
--
-- Now UPDATE the record count and error code
--
        IF l_errors = 'F' THEN
         l_error_ind := 'Y';
        ELSE
           l_error_ind := 'N';
        END IF;
--
-- keep a count of the rows processed and commit after every 1000
--
        i := i+1; 
--
        IF MOD(i,1000) = 0 THEN 
         COMMIT; 
        END IF;
--
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
--
    fsc_utils.proc_END;
--
    COMMIT;
--
    EXCEPTION
         WHEN OTHERS THEN
         s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
-- ********************************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 is
SELECT rowid rec_rowid,
       lssci_dl_seqno,
       lssci_seqno,
       lssci_clin_invoice_ref
  FROM dl_hsc_sci_service_chg_items
 WHERE lssci_dlb_batch_id   = p_batch_id
   AND lssci_dl_load_status = 'C';
--
-- *********************************************************
-- Constants for process_summary
--
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'DELETE';
ct       	VARCHAR2(30) := 'DL_HSC_SCI_SERVICE_CHG_ITEMS';
cs       	INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i        	INTEGER := 0;
l_clin_refno 	NUMBER(10);
BEGIN
--
    fsc_utils.proc_start('s_dl_hsc_sci_service_chg_items.dataload_delete');
    fsc_utils.debug_message('s_dl_hsc_sci_service_chg_items.dataload_delete',3 );
--
    cp := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
       SAVEPOINT SP1;
--
          cs := p1.lssci_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_clin_refno := NULL;
--
          l_clin_refno := s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lssci_clin_invoice_ref);
--
          DELETE 
            FROM SCI_SERVICE_CHARGE_ITEMS
           WHERE ssci_seqno      = p1.lssci_seqno
             AND ssci_clin_refno = l_clin_refno;
--
--
-- keep a count of the rows processed and commit after every 1000
--
          i := i+1; 
--
          IF MOD(i,1000) = 0 THEN 
           COMMIT; 
          END IF;
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
    l_an_tab := s_dl_hem_utils.dl_comp_stats('SCI_SERVICE_CHARGE_ITEMS');
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
END s_dl_hsc_sci_service_chg_items;
/
