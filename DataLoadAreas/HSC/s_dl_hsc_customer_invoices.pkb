CREATE OR REPLACE PACKAGE BODY s_dl_hsc_customer_invoices
AS
-- ***********************************************************************
  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION  DB VER  WHO  WHEN      WHY
  --      1.0          MTR  23/11/01  Dataload
  --
  --      2.0  5.9.0   VRS  29/03/06  CREATE Process failing due to Trigger CLIN_AR_IU
  --                                  on the MWI Check. Changes to the Validation Process
  --                                  to check this before the CREATE. DELETE Process 
  --                                  needs to be completed.
  --
  --      3.0  5.9.0   PJD  30/05/06  Added validation that major work project code
  --                                  is valid.
  --
  --      3.1  5.10.0  PH   02/06/06  Amended dataload to allow pro_propref, rac_pay_ref
  --                                  and par_refno to be supplied rather that internal
  --                                  sequences
  --
  --      3.2  5.10.0  PH   13/06/06  Removed lclin_inpo_insc_refno as this is a 
  --                                  sequence, added in LCLIN_INSC_SCIC_SCP_START_DATE
  --                                  and LCLIN_INSC_SCIC_SCP_CODE to derive this sequence
  --                                  Also Added in DB Version in this section.  
  --
  --      3.3  5.10.0  VRS  19/06/06  Amended ISG-DLO created error codes to HD1
  --      3.4  5.10.0  PH   19/07/06  Corrected compilation errors
  --      3.5  5.10.0  PH   11/10/06  Delete process had wrong variable for batch.
  --                                  Changed cp := p_batch... to cb := p_batch..;
  --
  --      3.6  5.10.0  VRS  11/10/06  Added additional Exception Handling into 
  --                                  DELETE process
  --      3.7  5.10.0  PH   02/01/07  Corrected cursor c_val_prop in validate.
--        4.0  5.13.0  PH   06/02/08  Now includes its own 
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
  UPDATE dl_hsc_customer_invoices
  SET lclin_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_customer_invoices');
     RAISE;
  --
END set_record_status_flag;
  --
  --  declare package variables AND constants
-- ***********************************************************************
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) is
SELECT rowid rec_rowid,
       lclin_dlb_batch_id,
       lclin_dl_seqno,
       lclin_dl_load_status,
       lclin_refno,
       lclin_class_code,
       lclin_inca_code,
       lclin_sco_code,
       lclin_invoice_ref,
       lclin_pro_propref,
       lclin_pay_ref,
       lclin_insc_scic_scp_start_date,
       lclin_insc_scic_scp_code,
       lclin_inpo_start_date,
       lclin_authorised_by,
       lclin_authorise_date,
       lclin_issued_by,
       lclin_issue_date,
       lclin_payment_due_date,
       lclin_transaction_raised_date,
       lclin_invoiced_period_start,
       lclin_invoiced_period_end,
       lclin_level2_authorised_by,
       lclin_level2_authorised_date,
       lclin_prev_sco_code,
       lclin_source_invoice_ref,
       lclin_arrears_possible_ind,
       lclin_mwp_reference,
       lclin_par_per_alt_ref,
       lclin_reconcile_only_ind
  FROM dl_hsc_customer_invoices
 WHERE lclin_dlb_batch_id    = p_batch_id
   AND lclin_dl_load_status = 'V';
--
-- ************************************************************************
--
CURSOR c_pro_refno (p_propref VARCHAR2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
-- ************************************************************************
--
CURSOR c_rac_accno (p_pay_ref VARCHAR2) IS
SELECT rac_accno
FROM   revenue_accounts
WHERE  rac_pay_ref = p_pay_ref;
--
-- ************************************************************************
--
CURSOR c_par_refno (p_par_alt_ref VARCHAR2) IS
SELECT par_refno
FROM   parties
WHERE  par_per_alt_ref = p_par_alt_ref;
--
-- ************************************************************************
--
CURSOR c_insc_refno (p_scp_start_date   DATE,
                     p_scp_code         VARCHAR2,
                     p_inca_code        VARCHAR2,
                     p_inpo_start_date  DATE) IS
SELECT insc_refno
FROM   invoice_points,
       invoice_schedules
WHERE  insc_refno               =  inpo_insc_refno
AND    insc_scic_scp_start_date =  p_scp_start_date
AND    insc_scic_scp_code       =  p_scp_code
AND    insc_scic_inca_code      =  p_inca_code
AND    inpo_start_date          =  p_inpo_start_date;
--
--
-- ************************************************************************
--
-- Constants for process_summary
--
cb       			VARCHAR2(30);
cd       			DATE;
cp       			VARCHAR2(30) := 'CREATE';
ct       			VARCHAR2(40) := 'DL_HSC_CUSTOMER_INVOICES';
cs      			INTEGER;
ce       			VARCHAR2(200);
l_id     ROWID;
l_an_tab 			VARCHAR2(1);
--
-- Other variables
--
i           			INTEGER := 0;
r_customer_liability_invoices	customer_liability_invoices%ROWTYPE;
l_pro_refno                     number(10);
l_rac_accno                     number(10);
l_par_refno                     number(10);
l_insc_refno                    number(10);
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hsc_customer_invoices.dataload_create');
    fsc_utils.debug_message( 's_dl_hsc_customer_invoices.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1(p_batch_id) LOOP
--
      BEGIN
--
          cs := p1.lclin_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_pro_refno  := null;
          l_rac_accno  := null;
          l_par_refno  := null;
          l_insc_refno := null;
--
-- Get the Pro Refno
--
          IF (p1.lclin_pro_propref is not null) THEN
            OPEN c_pro_refno(p1.lclin_pro_propref);
           FETCH c_pro_refno INTO l_pro_refno;
           CLOSE c_pro_refno;
          END IF;
--
--
-- Get the rac_accno
--
          IF (p1.lclin_pay_ref is not null) THEN
            OPEN c_rac_accno(p1.lclin_pay_ref);
           FETCH c_rac_accno INTO l_rac_accno;
           CLOSE c_rac_accno;
          END IF;
--
-- Get the par_refno
--
          IF (p1.lclin_par_per_alt_ref is not null) THEN
            OPEN c_par_refno(p1.lclin_par_per_alt_ref);
           FETCH c_par_refno INTO l_par_refno;
           CLOSE c_par_refno;
          END IF;
--
-- Get the insc_refno
--
          IF (p1.lclin_class_code = 'SCI_SCHED') THEN
            OPEN c_insc_refno(p1.lclin_insc_scic_scp_start_date, p1.lclin_insc_scic_scp_code,
                              p1.lclin_inca_code, p1.lclin_inpo_start_date);
           FETCH c_insc_refno into l_insc_refno;
           CLOSE c_insc_refno;
          END IF;
--
          INSERT INTO customer_liability_invoices (clin_refno,
             					   clin_class_code,
             					   clin_inca_code,
             					   clin_sco_code,
             					   clin_invoice_ref,
             					   clin_pro_refno,
             					   clin_rac_accno,
             					   clin_inpo_insc_refno,
             					   clin_inpo_start_date,
             					   clin_authorised_by,
             					   clin_authorise_date,
             					   clin_issued_by,
             					   clin_issue_date,
             					   clin_payment_due_date,
             					   clin_transaction_raised_date,
             					   clin_invoiced_period_start,
             					   clin_invoiced_period_end,
            				           clin_level2_authorised_by,
             					   clin_level2_authorised_date,
             					   clin_prev_sco_code,
             					   clin_clin_refno,
             					   clin_arrears_possible_ind,
						   clin_mwp_reference,
						   clin_par_refno,
						   clin_reconcile_only_ind
						  )
--
      					   VALUES (p1.lclin_refno,
             					   p1.lclin_class_code,
             					   p1.lclin_inca_code,
             					   p1.lclin_sco_code,
       						   p1.lclin_invoice_ref,
             					   l_pro_refno,
             					   l_rac_accno,
             					   l_insc_refno,
             					   p1.lclin_inpo_start_date,
             					   p1.lclin_authorised_by,
             					   p1.lclin_authorise_date,
             					   p1.lclin_issued_by,
             					   p1.lclin_issue_date,
             					   p1.lclin_payment_due_date,
             					   p1.lclin_transaction_raised_date,
             					   p1.lclin_invoiced_period_start,
             					   p1.lclin_invoiced_period_end,
             					   p1.lclin_level2_authorised_by,
             					   p1.lclin_level2_authorised_date,
             					   p1.lclin_prev_sco_code,
             					   s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lclin_source_invoice_ref),
             					   p1.lclin_arrears_possible_ind,
						   p1.lclin_mwp_reference,
						   l_par_refno,
						   p1.lclin_reconcile_only_ind
						  );

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
               ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
               set_record_status_flag(l_id,'O');
               s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('CUSTOMER_LIABILITY_INVOICES');
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
-- **************************************************************************************************
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
       lclin_dlb_batch_id,
       lclin_dl_seqno,
       lclin_dl_load_status,
       lclin_refno,
       lclin_class_code,
       lclin_inca_code,
       lclin_sco_code,
       lclin_invoice_ref,
       lclin_pro_propref,
       lclin_pay_ref,
       lclin_insc_scic_scp_start_date,
       lclin_insc_scic_scp_code,
       lclin_inpo_start_date,
       lclin_authorised_by,
       lclin_authorise_date,
       lclin_issued_by,
       lclin_issue_date,
       lclin_payment_due_date,
       lclin_transaction_raised_date,
       lclin_invoiced_period_start,
       lclin_invoiced_period_end,
       lclin_level2_authorised_by,
       lclin_level2_authorised_date,
       lclin_prev_sco_code,
       lclin_source_invoice_ref,
       lclin_arrears_possible_ind,
       lclin_mwp_reference,
       lclin_par_per_alt_ref,
       lclin_reconcile_only_ind
  FROM dl_hsc_customer_invoices
 WHERE lclin_dlb_batch_id    = p_batch_id
   AND lclin_dl_load_status IN ('L','F','O');
--
-- ************************************************************************
--
CURSOR c_val_prop (p_pro_propref VARCHAR2) IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = p_pro_propref
   AND (pro_type = 'HOU' OR pro_type = 'BOTH');
--
-- ************************************************************************
--
CURSOR c_val_rac (p_pay_ref  VARCHAR2) IS
SELECT rac_accno
  FROM revenue_accounts
 WHERE rac_pay_ref    = p_pay_ref
   AND rac_class_code = 'LIA';
--
-- ************************************************************************
--
CURSOR c_chk_mwp(p_mwp_reference IN VARCHAR2) IS
SELECT 'X'
  FROM major_works_projects
 WHERE mwp_reference = p_mwp_reference;
--
-- ************************************************************************
--
CURSOR c_val_pro (p_pro_propref VARCHAR2) IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = p_pro_propref;
--
-- ************************************************************************
--
CURSOR c_val_par (p_par_alt_ref VARCHAR2) IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_alt_ref;
--
-- ************************************************************************
--
CURSOR c_insc_refno (p_scp_start_date   DATE,
                     p_scp_code         VARCHAR2,
                     p_inca_code        VARCHAR2,
                     p_inpo_start_date  DATE) IS
SELECT insc_refno
FROM   invoice_points,
       invoice_schedules
WHERE  insc_refno               =  inpo_insc_refno
AND    insc_scic_scp_start_date =  p_scp_start_date
AND    insc_scic_scp_code       =  p_scp_code
AND    insc_scic_inca_code      =  p_inca_code
AND    inpo_start_date          =  p_inpo_start_date;
--
--
-- ************************************************************************
--
-- Constants for process_summary
--
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'VALIDATE';
ct       		VARCHAR2(40) := 'DL_HSC_CUSTOMER_INVOICES';
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
l_prop  		properties.pro_refno%TYPE;
l_rac   		revenue_accounts.rac_accno%TYPE;
l_par   		parties.par_refno%TYPE;
l_clin_clin_refno 	customer_liability_invoices.clin_clin_refno%TYPE;
l_insc_refno            invoice_schedules.insc_refno%TYPE;
--
-- Other variables
--
l_dummy             	VARCHAR2(10);
l_is_inactive       	BOOLEAN DEFAULT FALSE;
l_exists                VARCHAR2(1);
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hsc_customer_invoices.dataload_validate');
    fsc_utils.debug_message( 's_dl_hsc_customer_invoices.dataload_validate',3);
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
          cs := p1.lclin_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
-- ************************************************************************
--
-- Check Constraint for CLIN_CLASS_CODE
--
          IF (p1.lclin_class_code IS NOT NULL) THEN
--
           IF p1.lclin_class_code NOT IN ('SCI_SCHED', 'SCI_ADJ', 'SCI_RECON', 'SUNDRY', 'MWI') THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',117);
           END IF;
--
          END IF;
--
-- ************************************************************************
--
-- Validate CLIN_INCA_CODE
--
          IF (p1.lclin_inca_code IS NOT NULL) THEN
--
           IF (s_invoice_categories.chk_exists(p1.lclin_inca_code) = 'FALSE') THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',119);
           END IF;
--
          END IF;
--
-- ************************************************************************
--
-- Check Constraint for CLIN_SCO_CODE
--
          IF (p1.lclin_sco_code IS NOT NULL) THEN
--
           IF (p1.lclin_sco_code NOT IN ('ALL', 'AUT', 'ISS', 'PRE', 'RAI', 'AU1','PEN')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',118);
           END IF;
--
          END IF;
--
-- ************************************************************************
--
-- Validate CLIN_INVOICE_REF
--
          IF (p1.lclin_invoice_ref IS NOT NULL) THEN
--
           IF s_customer_liability_invoices.get_sco_code(s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lclin_invoice_ref)) IS NOT NULL THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',123);
           END IF;
--
          END IF;
--
-- ************************************************************************
--
-- Validate CLIN_PRO_REFNO
--
          IF (p1.lclin_pro_propref IS NOT NULL) THEN
--
            OPEN c_val_pro(p1.lclin_pro_propref);
           FETCH c_val_pro INTO l_prop;
--
           IF (c_val_pro%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',120);
           END IF;
--
           CLOSE c_val_pro;
--
          END IF;
--
-- ************************************************************************
--
-- Validate CLIN_PREV_SCO_CODE
--
          IF (p1.lclin_prev_sco_code IS NOT NULL) THEN
--
           IF (s_status_codes.get_status_name(p1.lclin_prev_sco_code) IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',121);
           END IF;
--
          END IF;
--
-- ************************************************************************
--
-- Validate CLIN_SOURCE_INVOICE_REF
--
          IF (p1.lclin_source_invoice_ref IS NOT NULL) THEN
--
           l_clin_clin_refno := s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lclin_source_invoice_ref);
--
           IF (s_customer_liability_invoices.get_sco_code(l_clin_clin_refno) IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',102);
           END IF;
--
          END IF;
--
-- ************************************************************************
--
-- Val CLIN_ARREARS_POSSIBLE_IND. By default this is set to Y
--
          IF (NOT s_dl_hem_utils.yornornull(p1.lclin_arrears_possible_ind)) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',121);
          END IF;
--
-- ************************************************************************
--
-- Replicate Trigger validation
--
-- Check Property is a HOU OR BOTH type.
--
          IF (p1.lclin_pro_propref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',124);
          ELSE
--
              OPEN c_val_prop(p1.lclin_pro_propref);
             FETCH c_val_prop INTO l_prop;
--
             IF (c_val_prop%NOTFOUND) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',124);
             END IF;
--
             CLOSE c_val_prop;
--
          END IF;
--
-- ************************************************************************
--
-- Validate CLIN_RAC_ACCNO
--
          IF (p1.lclin_pay_ref IS NOT NULL) THEN
--
            OPEN c_val_rac(p1.lclin_pay_ref);
           FETCH c_val_rac INTO l_rac;
--
           IF (c_val_rac%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',125);
           END IF;
--
           CLOSE c_val_rac;
--
          END IF;
--
-- ************************************************************************
--
-- Check an Invoice Point is assigned for SCI_SCHED subtype,
-- including both FK column values.
-- Amended this as we derive insc_refno sites can't supply it
--
--
          IF (p1.lclin_class_code = 'SCI_SCHED') THEN
--
           IF (   p1.lclin_insc_scic_scp_start_date IS NULL
               OR p1.lclin_insc_scic_scp_code IS NULL
               OR p1.lclin_inpo_start_date IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',126);
--
           END IF;
--
-- Get the insc_refno first
--
           l_insc_refno := null;
--
            OPEN c_insc_refno(p1.lclin_insc_scic_scp_start_date, p1.lclin_insc_scic_scp_code,
                              p1.lclin_inca_code, p1.lclin_inpo_start_date);
           FETCH c_insc_refno into l_insc_refno;
           CLOSE c_insc_refno;
--              
           IF (s_invoice_points.chk_exists(l_insc_refno,p1.lclin_inpo_start_date) = 'FALSE') THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'SCN',87);
           END IF;
--
          ELSE   --not an SCI_SCHED subtype
--
             IF (   p1.lclin_insc_scic_scp_start_date IS NOT NULL
                 OR p1.lclin_insc_scic_scp_code IS NOT NULL
                 OR p1.lclin_inpo_start_date IS NOT NULL) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',127);
--
             END IF;
--
          END IF;
--
-- ************************************************************************
--
--Check SCI_SCHED TYPE mandatory attributes.
--
          IF (p1.lclin_class_code = 'SCI_SCHED') THEN
-- 
           IF (   p1.lclin_invoiced_period_start IS NULL
               OR p1.lclin_invoiced_period_end   IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',128);
--
           END IF;
--
          ELSE --not an SCI_SCHED subtype
--
             IF (   p1.lclin_invoiced_period_start IS NOT NULL
                 OR p1.lclin_invoiced_period_end   IS NOT NULL) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',129);
--
             END IF;
--
          END IF;
--
-- ************************************************************************
--
-- Check SCI_RECON has a source Invoice Refno
--
          IF (p1.lclin_class_code = 'SCI_RECON') THEN
--
           IF (l_clin_clin_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',130);
           END IF;
--
          ELSE --not an SCI_RECON subtype
--
             IF (l_clin_clin_refno IS NOT NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',131);
             END IF;
--
          END IF;
--
-- ************************************************************************
--
-- Check MWI has an associated major works project
--
          IF (p1.lclin_class_code = 'MWI') THEN
--
           IF (p1.lclin_mwp_reference IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',132);
           ELSE
--
              l_exists := NULL;
--
               OPEN c_chk_mwp(p1.lclin_mwp_reference);
              FETCH c_chk_mwp into l_exists;
              CLOSE c_chk_mwp;
--
              IF (l_exists IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',134);
              END IF;
--
           END IF;
--
          ELSE --not a MWI class code
--
             IF (p1.lclin_mwp_reference IS NOT NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',133);
             END IF;
--
          END IF;
--
-- ************************************************************************
--
-- Validate CLIN_PAR_REFNO
--
          IF (p1.lclin_par_per_alt_ref IS NOT NULL) THEN
--
            OPEN c_val_par(p1.lclin_par_per_alt_ref);
           FETCH c_val_par INTO l_par;
--
           IF (c_val_par%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',135);
           END IF;
--
           CLOSE c_val_par;
--
          END IF;
--
-- ************************************************************************
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
               ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
               s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
               set_record_status_flag(l_id,'O');
--
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
-- **************************************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 is
SELECT rowid rec_rowid,
       lclin_dl_seqno,
       lclin_refno
  FROM dl_hsc_customer_invoices
 WHERE lclin_dlb_batch_id   = p_batch_id
   AND lclin_dl_load_status = 'C';
--
-- ************************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
ce       VARCHAR2(200);
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(40) := 'DL_HSC_CUSTOMER_INVOICES';
cs       INTEGER;
l_id     ROWID;
--
i        INTEGER := 0;
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hsc_customer_invoices.dataload_delete');
    fsc_utils.debug_message( 's_dl_hsc_customer_invoices.dataload_delete',3 );
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lclin_dl_seqno;
          l_id := p1.rec_rowid;
--
         DELETE 
           FROM CUSTOMER_LIABILITY_INVOICES
          WHERE clin_refno = p1.lclin_refno;
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
    fsc_utils.proc_end;
--
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
END s_dl_hsc_customer_invoices;
/
