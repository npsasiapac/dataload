CREATE OR REPLACE PACKAGE BODY s_dl_hrm_contractors
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      1.0  5.1.4     PJD  10-feb-2002  Bespoke Dataload for NCCW
--      1.1  5.1.4     PJD  27-feb-2002  Change to Validation on Printer
--      2.0  5.2.0     PJD  19-Sep-2002  Alter insert into job_role_object_rows
--      2.1  5.3.0     PJD  17-Jan-2003  Alter insert into j_r_o_r -- again
--	    2.2  5.3.0     DRH  10-Sep-2003  Add insert into con_site_trades
--      3.0  6.1.0     PH   20-JUL-2012  Added additional fields and validation
--      4.0  6.13      AJ   01-FEB-2016  Added additional con and cos mlang fields 
--                                       were added at v6.11
--                                       Added cos_hpm_authorised_ind and
--                                       cos_quality_assured_ind to insert as missing
--      5.0  6.13      AJ   02-FEB-2016  Added created by and created date to contractor_site insert
--                                       trigger COS_BR_I only deals with user being passed as a null value
--                                       does not deal with no created date being passed in
--      5.1  6.13      AJ   11-JUL-2016  1) Amended insert around job_role_object_rows table against
--                                       contractor sites updated because found not to work at Manitoba
--                                       2) Validation added for job role con site updates
--                                       3) Delete amended so job role object rows not deleted unless
--                                       all fields 43 - 46 provided then assume record created
--
-- ***********************************************************************
--
--  declare package variables and constants
--
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
 lcon_dlb_batch_id
,lcon_dl_seqno
,lcon_dl_load_status
,lcon_code
,lcon_name
,lcon_hrv_cty_code
,nvl(lcon_created_by, 'DATALOAD')        lcon_created_by
,nvl(lcon_created_date, trunc(sysdate))  lcon_created_date
,lcon_tax_reg_ind
,lcon_equal_op_ind
,lcon_vatno
,lcon_text
,lcon_business_no
,lcon_bn_start_date
,lcon_bn_end_date
,lcon_tax_start_date
,lcon_tax_end_date
,lcon_hrm_rcpt_start_date
,lcon_hrm_rcpt_end_date
,lcon_hpm_rcpt_start_date
,lcon_hpm_rcpt_end_date
,lcon_fk_vendor_id
,lcon_cos_code
,lcon_cos_name
,lcon_cos_max_wo_no
,lcon_cos_max_wos_total_value
,lcon_cos_max_wo_value
,lcon_cos_hs_cert_ind
,lcon_cos_quality_assured_ind
,lcon_cos_asbestos_approved_ind
,lcon_cos_hpm_authorised_ind
,lcon_cos_sco_code
,lcon_cos_fca_code
,lcon_cos_pre_inspect_limit
,lcon_cos_post_inspect_limit
,lcon_cos_year_regd
,lcon_cos_min_wo_value
,lcon_cos_max_job_est_cost_var
,lcon_cos_max_job_est_tax_var
,lcon_cos_no_current_wos
,lcon_cos_value_current_wos
,lcon_cos_fk_vendor_site_id
,lcon_cos_spr_printer_name
,lcon_cos_cos_code
,lcon_cos_payment_interval
,lcon_jrb_jro_code
,lcon_jrb_obj_name
,lcon_jrb_read_write_ind
,lcon_jrb_pk_code1
,lcon_cos_phone
,lcon_cos_appt_type_ind
,lcon_cos_tp_appt_ind
,lcon_cos_auto_inv_min_delay
,lcon_cos_auto_inv_delay_days
,lcon_cos_auto_rec_delay_days
,lcon_cos_auto_job_comp_ind
,lcon_cos_auto_job_comp_delay
,lcon_code_mlang
,lcon_name_mlang
,lcon_cos_code_mlang
,lcon_cos_name_mlang
FROM  dl_hrm_contractors
WHERE lcon_dlb_batch_id    = p_batch_id
AND   lcon_dl_load_status = 'V';
--
CURSOR c_con_exists(p_con_code varchar2) IS
SELECT 'X'
FROM   contractors
WHERE  con_code = p_con_code;
--
CURSOR c_jrb_exists(p_jrb_code varchar2,
                    p_cos_code varchar2) IS
SELECT 'X'
FROM job_role_object_rows
WHERE jrb_obj_name ='CONTRACTOR_SITES'
AND jrb_jro_code = p_jrb_code
AND jrb_pk_code1 = p_cos_code;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HRM_CONTRACTORS';
cs       INTEGER;
ce	   VARCHAR2(200);
l_an_tab VARCHAR2(1);
--
-- Other variables
--
l_pro_refno number;
i            INTEGER := 0;
l_exists     VARCHAR2(1);
l_exists_jrb VARCHAR2(1);
--
BEGIN
--
 fsc_utils.proc_start('s_dl_hrm_contractors.dataload_create');
 fsc_utils.debug_message( 's_dl_hrm_contractors.dataload_create',3);
--
 cb := p_batch_id;
 cd := p_date;
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 in c1 LOOP
--
  BEGIN
--
  cs := p1.lcon_dl_seqno;
--
  SAVEPOINT SP1;
--
-- get the pro_refno
--
  l_pro_refno := null;
--
  l_exists := null;
  OPEN  c_con_exists(p1.lcon_code);
  FETCH c_con_exists INTO l_exists;
  CLOSE c_con_exists;
--
  IF l_exists IS NULL THEN
   INSERT INTO contractors
   (con_code
   ,con_name
   ,con_hrv_cty_code
   ,con_created_by
   ,con_created_date
   ,con_tax_reg_ind
   ,con_equal_op_ind
   ,con_vatno
   ,con_text
   ,con_business_no
   ,con_bn_start_date
   ,con_bn_end_date
   ,con_tax_start_date
   ,con_tax_end_date
   ,con_hrm_rcpt_start_date
   ,con_hrm_rcpt_end_date
   ,con_hpm_rcpt_start_date
   ,con_hpm_rcpt_end_date
   ,con_fk_vendor_id
   ,con_reusable_refno
   ,con_code_mlang
   ,con_name_mlang
   )
   VALUES
   (p1.lcon_code
   ,p1.lcon_name
   ,p1.lcon_hrv_cty_code
   ,p1.lcon_created_by
   ,p1.lcon_created_date
   ,p1.lcon_tax_reg_ind
   ,p1.lcon_equal_op_ind
   ,p1.lcon_vatno
   ,p1.lcon_text
   ,p1.lcon_business_no
   ,p1.lcon_bn_start_date
   ,p1.lcon_bn_end_date
   ,p1.lcon_tax_start_date
   ,p1.lcon_tax_end_date
   ,p1.lcon_hrm_rcpt_start_date
   ,p1.lcon_hrm_rcpt_end_date
   ,p1.lcon_hpm_rcpt_start_date
   ,p1.lcon_hpm_rcpt_end_date
   ,p1.lcon_fk_vendor_id
   ,reusable_refno_seq.nextval
   ,p1.lcon_code_mlang
   ,p1.lcon_name_mlang
   );
--
  END IF;
--
   INSERT INTO Contractor_sites
   (cos_code
   ,cos_con_code
   ,cos_name
   ,cos_max_wos_no
   ,cos_max_wos_total_value
   ,cos_max_wo_value
   ,cos_hs_cert_ind
   ,cos_quality_assured_ind
   ,cos_sco_code
   ,cos_fca_code
   ,cos_pre_inspect_limit
   ,cos_post_inspect_limit
   ,cos_year_regd
   ,cos_min_wo_value
   ,cos_max_job_est_cost_variance
   ,cos_max_job_est_tax_variance
   ,cos_no_current_wos
   ,cos_value_current_wos
   ,cos_fk_vendor_site_id
   ,cos_spr_printer_name
   ,cos_cos_code
   ,cos_payment_interval
   ,cos_reusable_refno
   ,cos_appointment_type_ind
   ,cos_third_party_appt_ind
   ,cos_auto_invoice_min_delay_ind
   ,cos_auto_invoice_delay_days
   ,cos_auto_rec_delay_days
   ,cos_auto_job_complete_ind
   ,cos_auto_job_complete_delay
   ,cos_asbestos_approved_ind
   ,cos_hpm_authorised_ind
   ,cos_code_mlang
   ,cos_name_mlang
   ,cos_created_by
   ,cos_created_date
   )
   VALUES
   (p1.lcon_cos_code
   ,p1.lcon_code
   ,p1.lcon_cos_name
   ,p1.lcon_cos_max_wo_no
   ,p1.lcon_cos_max_wos_total_value
   ,p1.lcon_cos_max_wo_value
   ,p1.lcon_cos_hs_cert_ind
   ,p1.lcon_cos_quality_assured_ind
   ,p1.lcon_cos_sco_code
   ,p1.lcon_cos_fca_code
   ,p1.lcon_cos_pre_inspect_limit
   ,p1.lcon_cos_post_inspect_limit
   ,p1.lcon_cos_year_regd
   ,p1.lcon_cos_min_wo_value
   ,p1.lcon_cos_max_job_est_cost_var
   ,p1.lcon_cos_max_job_est_tax_var
   ,p1.lcon_cos_no_current_wos
   ,p1.lcon_cos_value_current_wos
   ,p1.lcon_cos_fk_vendor_site_id
   ,p1.lcon_cos_spr_printer_name
   ,p1.lcon_cos_cos_code
   ,p1.lcon_cos_payment_interval
   ,reusable_refno_seq.nextval
   ,p1.lcon_cos_appt_type_ind
   ,p1.lcon_cos_tp_appt_ind
   ,p1.lcon_cos_auto_inv_min_delay
   ,p1.lcon_cos_auto_inv_delay_days
   ,p1.lcon_cos_auto_rec_delay_days
   ,p1.lcon_cos_auto_job_comp_ind
   ,p1.lcon_cos_auto_job_comp_delay
   ,p1.lcon_cos_asbestos_approved_ind
   ,p1.lcon_cos_hpm_authorised_ind
   ,p1.lcon_cos_code_mlang
   ,p1.lcon_cos_name_mlang
   ,p1.lcon_created_by
   ,p1.lcon_created_date);
--
-- Put in to update created by as initial insert done by
-- trigger COS_BR_I but does not insert the created date
--
   IF p1.lcon_created_by IS NOT NULL
    THEN
     UPDATE contractor_sites
     SET cos_created_by = p1.lcon_created_by
     WHERE cos_code = p1.lcon_cos_code
     AND cos_con_code = p1.lcon_code
    AND cos_name = p1.lcon_cos_name;
   END IF;
--
-- only insert into job role objects if doesn't already exist
-- and the details have been provided as not mandatory
--
   l_exists_jrb := null;
   OPEN  c_jrb_exists(p1.lcon_jrb_jro_code,
                      p1.lcon_jrb_pk_code1);
   FETCH c_jrb_exists INTO l_exists_jrb;
   CLOSE c_jrb_exists;
--
  IF l_exists_jrb IS NULL THEN
--
   IF (p1.lcon_jrb_jro_code  IS NOT NULL 
   AND p1.lcon_jrb_obj_name  IS NOT NULL 
   AND p1.lcon_jrb_read_write_ind  IS NOT NULL 
   AND p1.lcon_jrb_pk_code1  IS NOT NULL) THEN
--
   INSERT INTO job_role_object_rows
   (jrb_jro_code
   ,jrb_obj_name
   ,jrb_read_write_ind
   ,jrb_pk_code1
   )
   VALUES
   (p1.lcon_jrb_jro_code
   ,p1.lcon_jrb_obj_name
   ,p1.lcon_jrb_read_write_ind
   ,p1.lcon_jrb_pk_code1
   );
   END IF;
  END IF;
--
-- keep a count of the rows processed and commit after every 5000
--
  i := i+1; IF MOD(i,5000)=0 THEN COMMIT; END If;
--
  s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
  s_dl_utils.set_record_status_flag(ct,cb,cs,'C');
--
  EXCEPTION
   WHEN OTHERS THEN
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
  END;
--
 END LOOP;
COMMIT;
--
--
-- Section to analyse the table(s) populated by this data load
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CONTRACTORS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CONTRACTOR_SITES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('JOB_ROLE_OBJECT_ROWS');
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
-- ***********************************************************************
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
 lcon_dlb_batch_id
,lcon_dl_seqno
,lcon_dl_load_status
,lcon_code
,lcon_name
,lcon_hrv_cty_code
,lcon_created_by
,lcon_created_date
,lcon_tax_reg_ind
,lcon_equal_op_ind
,lcon_vatno
,lcon_text
,lcon_business_no
,lcon_bn_start_date
,lcon_bn_end_date
,lcon_tax_start_date
,lcon_tax_end_date
,lcon_hrm_rcpt_start_date
,lcon_hrm_rcpt_end_date
,lcon_hpm_rcpt_start_date
,lcon_hpm_rcpt_end_date
,lcon_fk_vendor_id
,lcon_cos_code
,lcon_cos_name
,lcon_cos_max_wo_no
,lcon_cos_max_wos_total_value
,lcon_cos_max_wo_value
,lcon_cos_hs_cert_ind
,lcon_cos_quality_assured_ind
,lcon_cos_asbestos_approved_ind
,lcon_cos_hpm_authorised_ind
,lcon_cos_sco_code
,lcon_cos_fca_code
,lcon_cos_pre_inspect_limit
,lcon_cos_post_inspect_limit
,lcon_cos_year_regd
,lcon_cos_min_wo_value
,lcon_cos_max_job_est_cost_var
,lcon_cos_max_job_est_tax_var
,lcon_cos_no_current_wos
,lcon_cos_value_current_wos
,lcon_cos_fk_vendor_site_id
,lcon_cos_spr_printer_name
,lcon_cos_cos_code
,lcon_cos_payment_interval
,lcon_jrb_jro_code
,lcon_jrb_obj_name
,lcon_jrb_read_write_ind
,lcon_jrb_pk_code1
,lcon_cos_phone
,lcon_cos_appt_type_ind
,lcon_cos_tp_appt_ind
,lcon_cos_auto_inv_min_delay
,lcon_cos_auto_inv_delay_days
,lcon_cos_auto_rec_delay_days
,lcon_cos_auto_job_comp_ind
,lcon_cos_auto_job_comp_delay
,lcon_code_mlang
,lcon_name_mlang
,lcon_cos_code_mlang
,lcon_cos_name_mlang
FROM  dl_hrm_contractors
WHERE lcon_dlb_batch_id      = p_batch_id
AND   lcon_dl_load_status   in ('L','F','O');
--
CURSOR c_check_for_site(p_cos_code VARCHAR2) IS
SELECT 'X'
FROM   contractor_sites
WHERE  cos_code      = p_cos_code;
--
CURSOR c_business_no(p_business_no VARCHAR2) IS
SELECT 'X'
FROM   contractors
WHERE  con_business_no = p_business_no;
--
CURSOR c_calendar(p_fca_code VARCHAR2) IS
SELECT 'X'
FROM   frv_calendars
WHERE  fca_code = p_fca_code;
--
CURSOR c_printer(p_printer VARCHAR2) IS
SELECT 'X'
FROM   system_printers
WHERE  spr_printer_name = p_printer;
--
CURSOR c_sco(p_sco VARCHAR2) IS
SELECT 'X'
FROM   status_codes
WHERE  sco_code = p_sco;
--
CURSOR c_chk_cos_site(p_cos_code VARCHAR2) IS
SELECT 'X'
  FROM contractor_sites
 WHERE cos_code = p_cos_code;
--
--
CURSOR c_chk_jro (p_jro_code VARCHAR2) IS
SELECT 'X'
  FROM job_roles
 WHERE jro_code = p_jro_code;
--
CURSOR c_chk_cos_jro(p_jro_code     VARCHAR2, 
                     p_jrb_cos_code VARCHAR2) IS
SELECT 'X'
  FROM job_role_object_rows
 WHERE jrb_jro_code = p_jro_code
   AND jrb_obj_name = 'CONTRACTOR_SITES'
   AND jrb_pk_code1 = p_jrb_cos_code;
--
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRM_CONTRACTORS';
cs       INTEGER;
ce       VARCHAR2(200);
--
l_exists         VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_jro_code       VARCHAR2(15);
l_obj_name       VARCHAR2(255);
l_rw_ind         VARCHAR2(1);
l_cos_code       VARCHAR2(15);
l_jro_chk        VARCHAR2(1);
l_cos_jro_chk    VARCHAR2(1);
l_cos_chk        VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hrm_contractors.dataload_validate');
fsc_utils.debug_message( 's_dl_hrm_contractors.dataload_validate',3);
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
  cs := p1.lcon_dl_seqno;
--
  l_errors := 'V';
  l_error_ind := 'N';
--
-- Check the Links to Other Tables
--
-- Check the Site doe not already exist on the database
--
  l_exists := NULL;
  OPEN  c_check_for_site(p1.lcon_cos_code);
  FETCH c_check_for_site INTO l_exists;
  CLOSE c_check_for_site;
  IF l_exists IS NOT NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',895);
  END IF;
--
-- Check the Business Number
--
  IF p1.lcon_business_no IS NOT NULL
   THEN
    l_exists := NULL;
    OPEN  c_business_no(p1.lcon_business_no);
    FETCH c_business_no INTO l_exists;
     IF l_exists IS NOT NULL
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',896);
     END IF;
    CLOSE c_business_no;
  ELSE
   IF (p1.lcon_bn_start_date IS NOT NULL or p1.lcon_bn_end_date IS NOT NULL)
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',897);
   END IF;
  END IF;
--
-- Check the Calendar Code
--
  l_exists := NULL;
  OPEN  c_calendar(p1.lcon_cos_fca_code);
  FETCH c_calendar INTO l_exists;
  IF l_exists IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',898);
  END IF;
  CLOSE c_calendar;
--
-- Check the Printer Name
--
  IF p1.lcon_cos_spr_printer_name IS NOT NULL
   THEN
    l_exists := NULL;
    OPEN  c_printer(p1.lcon_cos_spr_printer_name);
    FETCH c_printer INTO l_exists;
    IF l_exists IS NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',899);
    END IF;
    CLOSE c_printer;
  END IF;
--
-- Check the SCO Code
  l_exists := NULL;
  OPEN  c_sco(p1.lcon_cos_sco_code);
  FETCH c_sco INTO l_exists;
  IF l_exists IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',900);
  END IF;
  CLOSE c_sco;
--
-- Validate the reference value fields
--
  IF (NOT s_hdl_utils.exists_frv('CON_TYPE',p1.lcon_hrv_cty_code,'Y'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',901);
  END IF;
--
-- Check the Y/N fields
--
  IF (NOT s_dl_hem_utils.yorn(p1.lcon_tax_reg_ind))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',902);
  END IF;
--
  IF (NOT s_dl_hem_utils.yorn(p1.lcon_equal_op_ind))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',903);
  END IF;
--
  IF (NOT s_dl_hem_utils.yorn(p1.lcon_cos_hs_cert_ind))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',904);
  END IF;
--
  IF (NOT s_dl_hem_utils.yorn(p1.lcon_cos_quality_assured_ind))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',905);
  END IF;
--
  IF (NOT s_dl_hem_utils.yorn(p1.lcon_cos_asbestos_approved_ind))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',906);
  END IF;
--
  IF (NOT s_dl_hem_utils.yorn(p1.lcon_cos_hpm_authorised_ind))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',907);
  END IF;
--
  IF (NOT s_dl_hem_utils.yorn(p1.lcon_cos_tp_appt_ind))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',871);
  END IF;
--
  IF (NOT s_dl_hem_utils.yorn(p1.lcon_cos_auto_inv_min_delay))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',872);
  END IF;
--
  IF (NOT s_dl_hem_utils.yorn(p1.lcon_cos_auto_job_comp_ind))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',873);
  END IF;
--
-- Check the other mandatory fields
--
  IF p1.lcon_code IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',908);
  END IF;
--
  IF p1.lcon_name IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',909);
  END IF;
--
  IF p1.lcon_cos_code IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',910);
  END IF;
--
  IF p1.lcon_cos_name IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',911);
  END IF;
--
  IF p1.lcon_cos_max_wo_no IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',912);
  END IF;
--
  IF p1.lcon_cos_max_wos_total_value IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',913);
  END IF;
--
  IF p1.lcon_cos_max_wo_value IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',914);
  END IF;
--
  IF p1.lcon_cos_no_current_wos IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',915);
  END IF;
--
  IF p1.lcon_cos_value_current_wos IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',916);
  END IF;
--
-- Any other checks for consistancy between fields etc.
--
  IF p1.lcon_tax_start_date IS NOT NULL
   THEN
    IF p1.lcon_tax_start_date > nvl(p1.lcon_tax_end_date, p1.lcon_tax_start_date)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',874);
    END IF;
  END IF;
--
  IF nvl(p1.lcon_cos_appt_type_ind, 'A')  NOT IN ( 'A', 'S' )
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',875);
  END IF;
--
  IF p1.lcon_cos_auto_job_comp_ind = 'Y'
   THEN
    IF NVL(p1.lcon_cos_auto_job_comp_delay,-1) < 0
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',876);
    END IF;
  END IF;
--
-- Additional Validate for job role object rows insert not previously included
-- only check if any one of the four linked fields are supplied
--
  IF (p1.lcon_jrb_jro_code  IS NOT NULL 
  OR p1.lcon_jrb_obj_name  IS NOT NULL 
  OR p1.lcon_jrb_read_write_ind  IS NOT NULL 
  OR p1.lcon_jrb_pk_code1  IS NOT NULL) THEN
--  
   l_jro_code  := UPPER(p1.lcon_jrb_jro_code);
   l_obj_name  := UPPER(p1.lcon_jrb_obj_name);
   l_rw_ind    := UPPER(p1.lcon_jrb_read_write_ind);
   l_cos_code  := UPPER(p1.lcon_jrb_pk_code1);
   l_jro_chk   := NULL;
   l_cos_jro_chk  := NULL;
   l_cos_chk   := NULL;
--
-- Check the Y/N field
--
   IF l_rw_ind NOT IN ('Y','N')   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',930);
   END IF;
--
-- Check the other mandatory fields
--
   IF l_jro_code IS NULL   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',927);
   END IF;
--
   IF l_rw_ind IS NULL   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',928);
   END IF;
--
   IF l_cos_code IS NULL   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',929);
   END IF;
--
   IF l_obj_name IS NULL   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',934);
   END IF;
--
-- Check that the Job Role Contractor Site code is the same as the
-- Contractor Contractor Site code for the same record
--
   IF l_cos_code != p1.lcon_cos_code   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',935);
   END IF;
--
   IF l_obj_name != 'CONTRACTOR_SITES'   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',934);
   END IF;
--
-- Check Contractor Site Exists
--
   IF (l_cos_code IS NOT NULL)  THEN
    OPEN  c_chk_cos_site(l_cos_code);
    FETCH c_chk_cos_site into l_cos_chk;  
    CLOSE c_chk_cos_site;
--
    IF l_cos_chk IS NULL   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',931);
    END IF;
   END IF;
--   
-- Check Job Role Exists
--
   IF (l_jro_code IS NOT NULL)  THEN
    OPEN  c_chk_jro(l_jro_code);
    FETCH c_chk_jro into l_jro_chk;  
    CLOSE c_chk_jro;
--
    IF l_jro_chk IS NULL   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',932);
    END IF;
   END IF;
--
-- Check the Job Role for Contractor Site does not already exist on the database
--
   IF (l_cos_chk IS NOT NULL AND l_jro_chk IS NOT NULL)  THEN
--
    OPEN  c_chk_cos_jro(l_jro_code, l_cos_code);
    FETCH c_chk_cos_jro INTO l_cos_jro_chk;
    CLOSE c_chk_cos_jro;
--
    IF l_cos_jro_chk IS NOT NULL   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',933);
    END IF;
   END IF;
--
  END IF; -- end of job role checks
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
  s_dl_utils.set_record_status_flag(ct,cb,cs,l_errors);
--
  EXCEPTION
   WHEN OTHERS THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
    s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
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
-- ***********************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT
 lcon_dlb_batch_id
,lcon_dl_seqno
,lcon_dl_load_status
,lcon_code
,lcon_cos_code
,lcon_jrb_jro_code
,lcon_jrb_obj_name
,lcon_jrb_read_write_ind
,lcon_jrb_pk_code1
FROM dl_hrm_contractors
WHERE lcon_dlb_batch_id     = p_batch_id
  AND lcon_dl_load_status   = 'C';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRM_CONTRACTORS';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
--
i integer := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hrm_contractors.dataload_delete');
fsc_utils.debug_message( 's_dl_hrm_contractors.dataload_delete',3 );
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
  cs := p1.lcon_dl_seqno;
--
  SAVEPOINT SP1;
--
-- Delete from Job Role Object Rows only if insert was done by this process
-- assuming it was if all job role fields have been provided
--
  IF (p1.lcon_jrb_jro_code  IS NOT NULL 
  AND p1.lcon_jrb_obj_name  IS NOT NULL 
  AND p1.lcon_jrb_read_write_ind  IS NOT NULL 
  AND p1.lcon_jrb_pk_code1  IS NOT NULL) THEN
--
   DELETE FROM job_role_object_rows
   WHERE jrb_obj_name = 'CONTRACTOR_SITES'
   AND   jrb_pk_code1 = p1.lcon_cos_code
   AND   jrb_jro_code = p1.lcon_jrb_jro_code;
  END IF;
--
-- Delete the contractor inspection policies and site trades
--
  DELETE FROM con_site_inspection_policies
  WHERE cip_cos_code = p1.lcon_cos_code;
--
  DELETE FROM con_site_trades
  WHERE csr_cos_code        = p1.lcon_cos_code;
--
-- Delete the contractor site
--
  DELETE FROM contractor_sites
  WHERE cos_code        = p1.lcon_cos_code;
--
-- Delete the contractor provided that there are no remaining sites linked to the
-- contractor
--
  DELETE FROM contractors
  WHERE con_code = p1.lcon_code
  AND NOT EXISTS (SELECT NULL
                  FROM contractor_sites
                  WHERE cos_con_code = con_code);
--
-- keep a count of the rows processed and commit after every 1000
--
  i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
  s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
  s_dl_utils.set_record_status_flag(ct,cb,cs,'V');
--
  EXCEPTION
   WHEN OTHERS THEN
    ROLLBACK TO SP1;
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
  END;
--
 END LOOP;
--
COMMIT;
-- Section to analyse the table(s) populated by this Dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CONTRACTORS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CONTRACTOR_SITES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('JOB_ROLE_OBJECT_ROWS');
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
END s_dl_hrm_contractors;
/
show errors
