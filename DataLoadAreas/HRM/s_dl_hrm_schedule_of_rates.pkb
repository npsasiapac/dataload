CREATE OR REPLACE PACKAGE BODY s_dl_hrm_schedule_of_rates      
AS                 
-- ***********************************************************************
--  DESCRIPTION:                            
--               
--  CHANGE CONTROL                          
--  VER DB Ver    WHO  WHEN        WHY         
--  1.0           PH   10/07/2001  Dataload  
--  1.1 5.1.4     PJD  11/04/2002  Default the IIT_CODE to SOR on  
--                                 S type records on create.
--  2.0 5.2.0     PH   08/07/2002  Changes for 5.2.0 Release. New fields added.
--                                 Create and validate changed
--  2.1 5.2.0     PJD  15/07/2002  Added delete from Job Role Object Rows
--  2.2 5.2.0     PJD  19/07/2002  Corrected delete from Job Role Object Rows
--  2.3 5.2.0     PH   15/08/2002  Changed Validation on vca_code from 
--                                 first_ref_domains to vat_categories
--  2.4 5.2.0     SB   06/02/2002  Changed Validation on liabililty_ind  
--  2.5 5.2.0     PJD  15/11/2002  Split some validation depending on item type
--  3.0 5.3.0     PH   10/03/2003  Added validation on lsop_price
--  3.1 5.3.0     SB   25/03/2003  Changed order on delete.
--  3.2 5.3.0     PH   29/05/2003  Added Validation on mandatory field lsor_description
--  3.3 5.3.0     PH   26/04/2004  Amended create process so that SOR's can
--                                 be loaded in for Planned Maintenance. Looks up
--                                 the Dataload Area
--  3.4 5.5.0     PH   28/04/2004  Removed the above change as 
--                                 getting the product area will not work 
--                                 as I initially thought. Have therefore had to 
--                                 add an additional field for HRM or HPM area. 
--                                 Added to validate also.
--  3.5 5.5.0     PJD  29/04/2004  Minor amendment to the above - will now default
--                                 arc code to HRM if not supplied.
--  3.6 5.7.0     PH   12/01/2005  Added new fields for 570 release. Included 
--                                 validates where necessary.
--  3.7 5.7.0     DH   02/03/2005  Removed nvl(01-JAN-2050) from sor_end_date and sop_end_date.
--  3.8 5.8.0     PH   12/07/2005  Added nvl(,0) around Warranty Period as this
--                                 is what happens within the application.
--  4.0 5.13.0    PH   06-FEB-2008 Now includes its own 
--                                 set_record_status_flag procedure.
--  4.1 6.6       PJD  12-SEP-2013 Improve validation of Default Job Role
--  4.2 6.11      MOK  04-JUN-2015 MLANG lsor_code_mlang lsor_description_mlang
--                                 and lsor_keywords_mlang added
--  4.3 6.11      AJ   14-SEP-2015 lsor_description and lsor_description_mlang amended
--                                 to VARCHAR2 (4000) to match table
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hrm_schedule_of_rates
  SET lsor_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hrm_schedule_of_rates');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
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
rowid rec_rowid
,lsor_dlb_batch_id
,lsor_dl_seqno
,lsor_dl_load_status
,lsor_code
,lsor_type
,lsor_description
,lsor_start_date
,lsor_current_ind
,lsor_pre_inspect_ind
,lsor_post_inspect_ind
,lsor_hrv_itt_code
,lsor_end_date
,lsor_reorder_period_no
,lsor_reorder_period_unit
,lsor_warranty_period_no
,lsor_warranty_period_unit
,lsor_keywords
,lsor_pri_code
,lsor_hrv_vca_code
,lsor_hrv_trd_code
,lsor_hrv_lia_code
,lsor_hrv_uom_code
,lsor_wdc_code
,lsor_liability_type_ind
,lsop_start_date
,lsop_price
,lsop_end_date
,lsor_coverage_amount
,lsor_hrv_jcl_code
,lsor_quantity
,lsor_hrv_loc_code
,nvl(lsor_arc_code,'HRM') lsor_arc_code
,lsor_repeat_unit
,lsor_repeat_period_ind
,nvl(lsor_hrm_element_update_ind, 'Y')  lsor_hrm_element_update_ind
,nvl(lsor_hpm_element_update_ind, 'Y')  lsor_hpm_element_update_ind
,nvl(lsor_allow_break_ind       , 'Y')  lsor_allow_break_ind
,lsor_code_mlang
,lsor_description_mlang
,lsor_keywords_mlang
FROM dl_hrm_schedule_of_rates
WHERE lsor_dlb_batch_id   = p_batch_id       
AND   lsor_dl_load_status = 'V';              
--                 
-- Cursor to get default Job Role
--
CURSOR c_job IS
SELECT pva_char_value
FROM   parameter_values,
       parameter_definition_usages,
       area_codes
WHERE  pdu_pgp_refno          = arc_pgp_refno
AND    pdu_pdf_name           = pva_pdu_pdf_name
AND    pdu_pdf_param_type     = pva_pdu_pdf_param_type
AND    pdu_pgp_refno          = pva_pdu_pgp_refno
AND    pdu_display_seqno      = pva_pdu_display_seqno
AND    pva_pdu_pdf_name       = 'SORJR'
AND    pva_pdu_pdf_param_type = 'SYSTEM';
--
-- Constants for process_summary              
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'CREATE';            
ct       VARCHAR2(30) := 'DL_HRM_SCHEDULE_OF_RATES';                      
cs       INTEGER;                             
ce	 VARCHAR2(200);
l_id     ROWID;
--                 
-- Other variables                            
--                 
l_an_tab   VARCHAR2(1);
i          integer := 0;
l_job      VARCHAR2(255);
l_iit_code VARCHAR2(10);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hrm_schedule_of_rates.dataload_create');
fsc_utils.debug_message( 's_dl_hrm_schedule_of_rates.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lsor_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
l_iit_code :=  NULL;
l_iit_code := p1.lsor_hrv_itt_code;
--
IF (p1.lsor_type = 'S' AND l_iit_code IS NULL)
THEN
  l_iit_code := 'SOR';
END IF;
--
--
--
-- Insert into the SCHEDULE_OF_RATES table.
--
    INSERT into SCHEDULE_OF_RATES (
      sor_code,                   sor_type,
      sor_description,            sor_start_date,
      sor_current_ind,            sor_created_by,
      sor_created_date,           sor_pre_inspect_ind,
      sor_post_inspect_ind,       sor_arc_code,
      sor_arc_sys_code,           sor_hrv_itt_code,
      sor_end_date,               sor_reorder_period_no,
      sor_reorder_period_unit,    sor_warranty_period_no,
      sor_warranty_period_unit,   sor_keywords,
      sor_pri_code,               sor_hrv_vca_code,
      sor_hrv_trd_code,           sor_hrv_lia_code,
      sor_hrv_uom_code,           sor_wdc_code,
      sor_liability_type_ind,     sor_coverage_amount,
      sor_hrv_jcl_code,           sor_quantity,
      sor_hrv_loc_code,           sor_repeat_unit,
      sor_repeat_period_ind,      sor_hrm_element_update_ind,
      sor_hpm_element_update_ind, sor_allow_break_ind,
	  sor_code_mlang,        	  sor_description_mlang,	   
	  sor_keywords_mlang)
    VALUES (
      p1.lsor_code,                             p1.lsor_type,
      p1.lsor_description,                      p1.lsor_start_date,
      p1.lsor_current_ind,                      'DATALOAD',
      trunc(sysdate),                           p1.lsor_pre_inspect_ind,
      p1.lsor_post_inspect_ind,                 p1.lsor_arc_code,
      'HOU',                                    l_iit_code,
      p1.lsor_end_date,
      p1.lsor_reorder_period_no,                p1.lsor_reorder_period_unit,
      nvl(p1.lsor_warranty_period_no, 0),       nvl(p1.lsor_warranty_period_unit,'M'),
      p1.lsor_keywords,                         p1.lsor_pri_code,
      p1.lsor_hrv_vca_code,                     p1.lsor_hrv_trd_code,
      p1.lsor_hrv_lia_code,                     p1.lsor_hrv_uom_code,
      p1.lsor_wdc_code,
      nvl(p1.lsor_liability_type_ind,'O'),      p1.lsor_coverage_amount,
      p1.lsor_hrv_jcl_code,                     p1.lsor_quantity,
      p1.lsor_hrv_loc_code,                     p1.lsor_repeat_unit,
      p1.lsor_repeat_period_ind,                p1.lsor_hrm_element_update_ind,
      p1.lsor_hpm_element_update_ind,           p1.lsor_allow_break_ind,
	  p1.lsor_code_mlang,                       p1.lsor_description_mlang,			
	  p1.lsor_keywords_mlang );
--
-- Insert into sor_prices
--
    INSERT into sor_prices (
      sop_sor_code,               sop_start_date,
      sop_price,                  sop_created_by,
      sop_created_date,           sop_end_date)
    VALUES (
      p1.lsor_code,               nvl(p1.lsop_start_date,p1.lsor_start_date),
      p1.lsop_price,              'DATALOAD',
      trunc(sysdate),
      p1.lsop_end_date);
--
   OPEN c_job;
    FETCH c_job into l_job;
   CLOSE c_job;
--
    INSERT into job_role_object_rows (
      jrb_jro_code,
      jrb_obj_name,
      jrb_read_write_ind,
      jrb_pk_code1)
    VALUES (
      l_job,
      'SCHEDULE_OF_RATES',
      'Y',
      p1.lsor_code);
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
commit;
--       
--          
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SCHEDULE_OF_RATES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SOR_PRICES');
--
fsc_utils.proc_end;                           
--                 
   EXCEPTION       
      WHEN OTHERS THEN                        
      s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');            
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
CURSOR c1 is       
SELECT             
 rowid rec_rowid    
,lsor_dlb_batch_id                            
,lsor_dl_seqno
,lsor_code
,lsor_type
,lsor_description
,lsor_start_date
,lsor_current_ind
,lsor_pre_inspect_ind
,lsor_post_inspect_ind
,lsor_hrv_itt_code
,lsor_end_date
,lsor_reorder_period_no
,lsor_reorder_period_unit
,lsor_warranty_period_no
,lsor_warranty_period_unit
,lsor_keywords
,lsor_pri_code
,lsor_hrv_vca_code
,lsor_hrv_trd_code
,lsor_hrv_lia_code
,lsor_hrv_uom_code
,lsor_wdc_code
,lsor_liability_type_ind
,lsop_price
,lsop_start_date
,lsop_end_date
,lsor_coverage_amount
,lsor_hrv_jcl_code
,lsor_quantity
,lsor_hrv_loc_code
,lsor_arc_code
,lsor_repeat_unit
,lsor_repeat_period_ind
,lsor_hrm_element_update_ind
,lsor_hpm_element_update_ind
,lsor_allow_break_ind
,lsor_code_mlang
,lsor_description_mlang
,lsor_keywords_mlang
FROM dl_hrm_schedule_of_rates
WHERE lsor_dlb_batch_id      = p_batch_id     
AND   lsor_dl_load_status       in ('L','F','O');                        
--                 
-- Cursor Definitions
--
--
CURSOR c_pri (p_pri_code varchar2) is
  SELECT 'x' from priorities
  WHERE  pri_code = p_pri_code;
--
CURSOR c_sor (p_sor_code varchar2) is
  SELECT 'x' from schedule_of_rates
  WHERE  sor_code = p_sor_code;
--
CURSOR c_wdc_code (p_code varchar2) is
  SELECT 'x' from work_descriptions
  WHERE wdc_code = p_code;
--
CURSOR c_vca (p_vca_code varchar2) is
  SELECT 'X' from vat_categories
  WHERE  vca_code = p_vca_code;
--
--mlang validation for sor_code_mlang
--
CURSOR c_sor_mlang (p_sor_code_mlang varchar2) is
  SELECT 'x' from schedule_of_rates
  WHERE  sor_code_mlang = p_sor_code_mlang;
--
-- Cursor to check default Job Role exists
--
CURSOR c_job IS
SELECT pva_char_value
FROM   parameter_values,
       parameter_definition_usages,
       area_codes
WHERE  pdu_pgp_refno          = arc_pgp_refno
AND    pdu_pdf_name           = pva_pdu_pdf_name
AND    pdu_pdf_param_type     = pva_pdu_pdf_param_type
AND    pdu_pgp_refno          = pva_pdu_pgp_refno
AND    pdu_display_seqno      = pva_pdu_display_seqno
AND    pva_pdu_pdf_name       = 'SORJR'
AND    pva_pdu_pdf_param_type = 'SYSTEM';
--                 
-- Constants for process_summary              
--                 
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'VALIDATE';          
ct       VARCHAR2(30) := 'DL_HRM_SCHEDULE_OF_RATES';                      
cs       INTEGER;   
ce       VARCHAR2(200);
l_id     ROWID;
--                 
l_exists         VARCHAR2(1);                 
l_errors         VARCHAR2(10);                
l_error_ind      VARCHAR2(10);                
i                INTEGER :=0;
l_job            VARCHAR2(255);                 
--                 
BEGIN              
--                 
fsc_utils.proc_start('s_dl_hrm_schedule_of_rates.dataload_validate');     
fsc_utils.debug_message( 's_dl_hrm_schedule_of_rates.dataload_validate',3);
--                 
cb := p_batch_id;                             
cd := p_DATE;      
--                 
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');                   
--                 
FOR p1 IN c1 LOOP                             
--                 
BEGIN              
--                 
cs := p1.lsor_dl_seqno;
l_id := p1.rec_rowid;                       
--                 
l_errors := 'V';   
l_error_ind := 'N';                           
--
--
-- Check all the hou_ref_value columns
-- Schedule item type
-- 
IF p1.lsor_type = 'S'
THEN 
  IF (NOT s_dl_hem_utils.exists_frv('ITEMTYPE',p1.lsor_hrv_itt_code,'N'))
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',701);
  END IF;
--
-- Trade Type
--
  IF (NOT s_dl_hem_utils.exists_frv('TRADE',p1.lsor_hrv_trd_code,'N'))
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',702);
  END IF;
ELSIF p1.lsor_type = 'M'
THEN 
  IF p1.lsor_hrv_itt_code IS NOT NULL
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',701);
  END IF;
--
-- WDC Code   
--
  IF p1.lsor_wdc_code IS NOT NULL
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',713);
  END IF;
END IF;
--
-- Liability Code
--
IF (NOT s_dl_hem_utils.exists_frv('LIABLE',p1.lsor_hrv_lia_code,'Y'))
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',703);
END IF;
--
-- Units of Measure
--
IF (NOT s_dl_hem_utils.exists_frv('UNITS',p1.lsor_hrv_uom_code,'Y'))
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',704);
END IF;
--
-- Job Class
--
IF (NOT s_dl_hem_utils.exists_frv('JOBCLASS',p1.lsor_hrv_jcl_code,'Y'))
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',414);
END IF;
--
-- Location Code
--
IF (NOT s_dl_hem_utils.exists_frv('LOCATION',p1.lsor_hrv_loc_code,'Y'))
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',415);
END IF;
--
-- Check that the VAT code exists
--
IF p1.lsor_hrv_vca_code IS NOT NULL
THEN
  OPEN c_vca(p1.lsor_hrv_vca_code);
  FETCH c_vca INTO l_exists;
  IF c_vca%NOTFOUND
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',705);
  END IF;
  CLOSE c_vca;
END IF;
--
-- Check Y N Columns
--
-- Post Inspect Indicator
--
IF nvl(p1.lsor_post_inspect_ind,'X') not in ('Y', 'N')
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',706);
END IF;
--
-- Pre Inspect Indicator
--
IF nvl(p1.lsor_pre_inspect_ind,'X') not in ('Y', 'N')
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',707);
END IF;
--
-- Current Indicator
--
IF nVL(p1.lsor_current_ind,'X') not in ('Y', 'N')
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',708);
END IF;
--
-- Check that the priority if present exists
--
IF p1.lsor_pri_code is not null
  THEN
  OPEN c_pri (p1.lsor_pri_code);
  FETCH c_pri into l_exists;
  IF c_pri%notfound
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',709);
  END IF;
  CLOSE c_pri;
END IF;
--
-- Check that this schedule of rates code does not already exist
--
OPEN c_sor(p1.lsor_code);
 FETCH c_sor into l_exists;
IF c_sor%found
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',712);
END IF;
CLOSE c_sor;
--
-- Check the sor_type is S or M
--
IF nvl(p1.lsor_type,'NULL') not in ('S', 'M')
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',767);
END IF;
--
-- Check the reorder_period_unit is D or M
--
IF p1.lsor_reorder_period_unit is not null
  THEN
  IF p1.lsor_reorder_period_unit not in ('D', 'M')
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',768);
  END IF;
END IF;
--
-- Check Warranty Period Units is equal to M
--
IF p1.lsor_warranty_period_unit is not null
  THEN
  IF p1.lsor_warranty_period_unit != 'M'
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',769);
  END IF;
END IF;
--
-- Check the Liability Type is set to O
--
IF p1.lsor_liability_type_ind is not null
  THEN
  IF p1.lsor_liability_type_ind not in ('O','F','S')
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',770);
  END IF;
END IF;
--
-- Check the wdc_code exists if supplied
--
IF p1.lsor_wdc_code is not null
  THEN
  OPEN c_wdc_code(p1.lsor_wdc_code);
  FETCH c_wdc_code into l_exists;
  IF c_wdc_code%notfound
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',713);
  END IF;
  CLOSE c_wdc_code;
END IF;
--
-- Check that a Default Job Role has been set up
--
l_job := NULL;
--
OPEN c_job;
FETCH c_job into l_job;
CLOSE c_job;
IF l_job IS NULL
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',864);
END IF;
--
-- Check that start date has been supplied
--
IF p1.lsor_start_date is null
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',865);
END IF;
--
-- Check that a price has been supplied
--
IF p1.lsop_price is null
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',991);
END IF;
--
-- Check the description has been supplied
--
IF p1.lsor_description is null
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',992);
END IF;
--
-- Check the Area Code is either HRM or HPM
--
IF nvl(p1.lsor_arc_code, 'HRM') not in ('HPM', 'HRM')
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',435);
END IF;
--
-- Check the Repeat Period Ind is a valid value
--
  IF p1.lsor_repeat_period_ind is not null
   THEN
    IF p1.lsor_repeat_period_ind not in ('D', 'W', 'M', 'Y')
       THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',486);
    END IF;
  END IF;
--
-- Check the Element Update inds. Default to Y if null
--
  IF nvl(p1.lsor_hrm_element_update_ind, 'Y') not in ('Y', 'N')
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',487);
  END IF;
--
  IF nvl(p1.lsor_hpm_element_update_ind, 'Y') not in ('Y', 'N')
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',488);
  END IF;
--
-- Check the allow break ind
--
  IF nvl(p1.lsor_allow_break_ind, 'Y') not in ('Y', 'N')
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',489);
  END IF;
--
-- Check that this schedule of rates code mlang does not already exist
--
l_exists:=null;

OPEN c_sor_mlang(p1.lsor_code_mlang); 
FETCH c_sor_mlang into l_exists;
IF c_sor_mlang%found
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',807);
END IF;
CLOSE c_sor_mlang;
--
--addition mlang validation
--
 IF p1.lsor_description_mlang is null AND p1.lsor_code_mlang is not null 
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',803);
 END IF;
 
 IF p1.lsor_code_mlang is null AND p1.lsor_description_mlang is not null 
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',803);
 END IF;  
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
COMMIT;
--              
fsc_utils.proc_END;                         
--              
   EXCEPTION    
      WHEN OTHERS THEN            
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');             
      RAISE;
--              
END dataload_validate;                                              
--                 
--                 
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,                
                           p_date            IN DATE) IS                 
--
CURSOR c1 IS       
SELECT
 sr1.rowid sr1_rowid
,sp1.rowid sp1_rowid
,d1.rowid  rec_rowid
,d1.lsor_dlb_batch_id
,d1.lsor_dl_seqno
,d1.lsor_DL_LOAD_STATUS
,d1.lsor_code
FROM schedule_of_rates sr1, dl_hrm_schedule_of_rates d1,
     sor_prices sp1
WHERE sr1.sor_code = d1.lsor_code
AND   sp1.sop_sor_code = d1.lsor_code
AND d1.lsor_dlb_batch_id = p_batch_id
AND d1.lsor_dl_load_status = 'C';
--
--           
-- Constants for process_summary              
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'DELETE';            
ct       VARCHAR2(30) := 'DL_HRM_SCHEDULE_OF_RATES';                      
cs       INTEGER;    
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--                 
i integer := 0;    
--                 
BEGIN              
--                 
-- fsc_utils.proc_start('s_dl_hrm_schedule_of_rates.dataload_delete');       
-- fsc_utils.debug_message( 's_dl_hrm_schedule_of_rates.dataload_delete',3 );                           
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
cs := p1.lsor_dl_seqno;
l_id := p1.rec_rowid;
--
DELETE from sor_prices
WHERE rowid = p1.sp1_rowid;
--
DELETE from job_role_object_rows
WHERE jrb_obj_name = 'SCHEDULE_OF_RATES'
AND   jrb_pk_code1 = p1.lsor_code;
--
DELETE from  schedule_of_rates 
WHERE rowid = p1.sr1_rowid;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SCHEDULE_OF_RATES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SOR_PRICES');
--
fsc_utils.proc_end;            
COMMIT; 
-- 
   EXCEPTION                   
      WHEN OTHERS THEN         
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');                  
      RAISE;                   
--      
END dataload_delete;
--                 
--                 
END s_dl_hrm_schedule_of_rates;                
/
