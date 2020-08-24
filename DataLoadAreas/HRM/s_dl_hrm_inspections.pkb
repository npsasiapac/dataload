CREATE OR REPLACE PACKAGE BODY s_dl_hrm_inspections
AS                 
-- ***********************************************************************                          
--  DESCRIPTION:                            
--               
--  CHANGE CONTROL                          
--  VERSION  DB VER  WHO  WHEN        WHY         
--      1.0          PH   20/07/2001  Dataload  
--      1.1          SB   05-12-2002  Validation - ensure linked to Insp Patch.
--                                    Extra validation on Mandatory Visit fields
--                                    Create - get Insp Patch for visit details.
--      1.2          PH   06-12-2002  Minor Amendment to Cursors on validate
--      3.0  5.3.0   PH   18/01/2003  Amendements to CURSORS c_pro_aun, c_aun_aun
--                                    on validate and create to try and improve
--                                    performance (not using prop_groupings now)
--                                    also moved c_get_insp_auy to outside the
--                                    loop on validate.
--      3.1  5.13.0  PH   06/02/2008  Rewrote the Delete process, now do
--                                    visits first and also get the refno's
--                                    to improve performance.
--                                    Now includes its own 
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
  UPDATE dl_hrm_inspections
  SET lins_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hrm_inspections');
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
,lins_dlb_batch_id
,lins_dl_seqno
,lins_dl_load_status
,lins_srq_legacy_refno
,lins_seqno
,lins_raised_date
,lins_target_date
,lins_printed_ind
,lins_hrv_ity_code
,lins_hrv_irn_code
,lins_pri_code
,lins_sco_code
,lins_aun_code
,lins_pro_propref
,lins_description
,lins_issued_date
,lins_completed_date
,lins_status_date
,lins_alternative_refno
,lins_legacy_refno
,livi_shortname
,livi_sco_code
,livi_status_date
,livi_visit_date
,livi_visit_description
,livi_ire_code
,livi_result_description
FROM  dl_hrm_inspections
WHERE lins_dlb_batch_id    = p_batch_id       
AND   lins_dl_load_status = 'V';
--
-- Cursor for propref
--
CURSOR c_pro_refno(p_propref varchar2) IS    
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
-- Cursor to get Service Request Details
--
CURSOR c_srq(p_srq_leg varchar2) IS
SELECT srq_no
FROM   service_requests
WHERE  srq_legacy_refno = p_srq_leg;
--
-- Cursor for Inspector details
--
CURSOR c_ipp(p_short varchar2) IS
SELECT ipp_refno
FROM   interested_parties
WHERE  ipp_shortname = p_short;
--
-- Cursor to get reusable_refno_seq
--
CURSOR c_reuse IS
select reusable_refno_seq.nextval from dual;
--
-- Get Insp Patch AU Type
--
CURSOR c_get_insp_auy IS
select s_parameter_values.pr_get_param_values('INSPATCH','SYSTEM','SYSTEM',null) 
from dual;
--
-- Cusor to get REP AU for property
--
CURSOR c_pro_aun (p_propref varchar2, p_insp_aun_type varchar2) IS
select agr_aun_code_parent
from  admin_groupings_self,
      admin_properties,
      properties
where pro_refno           = apr_pro_refno
and   pro_propref         = p_propref
and   apr_aun_code        = agr_aun_code_child
and   agr_auy_code_parent = p_insp_aun_type;
--
-- Cursor to get REP AU for Admin Unit
--
CURSOR c_aun_aun (p_aun_code Varchar2, p_insp_aun_type varchar2) IS
select agr_aun_code_parent
from admin_groupings_self
where agr_auy_code_parent = p_insp_aun_type
and agr_aun_code_child = p_aun_code;
--                 
-- Constants for process_summary              
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'CREATE';            
ct       VARCHAR2(30) := 'DL_HRM_INSPECTIONS';                      
cs       INTEGER;                             
ce	 VARCHAR2(200);
l_id     ROWID;
--                 
-- Other variables                            
--                 
l_pro_refno number; 
l_an_tab         VARCHAR2(1);
i                integer := 0;
l_srq_no         NUMBER;
l_ipprec         NUMBER;
l_reuse          NUMBER;
l_insp_patch     VARCHAR2(20);
l_insp_aun_type   VARCHAR2(3);
--                 
BEGIN              
--                 
fsc_utils.proc_start('s_dl_hrm_inspections.dataload_create');
fsc_utils.debug_message( 's_dl_hrm_inspections.dataload_create',3);
--                 
cb := p_batch_id;                             
cd := p_date;      
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');        
--
l_insp_aun_type := null;           
--        
OPEN c_get_insp_auy;
 FETCH c_get_insp_auy INTO l_insp_aun_type;
CLOSE c_get_insp_auy;
--
FOR p1 in c1 LOOP              
--                 
BEGIN              
--                 
cs := p1.lins_dl_seqno;
l_id := p1.rec_rowid;
--
-- get the pro_refno                          
--                 
l_pro_refno := null;
l_insp_patch := null;
--                
   OPEN  c_pro_refno(p1.lins_pro_propref);        
    FETCH c_pro_refno INTO l_pro_refno;           
   CLOSE c_pro_refno;
--
   OPEN c_srq(p1.lins_srq_legacy_refno);
    FETCH c_srq into l_srq_no;
   CLOSE c_srq;
--                 
   OPEN c_ipp(p1.livi_shortname);
    FETCH c_ipp into l_ipprec;             
   CLOSE c_ipp;
--
   OPEN c_reuse;
    FETCH c_reuse into l_reuse;
   CLOSE c_reuse;
--
   IF p1.lins_pro_propref IS NOT NULL 
   THEN
     OPEN c_pro_aun(p1.lins_pro_propref,l_insp_aun_type);
      FETCH c_pro_aun into l_insp_patch;
     CLOSE c_pro_aun;
   ELSE
     OPEN c_aun_aun(p1.lins_aun_code,l_insp_aun_type);
      FETCH c_aun_aun INTO l_insp_patch;
     CLOSE c_aun_aun;
   END IF;
--
   INSERT into inspections (
    ins_srq_no,
    ins_seqno,
    ins_raised_datetime,
    ins_target_datetime,
    ins_printed_ind,
    ins_created_by,
    ins_created_date,
    ins_arc_code,
    ins_arc_sys_code,
    ins_hrv_ity_code,
    ins_hrv_irn_code,
    ins_pri_code,
    ins_sco_code,
    ins_aun_code,
    ins_pro_refno,
    ins_description,
    ins_issued_datetime,
    ins_completed_datetime,
    ins_status_date,
    ins_alternative_refno,
    ins_legacy_refno,
    ins_reusable_refno)
   VALUES (
    l_srq_no,
    p1.lins_seqno,
    p1.lins_raised_date,
    p1.lins_target_date,
    p1.lins_printed_ind,
    'DATALOAD',
    trunc(sysdate),
    'HRM',
    'HOU',
    p1.lins_hrv_ity_code,
    p1.lins_hrv_irn_code,
    p1.lins_pri_code,
    p1.lins_sco_code,
    p1.lins_aun_code,
    l_pro_refno,
    p1.lins_description,
    p1.lins_issued_date,
    p1.lins_completed_date,
    p1.lins_status_date,
    p1.lins_alternative_refno,
    p1.lins_legacy_refno,
    l_reuse);
--
   INSERT into inspection_visits (
    ivi_ins_srq_no,
    ivi_ins_seqno,
    ivi_visit_no,
    ivi_status_date,
    ivi_sco_code,
    ivi_created_by,
    ivi_created_date,
    ivi_inspection_patch,
    ivi_ipp_refno,
    ivi_visit_datetime,
    ivi_visit_description,
    ivi_result_description)
   VALUES (
    l_srq_no,
    p1.lins_seqno,
    '1',
    p1.livi_status_date,
    p1.livi_sco_code,
    'DATALOAD',
    trunc(sysdate),
    l_insp_patch,
    l_ipprec,
    p1.livi_visit_date,
    p1.livi_visit_description,
    p1.livi_result_description);
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('INSPECTIONS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('INSPECTION_VISITS');
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
cursor c1 is       
select             
rowid rec_rowid    
,lins_dlb_batch_id                            
,lins_dl_seqno
,lins_dl_load_status
,lins_srq_legacy_refno
,lins_seqno
,lins_raised_date
,lins_target_date
,lins_printed_ind
,lins_hrv_ity_code
,lins_hrv_irn_code
,lins_pri_code
,lins_sco_code
,lins_aun_code
,lins_pro_propref
,lins_description
,lins_issued_date
,lins_completed_date
,lins_status_date
,lins_alternative_refno
,lins_legacy_refno
,livi_shortname
,livi_sco_code
,livi_status_date
,livi_visit_date
,livi_visit_description
,livi_ire_code
,livi_result_description
FROM  dl_hrm_inspections
where lins_dlb_batch_id      = p_batch_id     
and   lins_dl_load_status       in ('L','F','O');
--
-- Cursor for service charge details
--
CURSOR c_srq(p_srq_leg varchar2) IS
SELECT srq_no
FROM   service_requests
WHERE  srq_legacy_refno = p_srq_leg;
--
-- Cursor for Admin Unit
--
CURSOR c_aun_code(p_aun_code varchar2) IS     
SELECT 'x'
FROM  admin_units   
WHERE  aun_code = p_aun_code;
--
-- Cursor for priority codes
--
CURSOR c_pri (p_pri_code varchar2) IS
SELECT 'x'
FROM   priorities
WHERE  pri_code = p_pri_code;
--
-- Cursor for Inspector
--
CURSOR c_ipp(p_ipp_code varchar2) IS
SELECT 'x' from interested_parties
WHERE  ipp_shortname = p_ipp_code;
--
-- Cursor for result code
--
CURSOR c_ire(p_ire_code varchar2) IS
SELECT 'x'
FROM   inspection_results
WHERE  ire_code = p_ire_code;
--
-- Get Insp Patch AU Type
--
CURSOR c_get_insp_auy IS
select s_parameter_values.pr_get_param_values('INSPATCH','SYSTEM','SYSTEM',null) 
from dual;
--
-- Cusor to get Inspection AU for property
--
CURSOR c_pro_aun (p_propref varchar2, p_insp_aun_type varchar2) IS
select 'X'
from  admin_groupings_self,
      admin_properties,
      properties
where pro_refno           = apr_pro_refno
and   pro_propref         = p_propref
and   apr_aun_code        = agr_aun_code_child
and   agr_auy_code_parent = p_insp_aun_type;
--
-- Cursor to get Inspection Patch for Admin Unit
--
CURSOR c_aun_aun (p_aun_code Varchar2, p_insp_aun_type varchar2) IS
select 'X'
from admin_groupings_self
where agr_auy_code_parent = p_insp_aun_type
and agr_aun_code_child = p_aun_code;
--                 
-- Constants for process_summary              
--                 
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'VALIDATE';          
ct       VARCHAR2(30) := 'DL_HRM_INSPECTIONS';                      
cs       INTEGER;   
ce       VARCHAR2(200);
l_id     ROWID;
--                 
l_exists         VARCHAR2(1);                 
l_pro_refno      NUMBER(10);                  
l_errors         VARCHAR2(10);                
l_error_ind      VARCHAR2(10);                
i                INTEGER :=0;
l_srq_no         NUMBER(8);               
l_insp_aun_type   VARCHAR2(3);
--                 
BEGIN              
--                 
fsc_utils.proc_start('s_dl_hrm_inspections.dataload_validate');     
fsc_utils.debug_message( 's_dl_hrm_inspections.dataload_validate',3);                          
--                 
cb := p_batch_id;                             
cd := p_DATE;      
--                 
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');  
--
l_insp_aun_type := null;
--
OPEN c_get_insp_auy;
 FETCH c_get_insp_auy INTO l_insp_aun_type;
CLOSE c_get_insp_auy;                 
--                 
FOR p1 IN c1 LOOP                             
--                 
BEGIN              
--                 
cs := p1.lins_dl_seqno;
l_id := p1.rec_rowid;
--                 
l_errors := 'V';   
l_error_ind := 'N';                           
--
--                 
-- Check that at least a prop_ref or admin_unit has been supplied
--
   IF p1.lins_pro_propref is null
     AND p1.lins_aun_code is null
     THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',723);
   END IF;
--
-- Check that only one of prop_ref and admin_unit has been supplied
--
   IF p1.lins_pro_propref is not null
     AND p1.lins_aun_code is not null
     THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',724);
   END IF;
--
-- Check that the prop_ref supplied exists on PROPERTIES
--
   IF p1.lins_pro_propref is not null
    THEN
     IF (not s_dl_hem_utils.exists_propref(p1.lins_pro_propref))               
      THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',726);         
     END IF;
   END IF;
--
-- Check that the admin unit exists on ADMIN_UNITS
--
   IF p1.lins_aun_code is not null
    THEN
     OPEN c_aun_code(p1.lins_aun_code);          
      FETCH c_aun_code into l_exists;             
       IF c_aun_code%notfound
        THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',725);        
       END IF;          
     CLOSE c_aun_code;
   END IF;
--
-- Check that prop attached to Inspection Patch (for Visit Details)
--
   IF p1.lins_pro_propref is not null
     AND p1.lins_aun_code is null
   THEN 
     OPEN c_pro_aun(p1.lins_pro_propref, l_insp_aun_type);
      FETCH c_pro_aun INTO l_exists;
      IF c_pro_aun%NOTFOUND
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',983);
      END IF;
     CLOSE c_pro_aun;
   END IF;
--
-- Check that admin Unit is attached to Inspection Patch (for Visit Details)
--
   IF p1.lins_pro_propref is null
     AND p1.lins_aun_code is not null
   THEN 
     OPEN c_aun_aun(p1.lins_aun_code, l_insp_aun_type);
      FETCH c_aun_aun INTO l_exists;
      IF c_aun_aun%NOTFOUND
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',987);
      END IF;
     CLOSE c_aun_aun;
   END IF;
--
-- Check all the hou_ref_value columns
--
-- Inspection Type
--
   IF (NOT s_dl_hem_utils.exists_frv('INS_TYPE',p1.lins_hrv_ity_code,'N'))
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',757);
   END IF;
--
-- Inspection Reason
--
   IF (NOT s_dl_hem_utils.exists_frv('INS_REASON',p1.lins_hrv_irn_code,'N'))
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',758);
   END IF;
--
-- Validate Status Code
--
   IF p1.lins_sco_code not in ('RAI','CAN','ISS','COM')
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',759);
   END IF;
--
-- Priority Code
--
   OPEN c_pri(p1.lins_pri_code);
    FETCH c_pri into l_exists;
     IF c_pri%notfound
      THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',760);
     END IF;
   CLOSE c_pri;
--
-- Validate Printed Indicator
--
   IF p1.lins_printed_ind not in ('Y','N')
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',778);
   END IF;
--                 
-- Check the Service Request Exists
--
   OPEN c_srq(p1.lins_srq_legacy_refno);
    FETCH c_srq into l_srq_no;
     IF c_srq%notfound
      THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',746);
     END IF;
   CLOSE c_srq;
--
-- Check Raised Date
--
   IF p1.lins_raised_date is null
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',795);
   END IF;
--
-- Check Target Date
--
   IF p1.lins_target_date is null
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',789);
   END IF;
--
-- Check Completed date if status is COM
--
   IF p1.lins_sco_code = 'COM'
    THEN
     IF p1.lins_completed_date is null
      THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',796);
     END IF;
   END IF;
--
-- Check Status Date
--
   IF p1.lins_status_date is null
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',777);
   END IF;
--
-- Check Inspector Shortname
--
   IF p1.livi_shortname is null
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',984);
   ELSE
     OPEN c_ipp(p1.livi_shortname);
      FETCH c_ipp into l_exists;
       IF c_ipp%notfound
        THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',761);
       END IF;
     CLOSE c_ipp;
   END IF;
--
-- Check Visit status code
--
   IF P1.livi_sco_code is null
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',985);
   ELSE
     IF p1.livi_sco_code not in ('RAI','CAN','COM')
      THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',722);
     END IF;
   END IF;
--
-- Check Visit status date sipplied
--
   IF P1.livi_status_date is null
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',986);
   END IF;
--
-- Check Result Code
--
   IF p1.livi_ire_code is not null
    THEN
     OPEN c_ire(p1.livi_ire_code);
      FETCH c_ire into l_exists;
       IF c_ire%notfound
        THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',797);
       END IF;
     CLOSE c_ire;
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
 a1.rowid rec_rowid                           
,a1.lins_dlb_batch_id                         
,a1.lins_dl_seqno                             
,a1.lins_DL_LOAD_STATUS
,b1.rowid ins_rowid
,b1.ins_srq_no
,b1.ins_seqno
FROM  dl_hrm_inspections a1,
      inspections        b1
WHERE a1.lins_dlb_batch_id = p_batch_id
AND   a1.lins_legacy_refno = b1.ins_legacy_refno
AND   a1.lins_seqno        = b1.ins_seqno
AND   a1.lins_dl_load_status    = 'C';
--           
-- Constants for process_summary              
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'DELETE';            
ct       VARCHAR2(30) := 'DL_HRM_INSPECTIONS';                      
cs       INTEGER;    
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--                 
i integer := 0;    
--                 
BEGIN              
--                 
-- fsc_utils.proc_start('s_dl_hrm_inspections.dataload_delete');       
-- fsc_utils.debug_message( 's_dl_hrm_inspections.dataload_delete',3 );                           
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
cs := p1.lins_dl_seqno;
l_id := p1.rec_rowid;                       
--
-- Delete any Visits
--
DELETE from inspection_visits
WHERE  ivi_created_by = 'DATALOAD'
AND    ivi_ins_srq_no = p1.ins_srq_no
AND    ivi_ins_seqno  = p1.ins_seqno;
--
DELETE from inspections
WHERE rowid = p1.ins_rowid;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('INSPECTIONS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('INSPECTION_VISITS');
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
END s_dl_hrm_inspections;                
/
