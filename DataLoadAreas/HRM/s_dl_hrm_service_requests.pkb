CREATE OR REPLACE PACKAGE BODY s_dl_hrm_service_requests        
AS                 
-- ***********************************************************************
  --  DESCRIPTION:                            
  --               
  --  CHANGE CONTROL                          
  --  VERSION  DB Vers WHO  WHEN        WHY         
  --      1.0          PH   12/07/2001  Dataload  
  --      1.1          ??               Changed LOCATION to ELELOC
  --      1.2          SB   03-12-2002  Change ELELOC back to LOCATION
  --      1.3          PH   11/12/2003  New field lsrq_created_date can now
  --                                    be supplied, used to defaut to 
  --                                    sysdate
  --      1.4  5.8.0   PH   18/07/2005  Added DB Vesion to Change Control
  --                                    Added validate on lsrq_description
  --                                    as it's mandatory.
  --      1.5  5.12.0  PH    06/02/2008 Amended validate on PRI Code as its
  --                                    not mandatory.
  --      2.0  5.13.0  PH   06-FEB-2008 Now includes its own 
  --                                    set_record_status_flag procedure.
  --      2.1  5.13.0  PH   28-NOV-2008 Added nvl around mandatory fields
  --                                    for validate process.
  --      2.2  6.3.0   PH   15-FEB-2011 Added order by to create process so that
  --                                    most recent goes in last.
  --      2.3  6.13    AJ   11-Mar-2016 c_srq cursor in create commented out as
  --                                    variable l_srq_refno not used in insert statement
  --                                    associated Batch Question removed from 
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
  UPDATE dl_hrm_service_requests
  SET lsrq_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hrm_service_requests');
     RAISE;
  --
END set_record_status_flag;
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
 rowid rec_rowid
,lsrq_dlb_batch_id
,lsrq_dl_seqno
,lsrq_dl_load_status
,lsrq_legacy_refno
,lsrq_description
,lsrq_source
,lsrq_rtr_ind
,lsrq_rechargeable_ind
,lsrq_inspection_ind
,lsrq_works_order_ind
,lsrq_sco_code
,lsrq_status_date
,lsrq_printed_ind
,lsrq_hrv_loc_code
,lsrq_pro_propref
,lsrq_aun_code
,lsrq_target_datetime
,lsrq_access_notes
,lsrq_location_notes
,lsrq_reported_by
,lsrq_reported_datetime
,lsrq_pri_code
,lsrq_access_am
,lsrq_access_pm
,lsrq_comments
,lsrq_alternative_refno
,lsrq_hrv_rbr_code
,lsrq_hrv_rmt_code
,lsrq_hrv_acc_code
,lsrq_hrv_cby_code
,nvl(lsrq_created_date, trunc(sysdate)) lsrq_created_date
FROM dl_hrm_service_requests
WHERE lsrq_dlb_batch_id    = p_batch_id       
AND   lsrq_dl_load_status = 'V'
ORDER BY lsrq_dl_seqno;
--
-- Cursor for propref
--
CURSOR c_pro_refno(p_propref varchar2) IS    
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref; 
--
-- Cursor to get reusable_refno_seq
--
CURSOR c_reuse IS
SELECT reusable_refno_seq.nextval from dual;
--
-- Cursor to get srq_refno_seq
--
CURSOR c_srq IS
SELECT srq_refno_seq.nextval from dual;
--
-- Cursor to get the location notes if none supplied
--
CURSOR c_loc_notes(p_code VARCHAR2) IS
SELECT frv_name
FROM   first_ref_values
WHERE  frv_frd_domain = 'LOCATION'
AND    frv_code       = p_code;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'CREATE';            
ct       VARCHAR2(30) := 'DL_HRM_SERVICE_REQUESTS';                      
cs       INTEGER;                             
ce	 VARCHAR2(200);
l_id     ROWID;
--                 
-- Other variables                            
--                 
l_pro_refno NUMBER; 
l_an_tab    VARCHAR2(1);
i           PLS_INTEGER := 0;
l_reuse     NUMBER;
l_loc_note  VARCHAR2(40);
l_srq_refno NUMBER(8);
l_answer    VARCHAR2(1);
--                 
BEGIN              
--                 
fsc_utils.proc_start('s_dl_hrm_service_requests.dataload_create');
fsc_utils.debug_message( 's_dl_hrm_service_requests.dataload_create',3);
--                 
cb := p_batch_id;                             
cd := p_date;      
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');                   
--         
l_answer  := s_dl_batches.get_answer(p_batch_id, 1);
--        
FOR p1 in c1 LOOP              
--                 
BEGIN              
--                 
cs := p1.lsrq_dl_seqno;
l_id := p1.rec_rowid;
--                 
-- get the pro_refno                          
--                 
l_pro_refno := null;
--                
   OPEN  c_pro_refno(p1.lsrq_pro_propref);        
    FETCH c_pro_refno INTO l_pro_refno;           
   CLOSE c_pro_refno;
--
-- Get the next reusable_refno
--
   OPEN c_reuse;
    FETCH c_reuse into l_reuse;
   CLOSE c_reuse;
--                 
-- Default the Loc Notes if not supplied
--
   l_loc_note := p1.lsrq_location_notes;
   IF l_loc_note IS NULL
   THEN
     OPEN  c_loc_notes(p1.lsrq_hrv_loc_code);
     FETCH c_loc_notes into l_loc_note;
     CLOSE c_loc_notes;
   END IF;
--
-- Insert into Service Requests
--
-- Section commented out as not used in insert
-- of srq_no field
--
--   IF NVL(l_answer,'N') = 'N'
--   THEN
--     OPEN  c_srq;
--     FETCH c_srq INTO l_srq_refno;
--     CLOSE c_srq;
--
--     ELSE 
--       l_srq_refno := TO_NUMBER(p1.lsrq_alternative_refno);
--   END IF;
   --
   INSERT into SERVICE_REQUESTS (
     srq_no,                    srq_description,
     srq_source,                srq_rtr_ind,
     srq_rechargeable_ind,      srq_inspection_ind,
     srq_works_order_ind,       srq_status_date,
     srq_printed_ind,           srq_created_by,
     srq_created_date,          srq_hrv_loc_code,
     srq_aun_code,              srq_pro_refno,
     srq_target_datetime,       srq_access_notes,
     srq_location_notes,        srq_reported_by,
     srq_reported_datetime,     srq_pri_code,
     srq_access_am,             srq_access_pm,
     srq_comments,              srq_alternative_refno,
     srq_legacy_refno,          srq_reusable_refno,
     srq_hrv_rbr_code,          srq_hrv_rmt_code,
     srq_hrv_acc_code,          srq_hrv_cby_code,
     srq_sco_code)
   VALUES (
     srq_refno_seq.nextval,     p1.lsrq_description,
     p1.lsrq_source,            p1.lsrq_rtr_ind,
     p1.lsrq_rechargeable_ind,  p1.lsrq_inspection_ind,
     p1.lsrq_works_order_ind,   p1.lsrq_status_date,
     p1.lsrq_printed_ind,       'DATALOAD',
     p1.lsrq_created_date,      p1.lsrq_hrv_loc_code,
     p1.lsrq_aun_code,          l_pro_refno,
     p1.lsrq_target_datetime,   p1.lsrq_access_notes,
     l_loc_note,                p1.lsrq_reported_by,
     p1.lsrq_reported_datetime, p1.lsrq_pri_code,
     p1.lsrq_access_am,         p1.lsrq_access_pm,
     p1.lsrq_comments,          p1.lsrq_alternative_refno,
     p1.lsrq_legacy_refno,      l_reuse,
     p1.lsrq_hrv_rbr_code,      p1.lsrq_hrv_rmt_code,
     p1.lsrq_hrv_acc_code,      p1.lsrq_hrv_cby_code,
     p1.lsrq_sco_code);
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SERVICE_REQUESTS');
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
CURSOR c1 IS       
SELECT             
rowid rec_rowid    
,lsrq_dlb_batch_id                            
,lsrq_dl_seqno
,lsrq_description
,lsrq_source
,lsrq_rtr_ind
,lsrq_rechargeable_ind
,lsrq_inspection_ind
,lsrq_works_order_ind
,lsrq_sco_code
,lsrq_status_date
,lsrq_printed_ind
,lsrq_hrv_loc_code
,lsrq_pro_propref
,lsrq_aun_code
,lsrq_pri_code
,lsrq_access_am
,lsrq_access_pm
,lsrq_hrv_rbr_code
,lsrq_hrv_rmt_code
,lsrq_hrv_acc_code
,lsrq_hrv_cby_code  
,lsrq_location_notes 
FROM  dl_hrm_service_requests     
WHERE lsrq_dlb_batch_id      = p_batch_id     
AND   lsrq_dl_load_status    in ('L','F','O');                        
--                 
-- Cursors
--
-- Cursor for Admin Unit
--
CURSOR c_aun_code(p_aun_code varchar2) IS     
SELECT 'x'        
FROM  admin_units   
WHERE aun_code = p_aun_code;
--
-- Cursor for priority codes
--
CURSOR c_pri (p_pri_code varchar2) IS
SELECT 'x' from priorities
WHERE  pri_code = p_pri_code;
--                 
-- Constants for process_summary              
--                 
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'VALIDATE';          
ct       VARCHAR2(30) := 'DL_HRM_SERVICE_REQUESTS';                      
cs       INTEGER;   
ce       VARCHAR2(200);
l_id     ROWID;
--                 
l_exists         VARCHAR2(1);                 
l_pro_refno      NUMBER(10);                  
l_errors         VARCHAR2(10);                
l_error_ind      VARCHAR2(10);                
i                INTEGER :=0;
l_suberror       VARCHAR2(1);
--                 
BEGIN              
--                 
fsc_utils.proc_start('s_dl_hrm_service_requests.dataload_validate');     
fsc_utils.debug_message( 's_dl_hrm_service_requests.dataload_validate',3);                          
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
cs := p1.lsrq_dl_seqno;
l_id := p1.rec_rowid;                       
--                 
l_errors := 'V';   
l_error_ind := 'N';                           
-- 
-- Check that at least a prop_ref or admin_unit has been supplied
--
   IF p1.lsrq_pro_propref is null
     AND p1.lsrq_aun_code is null
     THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',723);
   END IF;
--
-- Check that only one of prop_ref and admin_unit has been supplied
--
   IF p1.lsrq_pro_propref is not null
     AND p1.lsrq_aun_code is not null
     THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',724);
   END IF;
--
-- Check that the prop_ref supplied exists on PROPERTIES
--
   IF p1.lsrq_pro_propref is not null
    THEN
     IF (not s_dl_hem_utils.exists_propref(p1.lsrq_pro_propref))               
      THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',726);         
     END IF;
   END IF;
--
-- Check that the admin unit exists on ADMIN_UNITS
--
   IF p1.lsrq_aun_code is not null
    THEN
     OPEN c_aun_code(p1.lsrq_aun_code);          
      FETCH c_aun_code into l_exists;             
       IF c_aun_code%notfound
        THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',725);        
       END IF;          
     CLOSE c_aun_code;
   END IF;
--
-- Check all the hou_ref_value columns
--
-- Location Code
--
   IF (NOT s_dl_hem_utils.exists_frv('LOCATION',p1.lsrq_hrv_loc_code,'Y'))
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',773);
   END IF;
--
--
-- Reported by code
--
   IF (NOT s_dl_hem_utils.exists_frv('REP_TYPE',p1.lsrq_hrv_rbr_code,'Y'))
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',720);
   END IF;
--
-- Reporting Method code
--
   IF (NOT s_dl_hem_utils.exists_frv('REP_METHOD',p1.lsrq_hrv_rmt_code,'Y'))
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',719);
   END IF;
--
-- Access Code
--
   IF (NOT s_dl_hem_utils.exists_frv('ACCESS',p1.lsrq_hrv_acc_code,'Y'))
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',727);
   END IF;
--
-- Caused by code
--
   IF (NOT s_dl_hem_utils.exists_frv('FAU_CAUSE',p1.lsrq_hrv_cby_code,'Y'))
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',721);
   END IF;
--
-- Validation on other fields
--
-- If the location Code is Null then the location note must be supplied
--
   IF (p1.lsrq_location_notes IS NULL AND p1.lsrq_hrv_loc_code IS NULL )
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',783);
   END IF;
--
-- Source of the Request
--
   IF p1.lsrq_source not in ('MANUAL','INSAUTO','WOAUTO')
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',774);
   END IF;
--
-- Right to Repair Indicator
--
   IF nvl(p1.lsrq_rtr_ind, 'X') not in ('Y','N')
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',733);
   END IF;
--
-- Rechargeable Indicator
--
   IF nvl(p1.lsrq_rechargeable_ind, 'X') not in ('Y','N')
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',732);
   END IF;
--
-- Inspection Indicator
--
   IF nvl(p1.lsrq_inspection_ind, 'X')  not in ('Y','N')
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',775);
   END IF;
--
-- Works Order Indicator
--
   IF nvl(p1.lsrq_works_order_ind, 'X')  not in ('Y','N')
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',776);
   END IF;
--
-- Current Status of the Request
--
   IF p1.lsrq_sco_code not in ('CAN','COM','RAI')
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',597);
   END IF;
--
-- Status Date
--
   IF p1.lsrq_status_date is null
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',777);
   END IF;
--
-- Printed Indicator
--
   IF nvl(p1.lsrq_printed_ind, 'X') not in ('Y','N')
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',778);
   END IF;
--
-- Priority Code
--
   IF p1.lsrq_pri_code IS NOT NULL
    THEN
      OPEN c_pri (p1.lsrq_pri_code);
       FETCH c_pri into l_exists;
        IF c_pri%notfound
         THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',731);
        END IF;
      CLOSE c_pri;
   END IF;
--
-- Validate am pm access                                                                            
--
l_suberror := 'N';
--
   IF p1.lsrq_access_am is not null
    THEN
     FOR i in 1..7 LOOP
      IF ((substr(p1.lsrq_access_am,i,1) not in ('Y','N'))
        AND (l_suberror = 'N'))
         THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',728);
         l_suberror := 'Y';
      END IF;
     END LOOP;
   END IF;
--
l_suberror := 'N';
--
   IF p1.lsrq_access_pm is not null
    THEN
     FOR i in 1..7 LOOP
      IF ((substr(p1.lsrq_access_pm,i,1) not in ('Y','N'))
        AND (l_suberror = 'N'))
         THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',729);
         l_suberror := 'Y';
      END IF;
    END LOOP;
   END IF;
--
-- Check Description has been supplied
--
   IF p1.lsrq_description IS NULL
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',927);
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
,a1.lsrq_dlb_batch_id
,a1.lsrq_dl_seqno
,a1.lsrq_DL_LOAD_STATUS
,a1.lsrq_legacy_refno
FROM dl_hrm_service_requests a1
WHERE a1.lsrq_dlb_batch_id = p_batch_id
AND   a1.lsrq_dl_load_status = 'C';
--           
-- Constants for process_summary              
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'DELETE';            
ct       VARCHAR2(30) := 'DL_HRM_SERVICE_REQUESTS';                      
cs       INTEGER;    
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--                 
i integer := 0;    
--                 
BEGIN              
--                 
-- fsc_utils.proc_start('s_dl_hrm_service_requests.dataload_delete');       
-- fsc_utils.debug_message( 's_dl_hrm_service_requests.dataload_delete',3 );                           
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
cs := p1.lsrq_dl_seqno;
l_id := p1.rec_rowid;                       
--
DELETE FROM service_requests
WHERE  p1.lsrq_legacy_refno = srq_legacy_refno;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SERVICE_REQUESTS');
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
END s_dl_hrm_service_requests;                
/
