CREATE OR REPLACE PACKAGE BODY s_dl_hpl_psl_lease_rents        
AS                 
-- ***********************************************************************                          
--  DESCRIPTION:                            
--               
--  CHANGE CONTROL                          
--  VERSION DB Ver  WHO  WHEN        WHY         
--  1.0     6.6.0   PJD  04-MAR-2013 YE Dataload for LBN
--  1.1     6.10    AJ   18-AUG-2015 pslr_den_paid_to_date removed from insert
--                                   as removed from table as v6.10
--  1.2     6.10    AJ   18-AUG-2015 removed grants and synonymy from bottom
--                                   as now included in standard data loads
--  1.3     6.10    AJ   21-AUG-2015 removed lpslr_end_existing from c1 cursors as
--                                   as not used in script users batch question instead
--  1.4     6.13    AJ   23-FEB-2015 added lpslr_comments to create no validate required
--
-- ************************************************************************
--               
--  declare package variables and constants                            
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)     
AS
-- 
BEGIN
  UPDATE dl_hpl_psl_lease_rents
  SET lpslr_dl_load_status = p_status
  WHERE rowid = p_rowid;
  -- 
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hpl_psl_lease_rents');
     RAISE;
  --
END set_record_status_flag;
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
,lpslr_dlb_batch_id
,lpslr_dl_seqno
,lpslr_dl_load_status
,lpslr_psl_ref_type
,lpslr_psl_ref
,lpslr_annual_rent
,lpslr_start_date
,lpslr_end_date
,lpslr_review_date
,lpslr_comments
FROM  dl_hpl_psl_lease_rents
WHERE lpslr_dlb_batch_id    = p_batch_id       
AND   lpslr_dl_load_status = 'V';
--
-- Cursor for propref
--
CURSOR c_psl_refno_prop(p_propref VARCHAR2
                       ,p_date    DATE  ) IS    
SELECT psl_refno
FROM   properties
,      psl_leases
WHERE  pro_propref = p_propref
  AND  pro_refno   = psl_pro_Refno
  AND  psl_lease_start_date < p_date
ORDER BY psl_lease_start_date
;
--
CURSOR c_psl_refno_psl(p_refno   VARCHAR2
                       ,p_date    DATE  ) IS    
SELECT psl_refno
FROM   properties
,      psl_leases
WHERE  psl_refno      = TO_NUMBER(p_refno)
  AND  psl_lease_start_date < p_date
ORDER BY psl_lease_start_date
;
--
-- Constants for process_summary              
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'CREATE';            
ct       VARCHAR2(30) := 'DL_HPL_PSL_LEASE_RENTS';                      
cs       INTEGER;                             
ce       VARCHAR2(200);
l_id     ROWID;
--                          
--                 
-- Other variables                            
--                 
l_pro_refno   NUMBER;
l_psl_refno   NUMBER;
l_rent_start  DATE; 
l_answer      VARCHAR2(1);
l_an_tab      VARCHAR2(1);
i             INTEGER := 0;
l_rent_end    DATE;
l_sl_start    DATE;
l_sl_end      DATE;
l_reuse       NUMBER;
--
--                 
BEGIN              
--                 
fsc_utils.proc_start('s_dl_hpl_psl_lease_rents.dataload_create');
fsc_utils.debug_message('s_dl_hpl_psl_lease_rents.dataload_create',3);
--                 
-- Get the answer to the 'End Existing '
--
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
cb := p_batch_id;                             
cd := p_date;      
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');                   
--                 
FOR p1 in c1 LOOP              
--                 
BEGIN              
--                 
cs := p1.lpslr_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;     
--                 
l_psl_refno := null;                          
--                
IF p1.lpslr_psl_ref_type = 'PROPREF'
THEN
  OPEN  c_psl_refno_prop(p1.lpslr_psl_ref,p1.lpslr_start_date);        
  FETCH c_psl_refno_prop INTO l_psl_refno;           
  CLOSE c_psl_refno_prop;                            
  --
ELSE
  OPEN  c_psl_refno_psl(p1.lpslr_psl_ref,p1.lpslr_start_date);        
  FETCH c_psl_refno_psl INTO l_psl_refno;           
  CLOSE c_psl_refno_psl;                            
  --
END IF;
--
-- End the old record
--
DELETE FROM psl_lease_rents
WHERE  pslr_psl_refno   = l_psl_refno
  AND  pslr_start_date > p1.lpslr_start_date -1
;  
--
UPDATE psl_lease_rents
SET    pslr_end_date  = p1.lpslr_start_date -1
WHERE  pslr_psl_refno = l_psl_refno
  AND  NVL(pslr_end_date,p1.lpslr_start_date) > p1.lpslr_start_date -1
;  
--
-- Insert into psl_lease_rents
--
INSERT INTO psl_lease_rents (
   pslr_psl_refno,
   pslr_start_date,
   pslr_annual_rent,
   pslr_created_by,
   pslr_created_date,
   pslr_end_date,
   pslr_review_date,
   pslr_modified_by,
   pslr_modified_date,
   pslr_comments)
VALUES (
   l_psl_refno,
   p1.lpslr_start_date,
   p1.lpslr_annual_rent,
   'DATALOAD',
   trunc(sysdate),
   p1.lpslr_end_date,
   p1.lpslr_review_date,
   null,
   null,
   p1.lpslr_comments);
--  
-- keep a count of the rows processed and commit after every 5000
-- 
i := i+1; IF MOD(i,2000)=0 THEN COMMIT; END If;               
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
--          
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PSL_LEASE_RENTS');
--
fsc_utils.proc_end;                           
--                 
   EXCEPTION       
      WHEN OTHERS THEN
      set_record_status_flag(l_id,'O');                    
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
,lpslr_dlb_batch_id
,lpslr_dl_seqno
,lpslr_dl_load_status
,lpslr_psl_ref_type
,lpslr_psl_ref
,lpslr_annual_rent
,lpslr_start_date
,lpslr_end_date
,lpslr_review_date
FROM  dl_hpl_psl_lease_rents
WHERE lpslr_dlb_batch_id    = p_batch_id       
AND   lpslr_dl_load_status in ('L','F','O'); 
--
--
CURSOR c_psl_refno_prop(p_propref VARCHAR2
                       ,p_date    DATE  ) IS    
SELECT psl_refno
FROM   properties
,      psl_leases
WHERE  pro_propref = p_propref
  AND  pro_refno   = psl_pro_Refno
  AND  psl_lease_start_date < p_date
ORDER BY psl_lease_start_date
;
--
CURSOR c_psl_refno_psl(p_refno   VARCHAR2
                       ,p_date    DATE  ) IS    
SELECT psl_refno
FROM   properties
,      psl_leases
WHERE  psl_refno      = TO_NUMBER(p_refno)
  AND  psl_lease_start_date < p_date
ORDER BY psl_lease_start_date
;
--
CURSOR c_chk_clash  (p_psl_refno  NUMBER
                    ,p_start_date DATE
                    ,p_end_Date   DATE
                    ) IS
SELECT 'Y'
FROM   psl_lease_rents
WHERE  pslr_psl_refno = p_psl_refno
  AND  p_start_date  BETWEEN pslr_start_date
                         AND NVL(pslr_end_date,p_start_date)
UNION
SELECT 'Y'
FROM   psl_lease_rents
WHERE  pslr_psl_refno = p_psl_refno
  AND  p_end_date IS NOT NULL
  AND  p_end_date  BETWEEN pslr_start_date
                         AND NVL(pslr_end_date,p_end_date)
;
--
--                 
-- Constants for process_summary              
--                 
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'VALIDATE';          
ct       VARCHAR2(30) := 'DL_HPL_PSL_LEASE_RENTS';                      
cs       INTEGER;   
ce       VARCHAR2(200);                
l_id     ROWID;          
-- 
l_psl_refno      NUMBER(10);                
l_answer         VARCHAR2(1);
l_exists         VARCHAR2(1);                 
l_start_day      VARCHAR2(3);                  
l_errors         VARCHAR2(10);                
l_error_ind      VARCHAR2(10);                
i                INTEGER :=0;
--                 
BEGIN              
--                 
fsc_utils.proc_start('s_dl_hpl_psl_lease_rents.dataload_validate'); 
fsc_utils.debug_message( 's_dl_hpl_psl_lease_rents.dataload_validate',3); 
--                 
-- Get the answer to the 'End Existing '
--
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
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
cs   := p1.lpslr_dl_seqno;
l_id := p1.rec_rowid;                       
--                 
l_errors := 'V';   
l_error_ind := 'N';
--
-- Check on the Propref/PSL Refno supplied 
--
IF p1.lpslr_psl_ref_type = 'PROPREF'
THEN
  OPEN  c_psl_refno_prop(p1.lpslr_psl_ref,p1.lpslr_start_date);        
  FETCH c_psl_refno_prop INTO l_psl_refno;           
  CLOSE c_psl_refno_prop;                            
  --
ELSE
  OPEN  c_psl_refno_psl(p1.lpslr_psl_ref,p1.lpslr_start_date);        
  FETCH c_psl_refno_psl INTO l_psl_refno;           
  CLOSE c_psl_refno_psl;                            
  --
END IF;
--
-- Check if this record dosn't clash
--
IF NVL(l_answer,'N') = 'N'
THEN
  l_exists := 'N';
  OPEN c_chk_clash(l_psl_refno,p1.lpslr_start_date,p1.lpslr_end_date);
  FETCH c_chk_clash INTO l_exists;
  CLOSE c_chk_clash;
  --
  IF NVL(l_exists,'N') = 'Y'
  THEN 
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',548);
  END IF;
END IF;
-- 
IF l_psl_refno IS NULL 
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',067);
END IF;
--
--Start Date is not null
--
IF p1.lpslr_start_date IS NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',541);
END IF;
--
--End Date before Stat Date
--
IF NVL(p1.lpslr_end_date,p1.lpslr_start_date) < p1.lpslr_start_date
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',542);
END IF;
--
IF NVL(p1.lpslr_review_date,p1.lpslr_start_date) < p1.lpslr_review_date
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',007);
END IF;
--
-- Now UPDATE the record count AND error code                            
--
IF l_errors = 'F' THEN                        
  l_error_ind := 'Y';                         
ELSE               
  l_error_ind := 'N';                         
END IF;            
--                 
-- keep a count of the rows processed and commit after every 2000        
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
rowid rec_rowid
,lpslr_dlb_batch_id
,lpslr_dl_seqno
,lpslr_dl_load_status
,lpslr_psl_ref_type
,lpslr_psl_ref
,lpslr_annual_rent
,lpslr_start_date
,lpslr_end_date
,lpslr_review_date
FROM  dl_hpl_psl_lease_rents
WHERE lpslr_dlb_batch_id    = p_batch_id       
AND   lpslr_dl_load_status = 'C'
;
--
CURSOR c_psl_refno_prop(p_propref VARCHAR2
                       ,p_date    DATE  ) IS
SELECT psl_refno
FROM   properties
,      psl_leases
WHERE  pro_propref = p_propref
  AND  pro_refno   = psl_pro_Refno
  AND  psl_lease_start_date < p_date
ORDER BY psl_lease_start_date
;
--           
-- Constants for process_summary              
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'DELETE';            
ct       VARCHAR2(30) := 'DL_HPL_PSL_LEASE_RENTS';                      
cs       INTEGER;    
ce       VARCHAR2(200);                               
l_answer VARCHAR2(1);
l_an_tab VARCHAR2(1);
l_id     ROWID;
--                 
i integer := 0;    
l_psl_refno NUMBER(10);
--                 
BEGIN              
--                 
fsc_utils.proc_start('s_dl_hpl_psl_lease_rents.dataload_delete'); 
fsc_utils.debug_message( 's_dl_hpl_psl_lease_rents.dataload_delete',3 ); 
--
-- Get the answer to the 'End Existing '
--
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
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
cs := p1.lpslr_dl_seqno;
l_id := p1.rec_rowid; 
--
SAVEPOINT SP1;                      
--
-- Check on the Propref/PSL Refno supplied 
--
IF p1.lpslr_psl_ref_type = 'PROPREF'
THEN
  OPEN  c_psl_refno_prop(p1.lpslr_psl_ref,p1.lpslr_start_date);        
  FETCH c_psl_refno_prop INTO l_psl_refno;           
  CLOSE c_psl_refno_prop;                            
  --
ELSE
  l_psl_refno := TO_NUMBER(p1.lpslr_psl_ref);        
  --
END IF;
--
DELETE from psl_lease_rents
WHERE  pslr_psl_refno = l_psl_refno
  AND  pslr_start_date = p1.lpslr_start_date
;
--
-- keep a count of the rows processed and commit after every 2000            
--      
i := i+1; IF MOD(i,2000)=0 THEN COMMIT; END IF;       
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
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PSL_LEASE_RENTS');
--
fsc_utils.proc_end;            
-- COMMIT; 
-- 
   EXCEPTION                   
      WHEN OTHERS THEN         
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');                  
      RAISE;                   
--      
END dataload_delete;
--                 
--                 
END s_dl_hpl_psl_lease_rents;                
/
show errors


