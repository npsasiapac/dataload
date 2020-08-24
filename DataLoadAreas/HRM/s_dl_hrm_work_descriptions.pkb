create or replace PACKAGE BODY s_dl_hrm_work_descriptions
AS                 
-- ***********************************************************************                          
--  DESCRIPTION:                            
--               
--  CHANGE CONTROL                          
--  VERSION         WHO     WHEN            WHY         
--    1.0           PH      09/07/2001      Dataload 
-- 
--    2.0	5.13.0	PH		06-FEB-2008    	Now includes its own set_record_status_flag procedure.
--    3.0   6.11    MOK     02-JUN-2015     Update for Multi Language additional Validation also.
--    4.0   6.12    AJ      25-SEP-2015     run "/" missing from end of package so does not run
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hrm_work_descriptions
  SET lwdc_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hrm_work_descriptions');
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
, lwdc_dlb_batch_id
, lwdc_dl_seqno
, lwdc_dl_load_status
, lwdc_code
, lwdc_description
, lwdc_current_ind
, lwdc_code_mlang
, lwdc_description_mlang
FROM dl_hrm_work_descriptions
WHERE lwdc_dlb_batch_id    = p_batch_id       
AND   lwdc_dl_load_status = 'V';              
--                 
--                 
-- Constants for process_summary              
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'CREATE';            
ct       VARCHAR2(30) := 'DL_HRM_WORK_DESCRIPTIONS';                      
cs       INTEGER;                             
ce	 VARCHAR2(200);
l_id     ROWID;
--                 
-- Other variables                            
--                 
l_an_tab VARCHAR2(1);
i           integer := 0;
--                 
BEGIN              
--                 
fsc_utils.proc_start('s_dl_hrm_work_descriptions.dataload_create');
fsc_utils.debug_message( 's_dl_hrm_work_descriptions.dataload_create',3);
--                 
cb := p_batch_id;                             
cd := p_date;      
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');                   
--                 
FOR p1 in c1 LOOP              
--                 
BEGIN              
--                 
cs := p1.lwdc_dl_seqno;
l_id := p1.rec_rowid;
--                 
-- Insert into work_descriptions                  
--                 
    INSERT INTO work_descriptions (
      wdc_code,
      wdc_description,
      wdc_current_ind,
      wdc_created_by,
      wdc_created_date,
	    wdc_code_mlang,
      wdc_description_mlang)
    VALUES (
      p1.lwdc_code,
      p1.lwdc_description,
      p1.lwdc_current_ind,
      'DATALOAD',
      trunc(sysdate),
	  p1.lwdc_code_mlang,
      p1.lwdc_description_mlang);


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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('WORK_DESCRIPTIONS');
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
,lwdc_dlb_batch_id                            
,lwdc_dl_seqno
,lwdc_code
,lwdc_description
,lwdc_code_mlang
,lwdc_description_mlang   
FROM  dl_hrm_work_descriptions                 
WHERE lwdc_dlb_batch_id      = p_batch_id     
AND   lwdc_dl_load_status    in ('L','F','O');                        
--
--*****************************************************
--
CURSOR c_wdc_code (p_code varchar2) is
SELECT 'x'
FROM   work_descriptions
WHERE  wdc_code = p_code;
--
--*******************************************************
--
CURSOR c_wdc_code_ml (p_code varchar2) is
SELECT 'x'
FROM   work_descriptions
WHERE  wdc_code_mlang = p_code;
--
--*******************************************************
--
CURSOR c_wdc_desc_ml (p_desc varchar2) is
SELECT 'x'
FROM   work_descriptions
WHERE  wdc_description_mlang = p_desc;
--
--*******************************************************
--
CURSOR c_lwdc_code (p_code varchar2, p_batch_id VARCHAR2) is
SELECT count(*)
FROM   dl_hrm_work_descriptions
WHERE  lwdc_code = p_code
AND    lwdc_dlb_batch_id    = p_batch_id;
--
--*******************************************************
--
CURSOR c_lwdc_code_ml (p_code varchar2,p_batch_id VARCHAR2) is
SELECT count(*)
FROM   dl_hrm_work_descriptions
WHERE  lwdc_code_mlang = p_code
AND    lwdc_dlb_batch_id    = p_batch_id;
--
--*******************************************************
--
CURSOR c_lwdc_desc_ml (p_desc varchar2, p_batch_id VARCHAR2) is
SELECT count (*)
FROM   dl_hrm_work_descriptions
WHERE  lwdc_description_mlang = p_desc
AND    lwdc_dlb_batch_id    = p_batch_id;
--
--
-- Constants for process_summary              
--                 
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'VALIDATE';          
ct       VARCHAR2(30) := 'DL_HRM_WORK_DESCRIPTIONS';                      
cs       INTEGER;   
ce       VARCHAR2(200);
l_id     ROWID;
--                 
l_exists         VARCHAR2(1);                                   
l_errors         VARCHAR2(10);                
l_error_ind      VARCHAR2(10);                
i                INTEGER :=0;  
l_count			 PLS_INTEGER :=0;        
--                 
BEGIN              
--                 
fsc_utils.proc_start('s_dl_hrm_work_descriptions.dataload_validate');     
fsc_utils.debug_message( 's_dl_hrm_work_descriptions.dataload_validate',3);
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
cs := p1.lwdc_dl_seqno;
l_id := p1.rec_rowid;                       
--                 
l_errors := 'V';   
l_error_ind := 'N';
                           
--                 
-- Check that it does not already exist on WORK_DESCRIPTION_CODES
-- 
   OPEN  c_wdc_code(p1.lwdc_code);
    FETCH c_wdc_code into l_exists;
    IF c_wdc_code%found
     THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',700);
    END IF;
   CLOSE c_wdc_code;
--                
-- Check that wdc_code_mlang it does not already exist on WORK_DESCRIPTION
-- 
l_exists:= null;

   OPEN  c_wdc_code_ml(p1.lwdc_code_mlang);
    FETCH c_wdc_code_ml into l_exists;
    IF c_wdc_code_ml%found
     THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',797);
    END IF;
   CLOSE c_wdc_code_ml;
--                 
-- Check that c_wdc_desc_mlang it does not already exist on WORK_DESCRIPTION
--
l_exists:= null;

   OPEN  c_wdc_desc_ml(p1.lwdc_description_mlang);
    FETCH c_wdc_desc_ml into l_exists;
    IF c_wdc_desc_ml%found
     THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',798);
    END IF;
   CLOSE c_wdc_desc_ml;
--                
--check for duplicates of wdc_code in dataload table
--
  OPEN c_lwdc_code (p1.lwdc_code, p1.lwdc_dlb_batch_id);
   FETCH c_lwdc_code into l_count;
  CLOSE c_lwdc_code;
  IF l_count >1 
   THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',799);
  END IF;
--                
--check for duplicates of wdc_code_mlang in dataload table
--
l_count:= null;

 OPEN c_lwdc_code_ml (p1.lwdc_code_mlang, p1.lwdc_dlb_batch_id);
  FETCH c_lwdc_code_ml into l_count;
 CLOSE c_lwdc_code_ml;
  IF l_count >1 
   THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',800);
  END IF;
--                
--check for duplicates of wdc_description_mlang in dataload table
--
l_count:= null;

 OPEN c_lwdc_desc_ml (p1.lwdc_description_mlang, p1.lwdc_dlb_batch_id);
  FETCH c_lwdc_desc_ml into l_count;
 CLOSE c_lwdc_desc_ml;
  IF l_count >1 
   THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',801);
  END IF;
--
--Additional Validation
--
 IF p1.lwdc_code is null 
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',802);
 END IF;
 
 IF p1.lwdc_description is null 
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',802);
 END IF;

 IF p1.lwdc_description_mlang is null AND p1.lwdc_code_mlang is not null 
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',803);
 END IF;
 
 IF p1.lwdc_code_mlang is null AND p1.lwdc_description_mlang is not null 
  THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',803);
 END IF;
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
 w1.rowid	rec_rowid
,d1.lwdc_dl_seqno
,d1.lwdc_dlb_batch_id
,d1.lwdc_dl_load_status
FROM  work_descriptions w1, dl_hrm_work_descriptions d1
WHERE w1.wdc_code            = d1.lwdc_code
AND   d1.lwdc_dlb_batch_id   = p_batch_id
AND   d1.lwdc_dl_load_status = 'C'; 
--           
-- Constants for process_summary              
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'DELETE';            
ct       VARCHAR2(30) := 'DL_HRM_WORK_DESCRIPTIONS';                      
cs       INTEGER;    
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--                 
i integer := 0;    
--                 
BEGIN              
--                 
-- fsc_utils.proc_start('s_dl_hrm_work_descriptions.dataload_delete');       
-- fsc_utils.debug_message( 's_dl_hrm_work_descriptions.dataload_delete',3 );                           
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
cs := p1.lwdc_dl_seqno;
l_id := p1.rec_rowid;
--
DELETE from work_descriptions
WHERE rowid = p1.rec_rowid;     
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('WORK_DESCRIPTIONS');
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
END s_dl_hrm_work_descriptions;
/