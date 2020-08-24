CREATE OR REPLACE PACKAGE BODY s_dl_hpl_psl_leases        
AS                 
-- ***********************************************************************
--  DESCRIPTION:                            
--               
--  CHANGE CONTROL                          
--  VERSION DB Ver  WHO  WHEN        WHY         
--  1.0     5.7.0   DRH  29/04/2005  Product Dataload  
--  1.1     5.9.0   DRH  12/04/2006  Removed replacement of end dates
--                                   for psl_lease_rents and psl_scheme_leases when null
--  1.2     5.15.0  DRH  12/04/2006  Default PSL Rent Start and End Dates
--                                   to lease start and end dates if not supplied
--                                   had been commented out so reinstated
--  1.3     6.6.0   PJD  16/05/2013  Correct setting of Rent Start and End dates
--                                   i.e. use supplied dates if supplied.
--                                   Carry out the reinsating mentioned above.
--  1.4     6.10    MJK  18/06/2015  pslr_den_paid_to_date removed from insert into psl_lease_rents
--  1.5     6.10    AJ   18/08/2015  grant and synonym create removed from bottom as now part
--                                   of the standard data loads
--  1.6     6.13    AJ   23/02/2016  Added lpsl_rent_comments to allow for pslr_comments in Leases data load
--                                   when creating records in psl_lease_rents
--  1.7     6.14/5  AJ   24/05/2017  Amended the following after validate and primary key failures at Queensland
--                                   1)Amended create adding get psl_refno and updating DL table also amending
--                                     formats for several cursors and defining variables used
--                                   2)Added further validation required to check schemes and dates
--  1.8     6.14/5  AJ   25/05/2017  1)Further amendments to validation around dates and also to check duplicates
--                                     in actual batch being loaded all so to prevent constraint failures on create
--                                   2)l_sl_end commented out as link to scheme can be open ended
--  1.9     6.14/5  AJ   30/05/2017  Update to dl_hpl_psl_leases added to delete to remove lpsl_refno 
-- 
-- ***********************************************************************
--               
--  declare package variables and constants                            
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)     
AS
-- 
BEGIN
  UPDATE dl_hpl_psl_leases
  SET lpsl_dl_load_status = p_status
  WHERE rowid = p_rowid;
  -- 
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hpl_psl_leases');
     RAISE;
  --
END set_record_status_flag;
--
-- **************************************
--
PROCEDURE dataload_create                     
(p_batch_id          IN VARCHAR2,             
 p_date              IN DATE)                 
AS                 
--                 
CURSOR c1 IS             
SELECT
rowid rec_rowid
,lpsl_dlb_batch_id
,lpsl_dl_seqno
,lpsl_dl_load_status
,lpsl_legacy_ref
,lpsl_pro_propref
,lpsl_lease_start_date
,lpsl_lease_end_date
,lpsl_psy_code
,lpsl_sco_code
,lpsl_status_date
,lpsl_scl_psls_code
,lpsl_rent_start_date
,lpsl_rent_end_date
,lpsl_extension_end_date
,lpsl_proposed_handback_date
,lpsl_ext_notice_served_date
,lpsl_hback_not_serve_date
,lpsl_actual_handback_date
,lpsl_pslr_annual_rent
,lpsl_comments
,lpsl_scl_start_date
,lpsl_scl_end_date
,lpsl_pslr_start_date
,lpsl_pslr_end_date
,lpsl_pslr_review_date
,lpsl_llord_paid_in_advance_ind
,lpsl_refno
,lpsl_rent_comments                 
FROM  dl_hpl_psl_leases
WHERE lpsl_dlb_batch_id    = p_batch_id       
AND   lpsl_dl_load_status = 'V';
--
-- ***************
-- Cursor for propref
--
CURSOR c_pro_refno(p_propref varchar2) IS    
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
-- ***************
-- Cursor to get rent start date
--
CURSOR c_rent_start(p_lease_start_date date,
                    p_psy_code varchar2) IS
SELECT (p_lease_start_date + nvl(psy_num_of_rent_free_days,0))
FROM   psl_lease_types
WHERE  psy_code = p_psy_code;
-- ***************
-- Cursor for reusable_refno
--
CURSOR c_reuse IS
select reusable_refno_seq.nextval from dual;
-- ***************
-- Cursor for psl_refno
--
CURSOR c_pslrefno IS
select psl_refno_seq.nextval from dual;
-- ***************
--                 
-- Constants for process_summary              
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'CREATE';            
ct       VARCHAR2(30) := 'DL_HPL_PSL_LEASES';                      
cs       INTEGER;                             
ce       VARCHAR2(200);
l_id     ROWID;
--                          
--                 
-- Other variables                            
--                 
l_pro_refno   NUMBER;
l_rent_start  DATE; 
l_an_tab      VARCHAR2(1);
i             INTEGER := 0;
l_rent_end    DATE;
l_sl_start    DATE;
l_sl_end      DATE;
l_reuse       NUMBER;
l_pslrefno    NUMBER;
--
--                 
BEGIN              
--                 
fsc_utils.proc_start('s_dl_hpl_psl_leases.dataload_create');
fsc_utils.debug_message( 's_dl_hpl_psl_leases.dataload_create',3);
--                 
cb := p_batch_id;                             
cd := p_date;      
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');                   
--                 
 FOR p1 in c1 LOOP              
--                 
  BEGIN              
--                 
  cs := p1.lpsl_dl_seqno;
  l_id := p1.rec_rowid;
--
  SAVEPOINT SP1;     
--                 
  l_pro_refno  := NULL;
  l_rent_start := NULL;
  l_rent_end   := NULL;
  l_sl_start   := NULL;
  l_sl_end     := NULL;
  l_reuse      := NULL;
  l_pslrefno   := NULL;
--                
  OPEN  c_pro_refno(p1.lpsl_pro_propref);        
  FETCH c_pro_refno INTO l_pro_refno;           
  CLOSE c_pro_refno;                            
--
  l_rent_start := p1.lpsl_rent_start_date;
  IF l_rent_start IS NULL
   THEN
    OPEN  c_rent_start(p1.lpsl_lease_start_date,
                       p1.lpsl_psy_code);
    FETCH c_rent_start INTO l_rent_start;
    CLOSE c_rent_start;
  END IF;
--
-- Set the rent end date if null
--
  l_rent_end := p1.lpsl_rent_end_date;
  IF l_rent_end IS NULL
   THEN
    l_rent_end:= nvl(p1.lpsl_extension_end_date,p1.lpsl_lease_end_date);
  END IF;
--
-- Set the scheme lease start date if null
--
  l_sl_start := p1.lpsl_scl_start_date;
  IF l_sl_start IS NULL
   THEN
    l_sl_start:= p1.lpsl_lease_start_date;
  END IF;
--
-- Set the scheme lease end date if null
-- NOT USED AS END DATE CAN BE NULL so commented out (AJ - 25MAY2017)
--
--  l_sl_end := p1.lpsl_scl_end_date;
--  IF l_sl_end IS NULL
--   THEN
--    l_sl_end:= p1.lpsl_lease_end_date;
--  END IF;
--
  OPEN  c_reuse;
  FETCH c_reuse into l_reuse;
  CLOSE c_reuse;
--
  OPEN  c_pslrefno;
  FETCH c_pslrefno into l_pslrefno;
  CLOSE c_pslrefno;
--
-- Insert into psl_leases
--
INSERT into PSL_LEASES (
   psl_refno,                              psl_lease_start_date,
   psl_lease_end_date,                     psl_sco_code,
   psl_status_date,                        psl_rent_start_date,
   psl_rent_end_date,                      psl_created_by,
   psl_created_date,                       psl_llord_paid_in_advance_ind,
   psl_pro_refno,                          psl_psy_code,
   psl_reusable_refno,                     psl_ext_notice_served_date,
   psl_extension_end_date,                 psl_handback_notice_serve_date,
   psl_proposed_handback_date,             psl_actual_handback_date,
   psl_modified_by,                        psl_modified_date,
   psl_comments)
VALUES (
   l_pslrefno                             ,p1.lpsl_lease_start_date
  ,p1.lpsl_lease_end_date                 ,p1.lpsl_sco_code
  ,p1.lpsl_status_date                    ,l_rent_start 
  ,l_rent_end                             ,'DATALOAD'
  ,trunc(sysdate)                         ,p1.lpsl_llord_paid_in_advance_ind
  ,l_pro_refno                            ,p1.lpsl_psy_code
  ,l_reuse                                ,p1.lpsl_ext_notice_served_date
  ,p1.lpsl_extension_end_date             ,p1.lpsl_hback_not_serve_date
  ,p1.lpsl_proposed_handback_date         ,p1.lpsl_actual_handback_date
  ,null                                   ,null      
  ,p1.lpsl_comments
  );
--
-- Insert into psl_scheme_leases
--
INSERT into PSL_SCHEME_LEASES (
   scl_psls_code,
   scl_psl_refno,
   scl_start_date,
   scl_created_by,
   scl_created_date,
   scl_end_date,
   scl_modified_by,
   scl_modified_date)
VALUES (
   p1.lpsl_scl_psls_code
  ,l_pslrefno
  ,l_sl_start
  ,'DATALOAD'
  ,trunc(SYSDATE)
  ,p1.lpsl_scl_end_date
  , null
  , null
  );
--
-- Insert into psl_lease_rents
--
INSERT into PSL_LEASE_RENTS (
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
    l_pslrefno
   ,l_rent_start
   ,p1.lpsl_pslr_annual_rent
   ,'DATALOAD'
   ,trunc(sysdate)
   ,l_rent_end
   ,p1.lpsl_pslr_review_date
   ,null
   ,null
   ,p1.lpsl_rent_comments
   );

   UPDATE dl_hpl_psl_leases
      SET lpsl_refno = l_pslrefno
    WHERE lpsl_dlb_batch_id = p1.lpsl_dlb_batch_id
      AND lpsl_dl_seqno = p1.lpsl_dl_seqno
      AND rowid = p1.rec_rowid;
--  
-- keep a count of the rows processed and commit after every 5000
-- 
  i := i+1;
  IF MOD(i,2000)=0 
   THEN 
    COMMIT;
  END If;               
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
COMMIT; 
--          
-- Section to analyse the table(s) populated by this data load
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PSL_LEASES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PSL_SCHEME_LEASES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PSL_LEASE_RENTS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HPL_PSL_LEASES');

--
fsc_utils.proc_end;
COMMIT;                           
--                 
 EXCEPTION       
  WHEN OTHERS THEN
   set_record_status_flag(l_id,'O');                    
   s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');            
   RAISE;        
--                 
END dataload_create;                                          
--
-- **************************************
--               
PROCEDURE dataload_validate                   
     (p_batch_id          IN VARCHAR2,        
      p_date              IN DATE)            
AS                 
--                 
CURSOR c1 IS             
SELECT
rowid rec_rowid
,lpsl_dlb_batch_id
,lpsl_dl_seqno
,lpsl_dl_load_status
,lpsl_legacy_ref
,lpsl_pro_propref
,lpsl_lease_start_date
,lpsl_lease_end_date
,lpsl_psy_code
,lpsl_sco_code
,lpsl_status_date
,lpsl_scl_psls_code
,lpsl_rent_start_date
,lpsl_rent_end_date
,lpsl_extension_end_date
,lpsl_proposed_handback_date
,lpsl_ext_notice_served_date
,lpsl_hback_not_serve_date
,lpsl_actual_handback_date
,lpsl_pslr_annual_rent
,lpsl_comments
,lpsl_scl_start_date
,lpsl_scl_end_date
,lpsl_pslr_start_date
,lpsl_pslr_end_date
,lpsl_pslr_review_date
,lpsl_llord_paid_in_advance_ind
,lpsl_refno
,lpsl_rent_comments                  
FROM  dl_hpl_psl_leases
WHERE lpsl_dlb_batch_id    = p_batch_id       
AND   lpsl_dl_load_status in ('L','F','O'); 
--
-- ***************
-- Cursor for property exists in property_landlords
CURSOR c_pld(p_propref VARCHAR2) IS
SELECT 'x'
FROM  property_landlords,
      properties
WHERE pld_pro_refno = pro_refno
AND   pro_propref = p_propref;
--
-- ***************
-- Cursor for start day is consistent with lease type
--
CURSOR c_start_day(p_psl_psy_code VARCHAR2) IS
SELECT psy_lease_start_day
FROM   psl_lease_types
WHERE  psy_code = p_psl_psy_code
AND    psy_lease_start_day is not null;
--
-- ***************
-- Cursor for lease type exists in psl_lease_types
--
CURSOR c_psy_code(p_psl_psy_code VARCHAR2) IS
select 'X'
from psl_lease_types
where psy_code = p_psl_psy_code;
--
-- ***************
-- Cursor for scheme code exists in psl_schemes
--
CURSOR c_psls_code(p_psls_code VARCHAR2) IS
SELECT 'X'
FROM   psl_schemes
WHERE  psls_code = p_psls_code;
--
-- ***************
-- Cursor for scheme code type and dates exists in psl_schemes
--
CURSOR c_psls_scheme( p_psl_scl_psls_code VARCHAR2
                     ,p_start_date        DATE
                     ,p_end_date          DATE    ) IS
SELECT 'X'
FROM   psl_schemes
WHERE  psls_code = p_psl_scl_psls_code
  AND  TRUNC(psls_start_date) <= p_start_date
  AND  nvl(TRUNC(psls_actual_end_date),p_start_date) >= p_start_date
  AND  nvl(TRUNC(psls_actual_end_date),p_end_date) <= p_end_date;
--
-- ***************
-- Cursor for scheme code type and dates exists in psl_schemes
--
CURSOR c_psl_lease( p_pro_refno   NUMBER
                   ,p_start_date  DATE
                   ,p_end_date    DATE   ) IS
SELECT 'X'
FROM   psl_leases
WHERE  psl_pro_refno = p_pro_refno
  AND  TRUNC(psl_lease_start_date) <= p_start_date
  AND  TRUNC(psl_lease_end_date) >= p_start_date
UNION
SELECT 'X'
FROM   psl_leases
WHERE  psl_pro_refno = p_pro_refno
  AND  TRUNC(psl_lease_start_date) >= p_start_date
  AND  TRUNC(psl_lease_end_date) <= p_end_date
UNION
SELECT 'X'
FROM   psl_leases
WHERE  psl_pro_refno = p_pro_refno
  AND  TRUNC(psl_lease_start_date) >= p_start_date
  AND  TRUNC(psl_lease_end_date) >= p_start_date
UNION
SELECT 'X'
FROM   psl_leases
WHERE  psl_pro_refno = p_pro_refno
  AND  TRUNC(psl_lease_start_date) <= p_start_date
  AND  TRUNC(psl_extension_end_date) >= p_start_date
UNION
SELECT 'X'
FROM   psl_leases
WHERE  psl_pro_refno = p_pro_refno
  AND  TRUNC(psl_lease_start_date) >= p_start_date
  AND  TRUNC(psl_extension_end_date) <= p_end_date
UNION
SELECT 'X'
FROM   psl_leases
WHERE  psl_pro_refno = p_pro_refno
  AND  TRUNC(psl_lease_start_date) >= p_start_date
  AND  TRUNC(psl_extension_end_date) >= p_start_date;
--
-- ***************
-- Cursor for propref
--
CURSOR c_pro_refno(p_propref VARCHAR2) IS    
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
-- ***************
-- Cursor to check duplicate legacy ref load file
--
CURSOR c_lpsl_leg (cp_lpsls_legacy_ref VARCHAR2
	              ,cp_batch_id         VARCHAR2) IS
SELECT count(*)
FROM   dl_hpl_psl_leases
WHERE  lpsl_legacy_ref = cp_lpsls_legacy_ref
AND    lpsl_dlb_batch_id = cp_batch_id;
--
-- ***************
-- Cursor for checking if overlapping duplicates exist in the
-- data load file to prevent constraint failures
--
CURSOR c_lpsl_dup ( p_pro_refno   VARCHAR2
                   ,p_start_date  DATE
                   ,p_end_date    DATE
                   ,cp_batch_id   VARCHAR2
                   ,cp_legacy_ref VARCHAR2) IS
SELECT 'X'
FROM   dl_hpl_psl_leases
WHERE  lpsl_pro_propref = p_pro_refno
  AND  lpsl_lease_start_date <= p_start_date
  AND  lpsl_lease_end_date >= p_start_date
  AND  lpsl_dlb_batch_id = cp_batch_id
  AND  lpsl_lease_start_date IS NOT NULL
  AND  lpsl_lease_end_date IS NOT NULL
  AND  lpsl_legacy_ref IS NOT NULL
  AND  lpsl_legacy_ref != cp_legacy_ref
UNION
SELECT 'X'
FROM   dl_hpl_psl_leases
WHERE  lpsl_pro_propref = p_pro_refno
  AND  lpsl_lease_start_date >= p_start_date
  AND  lpsl_lease_end_date <= p_end_date
  AND  lpsl_dlb_batch_id = cp_batch_id
  AND  lpsl_lease_start_date IS NOT NULL
  AND  lpsl_lease_end_date IS NOT NULL
  AND  lpsl_legacy_ref IS NOT NULL
  AND  lpsl_legacy_ref != cp_legacy_ref
UNION
SELECT 'X'
FROM   dl_hpl_psl_leases
WHERE  lpsl_pro_propref = p_pro_refno
  AND  lpsl_lease_start_date >= p_start_date
  AND  lpsl_lease_end_date >= p_start_date
  AND  lpsl_dlb_batch_id = cp_batch_id
  AND  lpsl_lease_start_date IS NOT NULL
  AND  lpsl_lease_end_date IS NOT NULL
  AND  lpsl_legacy_ref IS NOT NULL
  AND  lpsl_legacy_ref != cp_legacy_ref
UNION
SELECT 'X'
FROM   dl_hpl_psl_leases
WHERE  lpsl_pro_propref = p_pro_refno
  AND  lpsl_lease_start_date <= p_start_date
  AND  lpsl_extension_end_date >= p_start_date
  AND  lpsl_dlb_batch_id = cp_batch_id
  AND  lpsl_lease_start_date IS NOT NULL
  AND  lpsl_extension_end_date IS NOT NULL
  AND  lpsl_legacy_ref IS NOT NULL
  AND  lpsl_legacy_ref != cp_legacy_ref
UNION
SELECT 'X'
FROM   dl_hpl_psl_leases
WHERE  lpsl_pro_propref = p_pro_refno
  AND  lpsl_lease_start_date >= p_start_date
  AND  lpsl_extension_end_date <= p_end_date
  AND  lpsl_dlb_batch_id = cp_batch_id
  AND  lpsl_lease_start_date IS NOT NULL
  AND  lpsl_extension_end_date IS NOT NULL
  AND  lpsl_legacy_ref IS NOT NULL
  AND  lpsl_legacy_ref != cp_legacy_ref
UNION
SELECT 'X'
FROM   dl_hpl_psl_leases
WHERE  lpsl_pro_propref = p_pro_refno
  AND  lpsl_lease_start_date >= p_start_date
  AND  lpsl_extension_end_date >= p_start_date
  AND  lpsl_dlb_batch_id = cp_batch_id
  AND  lpsl_lease_start_date IS NOT NULL
  AND  lpsl_extension_end_date IS NOT NULL
  AND  lpsl_legacy_ref IS NOT NULL
  AND  lpsl_legacy_ref != cp_legacy_ref;
--
-- ***************
--
-- Constants for process_summary              
--                 
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'VALIDATE';          
ct       VARCHAR2(30) := 'DL_HPL_PSL_LEASES';                      
cs       INTEGER;   
ce       VARCHAR2(200);                
l_id     ROWID;          
--                 
l_exists         VARCHAR2(1);
l_pro_exists     VARCHAR2(1);
l_psy_exists     VARCHAR2(1);
l_psls_exists    VARCHAR2(1);
l_sch_exists     VARCHAR2(1);
l_lea_exists     VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_chk_end_date   DATE;
l_count_leg      NUMBER(3);
l_lpsl_dup       VARCHAR2(1);
l_start_day      VARCHAR2(3);                  
l_errors         VARCHAR2(10);                
l_error_ind      VARCHAR2(10);                
i                INTEGER :=0;
--
-- ***************                
BEGIN              
--                 
fsc_utils.proc_start('s_dl_hpl_psl_leases.dataload_validate');     
fsc_utils.debug_message( 's_dl_hpl_psl_leases.dataload_validate',3);                          
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
  cs   := p1.lpsl_dl_seqno;
  l_id := p1.rec_rowid;                                    
  l_errors := 'V';   
  l_error_ind := 'N';
-- 
  l_exists      := NULL;
  l_pro_exists  := NULL;
  l_psy_exists  := NULL;
  l_psls_exists := NULL;
  l_sch_exists  := NULL;
  l_lea_exists  := NULL;
  l_pro_refno   := NULL;
  l_chk_end_date:= NULL;
  l_count_leg   := NULL;
  l_lpsl_dup    := NULL;
  l_start_day   := NULL;

--
--prop_ref is not null
--
  IF (p1.lpsl_pro_propref IS NULL)
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',364);
  END IF;
--
-- Check The Property exists in property_landlords
--
  IF (p1.lpsl_pro_propref IS NOT NULL)
   THEN
    OPEN c_pld(p1.lpsl_pro_propref);
    FETCH c_pld into l_pro_exists;
    CLOSE c_pld;
    IF l_pro_exists IS NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',365);
    END IF;
  END IF;
--
--prop_ref exists in properties 
--
  IF (p1.lpsl_pro_propref IS NOT NULL)
   THEN
--
    IF (not s_dl_hem_utils.exists_propref(p1.lpsl_pro_propref))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',366);
    END IF;
--
-- Get pro_refno to use in other checks
--
    OPEN  c_pro_refno(p1.lpsl_pro_propref);        
    FETCH c_pro_refno INTO l_pro_refno;           
    CLOSE c_pro_refno;                            
--
  END IF;
--
--lease_start_date is not null
--
  IF (p1.lpsl_lease_start_date IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',367);
  END IF;
--
--lease type is not null
--
  IF (p1.lpsl_psy_code IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',371);
  END IF;
--
-- Check The lease type exists
--
  IF p1.lpsl_psy_code IS NOT NULL
   THEN
    OPEN c_psy_code(p1.lpsl_psy_code);
    FETCH c_psy_code into l_psy_exists;
    CLOSE c_psy_code;
    IF l_psy_exists IS NULL
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',372);
    END IF;
  END IF;
--
-- Check the lease start day is consistent with lease type
--
  IF ( p1.lpsl_psy_code IS NOT NULL         AND
       p1.lpsl_lease_start_date IS NOT NULL     )
   THEN   
    OPEN c_start_day (p1.lpsl_psy_code);
    FETCH c_start_day into l_start_day;
    CLOSE c_start_day;	 
    IF substr(to_char(p1.lpsl_lease_start_date,'DAY'),1,3)!= l_start_day               
      AND l_start_day != NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',368);
    END IF;
  END IF;
--
--lease end date is not null
--
  IF (p1.lpsl_lease_end_date IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',369);
  END IF;
--
--lease end date not before lease start date
--
  IF ( p1.lpsl_lease_end_date IS NOT NULL         AND
       p1.lpsl_lease_start_date IS NOT NULL     )
    THEN   
     IF (p1.lpsl_lease_start_date > p1.lpsl_lease_end_date)
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',370);
     END IF;
  END IF;
--
--lease end date not later than extension end date
--
  IF ( p1.lpsl_lease_end_date IS NOT NULL         AND
       p1.lpsl_extension_end_date IS NOT NULL     )
    THEN   
     IF (p1.lpsl_lease_end_date > p1.lpsl_extension_end_date)
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',230);
     END IF;
  END IF;
--
-- Set the latest end date of the lease to check
-- this is the later of the extension or lease end date
-- to be used to check scheme and lease period
--
  IF (p1.lpsl_lease_end_date IS NOT NULL)
   THEN  
    l_chk_end_date := p1.lpsl_extension_end_date;
    IF l_chk_end_date IS NULL
     THEN
      l_chk_end_date:= p1.lpsl_lease_end_date;
    END IF;
  END IF;
--
--status code is not null 
--
  IF (p1.lpsl_sco_code IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',373);
  END IF;
--
--status code is valid
--
  IF (p1.lpsl_sco_code IS NOT NULL)
   THEN  
    IF p1.lpsl_sco_code not in ('CUR','EXT','HBK','ORN','SUS')
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',374);
    END IF;
  END IF;
--
--status date is not null
--
  IF (p1.lpsl_status_date IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',375);
  END IF;
--
--scheme code is not null
--
  IF (p1.lpsl_scl_psls_code IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',376);
  END IF;
--
-- Check The scheme code exists
--
  IF (p1.lpsl_scl_psls_code IS NOT NULL)
   THEN
    OPEN c_psls_code(p1.lpsl_scl_psls_code);
    FETCH c_psls_code into l_psls_exists;
    CLOSE c_psls_code;	
    IF l_psls_exists IS NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',377);
    END IF;
  END IF;
--
-- Check The scheme exists for the period required
-- l_chk_end_date is latest of extension or lease end date
--
  IF  ( p1.lpsl_scl_psls_code    IS NOT NULL 
   AND  p1.lpsl_lease_start_date IS NOT NULL
   AND  p1.lpsl_lease_end_date   IS NOT NULL )
    THEN
     OPEN c_psls_scheme( p1.lpsl_scl_psls_code
                        ,p1.lpsl_lease_start_date
                        ,l_chk_end_date);
     FETCH c_psls_scheme into l_sch_exists;
     CLOSE c_psls_scheme;	
    IF l_sch_exists IS NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',228);
    END IF;
  END IF;
--
-- Check legacy reference is supplied
--
  IF (p1.lpsl_legacy_ref IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',226);
  END IF;
--
-- Check Annual Rent Charge is supplied
--
  IF (p1.lpsl_pslr_annual_rent IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',227);
  END IF;
--
-- Check a lease does not exists for the period required
-- l_chk_end_date is latest of extension or lease end date
--
  IF  ( l_pro_refno              IS NOT NULL 
   AND  p1.lpsl_lease_start_date IS NOT NULL
   AND  p1.lpsl_lease_end_date   IS NOT NULL )
    THEN
     OPEN c_psl_lease( l_pro_refno
                      ,p1.lpsl_lease_start_date
                      ,l_chk_end_date);
     FETCH c_psl_lease into l_lea_exists;
     CLOSE c_psl_lease;	
    IF l_lea_exists IS NOT NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',229);
    END IF;
  END IF;
--
-- Check to make sure legacy reference is not duplicated in load file
--
  IF (p1.lpsl_legacy_ref IS NOT NULL)
   THEN
    OPEN c_lpsl_leg( p1.lpsl_legacy_ref
                    ,p1.lpsl_dlb_batch_id);
    FETCH c_lpsl_leg into l_count_leg;
    CLOSE c_lpsl_leg;
    IF l_count_leg > 1
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',231);
    END IF;
  END IF;
--
-- Check to make sure leases are not duplicated in load file
--
  IF  ( p1.lpsl_pro_propref      IS NOT NULL 
   AND  p1.lpsl_lease_start_date IS NOT NULL
   AND  p1.lpsl_lease_end_date   IS NOT NULL
   AND  p1.lpsl_legacy_ref       IS NOT NULL
   AND  l_chk_end_date           IS NOT NULL   )
    THEN
     OPEN c_lpsl_dup( p1.lpsl_pro_propref
                     ,p1.lpsl_lease_start_date
                     ,l_chk_end_date
                     ,p1.lpsl_dlb_batch_id
                     ,p1.lpsl_legacy_ref );
     FETCH c_lpsl_dup into l_lpsl_dup;
     CLOSE c_lpsl_dup;	
    IF l_lpsl_dup IS NOT NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',232);
    END IF;
  END IF;
--
-- Now Check Lease Lease Rent and Lease Scheme dates supplied
-- Use l_chk_end_date as latest of the lease end and extension dates
--
--lease end date not before lease start date
--
  IF ( p1.lpsl_lease_end_date   IS NOT NULL         AND
       p1.lpsl_lease_start_date IS NOT NULL     )
    THEN
--
     IF (p1.lpsl_rent_start_date IS NOT NULL    AND
	     p1.lpsl_lease_start_date > p1.lpsl_rent_start_date)
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',233);
     END IF;
--
     IF (p1.lpsl_rent_end_date IS NOT NULL    AND
         p1.lpsl_lease_start_date > p1.lpsl_rent_end_date)
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',234);
     END IF;
  END IF;
-- 
  IF (p1.lpsl_rent_end_date   IS NOT NULL    AND
      p1.lpsl_rent_start_date IS NOT NULL    AND
      p1.lpsl_rent_end_date < p1.lpsl_rent_start_date+1)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',235);
  END IF;
--	 
  IF (p1.lpsl_scl_start_date   IS NOT NULL    AND
      p1.lpsl_scl_end_date     IS NOT NULL    AND
      p1.lpsl_scl_end_date < p1.lpsl_scl_start_date+1)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',236);
  END IF;
--	 
  IF (p1.lpsl_pslr_start_date IS NOT NULL    AND
      p1.lpsl_pslr_end_date   IS NOT NULL    AND
      p1.lpsl_pslr_end_date < p1.lpsl_pslr_start_date)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',237);
  END IF;
--	 
  IF (p1.lpsl_pslr_start_date    IS NOT NULL    AND
      p1.lpsl_pslr_review_date   IS NOT NULL    AND
      p1.lpsl_pslr_review_date < p1.lpsl_pslr_start_date)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',239);
  END IF;
--
  IF (p1.lpsl_llord_paid_in_advance_ind	NOT IN ('Y','N'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',238);
  END IF;
--	 
  IF ( p1.lpsl_status_date    IS NOT NULL    AND
       p1.lpsl_status_date > TRUNC(sysdate)     )
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',240);
  END IF;
--       
--***************************
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
COMMIT;                         
--              
 EXCEPTION    
  WHEN OTHERS THEN            
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');             
  RAISE;
--              
END dataload_validate;                                              
--
-- **************************************
--                 
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,                
                           p_date            IN DATE) IS                 
--
CURSOR c1 IS       
SELECT        
 a1.rowid rec_rowid                           
,a1.lpsl_dlb_batch_id                         
,a1.lpsl_dl_seqno                             
,a1.lpsl_dl_load_status
,b1.rowid psl_rowid
,c1.rowid scl_rowid
,d1.rowid pslr_rowid
FROM dl_hpl_psl_leases a1,
     psl_leases b1,
     psl_scheme_leases c1,
     psl_lease_rents d1
where a1.lpsl_dlb_batch_id   = p_batch_id
AND   a1.lpsl_dl_load_status = 'C'
AND   a1.lpsl_refno          = b1.psl_refno
AND   b1.psl_refno           = c1.scl_psl_refno
and   c1.scl_psl_refno       = d1.pslr_psl_refno;
--

--           
-- Constants for process_summary              
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'DELETE';            
ct       VARCHAR2(30) := 'DL_HPL_PSL_LEASES';                      
cs       INTEGER;    
ce       VARCHAR2(200);                               
l_an_tab VARCHAR2(1);
l_id     ROWID;
--                 
i integer := 0;    
--                 
BEGIN              
--                 
fsc_utils.proC_START('s_dl_hpl_psl_leases.dataload_delete');       
fsc_utils.debug_message( 's_dl_hpl_psl_leases.dataload_delete',3 );                           
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
  cs := p1.lpsl_dl_seqno;
  l_id := p1.rec_rowid; 
--
  SAVEPOINT SP1;                      
--
  DELETE from psl_lease_rents
  WHERE  rowid = p1.pslr_rowid;
--
  DELETE from psl_scheme_leases
  WHERE  rowid = p1.scl_rowid;  
--
  DELETE from psl_leases
  WHERE  rowid = p1.psl_rowid;   
--
   UPDATE dl_hpl_psl_leases
      SET lpsl_refno = NULL
    WHERE lpsl_dlb_batch_id = p1.lpsl_dlb_batch_id
      AND lpsl_dl_seqno = p1.lpsl_dl_seqno
      AND rowid = p1.rec_rowid;
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
COMMIT; 
--       
-- Section to analyse the table(s) populated by this Dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PSL_LEASES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PSL_SCHEME_LEASES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PSL_LEASE_RENTS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HPL_PSL_LEASES');
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
END s_dl_hpl_psl_leases;                
/

show errors
