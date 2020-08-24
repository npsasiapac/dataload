CREATE OR REPLACE PACKAGE BODY s_dl_hrm_con_site_prices       
AS                 
-- ***********************************************************************                          
  --  DESCRIPTION:                            
  --               
  --  CHANGE CONTROL                          
  --  VERSION  DB VER   WHO  WHEN         WHY         
  --      1.0           PH   11/07/2001   Dataload  
  --      2.0  5.2.0    PJD  06/11/2002   Changed validation of work prog
  --      2.2  5.2.0    PJD  07/NOV/2002  Slight change to create procedure
  --      2.3  5.2.0    SB   02-DEC-2002  Inc cspg_ppc_ppp_start_date on insert
  --      2.4  5.3.0    PH   29-MAY-2003  Added Validate on Pricing_policy_con_sites
  --                                      as supplied by Neil Russell
  --      3.0  5.3.0    PH   25-JUN-2003  Amended above validation to check date
  --                                      is between not equal to ppp_start.
  --      3.1  5.5.0    PJD  24-MAY-2004  Insert now uses cspg_refno_seq (rather than 
  --                                      cpg_refno_seq)
  --      3.2  5.6.0    PH   05-NOV-2004  Amended delete by setting l_cspg_refno to
  --                                      null within the loop.
  --      3.3  5.6.0    PH   11-NOV-2004  Amended create by removing nvl(...01-JAN-2050)
  --                                      from lcspg_end_date is it's not required
  --      3.4  5.8.0    PH   04-DEC-2005  Removed lcspg_end_date from validation
  --                                      check.
  --      4.0  5.13.0   PH   06-FEB-2008  Now includes its own 
  --                                      set_record_status_flag procedure.
  --      4.1  5.13.0   PH   23-JUL-2008  Amended delete process as on create
  --                                      a trigger updates the created by and
  --                                      date to be the actual user rather than
  --                                      dataload 
  --      4.2  5.15.1   PH   07-APR-2009  Amended create and deletes to resolve problem
  --                                      of created by/date trigger. Now do an update
  --                                      after initial insert.
  --                                      Also added trunc to ppc_ppp_start_dates on
  --                                      validate and create cursors and appended
  --                                      timestamp to create. Some sites do have a
  --                                      time element
  --      4.3  6.1.0    MB   25-OCT-2011  Amended validate cursor to undo change 3.0
  --      4.4  6.15     AJ   14-SEP-2017  Amended default preferred indicator to "N" if
  --                                      if not supplied as causing a oracle error at
  --                                      network "Cannot insert null into a null field"
  --               
  --  declare package variables and constants                            
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
  UPDATE dl_hrm_con_site_prices
  SET lcspg_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hrm_con_site_prices');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create                     
(p_batch_id          IN VARCHAR2,             
 p_date              IN DATE)                 
AS                 
--                 
CURSOR c1 IS             
SELECT             
 rowid rec_rowid
,lcspg_dlb_batch_id
,lcspg_dl_seqno
,lcspg_dl_load_status
,lcspg_ppc_ppp_ppg_code
,lcspg_ppc_ppp_wpr_code
,lcspg_ppc_ppp_start_date
,lcspg_ppc_cos_code
,lcspg_start_date
,lcspg_end_date
,lcsp_sor_code
,lcsp_price
,nvl(lcsp_preferred_ind,'N') lcsp_preferred_ind
FROM dl_hrm_con_site_prices
WHERE lcspg_dlb_batch_id    = p_batch_id       
AND   lcspg_dl_load_status = 'V';              
--
-- Cursors
--
CURSOR c_grp (p_pol  varchar2,
              p_wor  varchar2,
              p_psta date,
              p_cos  varchar2,
              p_csta date) IS
SELECT cspg_refno
FROM   con_site_price_groups
WHERE  cspg_ppc_ppp_ppg_code          = p_pol
AND    cspg_ppc_ppp_wpr_code          = p_wor
AND    trunc(cspg_ppc_ppp_start_date) = trunc(p_psta)
AND    cspg_ppc_cos_code              = p_cos
AND    cspg_start_date                = p_csta;
--
CURSOR c_trade (p_sor_code varchar2) IS
SELECT sor_hrv_trd_code
FROM   schedule_of_rates
WHERE  sor_code = p_sor_code;
--
CURSOR c_ppc_ppp_time (p_pol  varchar2,
                       p_wor  varchar2,
                       p_psta date,
                       p_cos  varchar2) IS
SELECT ppc_ppp_start_date
FROM   pricing_policy_con_sites
WHERE  ppc_ppp_ppg_code          = p_pol
AND    ppc_ppp_wpr_code          = p_wor
AND    trunc(ppc_ppp_start_date) = trunc(p_psta)
AND    ppc_cos_code              = p_cos;
--            
--                 
-- Constants for process_summary              
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'CREATE';            
ct       VARCHAR2(30) := 'DL_HRM_CON_SITE_PRICES';                      
cs       INTEGER;                             
ce	 VARCHAR2(200);
l_id     ROWID;
--                 
-- Other variables                            
--                 
l_an_tab                VARCHAR2(1);
i                       integer := 0;
l_grp_seqno             VARCHAR2(10);
l_trade                 VARCHAR2(10);
l_ppc_ppp_start_date    DATE;
--                 
BEGIN              
--                 
fsc_utils.proc_start('s_dl_hrm_con_site_prices.dataload_create');
fsc_utils.debug_message( 's_dl_hrm_con_site_prices.dataload_create',3);
--                 
cb := p_batch_id;                             
cd := p_date;      
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');                   
--                 
FOR p1 in c1 LOOP              
--                 
BEGIN              
--                 
cs := p1.lcspg_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- Get the date/time for the pricing policy con site record
-- Some may have a timestamp against them others may not
--
l_ppc_ppp_start_date    := NULL;
--
  OPEN c_ppc_ppp_time(p1.lcspg_ppc_ppp_ppg_code,
                      p1.lcspg_ppc_ppp_wpr_code,
                      nvl(p1.lcspg_ppc_ppp_start_date,trunc(sysdate)),
                      p1.lcspg_ppc_cos_code
                     );
   FETCH c_ppc_ppp_time INTO l_ppc_ppp_start_date;
  CLOSE c_ppc_ppp_time;
--
-- Find the Contractor Price Group associated
--
   l_grp_seqno := NULL;
--
   OPEN c_grp(p1.lcspg_ppc_ppp_ppg_code,
              p1.lcspg_ppc_ppp_wpr_code,
              l_ppc_ppp_start_date,
              p1.lcspg_ppc_cos_code,
              nvl(p1.lcspg_start_date,trunc(sysdate)));
    FETCH c_grp into l_grp_seqno;
   CLOSE c_grp;
--
    IF l_grp_seqno IS NULL
      THEN
--
       SELECT cspg_refno_seq.nextval into l_grp_seqno from dual;
--
        INSERT into con_site_price_groups
                 (cspg_refno,
                  cspg_ppc_cos_code,
                  cspg_ppc_ppp_ppg_code,
                  cspg_ppc_ppp_wpr_code,
                  cspg_ppc_ppp_start_date,
                  cspg_start_date,
                  cspg_end_date,
                  cspg_created_by,
                  cspg_created_date)
        VALUES   (l_grp_seqno,
                  p1.lcspg_ppc_cos_code,
                  p1.lcspg_ppc_ppp_ppg_code,
                  p1.lcspg_ppc_ppp_wpr_code,
                  l_ppc_ppp_start_date,
                  nvl(p1.lcspg_start_date,trunc(sysdate)),
                  p1.lcspg_end_date,
                  'DATALOAD',
                  trunc(sysdate)
                 );
--
-- Now update the record to set the correct values for
-- created_by
--
        UPDATE con_site_price_groups
           SET cspg_created_by         = 'DATALOAD'
         WHERE cspg_refno              = l_grp_seqno
           AND cspg_ppc_cos_code       = p1.lcspg_ppc_cos_code
           AND cspg_ppc_ppp_ppg_code   = p1.lcspg_ppc_ppp_ppg_code
           AND cspg_ppc_ppp_wpr_code   = p1.lcspg_ppc_ppp_wpr_code
           AND cspg_ppc_ppp_start_date = l_ppc_ppp_start_date
           AND cspg_start_date         = nvl(p1.lcspg_start_date,trunc(sysdate));
--
     END IF;
--
-- Get the trade code
--
   OPEN c_trade(p1.lcsp_sor_code);
    FETCH c_trade into l_trade;
   CLOSE c_trade;
--
-- Insert into con_site_prices
--
     INSERT into con_site_prices
                 (csp_cspg_refno,
                  csp_sor_code,
                  csp_price,
                  csp_preferred_ind,  -- error cannot insert null into null at network
                  csp_hrv_trd_code,
                  csp_created_by,
                  csp_created_date
                 )
     VALUES      
                (l_grp_seqno,
                  p1.lcsp_sor_code,
                  p1.lcsp_price,
                  p1.lcsp_preferred_ind, -- error cannot insert null into null at network
                  l_trade,
                  'DATALOAD',
                  trunc(sysdate));
--
-- Now update the record to set the correct values for
-- created_by
--
     UPDATE con_site_prices
        SET csp_created_by = 'DATALOAD'
      WHERE csp_cspg_refno = l_grp_seqno
        AND csp_sor_code   = p1.lcsp_sor_code;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CON_SITE_PRICES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CON_SITE_PRICE_GROUPS');
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
,lcspg_dlb_batch_id
,lcspg_dl_seqno
,lcspg_ppc_ppp_ppg_code
,lcspg_ppc_ppp_wpr_code
,lcspg_ppc_ppp_start_date
,lcspg_ppc_cos_code
,lcspg_start_date
,lcspg_end_date
,lcsp_sor_code
,lcsp_price
,lcsp_preferred_ind
FROM dl_hrm_con_site_prices                
WHERE lcspg_dlb_batch_id      = p_batch_id     
AND   lcspg_dl_load_status       in ('L','F','O');
--
CURSOR c_sor (p_sor_code varchar2) IS
SELECT 'x'
FROM   schedule_of_rates
WHERE  sor_code = p_sor_code;
--
CURSOR c_pol (p_pol_code varchar2) IS
SELECT 'x'
FROM   pricing_policy_groups
WHERE  ppg_code = p_pol_code;
--
CURSOR c_con (p_con_code varchar2) IS
SELECT 'x'
FROM   contractor_sites
WHERE  cos_code = p_con_code;             
--                 
CURSOR c_worp (p_wpr_code varchar2) IS
SELECT 'x'
FROM   work_programmes
WHERE  wpr_code = p_wpr_code;
--
-- NR - 09/05/2003 - New cursor to check
--      pricing_policy_con_sites.
-- PH - 25/06/2003 - Amended to check start and end
--      dates fall within ppc_ppp_start and ppp_end
--      rather than be equal to.
-- PH - Removed ed date from this check.
-- MB - 25/10/2011 - amended again to put it back to 
--      equals ppc_ppp_start_date, as ppp_start_date
--      is being passed in and not cspg_start_date.
--      This is in line with the Create cursor
--      c_ppc_ppp_time
--
CURSOR c_chk_ppc( cp_cos_code   IN VARCHAR2,
                  cp_ppg_code   IN VARCHAR2,
                  cp_wpr_code   IN VARCHAR2,
                  cp_start_date IN DATE ) IS
SELECT 'x'
FROM   pricing_policy_con_sites,
       pricing_policy_programmes
WHERE  ppc_cos_code                = cp_cos_code
AND    ppc_ppp_ppg_code            = cp_ppg_code
AND    ppc_ppp_wpr_code            = cp_wpr_code
AND    ppc_ppp_ppg_code            = ppp_ppg_code
AND    ppc_ppp_wpr_code            = ppp_wpr_code
AND    ppc_ppp_start_date          = ppp_start_date
AND    TRUNC( cp_start_date )     = TRUNC( ppc_ppp_start_date ) ;
--AND    TRUNC( nvl(cp_end_date, '01-JAN-50' ))  <= TRUNC( nvl(ppp_end_date, '01-JAN-50' ));
--
-- Constants for process_summary              
--                 
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'VALIDATE';          
ct       VARCHAR2(30) := 'DL_HRM_CON_SITE_PRICES';
cs       INTEGER;   
ce       VARCHAR2(200);
l_id     ROWID;
--                 
l_exists         VARCHAR2(1);
l_errors         VARCHAR2(10);                
l_error_ind      VARCHAR2(10);                
i                INTEGER :=0;                
--                 
BEGIN              
--                 
fsc_utils.proc_start('s_dl_hrm_con_site_prices.dataload_validate');     
fsc_utils.debug_message( 's_dl_hrm_con_site_prices.dataload_validate',3);                          
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
cs := p1.lcspg_dl_seqno;
l_id := p1.rec_rowid;
--                 
l_errors := 'V';   
l_error_ind := 'N';                           
--                 
--                
-- Test Y N Columns for preferred_ind check can be Y or N or NULL
-- create will default this to "N" if not supplied
--
   IF p1.lcsp_preferred_ind IS NOT NULL
    THEN
     IF p1.lcsp_preferred_ind not in ('Y', 'N')
      THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',686);
     END IF;
   END IF;
--
-- Check that the Schedule of Rates Code exists
--
   OPEN c_sor(p1.lcsp_sor_code);
    FETCH c_sor into l_exists;
     IF c_sor%notfound
      THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',715);
     END IF;
   CLOSE c_sor;
--
-- Check all the hou_ref_values
--
-- Work Program
--
   OPEN c_worp(p1.lcspg_ppc_ppp_wpr_code);
   FETCH c_worp INTO l_exists;
   IF c_worp%notfound
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',716);
   END IF;
   CLOSE c_worp;
--
-- Validate Policy
--
   OPEN c_pol(p1.lcspg_ppc_ppp_ppg_code);
    FETCH c_pol into l_exists;
     IF c_pol%notfound
      THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',717);
     END IF;
   CLOSE c_pol;
--
-- Validate Contractor Site
--
   OPEN c_con(p1.lcspg_ppc_cos_code);
    FETCH c_con into l_exists;
     IF c_con%notfound
      THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',718);
     END IF;
   CLOSE c_con;
--
-- Check values have been supplied For mandatory columns 
--
-- Policy Start Date
--
   IF p1.lcspg_ppc_ppp_start_date is null
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',771);
   END IF;
--
-- SOR Unit Price
--
   IF p1.lcsp_price is null
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',772);
   END IF;
--
-- NR - 09/05/2003 - Check if a corresponding record exists in
--      pricing_policy_con_sites.
--
   l_exists := NULL;
   OPEN  c_chk_ppc( p1.lcspg_ppc_cos_code,
                    p1.lcspg_ppc_ppp_ppg_code,
                    p1.lcspg_ppc_ppp_wpr_code,
                    p1.lcspg_ppc_ppp_start_date );
   FETCH c_chk_ppc into l_exists;
   IF c_chk_ppc%notfound
   OR l_exists IS NULL
   THEN
     CLOSE c_chk_ppc; 
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',917);
   ELSE
     CLOSE c_chk_ppc;
   END IF; /* c_chk_ppc%notfound */
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
 d1.rowid rec_rowid                           
,d1.lcspg_dlb_batch_id                         
,d1.lcspg_dl_seqno                             
,d1.lcspg_DL_LOAD_STATUS
,d1.lcspg_ppc_ppp_ppg_code
,d1.lcspg_ppc_ppp_wpr_code
,d1.lcspg_ppc_ppp_start_date
,d1.lcspg_ppc_cos_code
,d1.lcspg_start_date
,d1.lcsp_sor_code
FROM dl_hrm_con_site_prices d1
WHERE d1.lcspg_dlb_batch_id = p_batch_id
AND d1.lcspg_dl_load_status = 'C';
--
-- Cursor for cspg_refno
--
CURSOR c_cspg (p_pol  varchar2,
               p_wor  varchar2,
               p_psta date,
               p_cos  varchar2,
               p_csta date) IS
SELECT cspg_refno
FROM   con_site_price_groups
WHERE  cspg_ppc_ppp_ppg_code          = p_pol
AND    cspg_ppc_ppp_wpr_code          = p_wor
AND    trunc(cspg_ppc_ppp_start_date) = trunc(p_psta)
AND    cspg_ppc_cos_code              = p_cos
AND    cspg_start_date                = p_csta;
--           
-- Constants for process_summary              
cb       VARCHAR2(30);                        
cd       DATE;     
cp       VARCHAR2(30) := 'DELETE';            
ct       VARCHAR2(30) := 'DL_HRM_CON_SITE_PRICES';                      
cs       INTEGER;    
ce       VARCHAR2(200);
l_id     ROWID;
--
l_an_tab VARCHAR2(1); 
i integer := 0;
l_cspg_refno NUMBER(10);
--                 
BEGIN              
--                 
fsc_utils.proc_start('s_dl_hrm_con_site_prices.dataload_delete');       
fsc_utils.debug_message( 's_dl_hrm_con_site_prices.dataload_delete',3 );                           
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
cs := p1.lcspg_dl_seqno;
l_id := p1.rec_rowid;
--
-- Delete from con_site_prices     
--
l_cspg_refno := null;
--
   OPEN c_cspg(p1.lcspg_ppc_ppp_ppg_code,
               p1.lcspg_ppc_ppp_wpr_code,
               nvl(p1.lcspg_ppc_ppp_start_date,trunc(sysdate)),
               p1.lcspg_ppc_cos_code,
               nvl(p1.lcspg_start_date,trunc(sysdate)));
    FETCH c_cspg into l_cspg_refno;
   CLOSE c_cspg;
--
   DELETE from con_site_prices
   WHERE  csp_cspg_refno = l_cspg_refno
   AND    csp_sor_code   = p1.lcsp_sor_code
   AND    csp_created_by = 'DATALOAD'
   ;
--
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
-- Now clear up the con_site_price_groups
--
   DELETE FROM con_site_price_groups
   WHERE  cspg_created_by = 'DATALOAD'
   AND  NOT EXISTS(
                   SELECT null
                   FROM   con_site_prices
                   WHERE  csp_cspg_refno = cspg_refno);       
--
-- Section to analyse the table(s) populated by this Dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CON_SITE_PRICES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CON_SITE_PRICE_GROUPS');
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
END s_dl_hrm_con_site_prices;                
/

