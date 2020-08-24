CREATE OR REPLACE PACKAGE BODY s_dl_hrm_pp_con_sites
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VER  DB VER  WHO  WHEN         WHY
--  1.0  5.1.4   PH   13-JUN-02  Bespoke Dataload for PFP
--  3.0  5.3.0   PH   21-JAN-02  Commented out insert into 
--                               con_site_price_groups,
--                               con_site_prices
--                               as this will be done on
--                               con_site_prices DL
--  4.0  5.11.0  PH   18-JUN-07  Added additional field for
--                               min_wo_value
--                               and commented out delete from 
--                               con_site_price_groups,
--                               con_site_prices
--                               as this will be done in
--                               con_site_prices DL
--  4.1  5.11.0  PH   28-JUN-07  Above field introduced at 5.11 
--                               AT are on 5.10 so commented out.
--  4.1  6.13.0  PJD  07-Jun-16  Removed commenting out of min_wo_val
--                               Re-introduced validation against PPP
--  4.2  6.13.0  AJ   08-Jul-16  Version control renumbered 6.0 now
--                               4.1 this note now 4.2
--                                       
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
,lppc_dlb_batch_id
,lppc_dl_seqno
,lppc_dl_load_status
,lppc_ppp_wpr_code
,lppc_ppp_ppg_code
,lppc_start_date
,lppc_cos_code
,lppc_created_by
,lppc_created_date
,lppc_fca_code
,lppc_agreed_percentage
,lppc_works_orders_count
,lppc_min_wo_value
FROM dl_hrm_pp_con_sites
WHERE lppc_dlb_batch_id    = p_batch_id
AND   lppc_dl_load_status = 'V';
--
CURSOR c_cspg_refno IS
SELECT cpg_refno_seq.nextval
FROM   DUAL;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HRM_PP_CON_SITES';
cs       INTEGER;
ce	   VARCHAR2(200);
l_an_tab VARCHAR2(1);
--
-- Other variables
--
l_pro_refno  NUMBER;
l_cspg_refno INTEGER;
i            NUMBER := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hrm_pp_con_sites.dataload_create');
fsc_utils.debug_message( 's_dl_hrm_pp_con_sites.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lppc_dl_seqno;
--
SAVEPOINT SP1;
--
--
      INSERT INTO pricing_policy_con_sites
         (
          ppc_ppp_ppg_code
         ,ppc_ppp_wpr_code
         ,ppc_ppp_start_date
         ,ppc_cos_code
         ,ppc_created_by
         ,ppc_created_date
         ,ppc_fca_code
         ,ppc_agreed_percentage
         ,ppc_works_orders_count
         ,ppc_min_wo_value
         )
      VALUES
         (
          p1.lppc_ppp_ppg_code
         ,p1.lppc_ppp_wpr_code
         ,p1.lppc_start_date
         ,p1.lppc_cos_code
         ,NVL(p1.lppc_created_by,'DATALOAD')
         ,NVL(p1.lppc_created_date,SYSDATE)
         ,p1.lppc_fca_code
         ,p1.lppc_agreed_percentage
         ,NVL(p1.lppc_works_orders_count,0)
         ,p1.lppc_min_wo_value
         );
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
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('dummy');
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
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lppc_dlb_batch_id
,lppc_dl_seqno
,lppc_dl_load_status
,lppc_ppp_wpr_code
,lppc_ppp_ppg_code
,lppc_start_date
,lppc_cos_code
,lppc_created_by
,lppc_created_date
,lppc_fca_code
,lppc_agreed_percentage
,lppc_works_orders_count
FROM  dl_hrm_pp_con_sites
WHERE lppc_dlb_batch_id      = p_batch_id
AND   lppc_dl_load_status   in ('L','F','O');
--
CURSOR c_wpr_code(p_wpr_code VARCHAR2) IS
SELECT 'X'
FROM   work_programmes
WHERE  wpr_code      = p_wpr_code;
--
CURSOR c_ppg_code(p_ppg_code VARCHAR2) IS
SELECT 'X'
FROM   pricing_policy_groups
WHERE  ppg_code      = p_ppg_code;
--
CURSOR c_ppp
  (cp_ppp_ppg_code     pricing_policy_programmes.ppp_ppg_code%TYPE
  ,cp_ppp_wpr_code     pricing_policy_programmes.ppp_wpr_code%TYPE
  ,cp_ppp_start_date   pricing_policy_programmes.ppp_start_date%TYPE
  ) 
IS
  SELECT 'X'
  FROM   pricing_policy_programmes
  WHERE  ppp_ppg_code = cp_ppp_ppg_code
  AND    ppp_wpr_code = cp_ppp_wpr_code
  AND    ppp_start_date = cp_ppp_start_date;
--
CURSOR c_cos_code(p_cos_code VARCHAR2) IS
SELECT 'X'
FROM   contractor_sites
WHERE  cos_code      = p_cos_code;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRM_PP_CON_SITES';
cs       INTEGER;
ce       VARCHAR2(200);
--
l_exists         VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hrm_pp_con_sites.dataload_validate');
fsc_utils.debug_message( 's_dl_hrm_pp_con_sites.dataload_validate',3);
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
cs := p1.lppc_dl_seqno;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- Check the Links to Other Tables
--
  l_exists := NULL;
  OPEN  c_wpr_code(p1.lppc_ppp_wpr_code);
  FETCH c_wpr_code INTO l_exists;
  IF c_wpr_code%NOTFOUND 
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',918);
  END IF;
  CLOSE c_wpr_code;
  --
  l_exists := NULL;
  OPEN  c_ppg_code(p1.lppc_ppp_ppg_code);
  FETCH c_ppg_code INTO l_exists;
  IF c_ppg_code%NOTFOUND 
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',919);
  END IF;
  CLOSE c_ppg_code;
  --
  l_exists := NULL;
  OPEN  c_cos_code(p1.lppc_cos_code);
  FETCH c_cos_code INTO l_exists;
  IF c_cos_code%NOTFOUND 
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',718);
  END IF;
  CLOSE c_cos_code;
  --
  -- Validate the Pricing policy programme
  --
  l_exists := NULL;
  OPEN  c_ppp(p1.lppc_ppp_ppg_code 
              ,p1.lppc_ppp_wpr_code 
              ,p1.lppc_start_date);  
  FETCH c_ppp INTO l_exists;
  CLOSE c_ppp;    
  IF l_exists IS NULL  
  THEN  
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',896);  
  END IF;  
  --
  -- Validate the reference value fields  
  --
  IF (NOT s_hdl_utils.exists_frv('CALENDAR',p1.lppc_fca_code,'Y'))
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',898); 
  END IF;
--
-- Check the Y/N fields   - None in this dataload
--
-- Check the other mandatory fields                      
--
  IF p1.lppc_start_date IS NULL
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',863);
  END IF;
--
--
-- Any other checks for consistancy 
-- between fields etc. - None in this dataload
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
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lppc_dlb_batch_id
,lppc_dl_seqno
,lppc_dl_load_status
,lppc_ppp_wpr_code
,lppc_ppp_ppg_code
,lppc_start_date
,lppc_cos_code
,lppc_created_by
,lppc_created_date
,lppc_fca_code
,lppc_agreed_percentage
,lppc_works_orders_count
FROM dl_hrm_pp_con_sites a1
WHERE lppc_dlb_batch_id     = p_batch_id
  AND lppc_dl_load_status   = 'C';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRM_PP_CON_SITES';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
--
i integer := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hrm_pp_con_sites.dataload_delete');
fsc_utils.debug_message( 's_dl_hrm_pp_con_sites.dataload_delete',3 );
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
cs := p1.lppc_dl_seqno;
--
SAVEPOINT SP1;
--
--DELETE FROM con_site_prices
--WHERE  csp_cspg_refno = (SELECT cspg_refno 
--                         FROM con_site_price_groups
--                         WHERE  cspg_ppc_cos_code = p1.lppc_cos_code);
--
--DELETE FROM con_site_price_groups
--WHERE  cspg_ppc_cos_code = p1.lppc_cos_code;
--
DELETE FROM pricing_policy_con_sites
WHERE ppc_ppp_wpr_code      = p1.lppc_ppp_wpr_code  
AND   ppc_ppp_ppg_code      = p1.lppc_ppp_ppg_code  
AND   ppc_ppp_start_date    = p1.lppc_start_date    
AND   ppc_cos_code          = p1.lppc_cos_code;
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
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('dummy');
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
END s_dl_hrm_pp_con_sites;
/

