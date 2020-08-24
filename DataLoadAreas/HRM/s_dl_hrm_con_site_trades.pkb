CREATE OR REPLACE PACKAGE BODY s_dl_hrm_con_site_trades
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      2.0  5.2.0     PJD  10-sep-2002  Bespoke Dataload for GHT
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
 lcsr_dlb_batch_id
,lcsr_dl_seqno
,lcsr_dl_load_status
,lcsr_cos_code
,lcsr_hrv_trd_code
,lcsr_labour_day_rate
,lcsr_labour_overtime
,lcsr_trade_day_rate
,lcsr_trade_overtime
,lcsr_mileage_rate
,lcsr_call_out_charge
FROM  dl_hrm_con_site_trades
WHERE lcsr_dlb_batch_id    = p_batch_id
AND   lcsr_dl_load_status  = 'V';
--
CURSOR c_con_exists(p_cos_code varchar2) IS
SELECT 'X'
FROM   contractor_sites
WHERE  cos_code      = p_cos_code;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HRM_CON_SITE_TRADES';
cs       INTEGER;
ce	   VARCHAR2(200);
l_an_tab VARCHAR2(1);
--
-- Other variables
--
l_pro_refno number;
i           INTEGER := 0;
l_exists    VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hrm_con_site_trades.dataload_create');
fsc_utils.debug_message( 's_dl_hrm_con_site_trades.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lcsr_dl_seqno;
--
SAVEPOINT SP1;
--
--
INSERT INTO con_site_trades
  (CSR_COS_CODE       
  ,CSR_HRV_TRD_CODE   
  ,CSR_CREATED_BY     
  ,CSR_CREATED_DATE   
  ,CSR_LABOUR_DAY_RATE
  ,CSR_LABOUR_OVERTIME
  ,CSR_TRADE_DAY_RATE 
  ,CSR_TRADE_OVERTIME 
  ,CSR_MILEAGE_RATE   
  ,CSR_CALL_OUT_CHARGE
  )
  VALUES
  (p1.lCSR_COS_CODE       
  ,p1.lCSR_HRV_TRD_CODE   
  ,'DATALOAD'     
  ,SYSDATE   
  ,p1.lCSR_LABOUR_DAY_RATE
  ,p1.lCSR_LABOUR_OVERTIME
  ,p1.lCSR_TRADE_DAY_RATE 
  ,p1.lCSR_TRADE_OVERTIME 
  ,p1.lCSR_MILEAGE_RATE   
  ,p1.lCSR_CALL_OUT_CHARGE
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
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CON_SITE_TRADES');
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
 lcsr_dlb_batch_id
,lcsr_dl_seqno
,lcsr_dl_load_status
,lcsr_cos_code
,lcsr_hrv_trd_code
,lcsr_labour_day_rate
,lcsr_labour_overtime
,lcsr_trade_day_rate
,lcsr_trade_overtime
,lcsr_mileage_rate
,lcsr_call_out_charge
FROM  dl_hrm_con_site_trades
WHERE lcsr_dlb_batch_id    = p_batch_id
AND   lcsr_dl_load_status  IN ('L','F','O');
--
CURSOR c_check_for_site(p_cos_code VARCHAR2) IS
SELECT 'X'
FROM   contractor_sites
WHERE  cos_code      = p_cos_code;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRM_CON_SITE_TRADES';
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
fsc_utils.proc_start('s_dl_hrm_con_site_trades.dataload_validate');
fsc_utils.debug_message( 's_dl_hrm_con_site_trades.dataload_validate',3);
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
cs := p1.lcsr_dl_seqno;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- Check the Site already exists on the database
--
l_exists := NULL;
OPEN  c_check_for_site(p1.lcsr_cos_code);
FETCH c_check_for_site INTO l_exists;
CLOSE c_check_for_site;
IF l_exists IS NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',718);
END IF;
--
-- Check the trade code
--
IF (NOT s_dl_hem_utils.exists_frv('TRADE',p1.lcsr_hrv_trd_code,'Y'))
THEN 
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',748);
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
 lcsr_dlb_batch_id
,lcsr_dl_seqno
,lcsr_dl_load_status
,lcsr_cos_code
,lcsr_hrv_trd_code
,lcsr_labour_day_rate
,lcsr_labour_overtime
,lcsr_trade_day_rate
,lcsr_trade_overtime
,lcsr_mileage_rate
,lcsr_call_out_charge
FROM  dl_hrm_con_site_trades
WHERE lcsr_dlb_batch_id    = p_batch_id
AND   lcsr_dl_load_status  = 'C';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRM_CON_SITE_TRADES';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
--
i integer := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hrm_con_site_trades.dataload_delete');
fsc_utils.debug_message( 's_dl_hrm_con_site_trades.dataload_delete',3 );
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
cs := p1.lcsr_dl_seqno;
--
SAVEPOINT SP1;
--
--
DELETE FROM con_site_trades
WHERE csr_cos_code        = p1.lcsr_cos_code
  AND csr_hrv_trd_code    = p1.lcsr_hrv_trd_code;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CON_SITE_TRADES');
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
END s_dl_hrm_con_site_trades;
/

