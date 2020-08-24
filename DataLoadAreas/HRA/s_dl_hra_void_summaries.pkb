CREATE OR REPLACE PACKAGE BODY s_dl_hra_void_summaries
AS
-- ***********************************************************************

  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION  DB Ver   WHO  WHEN       WHY
  --      1.0           PJD  23/10/01   Dataload
  --      2.0  5.2.0    PJD  25/07/02   Added exception handler within p5
  --      2.1  5.2.0    PJD  15/10/02   Added hps_end_date into cursor c3
  --      3.0  5.3.0    PJD  27/02/03   Slight Change to cursor C3
  --                                    now looks at the hps_start
  --      3.1  5.4.0    PJD  08/02/04   Step 5 added to remove HRA069 runs
  --      3.2  5.5.0    PJD  04/05/04   Now processes 5 dummy rows
  --      3.3  5.7.0    PJD  01/04/05   Was missing record status update for step4
  --      3.4  5.10.0   PH   06/10/06   Amended Step 5 to only remove 'DATALOAD'
  --                                    HRA069 entries
  --      3.5  5.16.1   PH   19/02/10   Step 5 still causing live sites issues
  --                                    Have therefore added a batch question
  --                                    'Skip Process to Remove HRA069 Entries 
  --                                    from Batch Runs'.
  --      3.6 6:13      PJD  22/04/16   Added in Tcy Holding Check
  --      3.7 6.17.1    PML  21/11/18   Fixed l_balance to be number(11,2)
  --
  --  declare package variables AND constants
  --
  -- Reminder This Package contains just a create proc.
  -- It goes through the following.
  -- Step 1 Set Rac_residential Ind
  -- Step 2 Set Rac Last Aba Date
  -- Step 3 Create Prop Debit Statuses
  -- Step 4 Create Summary Pro Accounts
  -- Step 5 Check Tenancy Holdings
  -- Step 5 Remove redundant HRA069 entries from Batch Runs

PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT aba_rac_Accno,max(aba_date) aba_date
FROM account_balances
GROUP BY aba_rac_accno;
--
CURSOR c2(p_rac_accno number,p_date date) is
SELECT aba_balance
FROM account_balances
WHERE aba_rac_accno = p_rac_Accno
  AND aba_date      = p_date;
--
CURSOR c3 IS
SELECT distinct apr_pro_refno,
       decode(hps_hpc_type,'C',hps_start_date -1, NULL) hps_end_date
FROM   hou_prop_statuses
      ,admin_properties
      ,admin_groupings_self
WHERE apr_aun_code  = agr_aun_code_child
  AND agr_auy_code_parent = 'REN'
  AND hps_pro_refno = apr_pro_refno
  AND nvl(hps_end_date,sysdate+1) > sysdate
  AND hps_start_date              < sysdate
  AND not exists (SELECT null 
                  FROM   prop_debit_statuses
                  WHERE  pds_pro_refno = apr_pro_refno);
--
CURSOR c4(p_pro_refno number) IS
SELECT pro_hou_acquired_date
FROM properties
WHERE pro_refno = p_pro_refno;
--
CURSOR c5 IS
SELECT pds_pro_refno pro_refno
FROM prop_debit_statuses
WHERE nvl(pds_end_date,sysdate+1) > sysdate
MINUS
SELECT spa_pro_refno pro_refno
FROM summary_pro_accounts;
--
CURSOR c6 IS
select distinct bru_aun_code,bru_hrv_ate_code
from batch_runs
where bru_mod_name = 'HRA069';
--
CURSOR c7 (p_aun_code VARCHAR2, p_ate_code VARCHAR2) IS
SELECT bru_run_no 
FROM   batch_runs
WHERE  bru_mod_name     = 'HRA069'
  AND  bru_aun_code     = p_aun_code
  AND  bru_hrv_ate_code = p_ate_code
  AND  bru_created_by   = 'DATALOAD'
ORDER BY bru_run_no DESC;
--
CURSOR c8 (p_bru_run_no NUMBER) IS
SELECT count(*)
FROM   transactions
WHERE  tra_bru_run_no = p_bru_run_no;
--
CURSOR c_tho IS
SELECT tho_pro_Refno
,      tho_tcy_refno
,      tho_start_date
,      tho_end_Date
FROM   tenancy_holdings
WHERE  tho_rac_accno IS NULL
;
--
CURSOR c_dbr(p_pro_refno NUMBER
            ,p_tcy_refno NUMBER
            ,p_start_date DATE
            ,p_end_Date   DATE) IS 
SELECT dbr_rac_accno
FROM   debit_breakdowns
,      revenue_accounts
WHERE  rac_tcy_refno = p_tcy_refno
  AND  rac_accno     = dbr_rac_accno
  AND  dbr_pro_refno = p_pro_refno
  AND  dbr_status    = 'A'
  AND  dbr_start_date BETWEEN p_start_date 
                          AND NVL(p_end_Date,dbr_start_date )
;  




l_acquired_date  DATE;
l_balance        NUMBER(11,2);
l_an_tab         VARCHAR2(1);
l_tra_found      VARCHAR2(1);
l_tra_count      PLS_INTEGER;    
l_dbr_rac_accno  PLS_INTEGER;
i                PLS_INTEGER := 0;
l_answer         VARCHAR2(1);
l_process_count  NUMBER(1);
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HRA_VOID_SUMMARIES';
cs       INTEGER;
ce       VARCHAR2(200);
--
--
BEGIN
--
-- As we don't have any records to process we will create a few dummy records
-- that we can UPDATE as passed or failed
--
--
fsc_utils.proc_start('s_dl_hra_void_summaries.dataload_create');
fsc_utils.debug_message( 's_dl_hra_void_summaries.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
--
--
-- Get the answer to the 'Skip Process to Remove HRA069 Entries from Batch Runs'
--
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
  IF l_answer = 'Y'
   THEN
    l_process_count  := 5;
  ELSE
    l_process_count  := 6;
  END IF;
--
UPDATE dl_process_summary
SET   dps_total_records  = l_process_count
,     dps_status         = 'RUNNING'
WHERE dps_dlb_batch_id   = cb
AND   dps_process        = 'CREATE'
AND   dps_date           = cd;
--
commit;
--
s_dl_utils.UPDATE_process_summary(cb,cp,cd,'RUNNING');
--
DELETE FROM dl_hra_void_summaries
WHERE lvos_dlb_batch_id = cb;
--
INSERT INTO dl_hra_void_summaries
VALUES(cb,'V',1,'SETTING RES INDICATOR');
--
INSERT INTO dl_hra_void_summaries
VALUES(cb,'V',2,'SETTING LAST BALANCE FIELDS');
--
INSERT INTO dl_hra_void_summaries
VALUES(cb,'V',3,'PROP DEBIT STATUSES');
--
INSERT INTO dl_hra_void_summaries
VALUES(cb,'V',4,'SUMMARY PRO ACCOUNTS');
--
INSERT INTO dl_hra_void_summaries
VALUES(cb,'V',5,'CHECKING THO_RAC_ACCNO');
--
-- Only insert Batch Runs if Answer not supplied
--
  IF nvl(l_answer, 'N') != 'Y'
   THEN
    INSERT INTO dl_hra_void_summaries
    VALUES(cb,'V',6,'BATCH RUNS');
  END IF;
--
--Step 1 SET Rac_residential Ind
--
BEGIN
--
cs:=1;
--
UPDATE revenue_accounts
SET rac_res_ind = 'Y'
WHERE EXISTS
(SELECT NULL FROM prop_types
                 ,prop_type_VALUES
                 ,properties
                 ,tenancy_holdings 
                 WHERE tho_rac_accno     = rac_accno
                   AND tho_pro_refno     = pro_refno
                   AND pro_hou_ptv_refno = ptv_refno
                   AND ptv_pty_code      = pty_code
                   AND pty_res_ind       = 'Y')
AND NVL(rac_res_ind,'N') != 'Y';
--
  s_dl_process_summary.UPDATE_processed_count(cb,cp,cd,'N');
  s_dl_utils.SET_record_status_flag(ct,cb,cs,'C');
--
  EXCEPTION
    WHEN OTHERS THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    s_dl_utils.SET_record_status_flag(ct,cb,cs,'O');
    s_dl_process_summary.UPDATE_processed_count(cb,cp,cd,'Y');
--
COMMIT;
--
END;
--
-- Step 2 SET Rac Last Aba Date
--
cs:=2;
--
BEGIN
--
FOR p1 in c1 LOOP
--
l_balance := 0;
i := 1+1;
--
OPEN  c2(p1.aba_rac_accno,p1.aba_date);
FETCH c2 into l_balance;
CLOSE c2;
--
UPDATE revenue_accounts
SET rac_last_aba_balance = l_balance
,   rac_last_aba_date    = p1.aba_date
WHERE rac_Accno          = p1.aba_rac_Accno;
--
IF mod(i,1000)=0 THEN COMMIT; END IF;
--
END LOOP;
--
--
  s_dl_process_summary.UPDATE_processed_count(cb,cp,cd,'N');
  s_dl_utils.SET_record_status_flag(ct,cb,cs,'C');
--
  EXCEPTION
    WHEN OTHERS THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    s_dl_utils.SET_record_status_flag(ct,cb,cs,'O');
    s_dl_process_summary.UPDATE_processed_count(cb,cp,cd,'Y');
--
END;
--
COMMIT;
-------------------------------------
--
-- Step 3 - Create Prop Debit Statuses
--
cs:=3;
--
BEGIN
--
FOR p3 IN c3 LOOP
--
BEGIN
--
l_acquired_date := null;
--
--
OPEN  c4(p3.apr_pro_refno);
FETCH c4 INTO l_acquired_date;
CLOSE c4;
--
IF l_acquired_date IS NOT NULL
THEN
  INSERT INTO prop_debit_statuses(pds_pro_refno,pds_start_date,pds_end_date)
  VALUES (p3.apr_pro_refno,l_acquired_date,p3.hps_end_date);
  i := i+1;
END IF;
--
IF mod(i,1000)=0 then COMMIT; end if;
--
EXCEPTION
WHEN OTHERS THEN
  NULL;
END;
--
END LOOP;
--
  s_dl_process_summary.UPDATE_processed_count(cb,cp,cd,'N');
  s_dl_utils.SET_record_status_flag(ct,cb,cs,'C');
--
  EXCEPTION
    WHEN OTHERS THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    s_dl_utils.SET_record_status_flag(ct,cb,cs,'O');
    s_dl_process_summary.UPDATE_processed_count(cb,cp,cd,'Y');
--
END;
--
COMMIT;
--
--
-- Step 4 Create Summary Pro Accounts
--
cs:=4;
--
BEGIN
--
  i := 0;
  FOR p5 in c5 LOOP
--
  BEGIN
--
    i := i+1; 
--
    hra_rencon.p_assign_prop_sac(p5.pro_refno,TRUNC(sysdate));
--
    IF mod(i,1000) =0 THEN COMMIT; END IF;
--
   EXCEPTION
     WHEN OTHERS THEN
     NULL;
   END;
--
  END LOOP;
--
 s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
  s_dl_utils.set_record_status_flag(ct,cb,cs,'C');
  --
  EXCEPTION
    WHEN OTHERS THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
--
-- Step 5 Check Tenancy Holdings
--
cs:=5;
--
BEGIN
--
FOR p_tho IN c_tho LOOP
--
l_dbr_rac_accno := NULL;
OPEN c_dbr (p_tho.tho_pro_Refno
           ,p_tho.tho_tcy_refno
           ,p_tho.tho_start_date
           ,p_tho.tho_end_Date);
FETCH c_dbr INTO l_dbr_rac_accno;
CLOSE c_dbr;
--
IF l_dbr_rac_accno IS NOT NULL
THEN
  UPDATE tenancy_holdings
  SET tho_rac_accno = l_dbr_rac_accno 
  WHERE tho_pro_refno = p_tho.tho_pro_Refno
    AND tho_tcy_refno = p_tho.tho_tcy_refno
    AND tho_start_date = p_tho.tho_start_date
    AND tho_rac_accno IS NULL;
END IF;
--
END LOOP;
  s_dl_process_summary.UPDATE_processed_count(cb,cp,cd,'N');
  s_dl_utils.SET_record_status_flag(ct,cb,cs,'C');
--
  EXCEPTION
    WHEN OTHERS THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    s_dl_utils.SET_record_status_flag(ct,cb,cs,'O');
    s_dl_process_summary.UPDATE_processed_count(cb,cp,cd,'Y');
--
END;
--
COMMIT;

-- Step 6 Remove Surplus HRA069 records
-- skip this process if batch question
--  answered positively
--
  IF nvl(l_answer, 'N') != 'Y'  
   THEN
--
cs:=6;
--
BEGIN
--
  FOR p6 in c6 LOOP
  --
  l_tra_found := 'N';
  --
    FOR p7 in c7(p6.bru_aun_code, p6.bru_hrv_ate_code) LOOP
    --
    IF l_tra_found = 'N'
    THEN
      OPEN  c8(p7.bru_run_no);
      FETCH c8 into l_tra_count;
      CLOSE c8;
      --
      IF l_tra_count > 0
      THEN
        l_tra_found := 'Y';
      ELSE 
        l_tra_found := 'N';
        -- 
        DELETE FROM batch_runs
        WHERE bru_run_no = p7.bru_run_no;
        --
      END IF;
    --
    END IF;
    --
    END LOOP;
  -- 
  END LOOP;
  --
  s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
  s_dl_utils.set_record_status_flag(ct,cb,cs,'C');
  --
  EXCEPTION
    WHEN OTHERS THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
--
  END IF;  /*  l_answer = 'Y'  */
--
COMMIT;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUMMARY_PRO_ACCOUNTS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PROP_DEBIT_STATUSES');
--
   EXCEPTION
   WHEN OTHERS THEN
   s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
--
--
END dataload_create;
--
END s_dl_hra_void_summaries;    
/