CREATE OR REPLACE PACKAGE BODY s_dl_hsc_lease_summaries
AS
-- ***********************************************************************

  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION  DB Ver   WHO  WHEN       WHY
  --      1.0  5.4.0    PH   07/09/03   Initial Creation
  --      2.0  5.10.0   PH   05/10/06   Amended Create Summary pro Ac's
  --                                    Don't need to do this as no VRL
  --                                    just does summary rac acc's.
  --
  --  declare package variables AND constants
  --
  -- Reminder This Package contains just a create proc.
  -- It goes through the following.
  -- Step 1 Set Rac_residential Ind
  -- Step 2 Set Rac Last Aba Date
  -- Step 3 Create Summary Rac Accounts
--
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
CURSOR c4(p_pro_refno number) IS
SELECT pro_hou_acquired_date
FROM properties
WHERE pro_refno = p_pro_refno;
--
CURSOR c5 IS
SELECT rac_accno,
       rac_hrv_ate_code
FROM   revenue_accounts
WHERE  rac_hrv_ate_code in ('SER','MWO')
AND    not exists (select null
                   from   summary_rac_accounts
                   where  rac_accno = sra_rac_accno);
--
l_acquired_date  DATE;
l_balance        NUMBER(8,2);
l_an_tab         VARCHAR2(1);
i                INTEGER := 0;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HSC_LEASE_SUMMARIES';
cs       INTEGER;
ce       VARCHAR2(200);
--
BEGIN
--
-- As we don't have any records to process we will create a few dummy records
-- that we can UPDATE as passed or failed
--
--
fsc_utils.proc_start('s_dl_hsc_lease_summaries.dataload_create');
fsc_utils.debug_message( 's_dl_hsc_lease_summaries.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
--
UPDATE dl_process_summary
SET   dps_total_records  = 3
,     dps_status         = 'RUNNING'
WHERE dps_dlb_batch_id   = cb
AND   dps_process        = 'CREATE'
AND   dps_date           = cd;
--
commit;
--
s_dl_utils.UPDATE_process_summary(cb,cp,cd,'RUNNING');
--
DELETE FROM dl_hsc_lease_summaries
WHERE lles_dlb_batch_id = cb;
--
INSERT INTO dl_hsc_lease_summaries
VALUES(cb,'V',1,'SETTING RES INDICATOR');
--
INSERT INTO dl_hsc_lease_summaries
VALUES(cb,'V',2,'SETTING LAST BALANCE FIELDS');
--
INSERT INTO dl_hsc_lease_summaries
VALUES(cb,'V',3,'SUMMARY RAC ACCOUNTS');
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
                 WHERE rac_report_pro_refno = pro_refno
                   AND pro_hou_ptv_refno    = ptv_refno
                   AND ptv_pty_code         = pty_code
                   AND pty_res_ind          = 'Y')
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
-- Step 3 Create Summary RAC Accounts
--
cs:=3;
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
    hra_rencon.p_assign_rac_sac(p5.rac_accno,p5.rac_hrv_ate_code, TRUNC(sysdate));
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
--
  s_dl_process_summary.UPDATE_processed_count(cb,cp,cd,'N');
  s_dl_utils.SET_record_status_flag(ct,cb,cs,'C');
--
  EXCEPTION
    WHEN OTHERS THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    s_dl_utils.SET_record_status_flag(ct,cb,cs,'C');
    s_dl_process_summary.UPDATE_processed_count(cb,cp,cd,'Y');
END;
--
COMMIT;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUMMARY_PRO_ACCOUNTS');
--
   EXCEPTION
   WHEN OTHERS THEN
   s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
--
--
END dataload_create;
--
END s_dl_hsc_lease_summaries;    
/



