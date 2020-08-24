CREATE OR REPLACE PACKAGE BODY s_dl_hra_account_balances
AS
-- ***********************************************************************

  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION     WHO  WHEN       WHY
  --      1.0     MTR  23/11/00   Dataload
  --      1.1     PH   28/02/02   Changed Validation on account balances
  --                              table, now checks rac_accno and balance
  --                              date.
  --      3.0     PH   07/04/03   Changed 2nd Exception handler on Create
--
--   2.0 5.13.0   PH   06-FEB-2008 Now includes its own 
--                                 set_record_status_flag procedure.
--
-- ***********************************************************************
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hra_account_balances
  SET laba_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_account_balances');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
  --
  --  declare package variables AND constants


PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) is
SELECT
rowid rec_rowid
,laba_dlb_batch_id
,laba_dl_seqno
,laba_dl_load_status
,laba_pay_ref
,laba_balance
,laba_date
,laba_summarise_ind
FROM dl_hra_account_balances
WHERE laba_dlb_batch_id    = p_batch_id
AND   laba_dl_load_status = 'V';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HRA_ACCOUNT_BALANCES';
cs       INTEGER;
ce	 VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_rac_accno  NUMBER;
l_sab_refno  NUMBER;
i            INTEGER := 0;
l_an_tab     VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_account_balances.dataload_create');
fsc_utils.debug_message( 's_dl_hra_account_balances.dataload_create',3);
--
cb := p_batch_id;
cd := p_DATE;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
for p1 in c1(p_batch_id) loop
--
BEGIN
--
cs := p1.laba_dl_seqno;
l_id := p1.rec_rowid;
--
 l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref
                                             (p1.laba_pay_ref);
--
 l_sab_refno := -1;
 IF p1.laba_summarise_ind = 'Y'
 THEN
   l_sab_refno := '0';
 END IF;
--
INSERT INTO account_balances
(aba_rac_accno,
 aba_date,
 aba_balance,
 aba_claim_estimated_ind,
 aba_notional_balance,
 aba_notional_updated,
 aba_hb_due,
 aba_dp_due,
 aba_sab_refno )
 values
(l_rac_accno,
 p1.laba_date,
 p1.laba_balance,
 null,
 null,
 null,
 null,
 null,
 l_sab_refno );
--
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
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
END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ACCOUNT_BALANCES');
--
fsc_utils.proc_end;
commit;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
      RAISE;
COMMIT;
--
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
,laba_dlb_batch_id
,laba_dl_seqno,
laba_pay_ref,
laba_date
FROM dl_hra_account_balances
WHERE laba_dlb_batch_id      = p_batch_id
AND   laba_dl_load_status       in ('L','F','O');
--
CURSOR c_aye (p_aun_code VARCHAR2, p_date DATE) IS
SELECT 'X'
FROM   admin_years
WHERE  aye_aun_code         = p_aun_code
  AND  aye_start_date       < p_date
  AND  aye_end_date         > p_date;
--
CURSOR c_ape (p_aun_code VARCHAR2, p_date DATE) IS
SELECT 'X'
FROM admin_periods
WHERE ape_aye_aun_code      = p_aun_code
  AND ape_start_date        = p_date
  AND ape_admin_period_type = 'BAL';
--
CURSOR c_bal (p_rac_accno NUMBER, p_date DATE)  IS
SELECT 'X'
FROM  account_balances
WHERE aba_rac_accno = p_rac_accno
and   aba_date      = p_date;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_ACCOUNT_BALANCES';
cs       INTEGER;
ce	 VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_aye_exists     VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
-- Other variables
--
l_rac_accno       revenue_accounts.rac_accno%TYPE;
l_rac_aun_code    admin_units.aun_code%TYPE;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_account_balances.dataload_validate');
fsc_utils.debug_message( 's_dl_hra_account_balances.dataload_validate',3);
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
cs := p1.laba_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
  --
  --
  -- Check the payment reference exists on REVENUE_ACCOUNTS and
  -- get the account number
  --
  IF ( s_revenue_accounts2.exists_rac(p1.laba_pay_ref) )
  THEN
    l_rac_accno :=
        s_revenue_accounts2.get_rac_accno_from_pay_ref(p1.laba_pay_ref);
  --
  -- Check the account balance does not exist on ACCOUNT_BALANCES
  --
 OPEN c_bal (l_rac_accno, p1.laba_date);
  FETCH c_bal into l_exists;
   IF c_bal%found
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',115);
   END IF;
 CLOSE c_bal;
    --
    -- Check the balance date matches a period end
    --
      l_rac_aun_code := s_revenue_accounts.get_rac_aun_code(l_rac_accno);
    --
    l_aye_exists := NULL;
    OPEN  c_aye(l_rac_aun_code,p1.laba_date);
    FETCH c_aye into l_aye_exists;
    CLOSE c_aye;
    --
    IF l_aye_exists IS NOT NULL
      THEN
      IF ( NOT s_admin_periods.f_exists_ape_end
                               (l_rac_aun_code,'BAL',p1.laba_date))
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',116);
      END IF;
    END IF;
  --
  ELSE
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',117);
  END IF;
  --
-- Now UPDATE the record count AND error code
IF l_errors = 'F' THEN
  l_error_ind := 'Y';
ELSE
  l_error_ind := 'N';
END IF;
--
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
  set_record_status_flag(l_id,'O');
  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
 END;
--
END LOOP;
--
fsc_utils.proc_end;
commit;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.SET_record_status_flag(ct,cb,cs,'O');
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,laba_dlb_batch_id
,laba_dl_seqno
,laba_pay_ref
,laba_date
FROM  dl_hra_account_balances
WHERE laba_dlb_batch_id   = p_batch_id
AND   laba_dl_load_status = 'C';

-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRA_ACCOUNT_BALANCES';
cs       INTEGER;
ce	 VARCHAR2(200);
l_id     ROWID;
--
l_rac_accno     revenue_accounts.rac_accno%TYPE;
i               INTEGER := 0;
l_an_tab        VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_account_balances.dataload_delete');
fsc_utils.debug_message( 's_dl_hra_account_balances.dataload_delete',3 );
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
  --
  BEGIN
  cs := p1.laba_dl_seqno;
  i := i +1;
  l_id := p1.rec_rowid;
--
  l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref
                                              (p1.laba_pay_ref);
  --
  DELETE FROM ACCOUNT_BALANCES
  WHERE aba_rac_accno = l_rac_accno
    AND aba_date      = p1.laba_date;
  --
  --
  -- keep a count of the rows processed and commit after every 1000
  --
  i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
  --
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ACCOUNT_BALANCES');
--
fsc_utils.proc_end;
commit;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.SET_record_status_flag(ct,cb,cs,'O');
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_delete;
--
--
END s_dl_hra_account_balances;
/

