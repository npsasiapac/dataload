CREATE OR REPLACE PACKAGE BODY s_dl_hra_parallel_rents
AS
-- ***********************************************************************
--
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  WHEN       WHY
--      1.0     MTR  23/11/00   Dataload
--      1.1     SB   03/10/02   Changed to use s_summary_rents.get_gross_rent
--
--      2.0  5.13.0    PH   06-FEB-2008  Now includes its own 
--                                       set_record_status_flag procedure.
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
  UPDATE dl_hra_parallel_rents
  SET lpre_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_parallel_rents');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
  --
  --  declare package variables AND constants
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,lpre_dlb_batch_id
,lpre_dl_seqno,
lpre_pay_ref,
lpre_gross_rent,
lpre_balance,
lpre_date
FROM dl_hra_parallel_rents
WHERE lpre_dlb_batch_id      = p_batch_id
AND   lpre_dl_load_status       in ('L','F','O');
--

--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_PARALLEL_RENTS';
cs       INTEGER;
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
-- Other variables
--
l_rac_accno        revenue_accounts.rac_accno%TYPE;
l_summ             NUMBER(11,2);
l_aba_balance      account_balances.aba_balance%TYPE;
l_dummy1           INTEGER;
l_dummy2           DATE;
l_rac_aun_code     revenue_accounts.rac_aun_code%TYPE;
l_aba_date         account_balances.aba_date%TYPE;

--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_parallel_rents.dataload_validate');
fsc_utils.debug_message( 's_dl_hra_parallel_rents.dataload_validate',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
cs := p1.lpre_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
  --
  l_rac_accno    := s_revenue_accounts2.get_rac_accno_from_pay_ref (p1.lpre_pay_ref);
  l_rac_aun_code := s_revenue_accounts.get_rac_aun_code (l_rac_accno);
  -- Check the payment reference exists on REVENUE_ACCOUNTS
  IF l_rac_accno IS NULL
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',117);
  ELSE
    --
    -- Check that a corresponding entry exists on SUMMARY_RENTS and,
    -- if so, get the gross rent

    IF s_summary_rents.any_gross_rent(l_rac_accno, p1.lpre_date)
    THEN
      l_summ := null;
      l_summ := s_summary_rents.get_gross_rent(l_rac_accno, p1.lpre_date);
      --
      -- Check the summary rent matches the gross rent supplied
      IF (l_summ != p1.lpre_gross_rent)
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',161);
      END IF;
    --
    ELSE
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',160);
    END IF;

  END IF;
  --
  -- Get the relevant balance date and check that the balance on
  -- ACCOUNT_BALANCES matches the value supplied
  --
  l_aba_date := s_admin_periods.get_bal_period_date( l_rac_aun_code, p1.lpre_date, 'BAL' );
  IF (l_aba_date IS NULL)
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',162);
  ELSE
    IF s_account_balances.get_max_aba_date(l_rac_accno) != l_aba_date
    THEN
      s_account_balances.get_account_balances(l_rac_accno, p1.lpre_date, l_dummy1, l_dummy2, l_aba_balance);
      IF (l_aba_balance != p1.lpre_balance)
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',164);
      END IF;

    ELSE
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',163);
    END IF;

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
END LOOP;
--
fsc_utils.proc_end;
commit;
--
   EXCEPTION
      WHEN OTHERS THEN
      set_record_status_flag(l_id,'O');
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
--
END s_dl_hra_parallel_rents;
/
