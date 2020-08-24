CREATE OR REPLACE PACKAGE BODY s_dl_hra_nct_aun_links
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--
--      1.0  5.6.0     PH   21-OCT-2004  Initial Creation
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
  UPDATE dl_hra_nct_aun_links
  SET lnal_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_nct_aun_links');
     RAISE;
  --
END set_record_status_flag;
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
,lnal_dlb_batch_id
,lnal_dl_seqno
,lnal_dl_load_status
,lnal_pay_ref
,lnal_aun_code
FROM dl_hra_nct_aun_links
WHERE lnal_dlb_batch_id    = p_batch_id
AND   lnal_dl_load_status = 'V';
--
CURSOR c_auy_code(p_aun_code varchar2) IS
SELECT aun_auy_code
FROM   admin_units
WHERE  aun_code = p_aun_code;
--
CURSOR c_account(p_batch_id VARCHAR2) IS
SELECT distinct lnal_pay_ref,
       rac_hrv_ate_code
FROM   revenue_Accounts      rac,
       dl_hra_nct_aun_links  lnal
WHERE  lnal.lnal_dlb_batch_id   = p_batch_id
AND    lnal.lnal_dl_load_status = 'S'
AND    lnal.lnal_pay_ref        = rac.rac_pay_ref;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HRA_NCT_AUN_LINKS';
cs       INTEGER;
ce	 VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
l_rac_accno        NUMBER(10);
l_auy_code         VARCHAR2(3);
i                  INTEGER := 0;
l_rac_accno2       NUMBER(10);
l_rac_hrv_ate_code VARCHAR2(10);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_nct_aun_links.dataload_create');
fsc_utils.debug_message( 's_dl_hra_nct_aun_links.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lnal_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- Get Revenue Account number
--
     l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref
                    (p1.lnal_pay_ref);
--
-- get the Admin Unit Type
--
     l_auy_code := null;
--
OPEN  c_auy_code(p1.lnal_aun_code);
FETCH c_auy_code INTO l_auy_code;
CLOSE c_auy_code;
--
      INSERT INTO nct_aun_links
         (nal_rac_accno
         ,nal_aun_code
         ,nal_aun_auy_code
         )
      VALUES
         (l_rac_accno
         ,p1.lnal_aun_code
         ,l_auy_code
         );
--
-- keep a count of the rows processed and commit after every 5000
--
i := i+1; IF MOD(i,5000)=0 THEN COMMIT; END If;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
--
-- Set the Record Status Flag to S so it can be
-- picked up by the second loop
--
set_record_status_flag(l_id,'S');
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
-- Now the loop to generate the Summary Rac Account
--
i := 0;
--
  FOR r_account IN c_account(p_batch_id) LOOP
  BEGIN
--
   l_rac_hrv_ate_code := r_account.rac_hrv_ate_code;
--
-- Get Revenue Account number
--
       l_rac_accno2 := s_revenue_accounts2.get_rac_accno_from_pay_ref
                     ( r_account.lnal_pay_ref );
--
       hra_rencon.p_assign_nct_sac(
         l_rac_accno2,
         l_rac_hrv_ate_code,
         trunc(sysdate));
-- 
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,100)=0 THEN COMMIT; END IF;
--
update dl_hra_nct_aun_links
set    lnal_dl_load_status = 'C'
WHERE  lnal_dl_load_status = 'S'
  and  lnal_pay_ref        = r_account.lnal_pay_ref
  and  lnal_dlb_batch_id   = cb;
--
 EXCEPTION
   WHEN OTHERS THEN
 NULL;
END;
END LOOP;
--
fsc_utils.proc_end;
commit;
--
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('NCT_AUN_LINKS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUMMARY_RAC_ACCOUNTS');
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
,lnal_dlb_batch_id
,lnal_dl_seqno
,lnal_dl_load_status
,lnal_pay_ref
,lnal_aun_code
FROM dl_hra_nct_aun_links
WHERE lnal_dlb_batch_id      = p_batch_id
AND   lnal_dl_load_status   in ('L','F','O');
--
CURSOR c_auy_code(p_aun_code varchar2) IS
SELECT aun_auy_code
FROM   admin_units
WHERE  aun_code = p_aun_code;
--
CURSOR c_nal_exists(p_rac_accno number,
                    p_aun_code  varchar2,
                    p_auy_code  varchar2) IS
SELECT 'X'
FROM   nct_aun_links
WHERE  nal_rac_accno     = p_rac_accno
AND    nal_aun_code      = p_aun_code
AND    nal_aun_auy_code  = p_auy_code;
--
CURSOR c_tcy_check(p_rac_accno number) IS
SELECT 'X'
FROM   revenue_accounts
WHERE  rac_accno         = p_rac_accno
AND    rac_tcy_refno is not null;
--  
cursor c_aun_code(p_aun_code varchar2) IS
SELECT NULL
FROM   admin_units
WHERE  aun_code      = p_aun_code;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_NCT_AUN_LINKS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_rac_accno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_auy_code       VARCHAR2(3);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_nct_aun_links.dataload_validate');
fsc_utils.debug_message( 's_dl_hra_nct_aun_links.dataload_validate',3);
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
cs := p1.lnal_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- Check the Links to Other Tables
--
  -- Check the payment reference exists on REVENUE_ACCOUNTS and
  -- get the account number
  --
  IF ( s_revenue_accounts2.exists_rac(p1.lnal_pay_ref) )
  THEN
    l_rac_accno :=
        s_revenue_accounts2.get_rac_accno_from_pay_ref(p1.lnal_pay_ref);
  --
  -- Check the account is not linked to a tenancy
  --
     OPEN c_tcy_check(l_rac_accno);
      FETCH c_tcy_check into l_exists;
       IF l_exists IS NOT NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',199);
       END IF;
     CLOSE c_tcy_check;
  --
  -- Check the acount is not already linked to this admin unit
  -- Within the account check.
  -- 
    OPEN c_auy_code(p1.lnal_aun_code);
     FETCH c_auy_code INTO l_auy_code;
    CLOSE c_auy_code;
  --
    OPEN c_nal_exists(l_rac_accno, p1.lnal_aun_code, l_auy_code);
     FETCH c_nal_exists INTO l_exists;
      IF l_exists IS NOT NULL
       THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',198);
      END IF;
    CLOSE c_nal_exists;
  --
  ELSE
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',117);
  END IF;
--
-- Check the admin_unit code exists on ADMIN UNITS
--
  OPEN c_aun_code(p1.lnal_aun_code);
   FETCH c_aun_code into l_exists;
    IF c_aun_code%notfound then
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
    END IF;
  CLOSE c_aun_code;
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
,lnal_dlb_batch_id
,lnal_dl_seqno
,lnal_dl_load_status
,lnal_pay_ref
,lnal_aun_code
FROM dl_hra_nct_aun_links
WHERE lnal_dlb_batch_id      = p_batch_id
  AND lnal_dl_load_status    = 'C';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRA_NCT_AUN_LINKS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
l_rac_accno  number(10);
i integer := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_nct_aun_links.dataload_delete');
fsc_utils.debug_message( 's_dl_hra_nct_aun_links.dataload_delete',3 );
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
cs := p1.lnal_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- Get the Revenue Account number
--
  l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref
                                              (p1.lnal_pay_ref);
DELETE FROM nct_aun_links
WHERE nal_rac_accno        = l_rac_accno
AND   nal_aun_auy_code     = p1.lnal_aun_code;
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
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'C');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
--
END LOOP;
--
COMMIT;
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('NCT_AUN_LINKS');
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
END s_dl_hra_nct_aun_links;
/

