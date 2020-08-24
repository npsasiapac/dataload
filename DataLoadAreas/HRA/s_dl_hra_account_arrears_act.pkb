CREATE OR REPLACE PACKAGE BODY s_dl_hra_account_arrears_act
AS
-- ***********************************************************************

  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION  DB VER  WHO  WHEN       WHY
  --      1.0  5.1.6   MTR  23/11/00   Dataload
  --      1.1  5.1.6   PJD  21/06/02   Added validation on epo code
  --      1.2  5.2.0   PJD  21/08/02   Reset rac_Accno in validate proc
  --      1.3  5.2.0   SB   28/08/02   Only validate EPO code if supplied
  --      2.0  5.2.0   PJD  03/09/02   Validate policy/code combination 
  --      2.1  5.2.0   SB   10/09/02   Arrears Dispute insert.  Added
  --                                   column adp_refno to statement. 
  --      2.2  5.20.   PH   19/09/02   Added update to account_arrears_actions
  --                                   directly after insert as a trigger defaults
  --                                   the created_user and created_date.
  --	    2.3  5.2.0   IR   27/09/02   Added to PH update for auth_user, auth_date
  --                                   , print_user and print_date
  --      2.4  5.2.0   SB   27/11/02   Amended update statement to use auth/print
  --                                   details if present.
  --                                   Amended notepad insert to use 
  --                                   laca_created_date instead of sysdate.
  --      3.0  5.3.0   PH   05/02/03   Added validate on created_by and created_date
  --                                   as these are mandatory fields.
  --      3.1  5.3.0   SB   07/02/03   Added validate on arragement actions - these
  --                                   should be loaded via arrangements Dataload.
  --                                   Amended validation on AUTO actions to have EPO
  --                                   code, to excluded CLRD action.
  --      3.2  5.4.0   PJD  20/11/03   Expiry date must be supplied for NOTICE type acts.
  --      3.3  5.6.0   PH   21/12/04   Amended Delete process as you can't delete a 
  --                                   notepad entry unles you created it within
  --                                   the last hour.
  --      3.4  5.13.0  PH  06-FEB-2008 Now includes its own 
  --                                   set_record_status_flag procedure.
  --      3.5  6.7.0   PJD 26-SEP-2013 Removed the 3 Arrears Disputes Fields as they had
  --                                   been removed from the spec back in 2010!
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hra_account_arrears_actions
  SET laca_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_account_arrears_actions');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
  --
  --
  --  declare package variables AND constants
  --
  --
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) is
SELECT
rowid rec_rowid
,laca_dlb_batch_id
,laca_dl_load_status
,laca_dl_seqno
,laca_refno
,laca_balance
,laca_rac_accno
,laca_type
,laca_created_by
,laca_created_date
,laca_arrears_dispute_ind
,laca_ara_code
,decode(laca_status,'COMP','PRNT',laca_status) laca_status
,laca_hrv_adl_code
,laca_eac_epo_code
,laca_created_module
,laca_effective_date
,laca_expiry_date
,laca_next_action_date
,laca_auth_date
,laca_auth_username
,laca_print_date
,laca_del_date
,laca_del_username
,laca_print_username
,laca_nop_text
,laca_pay_ref
FROM dl_hra_account_arrears_actions
WHERE laca_dlb_batch_id    = p_batch_id
AND   laca_dl_load_status = 'V'
ORDER BY laca_dl_seqno;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HRA_ACCOUNT_ARREARS_ACTIONS';
cs       INTEGER;
ce	 VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
i                 INTEGER := 0;
l_reusable_refno  revenue_accounts.rac_reusable_refno%TYPE;
l_rac_accno       revenue_accounts.rac_accno%TYPE;
l_an_tab          VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_account_arrears_actions.dataload_create');
fsc_utils.debug_message( 's_dl_hra_account_arrears_actions.dataload_create',3);
--
cb := p_batch_id;
cd := p_DATE;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
for p1 in c1(p_batch_id) loop
--
BEGIN
  --
  cs := p1.laca_dl_seqno;
  l_id := p1.rec_rowid;
  --
  -- Get the new RAC_ACCNO
  l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref( p1.laca_pay_ref );
  --
  -- Get the REUSABLE_REFNO
  l_reusable_refno := fsc_utils.f_dynamic_value('reusable_refno_seq.NEXTVAL');
  --
  INSERT INTO account_arrears_actions
  (aca_refno,
   aca_balance,
   aca_rac_accno,
   aca_type,
   aca_created_by,
   aca_created_date,
   aca_arrears_dispute_ind,
   aca_ara_code,
   aca_status,
   aca_reusable_refno,
   aca_hrv_adl_code,
   aca_eac_epo_code,
   aca_created_module,
   aca_effective_date,
   aca_expiry_date,
   aca_next_action_date,
   aca_auth_date,
   aca_auth_username,
   aca_print_date,
   aca_del_date,
   aca_del_username,
   aca_print_username )
   VALUES
  (p1.laca_refno
  ,p1.laca_balance
  ,l_rac_accno
  ,p1.laca_type
  ,p1.laca_created_by
  ,p1.laca_created_date
  ,p1.laca_arrears_dispute_ind
  ,p1.laca_ara_code
  ,p1.laca_status
  ,l_reusable_refno
  ,p1.laca_hrv_adl_code
  ,p1.laca_eac_epo_code
  ,p1.laca_created_module
  ,p1.laca_effective_date
  ,p1.laca_expiry_date
  ,p1.laca_next_action_date
  ,p1.laca_auth_date
  ,p1.laca_auth_username
  ,p1.laca_print_date
  ,p1.laca_del_date
  ,p1.laca_del_username
  ,p1.laca_print_username);
  --
  -- Now update the record to set the correct values for
  -- created_by and created_date
  --
  UPDATE account_arrears_actions
     SET aca_created_by     = p1.laca_created_by,
         aca_created_date   = p1.laca_created_date,
         aca_auth_username  = nvl(p1.laca_auth_username,p1.laca_created_by),
         aca_auth_date      = nvl(p1.laca_auth_date,p1.laca_created_date),
         aca_print_username = nvl(p1.laca_print_username,p1.laca_created_by),
         aca_print_date     = nvl(p1.laca_print_date,p1.laca_created_date)
   WHERE aca_rac_accno      = l_rac_accno
     AND aca_reusable_refno = l_reusable_refno
     AND aca_refno          = p1.laca_refno;
  --
  --
  --
  -- IF there is a laca_nop_text THEN INSERT a row into NOTEPADS
  IF p1.laca_nop_text IS NOT NULL
  THEN
    INSERT into notepads
    (nop_reusable_refno,
    nop_type,
    nop_created_date,
    nop_created_by,
    nop_current_ind,
    nop_highlight_ind,
    nop_text,
    nop_modified_date,
    nop_modified_by )
   VALUES
   (l_reusable_refno,
    'ACA',
    p1.laca_created_date,
    'DATALOAD',
    'Y',
    'N',
    p1.laca_nop_text,
    NULL,
    NULL );
  END IF;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ACCOUNT_ARREARS_ACTIONS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ARREARS_DISPUTES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('NOTEPADS');
--
fsc_utils.proc_end;
commit;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
      s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
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
,laca_dlb_batch_id
,laca_dl_load_status
,laca_dl_seqno
,laca_refno
,laca_balance
,laca_rac_accno
,laca_type
,laca_created_by
,laca_created_date
,laca_arrears_dispute_ind
,laca_ara_code
,laca_status
,laca_hrv_adl_code
,laca_eac_epo_code
,laca_created_module
,laca_effective_date
,laca_expiry_date
,laca_next_action_date
,laca_auth_date
,laca_auth_username
,laca_print_date
,laca_del_date
,laca_del_username
,laca_print_username
,laca_nop_text
,laca_pay_ref
FROM DL_HRA_ACCOUNT_ARREARS_ACTIONS
WHERE laca_dlb_batch_id      = p_batch_id
AND   laca_dl_load_status       in ('L','F','O');
--
CURSOR c_ara(p_ara_code VARCHAR2) IS
SELECT ara_type from arrears_actions
WHERE  ara_code = p_ara_code;
--
CURSOR c_epo_code (p_epo_code VARCHAR2) IS
SELECT 'X'
FROM   escalation_policies
WHERE  epo_code = p_epo_code;
--
CURSOR c_eac_code (p_epo_code VARCHAR2, p_ara_code VARCHAR2) IS
SELECT 'X'
FROM escalation_policy_actions
WHERE eac_epo_code = p_epo_code
AND   eac_ara_code  = p_ara_code;

-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_ACCOUNT_ARREARS_ACTIONS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- Other variables
l_rac_accno      revenue_accounts.rac_accno%TYPE;
l_dummy          escalation_policies.epo_description%TYPE;
l_exists         VARCHAR2(1);
l_ara_exists     VARCHAR2(1);
l_ara_type       VARCHAR2(8);
l_pro_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hra_account_arrears_actions.dataload_validate');
  fsc_utils.debug_message( 's_dl_hra_account_arrears_actions.dataload_validate',3);
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
  cs := p1.laca_dl_seqno;
  l_id := p1.rec_rowid;
  --
  l_errors := 'V';
  l_error_ind := 'N';
  --
  -- Check the payment reference exists on REVENUE_ACCOUNTS
  l_rac_accno := NULL;
  --
  l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref( p1.laca_pay_ref );
  IF l_rac_accno IS NULL
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',117);
  END IF;
  --
-- Check for action type
--
l_ara_type := null;
--
OPEN  c_ara(p1.laca_ara_code);
FETCH c_ara INTO l_ara_type;
CLOSE c_ara;
  IF l_ara_type IS NULL
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',148);
  END IF;
  --
-- dbms_output.put_line('laca_type '||p1.laca_type);
-- dbms_output.put_line('ara_code '||p1.laca_ara_code);
-- dbms_output.put_line('l_ara_type '||l_ara_type);
  --
  IF (p1.laca_type NOT IN ('MANUAL','AUTO'))
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',400);
-- dbms_output.put_line('error 400  ');
  END IF;
-- 
  IF (l_ara_type = 'ARRANGE') 
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',990);
  END IF;
  --
  -- New validation added on NOTICE type actions
  --
  IF (l_ara_type = 'NOTICE' AND p1.laca_expiry_date IS NULL) 
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',176);
  END IF;
  --
  -- Check the Epo Code is valid
IF (p1.laca_eac_epo_code IS NOT NULL)
THEN
  OPEN c_epo_code(p1.laca_eac_epo_code);
  FETCH c_epo_code into l_exists;
  IF    c_epo_code%notfound 
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',403);
  END IF;
  CLOSE c_epo_code;
--
  OPEN c_eac_code(p1.laca_eac_epo_code,p1.laca_ara_code);
  FETCH c_eac_code into l_exists;
  IF    c_eac_code%notfound 
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',416);
  END IF;
  CLOSE c_eac_code;

END IF;
  -- Check that EPO Code supplied for AUTO actions
  IF (p1.laca_type = 'AUTO' and p1.laca_eac_epo_code IS NULL 
      and p1.laca_ara_code != 'CLRD')
  THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',404);
  END IF;
  -- Check Domain for  ACA_STATUS
  IF (p1.laca_status NOT IN ('DEL','PRNT','AUTH','PEND','COMP'))
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',401);
  END IF;
  -- Check Domain for  ACA_ARREARS_DISPUTE_IND
  IF (p1.laca_arrears_dispute_ind NOT IN ('Y','N'))
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',402);
  END IF;
  --
  --  Check Created By and Created Date
  --
  IF (p1.laca_created_by is null)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',953);
  END IF;
  --
  IF (p1.laca_created_date is null)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',952);
  END IF;
  --
  -- Now UPDATE the record count AND error code
  --
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
,laca_dlb_batch_id
,laca_dl_seqno
,laca_refno
,laca_pay_ref
,laca_nop_text
FROM  dl_hra_account_arrears_actions
WHERE laca_dlb_batch_id   = p_batch_id
AND   laca_dl_load_status = 'C';
--
-- Constants for process_summary
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'DELETE';
ct          VARCHAR2(30) := 'DL_HRA_ACCOUNT_ARREARS_ACTIONS';
cs          INTEGER;
ce          VARCHAR2(200);
l_id     ROWID;
--
i           INTEGER := 0;
l_rac_accno INTEGER;
l_an_tab    VARCHAR2(1);
--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hra_account_arrears_actions.dataload_delete');
  fsc_utils.debug_message( 's_dl_hra_account_arrears_actions.dataload_delete',3 );
  --
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  FOR p1 IN c1 LOOP
  --
  BEGIN
  --
  cs := p1.laca_dl_seqno;
  i := i +1;
  l_id := p1.rec_rowid;
  --
  IF p1.laca_nop_text IS NOT NULL
     THEN
      --
      -- New code added 21/12/2004 P Hearty
      -- Can't delete if you haven't created the record
      -- so update it first
      --
      UPDATE notepads
         SET nop_created_by     =  user,
             nop_created_date   = sysdate
       WHERE nop_type           = 'ACA'
         AND nop_reusable_refno = (SELECT aca_reusable_refno
                                     FROM account_arrears_actions
                                    WHERE aca_refno = p1.laca_refno);
      --
      DELETE FROM notepads
      WHERE  nop_type = 'ACA'
        AND  nop_reusable_refno = (SELECT aca_reusable_refno
                                   FROM account_arrears_actions
                                   WHERE  aca_refno = p1.laca_refno);
  END IF;
  --
  DELETE FROM account_arrears_actions
  WHERE aca_refno = p1.laca_refno;
  --
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
--
  EXCEPTION
  WHEN OTHERS THEN
  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
  set_record_status_flag(l_id,'C');
  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;

END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ACCOUNT_ARREARS_ACTIONS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ARREARS_DISPUTES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('NOTEPADS');
--
fsc_utils.proc_end;
commit;
--
   EXCEPTION
      WHEN OTHERS THEN
      set_record_status_flag(l_id,'O');
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_delete;
--
--
END s_dl_hra_account_arrears_act;

/
show errors
