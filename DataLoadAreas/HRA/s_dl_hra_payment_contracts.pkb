CREATE OR REPLACE PACKAGE BODY s_dl_hra_payment_contracts
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER   WHO  WHEN       WHY
--      1.0  5.1.6    MH  28/11/02    NCCW Dataload
--      2.0  5.3.0    PH  14/02/03    Fixed compilation errors
--      3.0  5.13.0   PH  06-FEB-2008 Now includes its own 
--                                    set_record_status_flag procedure.
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
  UPDATE dl_hra_payment_contracts
  SET lpct_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_payment_contracts');
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
,lpct_dlb_batch_id
,lpct_dl_load_status
,lpct_dl_seqno
,lpct_refno
,lpct_pay_ref     
,lpct_par_per_alt_ref     
,lpct_start_date    
,lpct_status        
,lpct_amount   
,lpct_percentage
,lpct_end_date       
FROM dl_hra_payment_contracts
WHERE lpct_dlb_batch_id    = p_batch_id
AND   lpct_dl_load_status = 'V'
ORDER BY lpct_dl_seqno;
--
--
cursor c_get_par_refno (p_alt_ref varchar2) is
select par_refno from parties
where  par_per_alt_ref = p_alt_ref;
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HRA_PAYMENT_CONTRACTS';
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
l_par_refno       INTEGER :=0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_payment_contracts.dataload_create');
fsc_utils.debug_message( 's_dl_hra_payment_contracts.dataload_create',3);
--
cb := p_batch_id;
cd := p_DATE;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
for p1 in c1(p_batch_id) loop
--
BEGIN
  --
  cs := p1.lpct_dl_seqno;
  l_id := p1.rec_rowid;
  --
  -- Get the new RAC_ACCNO
  l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref( p1.lpct_pay_ref );
  --
  -- Get the new PAR_REFNO
  open c_get_par_refno(p1.lpct_par_per_alt_ref);
    fetch c_get_par_refno into l_par_refno;
  close c_get_par_refno;
  --
  --
   INSERT INTO PAYMENT_CONTRACTS
  ( PCT_REFNO         
   ,PCT_RAC_ACCNO     
   ,PCT_PAR_REFNO     
   ,PCT_START_DATE    
   ,PCT_STATUS        
   ,PCT_CREATED_DATE  
   ,PCT_CREATED_BY    
   ,PCT_AMOUNT        
   ,PCT_PERCENTAGE    
   ,PCT_END_DATE      
   ,PCT_MODIFIED_DATE 
   ,PCT_MODIFIED_BY
  )
   VALUES
  (p1.lpct_refno
   ,l_rac_accno
   ,l_par_refno
   ,p1.lpct_start_date
   ,p1.lpct_status
   ,sysdate
   ,'DATALOAD'
   ,p1.lpct_amount
   ,p1.lpct_percentage
   ,p1.lpct_end_date
   ,sysdate
   ,'DATALOAD');

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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PAYMENT_CONTRCTS');
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
CURSOR c1(p_batch_id VARCHAR2) is
SELECT
rowid rec_rowid
,lpct_dlb_batch_id
,lpct_dl_load_status
,lpct_dl_seqno
,lpct_refno
,lpct_pay_ref     
,lpct_par_per_alt_ref     
,lpct_start_date    
,lpct_status        
,lpct_amount   
,lpct_percentage
,lpct_end_date       
FROM dl_hra_payment_contracts
WHERE lpct_dlb_batch_id      = p_batch_id
AND   lpct_dl_load_status    in ('L','F','O');
--
CURSOR c_per_exists(p_per_alt_ref varchar2) is
SELECT par_refno
FROM parties
WHERE par_per_alt_ref = p_per_alt_ref;
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_PAYMENT_CONTRACTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- Other variables
l_rac_accno      revenue_accounts.rac_accno%TYPE;
l_exists         VARCHAR2(1);
l_par_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hra_payment_contracts.dataload_validate');
  fsc_utils.debug_message( 's_dl_hra_payment_contracts.dataload_validate',3);
  --
  cb := p_batch_id;
  cd := p_date;
  --
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  for p1 in c1(p_batch_id) loop
  --
  BEGIN
  --
  cs := p1.lpct_dl_seqno;
  l_id := p1.rec_rowid;
  --
  l_errors := 'V';
  l_error_ind := 'N';
  --
  -- Check the payment reference exists on REVENUE_ACCOUNTS
  --
  l_rac_accno := NULL;
  --
  l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref( p1.lpct_pay_ref );
  IF l_rac_accno IS NULL
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',117);
  END IF;
  --
  -- Check per_alt_ref supplied exists on people
  --
  l_par_refno := null;
  OPEN c_per_exists(p1.lpct_par_per_alt_ref);
  FETCH c_per_exists INTO l_par_refno;
  CLOSE c_per_exists;
  IF l_par_refno IS NULL
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',243);
  END IF;
  --
  --
  -- Check the payment contract does not overlap with an existing one
  --
  --
  IF s_dl_hra_utils.overlapping_contract(l_rac_accno,
                                      l_par_refno,
                                      p1.lpct_start_date,
                                      p1.lpct_end_date)
  THEN 
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',982);
  END IF;
  -- 
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
CURSOR c1(p_batch_id VARCHAR2) is
SELECT
rowid rec_rowid
,lpct_dlb_batch_id
,lpct_dl_load_status
,lpct_dl_seqno
,lpct_refno
,lpct_pay_ref     
,lpct_par_per_alt_ref     
,lpct_start_date    
,lpct_status        
,lpct_amount   
,lpct_percentage
,lpct_end_date       
FROM dl_hra_payment_contracts
WHERE lpct_dlb_batch_id      = p_batch_id
AND   lpct_dl_load_status = 'C';
--
-- Constants for process_summary
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'DELETE';
ct          VARCHAR2(30) := 'DL_HRA_PAYMENT_CONTRACTS';
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
  fsc_utils.proc_start('s_dl_hra_payment_contracts.dataload_delete');
  fsc_utils.debug_message( 's_dl_hra_payment_contracts.dataload_delete',3 );
  --
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  for p1 in c1(p_batch_id) loop
  --
  BEGIN
  --
  cs := p1.lpct_dl_seqno;
  i := i +1;
  l_id := p1.rec_rowid;
  --
  delete from payment_contracts
  where  pct_refno = p1.lpct_refno;
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
-- Section to analyze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PAYMENT_CONTRACTS');
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
END s_dl_hra_payment_contracts;

/

