CREATE OR REPLACE PACKAGE BODY s_dl_hra_arrears_arrangements
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      1.5  5.5.0     PJD    Bespoke for Prime Focus
--      2.0  5.12.0    PH   07-DEC-2007  Added insert and delete for
--                                       notepads
--      3.0  5.15.1    MB  09-Jun-2009   Bespoked for Midland Heart 
--                                       - end existing arrangements question, 
--                                       set record status flag procedure
--      4.0  6.9.0     AJ  11-NOV-2013   Check out for issue at 6.9.0 and error codes
--                                       amended as HD1 errors file numbers have changed
--                                       error 558(OLD) amended to 756(NEW)
--      5.0  6.13.0    JS  21-MAR-2016   Added order by so arrangements display by desc created date
--                                       Added larr_due_date_method
--      5.1  6.13.0    AJ  27-APR-2017   Commented out larr_due_date_method as full changes not done
--      
-- ***********************************************************************
--
--  declare package variables and constants
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hra_arrears_arrangements
  SET larr_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_arrears_arrangements');
     RAISE;
  --
END set_record_status_flag;
--
-- **************************************************************************************************
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT rowid rec_rowid
,larr_dlb_batch_id
,larr_dl_seqno
,larr_dl_load_status
,larr_pay_ref
,larr_ara_code              
,larr_interval_period
,larr_interval_units
,larr_default_amount
,larr_balance
,larr_created_by
,larr_created_date
,larr_start_date
,larr_end_due_date
,larr_actual_end_date            
,larr_hrv_hre_code
,larr_end_username        
,larr_first_dd_taken_ind
,larr_pmy_code
,larr_nop_text
--,larr_due_date_method
FROM dl_hra_arrears_arrangements
WHERE larr_dlb_batch_id    = p_batch_id
AND   larr_dl_load_status = 'V'
ORDER BY larr_pay_ref, larr_start_date, larr_created_date;
--
-- ***********************************************************************
--
CURSOR c2(p_rac_accno NUMBER, p_start_date DATE) is
SELECT arr_refno
FROM 	account_arrears_arrangements
WHERE 	arr_rac_accno = p_rac_accno
AND 	p_start_date between arr_start_date and nvl(arr_actual_end_date,arr_end_due_date);
--
-- ***********************************************************************
--
-- Constants for process_summary
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'CREATE';
ct       	VARCHAR2(30) := 'DL_HRA_ARREARS_ARRANGEMENTS';
cs       	INTEGER;
ce	   		VARCHAR2(200);
l_an_tab 	VARCHAR2(1);
l_id 		ROWID;
--
-- Other variables
--
l_pro_refno       number;
i                 integer := 0;
l_reusable_refno  revenue_accounts.rac_reusable_refno%TYPE;
l_rac_accno       revenue_accounts.rac_accno%TYPE;
l_answer          VARCHAR2(1);
--
-- ***********************************************************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_arrears_arrangements.dataload_create');
fsc_utils.debug_message( 's_dl_hra_arrears_arrangements.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
l_answer  := s_dl_batches.get_answer(p_batch_id, 1);
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.larr_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- ***********************************************************************
--
-- Get the new RAC_ACCNO
  l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref( p1.larr_pay_ref);
--
-- Get the REUSABLE_REFNO
  l_reusable_refno := fsc_utils.f_dynamic_value('reusable_refno_seq.NEXTVAL');
--
--
-- Check whether current arrangements need to be ended
--
IF l_answer = 'Y'
THEN
	FOR p2 IN c2(l_rac_accno, p1.larr_start_date) LOOP
	--
	UPDATE account_arrears_arrangements
	SET arr_actual_end_date = p1.larr_start_date - 1,
		arr_end_due_date = p1.larr_start_date - 1
	WHERE arr_refno = p2.arr_refno;
	--
	DELETE FROM arrears_installments
	WHERE ain_arr_refno = p2.arr_refno
	AND ain_due_date > p1.larr_start_date - 1;
	--
	END LOOP;
	--
END IF; -- l_answer = 'Y'
--
-- ***********************************************************************
--
-- now create the new arrangement
-- 
    INSERT INTO account_arrears_arrangements
         (arr_refno
         ,arr_rac_accno
         ,arr_ara_code         
         ,arr_interval_period
         ,arr_interval_units
         ,arr_default_amount
         ,arr_balance
         ,arr_created_by
         ,arr_created_date
         ,arr_start_date
         ,arr_end_due_date
         ,arr_actual_end_date            
         ,arr_hrv_hre_code
         ,arr_end_username        
         ,arr_first_dd_taken_ind
         ,arr_pmy_code
         ,arr_reusable_refno
--         ,arr_due_date_method
          )
      VALUES
         (arr_refno_seq.nextval
         ,l_rac_Accno
         ,p1.larr_ara_code             
         ,p1.larr_interval_period
         ,p1.larr_interval_units
         ,p1.larr_default_amount
         ,p1.larr_balance
         ,p1.larr_created_by
         ,p1.larr_created_date
         ,p1.larr_start_date
         ,p1.larr_end_due_date
         ,p1.larr_actual_end_date            
         ,p1.larr_hrv_hre_code
         ,p1.larr_end_username        
         ,p1.larr_first_dd_taken_ind
         ,p1.larr_pmy_code
         ,l_reusable_refno
--         ,p1.larr_due_date_method
         );
--
-- This will now have created an entry in account_arrears_arrangements
-- as the user which ran the Create process and the date the process was
-- run. Therefore update this record to reflect the correct details.
--
    UPDATE account_arrears_actions a1
       SET a1.aca_created_by     = p1.larr_created_by,
           a1.aca_created_date   = p1.larr_created_date
     WHERE a1.aca_rac_accno      = l_rac_accno
       AND a1.aca_ara_code       = p1.larr_ara_code
       AND a1.aca_created_date   > sysdate -0.01
       AND a1.aca_type           = 'MANUAL'
       AND a1.aca_created_module = 'RAC215'
       AND a1.aca_refno          = (SELECT max(a2.aca_refno)
                                      FROM account_arrears_actions a2
                                     WHERE a1.aca_rac_accno = a2.aca_rac_accno
                                       AND a1.aca_ara_code  = a2.aca_ara_code);
--
-- IF there is a larr_nop_text THEN INSERT a row into NOTEPADS
--
  IF p1.larr_nop_text IS NOT NULL
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
    p1.larr_created_date,
    'DATALOAD',
    'Y',
    'N',
    p1.larr_nop_text,
    NULL,
    NULL );
  END IF;
--
--
-- ***********************************************************************
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
COMMIT;
--
-- ***********************************************************************
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ACCOUNT_ARREARS_ARRANGEMENTS');
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
-- **************************************************************************************************
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,larr_dlb_batch_id
,larr_dl_seqno
,larr_DL_LOAD_STATUS
,larr_pay_ref
,larr_ara_code           
,larr_interval_period
,larr_interval_units
,larr_default_amount
,larr_balance
,larr_created_by
,larr_created_date
,larr_start_date
,larr_end_due_date
,larr_actual_end_date            
,larr_hrv_hre_code
,larr_end_username        
,larr_first_dd_taken_ind
,larr_pmy_code           
FROM dl_hra_arrears_arrangements
WHERE larr_dlb_batch_id      = p_batch_id
AND   larr_dl_load_status   in ('L','F','O');
--
-- ***********************************************************************
--
CURSOR c_pmy_dets(p_pmy_code VARCHAR2) is
SELECT 'x'
FROM   payment_method_types
WHERE  pmy_code = p_pmy_code;
--
-- ***********************************************************************
--
CURSOR c_ara(p_ara_code VARCHAR2) IS
SELECT ara_type
FROM   arrears_actions
WHERE  ara_code = p_ara_code;
--
-- ***********************************************************************
--
CURSOR c_arr(p_rac_accno NUMBER, p_start_date DATE) is
SELECT 'x'
FROM 	account_arrears_arrangements
WHERE 	arr_rac_accno = p_rac_accno
AND 	p_start_date between arr_start_date and nvl(arr_actual_end_date,arr_end_due_date);
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_ARREARS_ARRANGEMENTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id		ROWID;
--
-- Other Constants
--

l_exists         VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_rac_accno      NUMBER(10);
l_ara_type       VARCHAR2(8);
l_answer          VARCHAR2(1);
--
-- ***********************************************************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_arrears_arrangements.dataload_validate');
fsc_utils.debug_message( 's_dl_hra_arrears_arrangements.dataload_validate',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- get answer to whether existing arrangements should be terminated...
--
l_answer  := s_dl_batches.get_answer(p_batch_id, 1);
--
-- ***********************************************************************
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.larr_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- ***********************************************************************
--
-- Check the Links to Other Tables
--
-- Check the payment reference exists on REVENUE_ACCOUNTS
--
l_rac_accno := NULL;
--
l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref( p1.larr_pay_ref );
--
IF l_rac_accno IS NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',117);
END IF;
--
-- ***********************************************************************
--
-- Check for action type
--
l_ara_type := null;
--
  OPEN  c_ara(p1.larr_ara_code);
  FETCH c_ara INTO l_ara_type;
  CLOSE c_ara;
  --
  IF l_ara_type IS NULL
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',148);
  END IF;
  --
  IF l_ara_type != 'ARRANGE'
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',400);
  END IF;
  --
--
-- ***********************************************************************
--
-- Validate the reference value fields
--
-- Check the End Reason is Valid
--
  IF p1.larr_hrv_hre_code IS NOT NULL
  THEN
    IF (NOT s_dl_hem_utils.exists_frv('ARR_END',p1.larr_hrv_hre_code))
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',950);
    END IF;
  END IF;
--
-- ***********************************************************************
--
-- Check the Periods code
--
  IF p1.larr_interval_period IS NOT NULL
  THEN
    IF (NOT s_dl_hem_utils.exists_frv('PERIODS',p1.larr_interval_units))
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',949);
    END IF;
  END IF;
--
-- ***********************************************************************
--
-- Check the PMY Code
--
  l_exists := NULL;
  IF p1.larr_pmy_code IS NOT NULL
  THEN
    OPEN  c_pmy_dets(p1.larr_pmy_code);
    FETCH c_pmy_dets INTO l_exists;
    CLOSE c_pmy_dets;
  --
    IF l_exists IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',103);
    END IF;    
  END IF;
--
-- ***********************************************************************
--
-- Check the Y/N fields
--
-- First direct debit taken       
--            
  IF ( p1.larr_first_dd_taken_ind NOT IN ('Y','N') )        
  THEN          
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',101);       
  END IF;     --
--
-- ***********************************************************************
--
-- Check the other mandatory fields
--
IF p1.larr_balance IS NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',951);
END IF;
--
-- ***********************************************************************
--
-- Check the Start Date
--
IF p1.larr_start_date IS NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',865);
END IF;
--
-- ***********************************************************************
--
-- Check the Creation Date
--
IF p1.larr_created_date IS NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',952);
END IF;
--
-- Check the Created by
--
IF p1.larr_created_by IS NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',953);
END IF;
--
-- ***********************************************************************
--
-- Interval Period  
--
IF p1.larr_interval_period IS NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',954);
END IF;
--
-- ***********************************************************************
--
-- Any other checks for consistency between fields etc.
--
IF (    p1.larr_end_due_date IS  NULL
    AND p1.larr_default_amount IS NULL)
THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',955);
END IF;
--
-- ***********************************************************************
--
-- Check for consistency between end user and end date
--
IF   (    p1.larr_actual_end_date IS  NULL
      AND p1.larr_end_username IS NOT NULL)
THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',956);
ELSIF(    p1.larr_actual_end_date IS NOT NULL
      AND p1.larr_end_username    IS NULL)
THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',956);
END IF;
--
-- ***********************************************************************
--
-- if not ending existing arrangements, then check to see that new arrangements are not overlapping with any current arrangement
--
IF l_answer = 'N'
THEN
	l_exists := NULL;
	OPEN c_arr(l_rac_accno,p1.larr_start_date);
	FETCH c_arr INTO l_exists;
	CLOSE c_arr;
	--
	IF l_exists IS NOT NULL
	THEN 
	l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',756);
	END IF;
END IF;
--
-- ***********************************************************************
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
-- **************************************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,larr_dlb_batch_id
,larr_dl_seqno
,larr_DL_LOAD_STATUS
,larr_pay_ref
,larr_ara_code              
,larr_interval_period
,larr_interval_units
,larr_default_amount
,larr_balance
,larr_created_by
,larr_created_date
,larr_start_date
,larr_end_due_date
,larr_actual_end_date            
,larr_hrv_hre_code
,larr_end_username        
,larr_first_dd_taken_ind
,larr_pmy_code           
,larr_nop_text
FROM dl_hra_arrears_arrangements
WHERE larr_dlb_batch_id     = p_batch_id
  AND larr_dl_load_status   = 'C';
--
-- ***********************************************************************
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRA_ARREARS_ARRANGEMENTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
l_id 	ROWID;
--
-- Other Constants
--
l_rac_accno INTEGER;
i integer := 0;
l_answer         VARCHAR2(1);
--
-- ***********************************************************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_arrears_arrangements.dataload_delete');
fsc_utils.debug_message( 's_dl_hra_arrears_arrangements.dataload_delete',3 );
--
cb := p_batch_id;
cd := p_date;
--
l_answer  := s_dl_batches.get_answer(p_batch_id, 1);
--
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- ***********************************************************************
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.larr_dl_seqno;
l_id := p1.rec_rowid;
--
-- Get the new RAC_ACCNO
  l_rac_accno := NULL;
  l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref( p1.larr_pay_ref);
--
SAVEPOINT SP1;
--
-- ***********************************************************************
--
  IF p1.larr_nop_text IS NOT NULL
     THEN
      --
      -- Can't delete if you haven't created the record
      -- so update it first
      --
      UPDATE notepads
         SET nop_created_by     =  user,
             nop_created_date   = sysdate
       WHERE nop_type           = 'ACA'
         AND nop_reusable_refno = (SELECT arr_reusable_refno
                                     FROM account_arrears_arrangements
                                    WHERE arr_rac_accno   = l_rac_accno
                                    AND   arr_start_date  = p1.larr_start_date
                                    AND   arr_ara_code    = p1.larr_ara_code);
      --
      DELETE FROM notepads
      WHERE  nop_type = 'ACA'
        AND  nop_reusable_refno = (SELECT arr_reusable_refno
                                     FROM account_arrears_arrangements
                                    WHERE arr_rac_accno   = l_rac_accno
                                    AND   arr_start_date  = p1.larr_start_date
                                    AND   arr_ara_code    = p1.larr_ara_code);
  END IF;
--
--
DELETE FROM account_arrears_actions
WHERE aca_rac_accno = l_rac_accno
AND   aca_Ara_code = p1.larr_ara_code
AND   aca_Reusable_refno = 		(SELECT arr_reusable_refno
                                     FROM account_arrears_arrangements
                                    WHERE arr_rac_accno   = l_rac_accno
                                    AND   arr_start_date  = p1.larr_start_date
                                    AND   arr_ara_code    = p1.larr_ara_code);
--
DELETE FROM account_arrears_arrangements
WHERE arr_rac_accno   = l_rac_accno
AND   arr_start_date  = p1.larr_start_date
AND   arr_ara_code    = p1.larr_ara_code;
--
-- ***********************************************************************
--
IF l_answer = 'Y'
THEN
	UPDATE account_arrears_arrangements
	SET	arr_actual_end_date = NULL, arr_end_username = NULL
	WHERE arr_Rac_Accno = l_rac_accno
	AND arr_actual_end_date = p1.larr_start_Date - 1;
END IF;
--
-- ***********************************************************************
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ACCOUNT_ARREARS_ARRANGEMENTS');
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
END s_dl_hra_arrears_arrangements;
/

