CREATE OR REPLACE PACKAGE BODY s_dl_hat_app_legacy_ref
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Ver   WHO  WHEN        WHY
--      1.0  6.14     AJ   02-OCT-2017 Initial Creation for Bespoke GNB
--      1.1  6.14     AJ   05-OCT-2017 Completed requires testing
--
-- ***********************************************************************
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hat_app_legacy_ref
  SET lalr_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_app_legacy_ref');
     RAISE;
--
END set_record_status_flag;
--
-- ***********************************************************************
--
--  declare package variables AND constants
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2
,p_date              IN DATE    )
AS
--
CURSOR c1 is
SELECT
rowid rec_rowid,
lalr_dlb_batch_id,
lalr_dl_seqno,
lalr_dl_load_status,
lalr_app_type,
lalr_app_refno,
lalr_app_legacy_ref,
lalr_ale_rli_code,
lalr_ale_alt_ref,
lalr_del_legacy_ref,
lalr_del_alt_ref,
lalr_update_req
FROM  dl_hat_app_legacy_ref
WHERE lalr_dlb_batch_id   = p_batch_id
AND   lalr_dl_load_status = 'V';
--
CURSOR c2(p_app_refno      NUMBER) IS
SELECT app_refno, app_legacy_ref
FROM   applications
WHERE  app_refno = p_app_refno;
--
CURSOR c2_app(p_batch VARCHAR2) IS
SELECT count(*)
FROM   dl_hat_app_legacy_ref
WHERE  lalr_dlb_batch_id = p_batch
AND    lalr_app_type = 'APP';
--
CURSOR c2_ale(p_batch VARCHAR2) IS
SELECT count(*)
FROM   dl_hat_app_legacy_ref
WHERE  lalr_dlb_batch_id = p_batch
AND    lalr_app_type = 'ALE';
--
CURSOR c3(p_ale_app_refno NUMBER,
          p_rli_code      VARCHAR) IS
SELECT ale_alt_ref
FROM   applic_list_entries
WHERE  ale_app_refno = p_ale_app_refno
AND    ale_rli_code  = p_rli_code;
--
-- ******************************
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_APP_LEGACY_REF';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
i                INTEGER := 0;
l_an_tab         VARCHAR2(1);
l_app_refno      INTEGER;
l_app_legacy_ref VARCHAR2(20);
l_app            INTEGER := 0;
l_ale            INTEGER := 0;
l_ale_alt_ref    applic_list_entries.ale_alt_ref%TYPE;
--
-- ******************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_app_legacy_ref.dataload_create');
fsc_utils.debug_message( 's_dl_hat_app_legacy_ref.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- check what type of records are being processed in the batch
--
OPEN c2_app (cb);
FETCH c2_app INTO l_app;
CLOSE c2_app;
--
OPEN c2_ale (cb);
FETCH c2_ale INTO l_ale;
CLOSE c2_ale;
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lalr_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_app_refno      := NULL;
  l_app_legacy_ref := NULL;
  l_ale_alt_ref    := NULL;
--
-- Check get the application reference
--
  OPEN c2 (p1.lalr_app_refno);
  FETCH c2 INTO l_app_refno, l_app_legacy_ref;
  CLOSE c2;
--
  IF p1.lalr_update_req ='Y'
   THEN
--   
    IF l_app_refno IS NOT NULL
     THEN
--
-- ***************    
-- Process for APP(applications)record types
-- APP_LEGACY_REF IS a NULLABLE Field
-- ***************  
   IF p1.lalr_app_type = 'APP'
    THEN
--
-- Firstly save the current app_legacy_ref so it can be
-- put back if delete process is used and the update 
-- is reversed set to 'X' if blank as nullable field
--
     UPDATE dl_hat_app_legacy_ref
     SET lalr_del_legacy_ref = NVL(l_app_legacy_ref,'X')
     WHERE rowid = p1.rec_rowid
     AND lalr_dlb_batch_id = p1.lalr_dlb_batch_id
     AND lalr_dl_seqno = p1.lalr_dl_seqno
     AND lalr_app_refno = p1.lalr_app_refno
     AND lalr_app_legacy_ref = p1.lalr_app_legacy_ref;
--
-- Then update the app_legacy_ref
-- 
     UPDATE applications
     SET app_legacy_ref = p1.lalr_app_legacy_ref
     WHERE app_refno = l_app_refno;
--
   END IF;  -- end of APP processing
--
-- ***************    
-- Process for ALE(applic_list_entries)record types
-- ALE_ALT_REF IS a NOT NULLABLE Field
-- ***************  
   IF p1.lalr_app_type = 'ALE'
    THEN
--
-- Firstly save the current ale_alt_ref so it can be
-- put back if delete process is used and the update 
-- is reversed
--
-- get the ale_alt_ref for record combination
--
     OPEN c3 (p1.lalr_app_refno, p1.lalr_ale_rli_code);
     FETCH c3 INTO l_ale_alt_ref;
     CLOSE c3;
--
     UPDATE dl_hat_app_legacy_ref
     SET lalr_del_alt_ref = l_ale_alt_ref
     WHERE rowid = p1.rec_rowid
     AND lalr_dlb_batch_id = p1.lalr_dlb_batch_id
     AND lalr_dl_seqno = p1.lalr_dl_seqno
     AND lalr_app_refno = p1.lalr_app_refno
     AND lalr_ale_rli_code = p1.lalr_ale_rli_code
     AND lalr_ale_alt_ref = p1.lalr_ale_alt_ref;
--
-- Then update the ale_alt_ref
-- 
     UPDATE applic_list_entries
     SET ale_alt_ref = p1.lalr_ale_alt_ref
     WHERE ale_app_refno = l_app_refno
     AND   ale_rli_code = p1.lalr_ale_rli_code;
--
   END IF;  -- end of ALE processing
--
   END IF;  -- end of All processing
  END IF;  -- end of update_reg 
--
-- ******************************
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
--
 END LOOP;
--
-- Section to analyse the table populated by this data load
--
IF l_app > 0 THEN
  l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLICATIONS');
END IF;
--
IF l_ale > 0 THEN
  l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLIC_LIST_ENTRIES');
END IF;
--
l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HAT_APP_LEGACY_REF');
--
fsc_utils.proc_end;
--
COMMIT;
--
EXCEPTION
 WHEN OTHERS THEN
 s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
 RAISE;
--
END dataload_create;
--
-- ************************************************************************
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2
     ,p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT
rowid rec_rowid,
lalr_dlb_batch_id,
lalr_dl_seqno,
lalr_dl_load_status,
lalr_app_type,
lalr_app_refno,
lalr_app_legacy_ref,
lalr_ale_rli_code,
lalr_ale_alt_ref,
lalr_del_legacy_ref,
lalr_del_alt_ref,
lalr_update_req
FROM  dl_hat_app_legacy_ref
WHERE lalr_dlb_batch_id   = p_batch_id
AND   lalr_dl_load_status IN ('L','F','O');
--
CURSOR chk_app_ref(p_app_refno NUMBER) IS
SELECT app_refno, app_legacy_ref
FROM   applications
WHERE  app_refno = p_app_refno;
--
CURSOR chk_ale_rec(p_ale_app_refno NUMBER,
                   p_rli_code      VARCHAR) IS
SELECT ale_app_refno, ale_alt_ref
FROM   applic_list_entries
WHERE  ale_app_refno = p_ale_app_refno
AND    ale_rli_code  = p_rli_code;
--
-- ******************************
-- constants FOR error process
--
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_APP_LEGACY_REF';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
--
l_an_tab  VARCHAR2(1);
--
l_exists            VARCHAR2(1);
l_errors            VARCHAR2(10);
l_error_ind         VARCHAR2(10);
i                   INTEGER := 0;
l_app_refno         applications.app_refno%TYPE;
l_app_legacy_ref    applications.app_legacy_ref%TYPE;
l_ale_alt_ref       applic_list_entries.ale_alt_ref%TYPE;
l_ale_app_refno     applic_list_entries.ale_app_refno%TYPE;
l_update_req        VARCHAR2(1);

--
-- ******************************
BEGIN
--
fsc_utils.proc_start('s_dl_hat_app_legacy_ref.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_app_legacy_ref.dataload_validate',3 );
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
  cs := p1.lalr_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors := 'V';
  l_error_ind := 'N';
  l_app_refno := NULL;
  l_exists    := NULL;
  l_app_legacy_ref := NULL;
  l_ale_alt_ref    := NULL;
  l_ale_app_refno  := NULL;
  l_update_req     := 'N';
--
-- ***************  
-- Mandatory Fields for ALL record types
-- ***************  
  IF nvl(p1.lalr_app_type,'X') NOT IN ('APP','ALE')
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',330);
  END IF;
--
  IF p1.lalr_app_refno IS NULL
  THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',329);
  END IF;
--
  IF p1.lalr_app_refno IS NOT NULL
  THEN
   OPEN chk_app_ref (p1.lalr_app_refno);
   FETCH chk_app_ref INTO l_app_refno, l_app_legacy_ref;
   CLOSE chk_app_ref;
   IF l_app_refno IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',317);
   END IF;
  END IF;
--
  IF p1.lalr_app_refno != l_app_refno
  THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',335);
  END IF;
--
-- ***************    
-- Checks for APP(applications)record types
-- ***************  
  IF p1.lalr_app_type = 'APP'
  THEN
--
   IF p1.lalr_app_legacy_ref IS NULL
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',319);
   END IF;
--
   IF ( p1.lalr_ale_rli_code IS NOT NULL 
     OR p1.lalr_ale_alt_ref  IS NOT NULL  )
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',331);
   END IF;
--
-- Update only required if lalr_app_legacy_ref is different to the app_legacy_ref
--
   IF nvl(p1.lalr_app_legacy_ref,nvl(l_app_legacy_ref,'BLANK')) = nvl(l_app_legacy_ref,'BLANK')
    THEN
     l_update_req := 'N';
   ELSE
     l_update_req := 'Y';
   END IF;
--
  END IF;  -- end of APP processing
--  
-- ***************   
-- Checks for ALE(application list entries)record types
-- ***************
  IF p1.lalr_app_type = 'ALE'
  THEN
--
   IF p1.lalr_ale_rli_code IS NULL
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',320);
   END IF;
--
-- A valid rehousing list code should has been supplied
--
   IF p1.lalr_ale_rli_code IS NOT NULL
    THEN
     IF (NOT s_dl_hat_utils.f_exists_rlicode(p1.lalr_ale_rli_code))
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',218);
     END IF;
   END IF;
--
   IF p1.lalr_app_refno IS NOT NULL AND p1.lalr_ale_rli_code IS NOT NULL
    THEN
     OPEN chk_ale_rec (p1.lalr_app_refno,p1.lalr_ale_rli_code);
     FETCH chk_ale_rec INTO l_ale_app_refno, l_ale_alt_ref;
     CLOSE chk_ale_rec;
     IF l_ale_app_refno IS NULL
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',334);
     END IF;
   END IF;
--   
   IF p1.lalr_ale_alt_ref IS NULL
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',333);
   END IF;
--
   IF ( p1.lalr_app_legacy_ref IS NOT NULL  )
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',332);
   END IF;
--
-- Update only required if lalr_ale_alt_ref is different to the ale_alt_ref
--
   IF nvl(p1.lalr_ale_alt_ref,nvl(l_ale_alt_ref,'BLANK')) = nvl(l_ale_alt_ref,'BLANK')
    THEN
     l_update_req := 'N';
   ELSE
     l_update_req := 'Y';
   END IF;
--
  END IF;  -- end of ALE processing
--
-- Lastly Update lalr_update_req indicator for create process
--
  UPDATE dl_hat_app_legacy_ref
  SET lalr_update_req = l_update_req
  WHERE rowid = p1.rec_rowid
  AND lalr_dlb_batch_id = p1.lalr_dlb_batch_id
  AND lalr_dl_seqno = p1.lalr_dl_seqno;
--
-- ******************************
-- Now UPDATE the record count and error code
  IF l_errors = 'F' THEN
   l_error_ind := 'Y';
  ELSE
   l_error_ind := 'N';
  END IF;
--
-- ******************************
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
--
  END;
--
 END LOOP;
--
-- Section to analyse the tables populated by this data load
--
l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HAT_APP_LEGACY_REF');
--
fsc_utils.proc_END;
--
COMMIT;
--
 EXCEPTION
  WHEN OTHERS THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
-- ************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 is
SELECT
rowid rec_rowid,
lalr_dlb_batch_id,
lalr_dl_seqno,
lalr_dl_load_status,
lalr_app_type,
lalr_app_refno,
lalr_app_legacy_ref,
lalr_ale_rli_code,
lalr_ale_alt_ref,
lalr_del_legacy_ref,
lalr_del_alt_ref,
lalr_update_req
FROM  dl_hat_app_legacy_ref
WHERE lalr_dlb_batch_id   = p_batch_id
AND   lalr_dl_load_status = 'C';
--
CURSOR c2_app(p_batch VARCHAR2) IS
SELECT count(*)
FROM   dl_hat_app_legacy_ref
WHERE  lalr_dlb_batch_id = p_batch
AND    lalr_app_type = 'APP';
--
CURSOR c2_ale(p_batch VARCHAR2) IS
SELECT count(*)
FROM   dl_hat_app_legacy_ref
WHERE  lalr_dlb_batch_id = p_batch
AND    lalr_app_type = 'ALE';
--
-- ******************************
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_APP_LEGACY_REF';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
i INTEGER := 0;
l_an_tab  VARCHAR2(1);
l_app     INTEGER := 0;
l_ale     INTEGER := 0;
--
-- ******************************
BEGIN
--
fsc_utils.proc_start('s_dl_hat_app_legacy_ref.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_app_legacy_ref.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- check what type of records are being processed in the batch
--
OPEN c2_app (cb);
FETCH c2_app INTO l_app;
CLOSE c2_app;
--
OPEN c2_ale (cb);
FETCH c2_ale INTO l_ale;
CLOSE c2_ale;
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lalr_dl_seqno;
  i  := i +1;
  l_id := p1.rec_rowid;
-- *************** 
-- DELETE ONLY IF RECORD WAS UPDATED
-- *************** 
  IF p1.lalr_update_req = 'Y'
   THEN
-- ***************
-- Process for APP(applications table)record types
-- APP_LEGACY_REF IS a NULLABLE Field
-- ***************
   IF p1.lalr_app_type = 'APP'
    THEN
--
-- Firstly update the app_legacy_ref with the
-- del_app_legacy_ref that was originally replaced
-- may have been null if it was the create process
-- will set the lalr_del_legacy_ref = 'X'
--
     IF p1.lalr_del_legacy_ref = 'X'
      THEN	 
       UPDATE applications
       SET app_legacy_ref = NULL
       WHERE app_refno = p1.lalr_app_refno;
     ELSE	   
       UPDATE applications
       SET app_legacy_ref = p1.lalr_del_legacy_ref
       WHERE app_refno = p1.lalr_app_refno;
     END IF;
--
-- Then remove the del_app_legacy_ref from
-- the data load table
--
     UPDATE dl_hat_app_legacy_ref
     SET lalr_del_legacy_ref = null
     WHERE rowid = p1.rec_rowid
     AND lalr_dlb_batch_id = p1.lalr_dlb_batch_id
     AND lalr_dl_seqno = p1.lalr_dl_seqno
     AND lalr_app_type = p1.lalr_app_type
     AND lalr_app_refno = p1.lalr_app_refno
     AND lalr_app_legacy_ref = p1.lalr_app_legacy_ref;
--
   END IF; -- end of APP processing
-- ***************    
-- Process for ALE(applic_list_entries table)record types
-- ALE_ALT_REF IS a NOT NULLABLE Field
-- *************** 
   IF p1.lalr_app_type = 'ALE'
    THEN
--
-- Firstly update the ale_alt_ref with the
-- lalr_del_alt_ref that was originally replaced
-- 
     UPDATE applic_list_entries
     SET ale_alt_ref = p1.lalr_del_alt_ref
     WHERE ale_app_refno = p1.lalr_app_refno
     AND   ale_rli_code = p1.lalr_ale_rli_code;
--
-- Then remove the lalr_del_alt_ref from
-- the data load table
--
     UPDATE dl_hat_app_legacy_ref
     SET lalr_del_alt_ref = null
     WHERE rowid = p1.rec_rowid
     AND lalr_dlb_batch_id = p1.lalr_dlb_batch_id
     AND lalr_dl_seqno = p1.lalr_dl_seqno
     AND lalr_app_type = p1.lalr_app_type
     AND lalr_app_refno = p1.lalr_app_refno
     AND lalr_ale_rli_code = p1.lalr_ale_rli_code
     AND lalr_ale_alt_ref = p1.lalr_ale_alt_ref;
--
   END IF; -- end of ALE processing
--
  END IF;  -- end of delete processing
--
-- *****************************
-- keep a count of the rows processed and commit after every 1000
--
  i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
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
-- Section to analyse the tables populated by this data load
--
IF l_app > 0 THEN
  l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLICATIONS');
END IF;
--
IF l_ale > 0 THEN
  l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLIC_LIST_ENTRIES');
END IF;
--
l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HAT_APP_LEGACY_REF');
--
fsc_utils.proc_end;
--
COMMIT;
--
EXCEPTION
  WHEN OTHERS THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
  RAISE;
--
END dataload_delete;
--
END s_dl_hat_app_legacy_ref;
/

