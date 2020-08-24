CREATE OR REPLACE PACKAGE BODY s_dl_hat_applic_statuses
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER   WHO  WHEN       WHY
--      1.0           RJ   03/04/2001  Dataload
--      2.0  5.2.0    PJD  12/07/2002  Changes for V520 
--                                     - setting of App Sco Code
--      2.1  5.2.0    PJD  24/07/2002  Use different version of 
--                                     s_applic_list_entries.all_mand_questions
--                                     _answered
--      2.2  5.2.0    PJD  22/08/2002  Corrected update of history table to
--                                     use app_refno
--      3.0  5.3.0    PH   31/01/2002  Amended section to update app_sco_code
--                                     to include HSD (Create process)
--      3.1  5.3.0    SB   20/03/2003  Added commit on validate
--      4.0  5.10.0   PH   19/02/2007  Amended error code for mandatory questions
--                                     check. Was 290 now 289.
--      4.1  5.13.0   PH   06-FEB-2008 Now includes its own 
--                                     set_record_status_flag procedure.
--      4.2   5.15.1  MB   03-DEC-2009   Commented out update to applic_list_entry_hist
--       4.3  5.16.0  MB    08-JUL-2010    Added validation check for applic_list_entry
--                                                      
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
  UPDATE dl_hat_applic_statuses
  SET lale_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_applic_statuses');
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
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2
,p_date              IN DATE    )
AS
--
CURSOR c1 IS
SELECT
     ROWID rec_rowid,
     LALE_DLB_BATCH_ID,
     LALE_DL_SEQNO,
     LALE_DL_LOAD_STATUS,
     LALE_RLI_CODE,
     LALE_LST_CODE,
     LALE_ALS_ACTIVE_IND,
     LALE_STATUS_START_DATE,
     LALE_STATUS_REVIEW_DATE,
     LALE_HRV_APS_CODE,
     LAPP_LEGACY_REF
FROM  dl_hat_applic_statuses
WHERE lale_dlb_batch_id   = p_batch_id
AND   lale_dl_load_status = 'V';

CURSOR c2 IS
SELECT lst_code
FROM  list_statuses
WHERE lst_status_type = 'U';
--
CURSOR c_lst_status(p_app_refno NUMBER) IS
SELECT lst_application_status_ind
FROM   list_statuses, applic_list_entries
WHERE  ale_app_refno = p_app_refno
AND    ale_lst_code  = lst_code
ORDER BY DECODE(lst_application_status_ind,'C',1,'N',2,'H',3,'X',4);
--
--
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_APPLIC_STATUSES';
cs       INTEGER;
ce	   VARCHAR2(200);
-- Other variables
--
l_an_tab         VARCHAR2(1);
i                INTEGER := 0;
l_app_refno      INTEGER;
l_app_sco_code   VARCHAR2(3);
l_lst_status     VARCHAR2(1);
l_id     ROWID;
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_applic_statuses.dataload_create');
fsc_utils.debug_message( 's_dl_hat_applic_statuses.dataload_create',3);
--
cb := p_batch_id;
cd := p_DATE;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lale_dl_seqno;
l_id := p1.rec_rowid;
--
l_app_refno := NULL;
l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lapp_legacy_ref);

IF p1.lale_lst_code IS NULL
THEN
   OPEN c2;
   IF c2%FOUND
   THEN
      FETCH c2 INTO p1.lale_lst_code;
   END IF;
   CLOSE c2;
END IF;
--
UPDATE applic_list_entries
SET ALE_LST_CODE           = p1.lale_lst_code,
    ALE_ALS_ACTIVE_IND     = nvl(p1.lale_als_active_ind,'N'),
    ALE_STATUS_START_DATE  = p1.lale_status_start_date,
    ALE_STATUS_REVIEW_DATE = p1.lale_status_review_date,
    ALE_HRV_APS_CODE       = p1.lale_hrv_aps_code
WHERE ale_app_refno       = l_app_refno
AND ale_rli_code           = p1.lale_rli_code;
--
/*
UPDATE applic_list_entry_history
SET LEH_LST_CODE           = p1.lale_lst_code,
    LEH_ALS_ACTIVE_IND     = nvl(p1.lale_als_active_ind,'N'),
    LEH_STATUS_START_DATE  = p1.lale_status_start_date,
    LEH_STATUS_REVIEW_DATE = p1.lale_status_review_date,
    LEH_APPLICATION_STATUS_REASON = p1.lale_hrv_aps_code
WHERE leh_app_refno        = l_app_refno
AND leh_rli_code           = p1.lale_rli_code;
*/
--
-- New secton added in 5.2.0 to update the application sco code
l_lst_status := NULL;
l_app_sco_code := NULL;
OPEN  c_lst_status (l_app_refno);
FETCH c_lst_status into l_lst_status;
CLOSE c_lst_status;
--
IF    l_lst_status = 'N' then l_app_sco_code := 'NEW';
ELSIF l_lst_status = 'C' then l_app_sco_code := 'CUR';
ELSIF l_lst_status = 'X' then l_app_sco_code := 'CLD';
ELSIF l_lst_status = 'H' then l_app_sco_code := 'HSD';
END IF;
--
UPDATE applications
SET    app_sco_code = l_app_sco_code 
WHERE  app_refno    = l_app_refno;
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
--
 END LOOP;
--
-- Section to analyse the table populated by this dataload
l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLIC_LIST_ENTRIES');
l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLIC_LIST_ENTRY_HISTORY');
fsc_utils.proc_end;
COMMIT;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_create;
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2
     ,p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
     ROWID rec_rowid,
     LALE_DLB_BATCH_ID,
     LALE_DL_SEQNO,
     LALE_DL_LOAD_STATUS,
     LALE_RLI_CODE,
     LALE_LST_CODE,
     LALE_ALS_ACTIVE_IND,
     LALE_STATUS_START_DATE,
     LALE_STATUS_REVIEW_DATE,
     LALE_HRV_APS_CODE,
     LAPP_LEGACY_REF
FROM  dl_hat_applic_statuses
WHERE lale_dlb_batch_id    = p_batch_id
AND   lale_dl_load_status IN ('L','F','O');
--
CURSOR C2 (p_lapp_legacy_ref VARCHAR2) is
SELECT ale_app_refno
FROM applic_list_entries
WHERE ale_alt_ref = p_lapp_legacy_ref;


-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_APPLIC_STATUSES';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_errors            VARCHAR2(10);
l_error_ind         VARCHAR2(10);
i                   INTEGER := 0;
l_dates_required    VARCHAR2(1) := NULL;
l_lale_app_refno    applic_list_entries.ale_app_refno%TYPE;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_applic_statuses.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_applic_statuses.dataload_validate',3 );
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
cs := p1.lale_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- Check that the application list entry must relate to an existing
-- application

 IF (NOT s_dl_hat_utils.f_exists_application(p1.lapp_legacy_ref))
 THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',216);
 END IF;
 --
 --

 -- A valid rehousing list code should have been supplied
  IF (NOT s_dl_hat_utils.f_exists_rlicode(p1.lale_rli_code))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',218);
  END IF;

-- MB 08/07/2010 Identified during OHMS migration testing
--
-- A valid applic_list_entries record must exist
  
 IF (NOT s_dl_hat_utils.f_exists_applistentry(p1.lapp_legacy_Ref,
                                              p1.lale_rli_code))
 THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',207);
 END IF;
  
-- A valid application status code must exist in list statuses
 IF (NOT s_dl_hat_utils.f_exists_lstcode(p1.lale_lst_code))
 THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',330);
 END IF;
--
-- the application status code must be a valid system reference domain
-- STATREASN
 IF p1.lale_hrv_aps_code IS NOT NULL
 THEN
    IF (NOT s_dl_hem_utils.exists_frv('STATREASN',p1.lale_hrv_aps_code))
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',293);
    END IF;
 END IF;
    --
-- In addition if the status requires start and end dates
-- these must be supplied
-- l_date_required = 'P' Then need both start and review date required
-- l_date_requied = 'S' Then need start date only

 l_dates_required := s_dl_hat_utils.f_dates_required(p1.lale_lst_code);

 IF l_dates_required = 'P'
 THEN
    IF p1.lale_status_start_date IS NULL OR
       p1.lale_status_review_date IS NULL
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',291);
    END IF;
 END IF;

 IF l_dates_required = 'S'
 THEN
    IF p1.lale_status_start_date IS NULL
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',341);
    END IF;
 END IF;
 -- Where the status is defined as an active status all mandatory
 -- questions must be answered
 -- Get the app refno for applic_list_entries table
 OPEN C2 (p1.lapp_legacy_ref);
 FETCH C2 INTO l_lale_app_refno;
 CLOSE C2;

 IF p1.lale_als_active_ind = 'Y'
 THEN
    IF (NOT s_applic_list_entries.all_mand_questions_answered
                                  (l_lale_app_refno,
                                   p1.lale_rli_code))
    THEN
 --      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',290); Incorrect error message
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',289);
    END IF;
 END IF;

-- Now UPDATE the record count and error code
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
--
s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
set_record_status_flag(l_id,l_errors);
--
   EXCEPTION
      WHEN OTHERS THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'C');
 END;
--
END LOOP;
--

--
fsc_utils.proc_END;
commit;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
--
--
--
END s_dl_hat_applic_statuses;
/

