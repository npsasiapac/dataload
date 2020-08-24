CREATE OR REPLACE PACKAGE BODY s_dl_hat_applic_list_stage
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Ver    WHO  WHEN       WHY
--      1.0            RJ  03/04/2001  Dataload
--      1.1 5.5.0     PJD  19/01/2004  ct now set to tablename in UPPERCASE
--      1.2 5.5.0     PH   02/03/2004  Added final commit to validate and
--                                     delete and also missing RAISE
--      1.3 5.5.0     PH   02/03/2004  Added validation on rehousing list 
--                                     stage and decision combination.
--      1.4 5.5.0     PH   20/04/2004  Corrected final commit on all processes
--      1.5 5.5.0     PJD  28/05/2004  Added update of created fields after Insert
--      1.6 5.9.0     PH   18/08/2006  Delete from applic_stage_decision_hist
--      3.0 5.13.0    PH   06-FEB-2008 Now includes its own 
--                                     set_record_status_flag procedure.
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
  UPDATE dl_hat_applic_list_stage_decis
  SET lals_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_applic_list_stage_decis');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
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
     LALS_DLB_BATCH_ID,
     LALS_DL_SEQNO,
     LALS_DL_LOAD_STATUS,
     LALS_ALE_RLI_CODE,
     LALS_RLS_CODE,
     LALS_SCO_CODE,
     LALS_STATUS_DATE,
     LALS_CREATED_DATE,
     LALS_CREATED_BY,
     LALS_COMMENTS,
     LALS_DECISION_DATE,
     LALS_DECISION_BY,
     LALS_AUTHORISED_DATE,
     LALS_AUTHORISED_BY,
     LALS_AUTH_STATUS_START_DATE,
     LALS_AUTH_STATUS_REVIEW_DATE,
     LALS_HRV_APS_CODE,
     LALS_HRV_SDR_CODE,
     LALS_RSD_HRV_LSD_CODE,
     LALS_PROVISIONAL_LST_CODE,
     LAPP_LEGACY_REF
FROM  dl_hat_applic_list_stage_decis
WHERE lals_dlb_batch_id   = p_batch_id
AND   lals_dl_load_status = 'V';
--
--
CURSOR C2 (p_lapp_legacy_ref VARCHAR2) IS
SELECT ale_app_refno
FROM applic_list_entries
WHERE ale_alt_ref = p_lapp_legacy_ref;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_APPLIC_LIST_STAGE_DECIS';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
-- Other variables
--
i                   INTEGER := 0;
l_an_tab            VARCHAR2(1);
l_als_ale_app_refno applic_list_stage_decisions.als_ale_app_refno%TYPE;
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_applic_list_stage.dataload_create');
fsc_utils.debug_message( 's_dl_hat_applic_list_stage.dataload_create',3);
--
cb := p_batch_id;
cd := p_DATE;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lals_dl_seqno;
l_id := p1.rec_rowid;
--
OPEN C2 (p1.lapp_legacy_ref);
FETCH C2 INTO l_als_ale_app_refno;
CLOSE C2;

IF p1.LALS_CREATED_DATE IS NULL
THEN
   p1.LALS_CREATED_DATE := SYSDATE;
END IF;

IF p1.LALS_CREATED_BY IS NULL
THEN
   p1.LALS_CREATED_BY := USER;
END IF;
--
INSERT INTO applic_list_stage_decisions
          ( ALS_ALE_APP_REFNO,
            ALS_ALE_RLI_CODE,
            ALS_RLS_CODE,
            ALS_SCO_CODE,
            ALS_STATUS_DATE,
            ALS_CREATED_DATE,
            ALS_CREATED_BY,
            ALS_COMMENTS,
            ALS_DECISION_DATE,
            ALS_DECISION_BY,
            ALS_AUTHORISED_DATE,
            ALS_AUTHORISED_BY,
            ALS_AUTH_STATUS_START_DATE,
            ALS_AUTH_STATUS_REVIEW_DATE,
            ALS_HRV_APS_CODE,
            ALS_HRV_SDR_CODE,
            ALS_RSD_HRV_LSD_CODE,
            ALS_PROVISIONAL_LST_CODE)
VALUES
    (l_als_ale_app_refno,
     p1.LALS_ALE_RLI_CODE,
     p1.LALS_RLS_CODE,
     p1.LALS_SCO_CODE,
     p1.LALS_STATUS_DATE,
     p1.LALS_CREATED_DATE,
     p1.LALS_CREATED_BY,
     p1.LALS_COMMENTS,
     p1.LALS_DECISION_DATE,
     p1.LALS_DECISION_BY,
     p1.LALS_AUTHORISED_DATE,
     p1.LALS_AUTHORISED_BY,
     p1.LALS_AUTH_STATUS_START_DATE,
     p1.LALS_AUTH_STATUS_REVIEW_DATE,
     p1.LALS_HRV_APS_CODE,
     p1.LALS_HRV_SDR_CODE,
     p1.LALS_RSD_HRV_LSD_CODE,
     p1.LALS_PROVISIONAL_LST_CODE );
--
-- Now need to go back and update because a trigger is setting the
-- lals_created_date and lals_created_by
--
UPDATE  applic_list_stage_decisions
SET     als_created_date = p1.lals_created_date,
        als_created_by   = p1.lals_created_by
WHERE  
        als_ale_app_refno = l_als_ale_app_refno
AND     als_ale_rli_code  = p1.lals_ale_rli_code
AND     als_rls_code      = p1.lals_rls_code
AND     als_sco_code      = p1.lals_sco_code
AND     als_status_date   = p1.lals_status_date;
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
COMMIT;
--
-- Section to analyse the table populated by this dataload

l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLIC_LIST_STAGE_DECISIONS');

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
     LALS_DLB_BATCH_ID,
     LALS_DL_SEQNO,
     LALS_DL_LOAD_STATUS,
     LALS_ALE_RLI_CODE,
     LALS_RLS_CODE,
     LALS_SCO_CODE,
     LALS_STATUS_DATE,
     LALS_CREATED_DATE,
     LALS_CREATED_BY,
     LALS_COMMENTS,
     LALS_DECISION_DATE,
     LALS_DECISION_BY,
     LALS_AUTHORISED_DATE,
     LALS_AUTHORISED_BY,
     LALS_AUTH_STATUS_START_DATE,
     LALS_AUTH_STATUS_REVIEW_DATE,
     LALS_HRV_APS_CODE,
     LALS_HRV_SDR_CODE,
     LALS_RSD_HRV_LSD_CODE,
     LALS_PROVISIONAL_LST_CODE,
     LAPP_LEGACY_REF
FROM  dl_hat_applic_list_stage_decis
WHERE lals_dlb_batch_id    = p_batch_id
AND   lals_dl_load_status IN ('L','F','O');
--
CURSOR C2 (p_lapp_legacy_ref VARCHAR2) IS
SELECT ale_app_refno
FROM applic_list_entries
WHERE ale_alt_ref = p_lapp_legacy_ref;
--
CURSOR c_rls_exist(p_rls_code   VARCHAR2,
                   p_lsd_code   VARCHAR2) IS
SELECT 'X'
FROM   rehousing_list_stage_decisions
WHERE  rsd_rls_code     = p_rls_code
AND    rsd_hrv_lsd_code = p_lsd_code;
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_APPLIC_LIST_STAGE_DECIS';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
--
l_errors            VARCHAR2(10);
l_error_ind         VARCHAR2(10);
i                   INTEGER := 0;
l_exists            VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_applic_list_stage.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_applic_list_stage.dataload_validate',3 );
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
cs := p1.lals_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
l_exists := null;

-- A rehousing list decision must relate to an application held on the
-- APPLICATIONS table


 IF (NOT s_dl_hat_utils.f_exists_application(p1.lapp_legacy_ref))
 THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',216);
 END IF;

  -- There should not be an existing rehousing list decision held against the
  -- application

  IF (s_dl_hat_utils.f_exists_rld(p1.lapp_legacy_ref,
                                  p1.lals_ale_rli_code,
                                  p1.lals_rls_code))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',205);
  END IF;

  -- A valid rehousing list code should have been supplied
  IF (NOT s_dl_hat_utils.f_exists_rlicode(p1.lals_ale_rli_code))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',218);
  END IF;

   -- check the rehousing list decision code if supplied
   IF p1.lals_rsd_hrv_lsd_code IS NOT NULL
   THEN
       IF (NOT s_dl_hat_utils.f_exists_rsdcode(p1.lals_rsd_hrv_lsd_code))
       THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',206);
       END IF;
   --
   -- Check its a valid combination
      OPEN c_rls_exist(p1.lals_rls_code, p1.lals_rsd_hrv_lsd_code);
       FETCH c_rls_exist into l_exists;
        IF c_rls_exist%notfound
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',250);
        END IF;
      CLOSE c_rls_exist;
   --
    END IF;

    -- check the rehousing list decision reason code if supplied
    IF p1.lals_hrv_sdr_code IS NOT NULL
    THEN
       IF (NOT s_dl_hem_utils.exists_frv('HCDREASON',p1.lals_hrv_sdr_code))
       THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',222);
        END IF;
     END IF;


    IF p1.lals_rsd_hrv_lsd_code IS NULL
    THEN
       p1.lals_sco_code := 'PRO';
    END IF;

    IF p1.lals_rsd_hrv_lsd_code IS NOT NULL
    AND (p1.lals_authorised_date IS NULL AND p1.lals_authorised_by IS NULL)
    THEN
       p1.lals_sco_code := 'DEC';
    END IF;

    IF p1.lals_rsd_hrv_lsd_code IS NOT NULL
       AND (p1.lals_authorised_date IS NOT NULL
       AND p1.lals_authorised_by IS NOT NULL)
    THEN
       p1.lals_sco_code := 'AUT';
    END IF;
--  UPDATE the dl_hat_applic_list_stage_decis.lals_sco_code field
   IF p1.lals_sco_code IS NOT NULL
   THEN
      UPDATE dl_hat_applic_list_stage_decis
      SET    lals_sco_code = p1.lals_sco_code
      WHERE  ROWID = p1.rec_rowid;
   END IF;

-- Now UPDATE the record count and error code
IF l_errors = 'F' THEN
  l_error_ind := 'Y';
ELSE
  l_error_ind := 'N';
END IF;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
set_record_status_flag(l_id,l_errors);
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
--
--
   EXCEPTION
      WHEN OTHERS THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
 END;
--
END LOOP;
--
COMMIT;
--
fsc_utils.proc_END;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
     RAISE;
--
COMMIT;
--
END dataload_validate;
--
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 IS
SELECT
     ROWID rec_rowid,
     LALS_DLB_BATCH_ID,
     LALS_DL_SEQNO,
     LALS_DL_LOAD_STATUS,
     LAPP_LEGACY_REF,
     LALS_ALE_RLI_CODE,
     LALS_RLS_CODE
FROM  dl_hat_applic_list_stage_decis
WHERE lals_dlb_batch_id   = p_batch_id
AND   lals_dl_load_status = 'C';
--
--
--
CURSOR C2 (p_lapp_legacy_ref VARCHAR2) IS
SELECT ale_app_refno
FROM applic_list_entries
WHERE ale_alt_ref = p_lapp_legacy_ref;

--
i         INTEGER := 0;
l_an_tab  VARCHAR2(1);
l_als_ale_app_refno applic_list_stage_decisions.als_ale_app_refno%TYPE;
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_APPLIC_LIST_STAGE_DECIS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_applic_list_stage.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_applic_list_stage.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lals_dl_seqno;
i  := i +1;
l_id := p1.rec_rowid;
--
--
OPEN C2 (p1.lapp_legacy_ref);
FETCH C2 INTO l_als_ale_app_refno;
CLOSE C2;
--
DELETE FROM applic_stage_decision_hist
WHERE  SDH_ALS_ALE_APP_REFNO = l_als_ale_app_refno
AND    SDH_ALS_ALE_RLI_CODE  = p1.lals_ale_rli_code
AND    SDH_ALS_RLS_CODE      = p1.lals_rls_code;
--
DELETE FROM applic_list_stage_decisions
WHERE  ALS_ALE_APP_REFNO = l_als_ale_app_refno
AND    ALS_ALE_RLI_CODE  = p1.lals_ale_rli_code
AND    ALS_RLS_CODE      = p1.lals_rls_code;
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
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'C');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
--
END LOOP;
--
COMMIT;
--
-- Section to analyse the table populated by this dataload

l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLIC_LIST_STAGE_DECISIONS');

fsc_utils.proc_end;
COMMIT;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
COMMIT;
--
END dataload_delete;
--
--
END s_dl_hat_applic_list_stage;
/

