CREATE OR REPLACE PACKAGE BODY s_dl_hat_hless_ins_stage_decis
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Ver    WHO  WHEN       WHY
--  1.0            5.15.1    MB   03/12/2009    Initial Version
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
  UPDATE dl_hat_hless_ins_stage_decis
  SET lhid_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_hless_ins_stage_decis');
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
     LHID_DLB_BATCH_ID,
     LHID_DL_SEQNO,
     LHID_DL_LOAD_STATUS,
     LHID_RLS_CODE,
     LHID_SCO_CODE,
     LHID_STATUS_DATE,
     LHID_CREATED_DATE,
     LHID_CREATED_BY,
     LHID_COMMENTS,
     LHID_DECISION_DATE,
     LHID_DECISION_BY,
     LHID_AUTHORISED_DATE,
     LHID_AUTHORISED_BY,
     LHID_AUTH_STATUS_START_DATE,
     LHID_AUTH_STATUS_REVIEW_DATE,
     LHID_HRV_APS_CODE,
     LHID_HRV_SDR_CODE,
     LHID_RSD_HRV_LSD_CODE,
     LHID_PROVISIONAL_LST_CODE,
     LHIN_INSTANCE_REFNO
FROM  dl_hat_hless_ins_stage_decis
WHERE lhid_dlb_batch_id   = p_batch_id
AND   lhid_dl_load_status = 'V';
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_HLESS_INS_STAGE_DECIS';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
-- Other variables
--
i                   INTEGER := 0;
l_an_tab            VARCHAR2(1);
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_hless_ins_stage.dataload_create');
fsc_utils.debug_message( 's_dl_hat_hless_ins_stage.dataload_create',3);
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
cs := p1.lhid_dl_seqno;
l_id := p1.rec_rowid;
--

IF p1.lhid_CREATED_DATE IS NULL
THEN
   p1.lhid_CREATED_DATE := SYSDATE;
END IF;

IF p1.lhid_CREATED_BY IS NULL
THEN
   p1.lhid_CREATED_BY := USER;
END IF;
--
INSERT INTO hless_ins_stage_decisions
          ( HID_HIN_INSTANCE_REFNO,
            HID_RLS_CODE,
            HID_SCO_CODE,
            HID_STATUS_DATE,
            HID_CREATED_DATE,
            HID_CREATED_BY,
            HID_COMMENTS,
            HID_DECISION_DATE,
            HID_DECISION_BY,
            HID_AUTHORISED_DATE,
            HID_AUTHORISED_BY,
            HID_AUTH_STATUS_START_DATE,
            HID_AUTH_STATUS_REVIEW_DATE,
            HID_HRV_APS_CODE,
            HID_HRV_SDR_CODE,
            HID_RSD_HRV_LSD_CODE,
            HID_PROVISIONAL_LST_CODE)
VALUES
    (p1.lhin_instance_refno,
     p1.lhid_RLS_CODE,
     p1.lhid_SCO_CODE,
     p1.lhid_STATUS_DATE,
     p1.lhid_CREATED_DATE,
     p1.lhid_CREATED_BY,
     p1.lhid_COMMENTS,
     p1.lhid_DECISION_DATE,
     p1.lhid_DECISION_BY,
     p1.lhid_AUTHORISED_DATE,
     p1.lhid_AUTHORISED_BY,
     p1.lhid_AUTH_STATUS_START_DATE,
     p1.lhid_AUTH_STATUS_REVIEW_DATE,
     p1.lhid_HRV_APS_CODE,
     p1.lhid_HRV_SDR_CODE,
     p1.lhid_RSD_HRV_LSD_CODE,
     p1.lhid_PROVISIONAL_LST_CODE );
--
-- Now need to go back and update because a trigger is setting the
-- lhid_created_date and lhid_created_by
--
UPDATE  hless_ins_stage_decisions
SET     HID_created_date = p1.lhid_created_date,
        HID_created_by   = p1.lhid_created_by
WHERE   HID_hin_instance_Refno = p1.lhin_instance_refno
AND     HID_rls_code      = p1.lhid_rls_code
AND     HID_sco_code      = p1.lhid_sco_code
AND     HID_status_date   = p1.lhid_status_date;
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

l_an_tab := s_dl_hem_utils.dl_comp_stats('HLESS_INS_STAGE_DECISIONS');

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
     lhid_DLB_BATCH_ID,
     lhid_DL_SEQNO,
     lhid_DL_LOAD_STATUS,
     lhid_RLS_CODE,
     lhid_SCO_CODE,
     lhid_STATUS_DATE,
     lhid_CREATED_DATE,
     lhid_CREATED_BY,
     lhid_COMMENTS,
     lhid_DECISION_DATE,
     lhid_DECISION_BY,
     lhid_AUTHORISED_DATE,
     lhid_AUTHORISED_BY,
     lhid_AUTH_STATUS_START_DATE,
     lhid_AUTH_STATUS_REVIEW_DATE,
     lhid_HRV_APS_CODE,
     lhid_HRV_SDR_CODE,
     lhid_RSD_HRV_LSD_CODE,
     lhid_PROVISIONAL_LST_CODE,
     Lhin_instance_Refno
FROM  dl_hat_hless_ins_stage_decis
WHERE lhid_dlb_batch_id    = p_batch_id
AND   lhid_dl_load_status IN ('L','F','O');
--
CURSOR c_rls_exist(p_rls_code   VARCHAR2,
                   p_lsd_code   VARCHAR2) IS
SELECT 'X'
FROM   rehousing_list_stage_decisions
WHERE  rsd_rls_code     = p_rls_code
AND    rsd_hrv_lsd_code = p_lsd_code;
--
cursor c_hin_exist(p_hin_instance_refno VARCHAR2) IS
SELECT 'X'
FROM hless_instances
WHERE hin_instance_refno = p_hin_instance_refno;
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_HLESS_INS_STAGE_DECIS';
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
fsc_utils.proc_start('s_dl_hat_hless_ins_stage.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_hless_ins_stage.dataload_validate',3 );
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
cs := p1.lhid_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
l_exists := null;

-- A stage decision must relate to a homeless instance record
--
OPEN c_hin_exist(p1.lhin_instance_refno);
FETCH c_hin_exist INTO l_exists;
IF c_hin_exist%notfound
THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',680);
END IF;
CLOSE c_hin_exist;
 --
  -- There should not be an existing homeless instance decision held against the
  -- homeless instance

  IF (s_dl_hat_utils.f_exists_hid(p1.lhin_instance_refno,
                                  p1.lhid_rls_code))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',683);
  END IF;

   -- check the rehousing list decision code if supplied
   IF p1.lhid_rsd_hrv_lsd_code IS NOT NULL
   THEN
       IF (NOT s_dl_hat_utils.f_exists_rsdcode(p1.lhid_rsd_hrv_lsd_code))
       THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',206);
       END IF;
   --
   -- Check its a valid combination
      OPEN c_rls_exist(p1.lhid_rls_code, p1.lhid_rsd_hrv_lsd_code);
       FETCH c_rls_exist into l_exists;
        IF c_rls_exist%notfound
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',250);
        END IF;
      CLOSE c_rls_exist;
   --
    END IF;

    -- check the rehousing list decision reason code if supplied
    IF p1.lhid_hrv_sdr_code IS NOT NULL
    THEN
       IF (NOT s_dl_hem_utils.exists_frv('HCDREASON',p1.lhid_hrv_sdr_code))
       THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',222);
        END IF;
     END IF;


    IF p1.lhid_rsd_hrv_lsd_code IS NULL
    THEN
       p1.lhid_sco_code := 'PRO';
    END IF;

    IF p1.lhid_rsd_hrv_lsd_code IS NOT NULL
    AND (p1.lhid_authorised_date IS NULL AND p1.lhid_authorised_by IS NULL)
    THEN
       p1.lhid_sco_code := 'DEC';
    END IF;

    IF p1.lhid_rsd_hrv_lsd_code IS NOT NULL
       AND (p1.lhid_authorised_date IS NOT NULL
       AND p1.lhid_authorised_by IS NOT NULL)
    THEN
       p1.lhid_sco_code := 'AUT';
    END IF;
--  UPDATE the dl_hat_hless_ins_stage_decis.lhid_sco_code field
   IF p1.lhid_sco_code IS NOT NULL
   THEN
      UPDATE dl_hat_hless_ins_stage_decis
      SET    lhid_sco_code = p1.lhid_sco_code
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
     ROWID REC_ROWID,
     LHID_DLB_BATCH_ID,
     LHID_DL_SEQNO,
     LHID_DL_LOAD_STATUS,
     LHIN_INSTANCE_REFNO,
     LHID_RLS_CODE
FROM  dl_hat_hless_ins_stage_decis
WHERE lhid_dlb_batch_id   = p_batch_id
AND   lhid_dl_load_status = 'C';
--
--
i         INTEGER := 0;
l_an_tab  VARCHAR2(1);
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_HLESS_INS_STAGE_DECIS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_hless_ins_stage.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_hless_ins_stage.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lhid_dl_seqno;
i  := i +1;
l_id := p1.rec_rowid;
--
--
DELETE FROM hless_ins_stge_decision_hist
WHERE  HIDH_HID_HIN_INSTANCE_REFNO = p1.lhin_instance_refno
AND    HIDH_HID_RLS_CODE      = p1.lhid_rls_code;
--
DELETE FROM hless_ins_stage_decisions
WHERE  hid_hin_instance_refno=  p1.lhin_instance_refno
AND    HID_RLS_CODE      = p1.lhid_rls_code;
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

l_an_tab := s_dl_hem_utils.dl_comp_stats('HLESS_INS_STAGE_DECISIONS');

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
END s_dl_hat_hless_ins_stage_decis;
/

