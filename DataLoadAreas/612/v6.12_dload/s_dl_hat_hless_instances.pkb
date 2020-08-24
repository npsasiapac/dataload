CREATE OR REPLACE PACKAGE BODY s_dl_hat_hless_instances
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER   WHO  WHEN       WHY
--      1.0     5.15.1      MB  03/12/2009   Initial Version
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
  UPDATE dl_hat_hless_instances
  SET lhin_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_hless_instances');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
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
     ROWID REC_ROWID,
     LHIN_DLB_BATCH_ID,
     LHIN_DL_SEQNO,
     LHIN_DL_LOAD_STATUS,
	 LAPP_LEGACY_REF,
	 LHIN_ALE_RLI_CODE,
	 LHIN_INSTANCE_REFNO,
     LHIN_EXPECTED_HLESS_DATE,
	 LHIN_PRESENTED_DATE,
	 LHIN_ACCEPTED_HLESS_DATE,
     LHIN_HRV_HCR_CODE,
     LHIN_HRV_HOR_CODE,
     LIPT_CODE,
	 LHIN_COMMENTS,
	 LHIN_CREATED_BY,
	 LHIN_CREATED_DATE
FROM  dl_hat_hless_instances
WHERE lhin_dlb_batch_id   = p_batch_id
AND   lhin_dl_load_status = 'V';
--
CURSOR c3 (p_lipt_code IN VARCHAR2) IS
SELECT ipp_refno
FROM interested_parties
WHERE ipp_shortname = p_lipt_code
  AND EXISTS (select null from interested_party_types
              WHERE ipt_code = ipp_ipt_code
                AND ipt_hrv_fiy_code = 'HLCW');
--
CURSOR c6 (p_app_legacy_ref VARCHAR2) IS
SELECT app_refno
FROM applications
WHERE app_legacy_ref = p_app_legacy_ref;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_HLESS_INSTANCES';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
-- Other variables
l_an_tab         VARCHAR2(1);
i                INTEGER := 0;
l_ipp_refno      interested_parties.ipp_refno%TYPE;
l_app_refno      applications.app_refno%TYPE;
l_exists         VARCHAR2(1);
l_already_exists VARCHAR2(1);
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_hless_instances.dataload_create');
fsc_utils.debug_message( 's_dl_hat_hless_instances.dataload_create',3);
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
  cs := p1.lhin_dl_seqno;
  l_app_refno := NULL;
  l_already_exists := 'N';
  l_id := p1.rec_rowid;
  --
    OPEN  c6 (p1.lapp_legacy_ref);
    FETCH c6 INTO l_app_refno;
    CLOSE c6;
  --
	IF  p1.LHIN_CREATED_BY IS NULL
	THEN
	p1.LHIN_CREATED_BY := 'DATALOAD';
	END IF;

	IF p1.LHIN_CREATED_DATE IS NULL
	THEN
     p1.LHIN_CREATED_DATE := SYSDATE;
	END IF;
  --
  -- Establish a savepoint to ensure commit consistancy
  --
  SAVEPOINT SP1;
  --
  INSERT INTO hless_instances
          (HIN_INSTANCE_REFNO,
		   HIN_ALE_APP_REFNO,
           HIN_ALE_RLI_CODE,
		   HIN_EXPECTED_HLESS_DATE,
		   HIN_PRESENTED_DATE,
		   HIN_REUSABLE_REFNO,
		   HIN_CREATED_BY,
		   HIN_CREATED_DATE,
		   HIN_ACCEPTED_HLESS_DATE,
           HIN_HRV_HCR_CODE,
           HIN_HRV_HOR_CODE,
           HIN_COMMENTS)
    values
     (p1.lhin_instance_refno,
	  l_app_Refno,
	  p1.lhin_ale_rli_code,
      p1.lhin_EXPECTED_HLESS_DATE,
      p1.lhin_presented_DATE,	  
	  REUSABLE_REFNO_SEQ.nextval,
      p1.lhin_created_by,
	  p1.lhin_created_Date,
      p1.lhin_accepteD_HLESS_DATE,
      p1.lhin_HRV_HCR_CODE,
      p1.lhin_HRV_HOR_CODE,
      p1.lhin_comments);
    --
    -- Get the interested parties reference number
    --
    l_ipp_refno := null;
    --
    OPEN c3(p1.lipt_code);
    FETCH c3 INTO l_ipp_refno;
    CLOSE c3;
    --
    --
    INSERT INTO  INTERESTED_PARTY_USAGES
          (IPUS_REFNO
          ,IPUS_IPP_REFNO
          ,IPUS_START_DATE
          ,IPUS_HIN_INSTANCE_REFNO)
    VALUES
         (IPUS_REFNO_SEQ.nextval,
          l_ipp_refno,
          p1.lhin_presented_DATE,
          p1.lhin_instance_refno);

  --
  -- update the processed count and record status flag
  --
  s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
  set_record_status_flag(l_id,'C');
  --
  -- keep a count of the rows processed and commit after every 1000
  --
  i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
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
--
-- Section to analyse the table populated by the dataload
--
l_an_tab := s_dl_hem_utils.dl_comp_stats('HLESS_INSTANCES');
l_an_tab := s_dl_hem_utils.dl_comp_stats('INTERESTED_PARTY_USAGES');
--
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
CURSOR c1 is
SELECT
     ROWID REC_ROWID,
     LHIN_DLB_BATCH_ID,
     LHIN_DL_SEQNO,
     LHIN_DL_LOAD_STATUS,
	 LAPP_LEGACY_REF,
	 LHIN_ALE_RLI_CODE,
	 LHIN_INSTANCE_REFNO,
     LHIN_EXPECTED_HLESS_DATE,
	 LHIN_PRESENTED_DATE,
	 LHIN_ACCEPTED_HLESS_DATE,
     LHIN_HRV_HCR_CODE,
     LHIN_HRV_HOR_CODE,
     LIPT_CODE,
	 LHIN_COMMENTS,
	 LHIN_CREATED_BY,
	 LHIN_CREATED_DATE
FROM  dl_hat_hless_instances
WHERE lhin_dlb_batch_id    = p_batch_id
AND   lhin_dl_load_status IN ('L','F','O');
--
CURSOR c_ipt (p_ipp VARCHAR2) IS
SELECT ipt_hrv_fiy_code
FROM interested_party_types
,    interested_parties
WHERE ipp_shortname    = p_ipp
AND   ipp_ipt_code     = ipt_code;
--
CURSOR c_rli_type (p_rli_code VARCHAR2) IS
SELECT 'X'
FROM rehousing_lists
WHERE rli_code = p_rli_code
AND rli_type = 'H';
--
CURSOR c_hin_exists(p_hin_instance_refno VARCHAR2) IS
SELECT 'X'
FROM hless_instances
WHERE hin_instance_Refno = p_hin_instance_refno;
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_HLESS_INSTANCES';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
--
l_exists            VARCHAR2(1);
l_ipt_hrv_fiy_code  VARCHAR2(10);
l_errors            VARCHAR2(10);
l_error_ind         VARCHAR2(10);
i                   INTEGER := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_applications.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_applications.dataload_validate',3 );
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
  cs := p1.lhin_dl_seqno;
  l_id := p1.rec_rowid;
  --
  l_errors := 'V';
  l_error_ind := 'N';
  --
  -- The application legacy ref must be valid
  -- 
 IF (NOT s_dl_hat_utils.f_exists_application(p1.lapp_legacy_ref))
 THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',216);
 END IF;
 --
   -- A valid rehousing list code should have been supplied
  IF (NOT s_dl_hat_utils.f_exists_rlicode(p1.lhin_ale_rli_code))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',218);
  END IF;
 --
 -- The rehousing list should be of type 'H'
 --
 OPEN c_rli_type(p1.lhin_ale_rli_code);
 FETCH c_rli_type into l_exists;
 IF c_rli_type%notfound
	THEN
	l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',684);
 END IF;
 CLOSE c_rli_type; 
 --
 -- The homeless instance record should not already exist
 OPEN c_hin_exists(p1.LHIN_INSTANCE_REFNO);
 FETCH c_hin_exists INTO l_exists;
 IF c_hin_exists%found
	THEN
	l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',685);
 END IF;
 CLOSE c_hin_exists;
 
  --
  -- A case worker must be supplied and the case worker must be a valid interested party 
  -- that has an interested party type of 'HLCW'
  --
  l_ipt_hrv_fiy_code := NULL;
  OPEN  c_ipt(p1.lipt_code);
  FETCH c_ipt INTO l_ipt_hrv_fiy_code;
  CLOSE c_ipt;
  --
  IF l_ipt_hrv_fiy_code IS NULL
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',212);
  ELSIF l_ipt_hrv_fiy_code != 'HLCW'
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',211);
  END IF;
  --
  -- A valid reference value must be defined for homeless reason
  --
  IF (NOT s_dl_hem_utils.exists_frv('HCREASON',p1.lhin_hrv_hcr_code,'Y'))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',213);
  END IF;
  --
  -- A valid reference value must be defined for homeless origin
  --
  IF (NOT s_dl_hem_utils.exists_frv('HCORIGIN',p1.lhin_hrv_hor_code,'Y'))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',214);
  END IF;
  --
  -- A date is required for when the applicant is expected to be homeless
  --
  IF p1.lhin_expected_hless_date IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',204);
  END IF;
  --
  -- Now UPDATE the record count and error code
  --
  IF l_errors = 'F' 
  THEN
    l_error_ind := 'Y';
  ELSE
    l_error_ind := 'N';
  END IF;
  --
  -- update the processed count and record status flag
  --
  s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
  set_record_status_flag(l_id,l_errors);
  --
  -- keep a count of the rows processed and commit after every 1000
  --
  i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
  --
  EXCEPTION
  WHEN OTHERS 
  THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
    set_record_status_flag(l_id,'O');
  END;
--
END LOOP;
--
fsc_utils.proc_END;
COMMIT;
--
EXCEPTION
WHEN OTHERS 
THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 is
SELECT
     ROWID REC_ROWID,
     LHIN_DLB_BATCH_ID,
     LHIN_DL_SEQNO,
     LHIN_DL_LOAD_STATUS,
	 LAPP_LEGACY_REF,
	 LHIN_ALE_RLI_CODE,
	 LHIN_INSTANCE_REFNO,
	 LHIN_PRESENTED_DATE
FROM  dl_hat_hless_instances
WHERE lhin_dlb_batch_id   = p_batch_id
AND   lhin_dl_load_status = 'C';
--
--
i INTEGER := 0;
l_an_tab  VARCHAR2(1);
l_app_refno applications.app_refno%TYPE;
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_HLESS_INSTANCES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_hless_instances.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_hless_instances.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lhin_dl_seqno;
i  := i +1;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- Check the order of delete
--
DELETE FROM INTERESTED_PARTY_USAGES
WHERE ipus_hin_instance_refno = p1.lhin_instance_refno
AND   ipus_start_date = p1.lhin_presented_date;
--
DELETE FROM HLESS_INSTANCES
WHERE hin_instance_refno = p1.lhin_instance_refno;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'V');
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
EXCEPTION
WHEN OTHERS 
THEN
  ROLLBACK TO SP1;
  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
  set_record_status_flag(l_id,'C');
  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
--
END LOOP;
--
-- Section to analyse the table populated by the dataload
--
l_an_tab := s_dl_hem_utils.dl_comp_stats('HLESS_INSTANCES');
l_an_tab := s_dl_hem_utils.dl_comp_stats('INTERESTED_PARTY_USAGES');
--
fsc_utils.proc_end;
COMMIT;
--
EXCEPTION
WHEN OTHERS 
THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
  RAISE;
--
END dataload_delete;
--
--
END s_dl_hat_hless_instances;
/
