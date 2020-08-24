CREATE OR REPLACE PACKAGE BODY s_dl_hat_hml_applications
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER   WHO  WHEN       WHY
--      1.0            RJ  03/04/2001  Dataload
--      2.0            SB  20/03/2003  Added commit on Validate
--      3.0  5.4.0    PJD  07/11/2003  New column for lapp_refno
--      3.1  5.4.0    PJD  10/11/2003  Homeless Origin and Reason not 
--                                     mandatory at 5.4
--      3.2  5.4.0    MH   21/11/2003  Homeless caseworker IPP details
--                                     changed (Cursor c3 create)
--      3.3  5.4.0    PH   11/12/2003  Amended above cursor to get ipp_refno
--                                     Added Savepoints to Create and Delete. 
--                                     Also removed blank lines
--      3.4  5.4.0    PH   15/12/2003  Amended Ins into interested_party_usages
--                                     can only insert one of app_refno or 
--                                     tcy_refno,not both.
--                                     Also removed validate on 
--                                     dl_hat_involved_parties
--                                     as this is no longer required 
--                                     (and slows it down)
--      3.5  5.4.0    PJD  22/01/2004  Remove validation that application
--                                     already exists.
--                                     Allows App refno to be supplied.
--                                     Minor Tidying of some bits of code
--      3.6  5.4.0    PJD  03/02/2004  l_ipp_refno now been set for Update cases
--                                     within the Create Procedure
--      3.7  5.5.0    PH   08/06/2004  Amended create process by setting l_tcy_refno
--                                     to null within the loop.
--      4.0  5.12.0   PH   17/07/2007  New fields for Source and Case Advice Ref
--      4.1  5.13.0   PH   06-FEB-2008 Now includes its own 
--                                     set_record_status_flag procedure.
--      4.2  6.18     PJD  19-DEC-2018 Various bits of tidying up of variables
--                                     Added in the validation of mandatory
--                                     fields which had been added to the main
--                                     Applicatoins dataload some while back.
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
  UPDATE dl_hat_hml_applications
  SET lapp_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_hml_applications');
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
     rowid rec_rowid,
     lapp_dlb_batch_id,
     lapp_dl_seqno,
     lapp_dl_load_status,
     NVL(lapp_offer_flag,'N') l_offer,
     NVL(lapp_nomination_flag,'N') l_nom,
     lapp_received_date,
     lapp_corr_name,
     NVL(lapp_sco_code,'NEW') l_sco,
     nvl(lapp_status_date,lapp_received_date) l_status,
     lapp_rent_account_details,
     lapp_expected_hless_date,
     lapp_legacy_ref,
     lapp_hrv_hcr_code,
     lapp_hrv_hor_code,
     LIPT_CODE,
     LTCY_ALT_REF,
     lapp_refno,
     lapp_hrv_fssa_code,
     lapp_acas_alt_ref
FROM  dl_hat_hml_applications
WHERE lapp_dlb_batch_id   = p_batch_id
AND   lapp_dl_load_status = 'V';
--
CURSOR c2(p_ltcy_alt_ref IN VARCHAR2) IS
SELECT tcy_refno
FROM tenancies
WHERE tcy_alt_ref = p_ltcy_alt_ref;
--
CURSOR c3 (p_lipt_code IN VARCHAR2) IS
SELECT ipp_refno
FROM interested_parties
WHERE ipp_shortname = p_lipt_code
  AND EXISTS (select null from interested_party_types
              WHERE ipt_code = ipp_ipt_code
                AND ipt_hrv_fiy_code = 'HLCW');
--
CURSOR c4 IS
SELECT app_refno_seq.nextval FROM dual;
--
CURSOR c5 (p_app_refno NUMBER) IS
SELECT 'X'
FROM applications
WHERE app_refno = p_app_refno;
--
CURSOR c6 (p_app_legacy_ref VARCHAR2) IS
SELECT app_refno
FROM applications
WHERE app_legacy_ref = p_app_legacy_ref;
--
CURSOR c_acas_ref(p_acas_alt_ref IN VARCHAR2) IS
SELECT acas_reference
FROM   advice_cases
WHERE  acas_alternate_reference = p_acas_alt_ref;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_HML_APPLICATIONS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
-- Other variables
l_an_tab         VARCHAR2(1);
i1               PLS_INTEGER := 0;
l_ipp_refno      interested_parties.ipp_refno%TYPE;
l_tcy_refno      applications.app_tcy_refno%TYPE;
l_app_refno      applications.app_refno%TYPE;
l_exists         VARCHAR2(1);
l_already_exists VARCHAR2(1);
l_acas_ref       PLS_INTEGER;
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_hml_applications.dataload_create');
fsc_utils.debug_message( 's_dl_hat_hml_applications.dataload_create',3);
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
  cs := p1.lapp_dl_seqno;
  l_app_refno := NULL;
  l_already_exists := 'N';
  l_id := p1.rec_rowid;
  --
  -- Get the correspondence name from DL_HAT_INVOLVED_PARTIES
  -- NO! This field is manadatory so the validation will have checked it exists
  -- IF p1.lapp_corr_name IS NULL OR p1.lapp_corr_name = ' '
  -- THEN
  --  p1.lapp_corr_name := s_dl_hat_utils.f_correspondence_name
  --                                                  (p1.lapp_legacy_ref);
  --END IF;
  -- Get the tenancy reference number
  --
  l_tcy_refno := null;
  --
  IF p1.ltcy_alt_ref IS NOT NULL
  THEN
     OPEN c2 (p1.ltcy_alt_ref);
     FETCH c2 INTO l_tcy_refno;
     CLOSE c2;
  END IF;
  --
  -- New bit added to allow for update to existing application
  --
  IF p1.lapp_refno IS NULL
  THEN
    OPEN  c6 (p1.lapp_legacy_ref);
    FETCH c6 INTO l_app_refno;
    IF c6%FOUND 
      THEN l_already_exists := 'Y';
    END IF;
    CLOSE c6;
  ELSE
    OPEN  c5 (p1.lapp_refno);
    FETCH c5 INTO l_exists;
    IF c5%FOUND 
      THEN l_already_exists := 'Y';
           l_app_refno      := p1.lapp_refno;
    END IF;
    CLOSE c5;
  END IF;
  --
  --
  -- Get the advice Case ref
  --
  l_acas_ref := NULL;
  --
  IF p1.lapp_hrv_fssa_code IS NOT NULL
   THEN
    OPEN c_acas_ref(p1.lapp_acas_alt_ref);
     FETCH c_acas_ref into l_acas_ref;
    CLOSE c_acas_ref;
  END IF;
  --
  -- Establish a savepoint to ensure commit consistancy
  --
  SAVEPOINT SP1;
  --
  -- Now Update or Insert
  --
  IF l_already_exists = 'Y' 
  THEN
    UPDATE applications
    SET
    app_offer_flag           = p1.l_offer,  
    app_nomination_flag      = p1.l_nom,
    app_received_date        = p1.lapp_received_date,
    app_corr_name            = p1.lapp_corr_name,
    app_sco_code             = p1.l_sco,
    app_status_date          = p1.l_status,
    app_rent_account_details = p1.lapp_rent_account_details,
    app_expected_hless_date  = p1.lapp_expected_hless_date,
    app_tcy_refno            = nvl(l_tcy_refno,APP_TCY_REFNO),
    app_legacy_ref           = nvl(app_legacy_ref,p1.lapp_legacy_ref),
    app_hrv_hcr_code         = p1.lapp_hrv_hcr_code,
    app_hrv_hor_code         = p1.lapp_hrv_hor_code,
    app_hrv_fssa_code        = p1.lapp_hrv_fssa_code,
    app_acas_reference       = l_acas_ref
    WHERE app_refno  = l_app_refno;
    --
    -- Get the interested parties reference number
    --
    l_ipp_refno := null;
    --
    OPEN c3(p1.lipt_code);
    FETCH c3 INTO l_ipp_refno;
    CLOSE c3;
    --
    INSERT INTO INTERESTED_PARTY_USAGES
          (ipus_refno
          ,ipus_ipp_refno
          ,ipus_start_date
          ,ipus_app_refno)
    SELECT
          IPUS_REFNO_SEQ.nextval,
          l_ipp_refno,
          p1.lapp_received_date, -- ?AIP_START_DATE
          l_app_refno 
    FROM DUAL
    WHERE NOT EXISTS (SELECT NULL
                      FROM interested_party_types
                      ,    interested_parties
                      ,    interested_party_usages
                      WHERE ipus_app_refno   = l_app_refno
                      AND   ipus_ipp_refno   = ipp_refno
                      AND   ipp_ipt_code     = ipt_code
                      AND   ipt_hrv_fiy_code = 'HLCW');
    --  
  ELSE -- need to insert new application
    -- Get the application reference number
    l_app_refno := p1.lapp_refno;
    IF l_app_refno IS NULL 
    THEN
      OPEN c4;
      FETCH c4 INTO l_app_refno;
      CLOSE c4;
    END IF;
    
    --
    INSERT INTO applications
          (app_refno,
           app_reusable_refno,
           app_offer_flag,
           app_nomination_flag,
           app_received_date,
           app_corr_name,
           app_sco_code,
           app_status_date,
           app_rent_account_details,
           app_expected_hless_date,
           app_tcy_refno,
           app_legacy_ref,
           app_hrv_hcr_code,
           app_hrv_hor_code,
           app_hrv_fssa_code,
           app_acas_reference)
    VALUES
     (l_app_refno,
      REUSABLE_REFNO_SEQ.nextval,
      p1.l_offer, --p1.lapp_offer_flag,
      p1.l_nom, --p1.lapp_nomination_flag,
      p1.lapp_received_date,
      p1.lapp_corr_name,
      p1.l_sco,--p1.lapp_sco_code,
      p1.l_status, --p1.lapp_status_date,
      p1.lapp_rent_account_details,
      p1.lapp_expected_hless_date,
      l_tcy_refno,
      p1.lapp_legacy_ref,
      p1.lapp_hrv_hcr_code,
      p1.lapp_hrv_hor_code,
      p1.lapp_hrv_fssa_code,
      l_acas_ref);
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
    INSERT INTO  interested_party_usages
          (ipus_refno
          ,ipus_ipp_refno
          ,ipus_start_date
          ,ipus_app_refno)
    VALUES
         (IPUS_REFNO_SEQ.nextval,
          l_ipp_refno,
          p1.lapp_received_date, -- ?AIP_START_DATE
          l_app_refno);
  END IF;
  --
  -- update the processed count and record status flag
  --
  s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
  set_record_status_flag(l_id,'C');
  --
  -- keep a count of the rows processed and commit after every 1000
  --
  i1 := i1+1; IF MOD(i1,1000)=0 THEN COMMIT; END IF;
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
l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLICATIONS');
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
     rowid rec_rowid,
     lapp_dlb_batch_id,
     lapp_dl_seqno,
     lapp_dl_load_status,
     lapp_offer_flag,
     lapp_nomination_flag,
     lapp_received_date,
     lapp_corr_name,
     lapp_sco_code,
     lapp_status_date,
     lapp_rent_account_details,
     lapp_expected_hless_date,
     lapp_legacy_ref,
     lapp_hrv_hcr_code,
     lapp_hrv_hor_code,
     lipt_code,
     ltcy_alt_ref,
     lapp_refno,
     lapp_hrv_fssa_code,
     lapp_acas_alt_ref
FROM  dl_hat_hml_applications
WHERE lapp_dlb_batch_id    = p_batch_id
AND   lapp_dl_load_status IN ('L','F','O');
--
CURSOR c_ipt (p_ipp VARCHAR2) IS
SELECT ipt_hrv_fiy_code
FROM interested_party_types
,    interested_parties
WHERE ipp_shortname    = p_ipp
AND   ipp_ipt_code     = ipt_code;
--
CURSOR c_acas_ref(p_acas_alt_ref IN VARCHAR2) IS
SELECT 'X'
FROM   advice_cases
WHERE  acas_alternate_reference = p_acas_alt_ref;
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_HML_APPLICATIONS';
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
i1                  PLS_INTEGER := 0;
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
  cs := p1.lapp_dl_seqno;
  l_id := p1.rec_rowid;
  --
  l_errors := 'V';
  l_error_ind := 'N';
  --
  -- Check that the tenancy ref if supplied must exist on the tenancies table
  --
  IF p1.ltcy_alt_ref IS NOT NULL
  AND (NOT s_dl_hat_utils.f_exists_tenacy_ref(p1.ltcy_alt_ref))
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',080);
  END IF;
  --
  -- The application received date must not be after the current date
  --
  IF p1.lapp_received_date > TRUNC(SYSDATE)
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',202);
  END IF;
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
  IF (NOT s_dl_hem_utils.exists_frv('HCREASON',p1.lapp_hrv_hcr_code,'Y'))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',213);
  END IF;
  --
  -- A valid reference value must be defined for homeless origin
  --
  IF (NOT s_dl_hem_utils.exists_frv('HCORIGIN',p1.lapp_hrv_hor_code,'Y'))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',214);
  END IF;
  --
  -- A date is required for when the applicant is expected to be homeless
  --
  IF p1.lapp_expected_hless_date IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',204);
  END IF;
  --
  -- Check Source
  --
    IF (NOT s_dl_hem_utils.exists_frv('SSAPPSRC',p1.lapp_hrv_fssa_code,'Y'))
     THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',274);
    END IF;
  --
  -- Check the Advice Case exists
  --
    IF p1.lapp_acas_alt_ref IS NOT NULL
     THEN
      OPEN c_acas_ref(p1.lapp_acas_alt_ref);
       FETCH c_acas_ref into l_exists;
        IF c_acas_ref%notfound
         THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',275);
        END IF;
      CLOSE c_acas_ref;
    END IF;
  --
-- Added validate for mand fields so NOT NULL is removed from Data Load table
-- We want the validate to fail if not supplied not the actual load itself
--     
  IF p1.lapp_offer_flag IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',860);
  END IF;
--
  IF p1.lapp_nomination_flag IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',861);
  END IF;
--
  IF p1.lapp_received_date IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',862);
  END IF;
--
  IF LTRIM(RTRIM(p1.lapp_corr_name)) IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',863);
  END IF;
--
  IF p1.lapp_sco_code IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',864);
  END IF;
--
  IF p1.lapp_status_date IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',865);
  END IF;
--
  IF p1.lapp_legacy_ref IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',866);
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
  i1 := i1 +1; IF MOD(i1,1000)=0 THEN COMMIT; END IF;
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
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 is
SELECT
     rowid rec_rowid,
     lapp_dlb_batch_id,
     lapp_dl_seqno,
     lapp_dl_load_status,
     lapp_legacy_ref,
     lapp_received_date
FROM  dl_hat_hml_applications
WHERE lapp_dlb_batch_id   = p_batch_id
AND   lapp_dl_load_status = 'C';
--
--
i1        PLS_INTEGER := 0;
l_an_tab  VARCHAR2(1);
l_app_refno applications.app_refno%TYPE;
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_HML_APPLICATIONS';
cs       PLS_INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_hml_applications.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_hml_applications.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs   := p1.lapp_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lapp_legacy_ref);
--
-- Check the order of delete
--
DELETE FROM INTERESTED_PARTY_USAGES
WHERE ipus_app_refno = l_app_refno
AND   ipus_start_date = p1.lapp_received_date;
--
DELETE FROM APPLICATIONS
WHERE app_refno = l_app_refno;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'V');
--
-- keep a count of the rows processed and commit after every 1000
--
i1 := i1 +1; IF MOD(i1,1000)=0 THEN COMMIT; END IF;
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
l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLICATIONS');
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
END s_dl_hat_hml_applications;
/
