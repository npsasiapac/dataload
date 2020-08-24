CREATE OR REPLACE PACKAGE BODY s_dl_hat_medical_referrals
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Ver    WHO  WHEN       WHY
--      1.0           RJ   03/04/2001  Dataload
--      1.1 5.1.6     PJD  22/05/2002  Added validation on lapp_legacy_ref
--      1.2 5.3.0     SB   20/03/2003  Added commit on validate
--      1.3 5.10.0    PH   15/08/2006  Commented out validate on assesment
--                                     date and award date if status = CUR
--      2.0 5.13.0    PH   06-FEB-2008 Now includes its own 
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
  UPDATE dl_hat_medical_referrals
  SET lmrf_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_medical_referrals');
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
CURSOR c1 is
SELECT
     rowid rec_rowid,
     LMRF_DLB_BATCH_ID,
     LMRF_DL_SEQNO,
     LMRF_DL_LOAD_STATUS,
     LPAR_PER_ALT_REF,
     LMRF_REFERRAL_DATE,
     LMRF_STATUS_CODE,
     LMRF_STATUS_DATE,
     LMRF_ASSESSMENT_DATE,
     LMRF_AWARD_DATE,
     LMRF_COMMENTS,
     LMRF_HRV_MRR_CODE,
     LAPP_LEGACY_REF,
     LMRF_ASSESMENT_LEGACY_REF
FROM  dl_hat_medical_referrals
WHERE lmrf_dlb_batch_id   = p_batch_id
AND   lmrf_dl_load_status = 'V';
--
--
-- Other cursors
--
CURSOR c2 (p_lpar_per_alt_ref VARCHAR2) IS
SELECT ipa_refno
FROM parties, involved_parties
WHERE par_per_alt_ref = p_lpar_per_alt_ref
AND par_refno = ipa_par_refno;
--
CURSOR c3(p_app_refno NUMBER, p_ipa_refno NUMBER,
          p_ref_date  DATE  , p_status_code VARCHAR2) IS
SELECT 'X'
FROM medical_referrals
WHERE mrf_app_refno = p_app_refno
  AND mrf_ipa_refno = p_ipa_refno
  AND mrf_referral_date = p_ref_date
  AND mrf_status_code   = p_status_code;
--

-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_MEDICAL_REFERRALS';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
-- Other variables


i                INTEGER := 0;
l_an_tab         VARCHAR2(1);
l_app_refno      applications.app_refno%TYPE;
l_ipa_refno      involved_parties.ipa_refno%TYPE;
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_medical_referrals.dataload_create');
fsc_utils.debug_message( 's_dl_hat_medical_referrals.dataload_create',3);
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
cs := p1.lmrf_dl_seqno;
l_id := p1.rec_rowid;
--

-- Get the application reference number

l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lapp_legacy_ref);

-- Get the involved party reference number.

OPEN c2 (p1.lpar_per_alt_ref);
FETCH c2 INTO l_ipa_refno;
CLOSE c2;

-- Get the next sequence value if lmrf_assesment_legacy_ref is null

IF p1.lmrf_assesment_legacy_ref IS NULL
THEN
   SELECT MRF_ASSESSMENT_REFNO_SEQ.nextval INTO p1.lmrf_assesment_legacy_ref
   FROM dual;
END IF;


INSERT INTO medical_referrals
          ( MRF_ASSESSMENT_REFNO
            ,MRF_APP_REFNO
            ,MRF_REFERRAL_DATE
            ,MRF_STATUS_CODE
            ,MRF_STATUS_DATE
            ,MRF_IPA_REFNO
            ,MRF_ASSESSMENT_DATE
            ,MRF_AWARD_DATE
            ,MRF_COMMENTS
            ,MRF_HRV_MRR_CODE)
values
        (p1.LMRF_ASSESMENT_LEGACY_REF,
         l_app_refno,
         p1.LMRF_REFERRAL_DATE,
         p1.LMRF_STATUS_CODE,
         p1.LMRF_STATUS_DATE,
         l_ipa_refno,
         p1.LMRF_ASSESSMENT_DATE,
         p1.LMRF_AWARD_DATE,
         p1.LMRF_COMMENTS,
         p1.LMRF_HRV_MRR_CODE);
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
-- Section to analyse the table(s) populated with this dataload

l_an_tab := s_dl_hem_utils.dl_comp_stats('MEDICAL_REFERRALS');
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
     LMRF_DLB_BATCH_ID,
     LMRF_DL_SEQNO,
     LMRF_DL_LOAD_STATUS,
     LPAR_PER_ALT_REF,
     LMRF_REFERRAL_DATE,
     LMRF_STATUS_CODE,
     LMRF_STATUS_DATE,
     LMRF_ASSESSMENT_DATE,
     LMRF_AWARD_DATE,
     LMRF_COMMENTS,
     LMRF_HRV_MRR_CODE,
     LAPP_LEGACY_REF,
     LMRF_ASSESMENT_LEGACY_REF
FROM  dl_hat_medical_referrals
WHERE lmrf_dlb_batch_id    = p_batch_id
-- AND   ROWNUM < 10
AND   lmrf_dl_load_status IN ('L','F','O');
--
-- Other cursors
--
CURSOR c2 (p_lpar_per_alt_ref VARCHAR2) IS
SELECT ipa_refno
FROM  parties, involved_parties
WHERE par_per_alt_ref = p_lpar_per_alt_ref
AND   par_refno = ipa_par_refno;
--
CURSOR c3(p_app_legacy_ref   VARCHAR2, p_ipa_refno   NUMBER,
          p_ref_date         DATE    , p_status_code VARCHAR2) IS
SELECT 'X'
FROM medical_referrals,applications
WHERE app_legacy_ref    = p_app_legacy_ref
  AND mrf_app_refno     = app_refno
  AND nvl(mrf_ipa_refno,0)
                        = nvl(p_ipa_refno,0)
  AND mrf_referral_date = p_ref_date
  AND mrf_status_code   = p_status_code;
--
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_MEDICAL_REFERRALS';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_errors            VARCHAR2(10);
l_error_ind         VARCHAR2(10);
i                   INTEGER := 0;
l_exists            VARCHAR2(1);
l_ipa_refno         INTEGER;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_medical_referrals.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_medical_referrals.dataload_validate',3 );
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
cs := p1.lmrf_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- The user application reference should already be loaded as part of the
-- applications load
--
  IF (NOT s_dl_hat_utils.f_exists_application(p1.lapp_legacy_ref))
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',233);
  END IF;
--
-- The record must not already exist on the medical referrals table
--
  IF (s_dl_hat_utils.f_exists_menref(p1.lmrf_assesment_legacy_ref)
      AND p1.lmrf_assesment_legacy_ref IS NOT NULL)
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',343);
--
-- Get the involved party reference number.
--
  ELSE  
    l_ipa_refno := NULL;
--
    OPEN c2 (p1.lpar_per_alt_ref);
    FETCH c2 INTO l_ipa_refno;
    CLOSE c2;
--
    l_exists := NULL;
    dbms_output.put_line('leg_ref = '||p1.lapp_legacy_ref);
    dbms_output.put_line('ipa_ref = '||l_ipa_refno);
    dbms_output.put_line('r_date  = '||p1.lmrf_referral_date);
    dbms_output.put_line('status  = '||p1.lmrf_status_code);
--
    OPEN  c3(p1.lapp_legacy_ref   ,l_ipa_refno,
             p1.lmrf_referral_date,p1.lmrf_status_code);
    FETCH c3 INTO l_exists;
    CLOSE c3;
--
    IF l_exists IS NOT NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',343);
    END IF;
  END IF;
--
--
-- Check that the status code should be LOG or CUR or CLD
  IF p1.lmrf_status_code NOT IN ('LOG','CUR','CLD')
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',344);
  END IF;
--
-- IF the status code is set to CUR the award date and assesment code should be supplied
-- Commented this out as this is incorrect. From within the
-- application you can set these dates to null if CUR
--
--  IF p1.lmrf_status_code = 'CUR' AND (p1.lmrf_assessment_date IS NULL OR
--                                    p1.lmrf_award_date IS NULL)
--  THEN
--     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',345);
--  END IF;
--
-- A valid code for the assessment should be supplied
--
 IF (p1.lmrf_hrv_mrr_code IS NOT NULL)
  THEN
     IF (NOT s_dl_hem_utils.exists_frv('MEDRES',p1.lmrf_hrv_mrr_code))
     THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',346);
     END IF;
  END IF;
--
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
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 is
SELECT
     rowid rec_rowid,
     LMRF_DLB_BATCH_ID,
     LMRF_DL_SEQNO,
     LMRF_DL_LOAD_STATUS,
     LMRF_ASSESMENT_LEGACY_REF,
     LMRF_REFERRAL_DATE,
     LAPP_LEGACY_REF
FROM  dl_hat_medical_referrals
WHERE lmrf_dlb_batch_id   = p_batch_id
AND   lmrf_dl_load_status = 'C';
--

i            INTEGER := 0;
l_an_tab     VARCHAR2(1);
l_app_refno   applications.app_refno%TYPE;
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_MEDICAL_REFERRALS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_medical_referrals.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_medical_referrals.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lmrf_dl_seqno;
i  := i +1;
l_id := p1.rec_rowid;
--
--
--
-- Get the application reference number
l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lapp_legacy_ref);
--
IF p1.lmrf_assesment_legacy_ref IS NOT NULL
THEN
   DELETE FROM MEDICAL_REFERRALS
   WHERE mrf_assessment_refno = p1.lmrf_assesment_legacy_ref
   AND   mrf_app_refno  = l_app_refno;
ELSE
   DELETE FROM MEDICAL_REFERRALS
   WHERE mrf_app_refno  = l_app_refno
   AND   mrf_referral_date = p1.lmrf_referral_date;
END IF;
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
-- Section to analyse the table(s) populated with this dataload

l_an_tab := s_dl_hem_utils.dl_comp_stats('MEDICAL_REFERRALS');

fsc_utils.proc_end;
COMMIT;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_delete;
--
--
END s_dl_hat_medical_referrals;
/

