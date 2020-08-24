CREATE OR REPLACE PACKAGE BODY s_dl_hat_medical_answers
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB VER    WHO  WHEN        WHY
--      1.0           RJ   03/04/2001  Dataload
--      1.1  5.1.6    PJD  21/05/2002  Moved the analyze table command to the correct
--                                     place in the create process. i.e outside the loop
--      1.2  5.1.6    PJD  22/05/2002  Added validation on lapp_legacy_ref;
--      1.3  5.1.6    PJD  14/06/2002  Added variable l_mrf_assessment_refno in create proc
--      2.0  5.3.0    SB   20/03/2003  Added commit on validate
--      3.0  5.13.0   PH   06-FEB-2008 Now includes its own 
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
  UPDATE dl_hat_medical_answers
  SET lman_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_medical_answers');
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
     LMAN_DLB_BATCH_ID,
     LMAN_DL_SEQNO,
     LMAN_DL_LOAD_STATUS,
     LMAN_QUE_REFNO,
     LMAN_CREATED_BY,
     LMAN_CREATED_DATE,
     LMAN_DATE_VALUE,
     LMAN_NUMBER_VALUE,
     LMAN_CHAR_VALUE,
     LMAN_QOR_CODE,
     LMAN_OTHER_CODE,
     LMAN_OTHER_DATE,
     LMAN_COMMENTS,
     LAPP_LEGACY_REF,
     LMRF_REFERRAL_DATE,
     LMAN_MRF_ASSESSMENT_REFNO
FROM  dl_hat_medical_answers
WHERE lman_dlb_batch_id   = p_batch_id
AND   lman_dl_load_status = 'V';
--
--
CURSOR c2 (p_mrf_app_refno NUMBER, p_mrf_referral_date DATE) IS
SELECT mrf_assessment_refno
FROM medical_referrals
WHERE mrf_app_refno   = p_mrf_app_refno
AND   mrf_referral_date = p_mrf_referral_date;

-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_MEDICAL_ANSWERS';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
-- Other variables
--
i                      INTEGER := 0;
l_an_tab               VARCHAR2(1);
l_app_refno            applications.app_refno%TYPE;
l_mrf_assessment_refno VARCHAR2(10);
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_medical_answers.dataload_create');
fsc_utils.debug_message( 's_dl_hat_medical_answers.dataload_create',3);
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
cs := p1.lman_dl_seqno;
l_id := p1.rec_rowid;
--
--
-- Get the application reference number
--
l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lapp_legacy_ref);
--
-- Get the MAN_MRF_ASSESSMENT_REFNO if null
--
l_mrf_assessment_refno := p1.lman_mrf_assessment_refno;
--
IF l_mrf_assessment_refno IS NULL
THEN
   OPEN c2 (l_app_refno,p1.lmrf_referral_date);
   FETCH c2 INTO  l_mrf_assessment_refno;
   CLOSE c2;
END IF;
--
INSERT INTO medical_answers
          (MAN_QUE_REFNO,
           MAN_MRF_ASSESSMENT_REFNO,
           MAN_APP_REFNO,
           MAN_CREATED_BY,
           MAN_CREATED_DATE,
           MAN_DATE_VALUE,
           MAN_NUMBER_VALUE,
           MAN_CHAR_VALUE,
           MAN_OTHER_CODE,
           MAN_OTHER_DATE,
           MAN_COMMENTS,
           MAN_QOR_CODE)
values
        (P1.LMAN_QUE_REFNO,
         L_MRF_ASSESSMENT_REFNO,
         l_app_refno,
         P1.LMAN_CREATED_BY,
         P1.LMAN_CREATED_DATE,
         P1.LMAN_DATE_VALUE,
         P1.LMAN_NUMBER_VALUE,
         P1.LMAN_CHAR_VALUE,
         P1.LMAN_OTHER_CODE,
         P1.LMAN_OTHER_DATE,
         P1.LMAN_COMMENTS,
         P1.LMAN_QOR_CODE);
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
-- Section to analyse the table(s) populated with this dataload
--
l_an_tab := s_dl_hem_utils.dl_comp_stats('MEDICAL_ANSWERS');
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
     LMAN_DLB_BATCH_ID,
     LMAN_DL_SEQNO,
     LMAN_DL_LOAD_STATUS,
     LMAN_QUE_REFNO,
     LMAN_CREATED_BY,
     LMAN_CREATED_DATE,
     LMAN_DATE_VALUE,
     LMAN_NUMBER_VALUE,
     LMAN_CHAR_VALUE,
     LMAN_QOR_CODE,
     LMAN_OTHER_CODE,
     LMAN_OTHER_DATE,
     LMAN_COMMENTS,
     LAPP_LEGACY_REF,
     LMRF_REFERRAL_DATE,
     LMAN_MRF_ASSESSMENT_REFNO
FROM  dl_hat_medical_answers
WHERE lman_dlb_batch_id    = p_batch_id
AND   lman_dl_load_status IN ('L','F','O');
--
-- constants FOR error process
--
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_MEDICAL_ANSWERS';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_errors            VARCHAR2(10);
l_error_ind         VARCHAR2(10);
i                   INTEGER := 0;
l_app_refno         applications.app_refno%TYPE;

BEGIN
--
fsc_utils.proc_start('s_dl_hat_medical_answers.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_medical_answers.dataload_validate',3 );
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
cs := p1.lman_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
--
-- get the application reference number
l_app_refno := null;
l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lapp_legacy_ref);
--
-- The user application reference should already be loaded as part of the
-- applications load
--
IF l_app_refno IS NULL
THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',233);
END IF;
 -- The record must not already exist on the medical answers table
IF p1.lman_mrf_assessment_refno IS NOT NULL
THEN
  IF (s_dl_hat_utils.f_exists_menans(p1.lman_que_refno,
     p1.lman_mrf_assessment_refno))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',337);
  END IF;
END IF;
--
--
-- If the medical assesment reference is supplied check that it exists in
-- the medical_referrals table
--
IF p1.lman_mrf_assessment_refno IS NOT NULL
THEN
   IF (NOT s_dl_hat_utils.f_exists_medassref(p1.lman_mrf_assessment_refno,l_app_refno))
   THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',347);
  END IF;
END IF;
--
-- If the referral date is supplied check that it exists in
-- the medical_referrals table
--
IF p1.lmrf_referral_date IS NOT NULL
THEN
   IF (NOT s_dl_hat_utils.f_exists_refdate(p1.lmrf_referral_date,l_app_refno))
   THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',348);
   END IF;
END IF;
--
-- Either the referral date or the medical assessment reference should
-- be supplied
IF p1.lmrf_referral_date IS NULL AND p1.lman_mrf_assessment_refno IS NULL
THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',349);
END IF;
--
-- Check that the question reference number must exist on the questions
-- table
  IF (NOT s_dl_hat_utils.f_exists_quenum(p1.lman_que_refno))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',270);
  END IF;
--
-- For medical answers the question category (que_question_category) should be
-- MA
--
  IF (NOT s_dl_hat_utils.f_valid_quecat(p1.lman_que_refno,'M'))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',338);
  END IF;
--
 -- Where a coded optional response is included
 -- within the record this will be validated against
 -- the QUESTION_OPTION_RESPONSES table
--
 IF (p1.lman_qor_code IS NOT NULL
    AND NOT s_dl_hat_utils.f_exists_qor(p1.lman_qor_code))
 THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',297);
 END IF;
--
 -- Where the associated questions has a datatype of 'C'oded
 -- then a valid answer must exist in Question_permitted_respones
--
 IF p1.lman_char_value IS NOT NULL
 THEN
    IF (NOT s_dl_hat_utils.f_exists_char(p1.lman_que_refno,p1.lman_char_value))
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',279);
    END IF;
 END IF;
--
-- Where the associated question has a datatype of numeric then valid
-- number must be entered or, where a QUESTION_PERMITTED_RESPONSE exists
-- then must be in the specified range
--
 IF p1.lman_number_value IS NOT NULL
 THEN
    IF (NOT s_dl_hat_utils.f_exists_num(p1.lman_que_refno,p1.lman_number_value))
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',278);
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
     LMAN_DLB_BATCH_ID,
     LMAN_DL_SEQNO,
     LMAN_DL_LOAD_STATUS,
     LMAN_QUE_REFNO,
     LMAN_MRF_ASSESSMENT_REFNO,
     LMAN_CREATED_BY,
     LMAN_CREATED_DATE
FROM  dl_hat_medical_answers
WHERE lman_dlb_batch_id   = p_batch_id
AND   lman_dl_load_status = 'C';
--
--
i            INTEGER := 0;
l_an_tab     VARCHAR2(1);
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_MEDICAL_ANSWERS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_medical_answers.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_medical_answers.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lman_dl_seqno;
i  := i +1;
l_id := p1.rec_rowid;
--
--
--
--
IF p1.lman_mrf_assessment_refno IS NOT NULL
THEN
   DELETE FROM MEDICAL_ANSWERS
   WHERE man_que_refno = p1.lman_que_refno
   AND man_mrf_assessment_refno = p1.lman_mrf_assessment_refno;
ELSE
   -- Check this as not the best solution if lman_mrf_assessment_refno not supplied
   -- Other fields for delete?
   --
   DELETE FROM MEDICAL_ANSWERS
   WHERE man_que_refno = p1.lman_que_refno
   AND   man_created_by = p1.lman_created_by
   AND   man_created_date = p1.lman_created_date;
--
END IF;
--
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

l_an_tab := s_dl_hem_utils.dl_comp_stats('MEDICAL_ANSWERS');

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
END s_dl_hat_medical_answers;
--
/



