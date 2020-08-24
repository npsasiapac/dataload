CREATE OR REPLACE PACKAGE BODY s_dl_hat_hless_ins_answers
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Ver   WHO  WHEN       WHY
--      1.0            MB    03/12/2009   Initial version
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
  UPDATE DL_HAT_HLESS_INS_ANSWERS
  SET lhia_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of DL_HAT_HLESS_INS_ANSWERS');
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
     lhia_DLB_BATCH_ID,
     lhia_DL_SEQNO,
     lhia_DL_LOAD_STATUS,
     lhia_QUE_REFNO,
     lhia_CREATED_BY,
     lhia_CREATED_DATE,
     lhia_DATE_VALUE,
     lhia_NUMBER_VALUE,
     lhia_CHAR_VALUE,
     lhia_QOR_CODE,
     lhia_OTHER_CODE,
     lhia_OTHER_DATE,
     lhia_COMMENTS,
     LHIN_INSTANCE_REFNO
FROM  DL_HAT_HLESS_INS_ANSWERS
WHERE lhia_dlb_batch_id   = p_batch_id
AND   lhia_dl_load_status = 'V';
--
--
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_HLESS_INS_ANSWERS';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
-- Other variables
--
l_an_tab         VARCHAR2(1);
i                INTEGER := 0;
l_hin_instance_refno      hless_instances.hin_instance_refno%TYPE;
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_hless_ins_answers.dataload_create');
fsc_utils.debug_message( 's_dl_hat_hless_ins_answers.dataload_create',3);
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
cs := p1.lhia_dl_seqno;
l_id := p1.rec_rowid;
--
l_hin_instance_refno := p1.lhin_instance_refno;
--
--
INSERT INTO hless_ins_answers
          (HIA_HIN_INSTANCE_REFNO,
           HIA_QUE_REFNO,
           HIA_CREATED_BY,
           HIA_CREATED_DATE,
           HIA_DATE_VALUE,
           HIA_NUMBER_VALUE,
           HIA_CHAR_VALUE,
           HIA_OTHER_CODE,
           HIA_OTHER_DATE,
           HIA_COMMENTS,
           HIA_QOR_CODE)
values
    (l_hin_instance_refno,
     P1.lhia_QUE_REFNO,
     P1.lhia_CREATED_BY,
     P1.lhia_CREATED_DATE,
     P1.lhia_DATE_VALUE,
     P1.lhia_NUMBER_VALUE,
     P1.lhia_CHAR_VALUE,
     P1.lhia_OTHER_CODE,
     P1.lhia_OTHER_DATE,
     P1.lhia_COMMENTS,
     P1.lhia_QOR_CODE);
--
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

-- Section to analyse table populated by this dataload

l_an_tab := s_dl_hem_utils.dl_comp_stats('HLESS_INS_ANSWERS');

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
     lhia_DLB_BATCH_ID,
     lhia_DL_SEQNO,
     lhia_DL_LOAD_STATUS,
     lhia_QUE_REFNO,
     lhia_CREATED_BY,
     lhia_CREATED_DATE,
     lhia_DATE_VALUE,
     lhia_NUMBER_VALUE,
     lhia_CHAR_VALUE,
     lhia_QOR_CODE,
     lhia_OTHER_CODE,
     lhia_OTHER_DATE,
     lhia_COMMENTS,
     LHIN_INSTANCE_REFNO
FROM  DL_HAT_HLESS_INS_ANSWERS
WHERE lhia_dlb_batch_id    = p_batch_id
AND   lhia_dl_load_status IN ('L','F','O');
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
ct VARCHAR2(30) := 'DL_HAT_HLESS_INS_ANSWERS';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_exists			VARCHAR2(1);
l_errors            VARCHAR2(10);
l_error_ind         VARCHAR2(10);
i                   INTEGER := 0;

BEGIN
--
fsc_utils.proc_start('s_dl_hat_hless_ins_answers.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_hless_ins_answers.dataload_validate',3 );
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
cs := p1.lhia_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- The homeless instance reference should already be loaded as part of the
-- homeless instances load
--
OPEN c_hin_exist(p1.lhin_instance_refno);
FETCH c_hin_exist INTO l_exists;
IF c_hin_exist%notfound
THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',680);
END IF;
CLOSE c_hin_exist;
 --
 -- The record must not already exist on the hless_ins_answers table

  IF (s_dl_hat_utils.f_exists_hiaans(p1.lhia_que_refno,p1.lhin_instance_refno))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',681);
  END IF;
--

-- Check that the question reference number must exist on the questions
-- table
  IF (NOT s_dl_hat_utils.f_exists_quenum(p1.lhia_que_refno))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',270);
  END IF;
--
-- For hless ins answers the question category (que_question_category) should be HI
--
  IF (NOT s_dl_hat_utils.f_valid_quecat(p1.lhia_que_refno,'H'))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',682);
  END IF;

 -- Where a coded optional response is included
 -- within the record this will be validated against
 -- the QUESTION_OPTION_RESPONSES table
 IF p1.lhia_qor_code IS NOT NULL
 THEN
    IF (NOT s_dl_hat_utils.f_exists_qor(p1.lhia_qor_code))
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',297);
    END IF;
 END IF;

 --
 -- Where the associated questions has a datatype of 'C'oded
 -- then a valid answer must exist in Question_permitted_respones

  IF p1.lhia_char_value IS NOT NULL
  THEN
     IF (NOT s_dl_hat_utils.f_exists_char(p1.lhia_que_refno,p1.lhia_char_value))
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',279);
     END IF;
  END IF;

-- Where the associated question has a datatype of numeric then valid
-- number must be entered or, where a QUESTION_PERMITTED_RESPONSE exists
-- then must be in the specified range
--
 IF p1.lhia_number_value IS NOT NULL
 THEN
    IF (NOT s_dl_hat_utils.f_exists_num(p1.lhia_que_refno,p1.lhia_number_value))
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',278);
    END IF;
 END IF;
--
-- Check that only one of the Date or Coded answer has been supplied
--
   IF p1.lhia_char_value IS NOT NULL
    AND p1.lhia_date_value IS NOT NULL
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',500);
   END IF;
--
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
     lhia_DLB_BATCH_ID,
     lhia_DL_SEQNO,
     lhia_DL_LOAD_STATUS,
     LHIN_INSTANCE_REFNO,
     lhia_QUE_REFNO
FROM  DL_HAT_HLESS_INS_ANSWERS
WHERE lhia_dlb_batch_id   = p_batch_id
AND   lhia_dl_load_status = 'C';
--

i INTEGER := 0;
l_an_tab VARCHAR2(1);
l_hin_instance_refno  applications.app_refno%TYPE;
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_HLESS_INS_ANSWERS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_hless_ins_answers.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_hless_ins_answers.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lhia_dl_seqno;
i  := i +1;
l_id := p1.rec_rowid;
--
l_hin_instance_refno := p1.lhin_instance_refno;
--
--
--
DELETE FROM hless_ins_answers
WHERE hia_hin_instance_refno = l_hin_instance_refno
AND hia_que_refno = p1.lhia_que_refno;
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
-- Section to analyse table populated by this dataload

l_an_tab := s_dl_hem_utils.dl_comp_stats('HLESS_INS_ANSWERS');

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
END s_dl_hat_hless_ins_answers;
/

