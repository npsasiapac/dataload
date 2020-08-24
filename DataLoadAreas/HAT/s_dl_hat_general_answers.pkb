CREATE OR REPLACE PACKAGE BODY s_dl_hat_general_answers
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Ver   WHO  WHEN       WHY
--      1.0            RJ  03/04/2001  Dataload
--      1.1  5.1.6    PJD  22/05/2002  Added validation on app legacy ref
--      1.2  5.3.0     SB  20/03/2003  Added commit on validate
--      1.3  5.12.0   PH   29/10/2007  Added validate on date/char value
--                                     only one can be supplied.
--      2.0  5.13.0   PH   06-FEB-2008 Now includes its own 
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
  UPDATE dl_hat_general_answers
  SET lgan_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_general_answers');
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
     LGAN_DLB_BATCH_ID,
     LGAN_DL_SEQNO,
     LGAN_DL_LOAD_STATUS,
     LGAN_QUE_REFNO,
     LGAN_CREATED_BY,
     LGAN_CREATED_DATE,
     LGAN_DATE_VALUE,
     LGAN_NUMBER_VALUE,
     LGAN_CHAR_VALUE,
     LGAN_QOR_CODE,
     LGAN_OTHER_CODE,
     LGAN_OTHER_DATE,
     LGAN_COMMENTS,
     LAPP_LEGACY_REF
FROM  dl_hat_general_answers
WHERE lgan_dlb_batch_id   = p_batch_id
AND   lgan_dl_load_status = 'V';
--
--
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_GENERAL_ANSWERS';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
-- Other variables
--
l_an_tab         VARCHAR2(1);
i                INTEGER := 0;
l_app_refno      applications.app_refno%TYPE;
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_general_answers.dataload_create');
fsc_utils.debug_message( 's_dl_hat_general_answers.dataload_create',3);
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
cs := p1.lgan_dl_seqno;
l_id := p1.rec_rowid;
--
l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lapp_legacy_ref);


INSERT INTO general_answers
          (GAN_APP_REFNO,
           GAN_QUE_REFNO,
           GAN_CREATED_BY,
           GAN_CREATED_DATE,
           GAN_DATE_VALUE,
           GAN_NUMBER_VALUE,
           GAN_CHAR_VALUE,
           GAN_OTHER_CODE,
           GAN_OTHER_DATE,
           GAN_COMMENTS,
           GAN_QOR_CODE)
values
    (l_app_refno,
     P1.LGAN_QUE_REFNO,
     P1.LGAN_CREATED_BY,
     P1.LGAN_CREATED_DATE,
     P1.LGAN_DATE_VALUE,
     P1.LGAN_NUMBER_VALUE,
     P1.LGAN_CHAR_VALUE,
     P1.LGAN_OTHER_CODE,
     P1.LGAN_OTHER_DATE,
     P1.LGAN_COMMENTS,
     P1.LGAN_QOR_CODE);


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

l_an_tab := s_dl_hem_utils.dl_comp_stats('GENERAL_ANSWERS');

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
     LGAN_DLB_BATCH_ID,
     LGAN_DL_SEQNO,
     LGAN_DL_LOAD_STATUS,
     LGAN_QUE_REFNO,
     LGAN_CREATED_BY,
     LGAN_CREATED_DATE,
     LGAN_DATE_VALUE,
     LGAN_NUMBER_VALUE,
     LGAN_CHAR_VALUE,
     LGAN_QOR_CODE,
     LGAN_OTHER_CODE,
     LGAN_OTHER_DATE,
     LGAN_COMMENTS,
     LAPP_LEGACY_REF
FROM  dl_hat_general_answers
WHERE lgan_dlb_batch_id    = p_batch_id
AND   lgan_dl_load_status IN ('L','F','O');
--


-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_GENERAL_ANSWERS';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_errors            VARCHAR2(10);
l_error_ind         VARCHAR2(10);
i                   INTEGER := 0;

BEGIN
--
fsc_utils.proc_start('s_dl_hat_general_answers.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_general_answers.dataload_validate',3 );
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
cs := p1.lgan_dl_seqno;
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
 -- The record must not already exist on the general answers table

  IF (s_dl_hat_utils.f_exists_genans(p1.lgan_que_refno,p1.lapp_legacy_ref))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',332);
  END IF;
--

-- Check that the question reference number must exist on the questions
-- table
  IF (NOT s_dl_hat_utils.f_exists_quenum(p1.lgan_que_refno))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',270);
  END IF;
--
-- For general answers the question category (que_question_category) should be
-- CC,HR,OS,GA,SG,MD,DQ
  IF (NOT s_dl_hat_utils.f_valid_quecat(p1.lgan_que_refno,'G'))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',333);
  END IF;

 -- Where a coded optional response is included
 -- within the record this will be validated against
 -- the QUESTION_OPTION_RESPONSES table
 IF p1.lgan_qor_code IS NOT NULL
 THEN
    IF (NOT s_dl_hat_utils.f_exists_qor(p1.lgan_qor_code))
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',297);
    END IF;
 END IF;

 --
 -- Where the associated questions has a datatype of 'C'oded
 -- then a valid answer must exist in Question_permitted_respones

  IF p1.lgan_char_value IS NOT NULL
  THEN
     IF (NOT s_dl_hat_utils.f_exists_char(p1.lgan_que_refno,p1.lgan_char_value))
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',279);
     END IF;
  END IF;

-- Where the associated question has a datatype of numeric then valid
-- number must be entered or, where a QUESTION_PERMITTED_RESPONSE exists
-- then must be in the specified range
--
 IF p1.lgan_number_value IS NOT NULL
 THEN
    IF (NOT s_dl_hat_utils.f_exists_num(p1.lgan_que_refno,p1.lgan_number_value))
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',278);
    END IF;
 END IF;
--
-- Check that only one of the Date or Coded answer has been supplied
--
   IF p1.lgan_char_value IS NOT NULL
    AND p1.lgan_date_value IS NOT NULL
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
     LGAN_DLB_BATCH_ID,
     LGAN_DL_SEQNO,
     LGAN_DL_LOAD_STATUS,
     LAPP_LEGACY_REF,
     LGAN_QUE_REFNO
FROM  dl_hat_general_answers
WHERE lgan_dlb_batch_id   = p_batch_id
AND   lgan_dl_load_status = 'C';
--

i INTEGER := 0;
l_an_tab VARCHAR2(1);
l_app_refno  applications.app_refno%TYPE;
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_GENERAL_ANSWERS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_general_answers.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_general_answers.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lgan_dl_seqno;
i  := i +1;
l_id := p1.rec_rowid;
--
l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lapp_legacy_ref);
--
--
--
DELETE FROM GENERAL_ANSWERS
WHERE gan_app_refno = l_app_refno
AND gan_que_refno = p1.lgan_que_refno;
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

l_an_tab := s_dl_hem_utils.dl_comp_stats('GENERAL_ANSWERS');

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
END s_dl_hat_general_answers;
/

