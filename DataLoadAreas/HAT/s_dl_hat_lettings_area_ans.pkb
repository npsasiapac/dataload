CREATE OR REPLACE PACKAGE BODY s_dl_hat_lettings_area_ans
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Ver   WHO  WHEN        WHY
--      1.0           RJ   03/04/2001  Dataload
--      1.1           SB   10/04/2002  Missing check to see if llaa_qor_code null.
--      1.2  5.1.6    PJD  22/05/2002  Added validation on lapp legacy ref
--      1.3  5.2.0    PH   19/09/2002  Default LAA_CHAR_VALUE to Y on Insert
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
  UPDATE dl_hat_lettings_area_answers
  SET llaa_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_lettings_area_answers');
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
     LLAA_DLB_BATCH_ID,
     LLAA_DL_SEQNO,
     LLAA_DL_LOAD_STATUS,
     LLAA_LAR_CODE,
     LLAA_QUE_REFNO,
     LLAA_CREATED_BY,
     LLAA_CREATED_DATE,
     LLAA_DATE_VALUE,
     LLAA_NUMBER_VALUE,
     LLAA_CHAR_VALUE,
     LLAA_QOR_CODE,
     LLAA_OTHER_CODE,
     LLAA_OTHER_DATE,
     LLAA_COMMENTS,
     LAPP_LEGACY_REF
FROM  DL_HAT_LETTINGS_AREA_ANSWERS
WHERE llaa_dlb_batch_id   = p_batch_id
AND   llaa_dl_load_status = 'V';
--
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_LETTINGS_AREA_ANSWERS';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
-- Other variables

l_an_tab         VARCHAR2(1);
i                INTEGER := 0;
l_app_refno      applications.app_refno%TYPE;
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_lettings_area_answers.dataload_create');
fsc_utils.debug_message( 's_dl_hat_lettings_area_answers.dataload_create',3);
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
cs := p1.llaa_dl_seqno;
l_id := p1.rec_rowid;
--

l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lapp_legacy_ref);

IF p1.llaa_created_by IS NULL
THEN
   p1.llaa_created_by := USER;
END IF;

IF p1.llaa_created_date IS NULL
THEN
   p1.llaa_created_date := SYSDATE;
END IF;

INSERT INTO LETTINGS_AREA_ANSWERS
          (LAA_LAR_CODE,
           LAA_APP_REFNO,
           LAA_QUE_REFNO,
           LAA_CREATED_BY,
           LAA_CREATED_DATE,
           LAA_DATE_VALUE,
           LAA_NUMBER_VALUE,
           LAA_CHAR_VALUE,
           LAA_QOR_CODE,
           LAA_OTHER_CODE,
           LAA_OTHER_DATE,
           LAA_COMMENTS)
VALUES
    (P1.LLAA_LAR_CODE,
     l_app_refno,
     P1.LLAA_QUE_REFNO,
     P1.LLAA_CREATED_BY,
     P1.LLAA_CREATED_DATE,
     P1.LLAA_DATE_VALUE,
     P1.LLAA_NUMBER_VALUE,
     'Y',
     P1.LLAA_QOR_CODE,
     P1.LLAA_OTHER_CODE,
     P1.LLAA_OTHER_DATE,
     P1.LLAA_COMMENTS);


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
-- Section to analyse the table populated with this dataload

l_an_tab := s_dl_hem_utils.dl_comp_stats('LETTINGS_AREA_ANSWERS');

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
     LLAA_DLB_BATCH_ID,
     LLAA_DL_SEQNO,
     LLAA_DL_LOAD_STATUS,
     LLAA_LAR_CODE,
     LLAA_QUE_REFNO,
     LLAA_CREATED_BY,
     LLAA_CREATED_DATE,
     LLAA_DATE_VALUE,
     LLAA_NUMBER_VALUE,
     LLAA_CHAR_VALUE,
     LLAA_QOR_CODE,
     LLAA_OTHER_CODE,
     LLAA_OTHER_DATE,
     LLAA_COMMENTS,
     LAPP_LEGACY_REF
FROM  DL_HAT_LETTINGS_AREA_ANSWERS
WHERE llaa_dlb_batch_id    = p_batch_id
AND   llaa_dl_load_status IN ('L','F','O');
--


-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_LETTINGS_AREA_ANSWERS';
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
fsc_utils.proc_start('s_dl_hat_lettings_area_ans.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_lettings_area_ans.dataload_validate',3 );
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
cs := p1.llaa_dl_seqno;
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
 -- The record must not already exist on the lettings_area_answers table
 --
  IF (s_dl_hat_utils.f_exists_letans(p1.llaa_lar_code,p1.lapp_legacy_ref,
      p1.llaa_que_refno))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',339);
  END IF;
--
-- Check that the question reference number must exist on the questions
-- table
  IF (NOT s_dl_hat_utils.f_exists_quenum(p1.llaa_que_refno))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',270);
  END IF;
--
-- For lettings areas the question category (que_question_category) should be
-- LA
  IF (NOT s_dl_hat_utils.f_valid_quecat(p1.llaa_que_refno,'L'))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',340);
  END IF;

 -- Where the question relates to the area preference QCAT domain LA
 -- lettings areas the application will validate that the lettings area
 -- provided is valid

 IF (NOT s_dl_hat_utils.f_valid_larcode(p1.llaa_lar_code))
 THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',272);
 END IF;

 -- Lettings area code is the lowest level within the area hierarchy

 IF (NOT s_dl_hat_utils.f_valid_child_larcode(p1.llaa_lar_code))
 THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',273);
 END IF;

 -- Where a coded optional response is included
 -- within the record this will be validated against
 -- the QUESTION_OPTION_RESPONSES table

-- DBMS_OUTPUT.PUT_LINE('QOR_CODE= '||p1.llaa_qor_code);
IF p1.llaa_qor_code IS NOT NULL
THEN
   IF (NOT S_Dl_Hat_Utils.f_exists_qor(p1.llaa_qor_code))
   THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',297);
   END IF;
END IF;


 -- Where the associated questions has a datatype of 'C'oded
 -- then a valid answer must exist in Question_permitted_respones

 IF p1.llaa_char_value IS NOT NULL
 THEN
    IF (NOT S_Dl_Hat_Utils.f_exists_char(p1.llaa_que_refno,p1.llaa_char_value))
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',279);
    END IF;
 END IF;

-- Where the associated question has a datatype of numeric then valid
-- number must be entered or, where a QUESTION_PERMITTED_RESPONSE exists
-- then must be in the specified range

 IF p1.llaa_number_value IS NOT NULL
 THEN
    IF (NOT S_Dl_Hat_Utils.f_exists_num(p1.llaa_que_refno,p1.llaa_number_value))
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',278);
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
CURSOR c1 IS
SELECT
     ROWID rec_rowid,
     LLAA_DLB_BATCH_ID,
     LLAA_DL_SEQNO,
     LLAA_DL_LOAD_STATUS,
     LLAA_LAR_CODE,
     LAPP_LEGACY_REF,
     LLAA_QUE_REFNO
FROM  DL_HAT_LETTINGS_AREA_ANSWERS
WHERE llaa_dlb_batch_id   = p_batch_id
AND   llaa_dl_load_status = 'C';
--

l_app_refno applications.app_refno%TYPE;
i INTEGER := 0;
l_an_tab VARCHAR2(1);
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_LETTINGS_AREA_ANSWERS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_lettings_area_ans.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_lettings_area_ans.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.llaa_dl_seqno;
i  := i +1;
l_id := p1.rec_rowid;
--
--
l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lapp_legacy_ref);
--
--
DELETE FROM LETTINGS_AREA_ANSWERS
WHERE laa_lar_code = p1.llaa_lar_code
AND laa_app_refno  = l_app_refno
AND laa_que_refno = p1.llaa_que_refno;
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
-- Section to analyse the table populated with this dataload

l_an_tab := s_dl_hem_utils.dl_comp_stats('LETTINGS_AREA_ANSWERS');

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
END s_dl_hat_lettings_area_ans;
/

