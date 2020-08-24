CREATE OR REPLACE PACKAGE BODY s_dl_hat_involved_party_ans
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Ver    WHO  WHEN       WHY
--      1.0           RJ   03/04/2001 Dataload
--      1.1  5.1.6    PJD  22/05/2002 Added validation on lapp_legacy_ref
--      1.2  5.1.5    PJD  28/05/2002 Added missing comma to cursor c1 in validate procedure
--      1.3  5.1.6    PJD  14/06/2002 Added validation that person ref must be supplied
--      2.0  5.3.0    SB   20/03/2003 Added commit on validate.
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
  UPDATE dl_hat_involved_party_answers
  SET lipn_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_involved_party_answers');
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
     LIPN_DLB_BATCH_ID,
     LIPN_DL_SEQNO,
     LIPN_DL_LOAD_STATUS,
     LIPN_QUE_REFNO,
     LIPN_CREATED_BY,
     LIPN_CREATED_DATE,
     LIPN_DATE_VALUE,
     LIPN_NUMBER_VALUE,
     LIPN_CHAR_VALUE,
     LIPN_QOR_CODE,
     LIPN_OTHER_CODE,
     LIPN_OTHER_DATE,
     LIPN_COMMENTS,
     LAPP_LEGACY_REF,
     LPAR_PER_ALT_REF
FROM  dl_hat_involved_party_answers
WHERE lipn_dlb_batch_id   = p_batch_id
AND   lipn_dl_load_status = 'V';
--
--
-- Check this
CURSOR c2 (p_lpar_per_alt_ref VARCHAR2) IS
SELECT ipa_refno
FROM parties, involved_parties
WHERE par_per_alt_ref = p_lpar_per_alt_ref
AND par_refno = ipa_par_refno;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_INVOLVED_PARTY_ANSWERS';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
-- Other variables
i                INTEGER := 0;
l_an_tab         VARCHAR2(1);
l_app_refno      applications.app_refno%TYPE;
l_ipn_ipa_refno  involved_party_answers.ipn_ipa_refno%TYPE;
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_involved_party_ans.dataload_create');
fsc_utils.debug_message( 's_dl_hat_involved_party_ans.dataload_create',3);
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
cs := p1.lipn_dl_seqno;
l_id := p1.rec_rowid;
--
-- Get the application reference number
--
l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lapp_legacy_ref);
--
-- Get the involved party reference number.
--
l_ipn_ipa_refno := NULL;
--
OPEN c2 (p1.lpar_per_alt_ref);
FETCH c2 INTO l_ipn_ipa_refno;
CLOSE c2;
--
INSERT INTO INVOLVED_PARTY_ANSWERS
          (IPN_IPA_REFNO,
           IPN_QUE_REFNO,
           IPN_APP_REFNO,
           IPN_CREATED_BY,
           IPN_CREATED_DATE,
           IPN_DATE_VALUE,
           IPN_NUMBER_VALUE,
           IPN_CHAR_VALUE,
           IPN_OTHER_CODE,
           IPN_OTHER_DATE,
           IPN_COMMENTS,
           IPN_QOR_CODE)
values
       (l_ipn_ipa_refno,
        P1.LIPN_QUE_REFNO,
        l_app_refno,
        P1.LIPN_CREATED_BY,
        P1.LIPN_CREATED_DATE,
        P1.LIPN_DATE_VALUE,
        P1.LIPN_NUMBER_VALUE,
        P1.LIPN_CHAR_VALUE,
        P1.LIPN_OTHER_CODE,
        P1.LIPN_OTHER_DATE,
        P1.LIPN_COMMENTS,
        P1.LIPN_QOR_CODE);


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
-- analyse the table populated by this dataload

l_an_tab := s_dl_hem_utils.dl_comp_stats('INVOLVED_PARTY_ANSWERS');


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
     LIPN_DLB_BATCH_ID,
     LIPN_DL_SEQNO,
     LIPN_DL_LOAD_STATUS,
     LIPN_QUE_REFNO,
     LIPN_CREATED_BY,
     LIPN_CREATED_DATE,
     LIPN_DATE_VALUE,
     LIPN_NUMBER_VALUE,
     LIPN_CHAR_VALUE,
     LIPN_QOR_CODE,
     LIPN_OTHER_CODE,
     LIPN_OTHER_DATE,
     LIPN_COMMENTS,
     LAPP_LEGACY_REF,
     LPAR_PER_ALT_REF
FROM  dl_hat_involved_party_answers
WHERE lipn_dlb_batch_id    = p_batch_id
AND   lipn_dl_load_status IN ('L','F','O');
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_INVOLVED_PARTY_ANSWERS';
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
fsc_utils.proc_start('s_dl_hat_involved_party_ans.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_involved_party_ans.dataload_validate',3 );
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
cs := p1.lipn_dl_seqno;
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
 -- The record must not already exist on the involved party answer table

  IF (s_dl_hat_utils.f_exists_invpans(p1.lpar_per_alt_ref,p1.lipn_que_refno))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',334);
  END IF;
--

-- Check that the question reference number must exist on the questions
-- table
  IF (NOT s_dl_hat_utils.f_exists_quenum(p1.lipn_que_refno))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',270);
  END IF;
--
-- For general answers the question category (que_question_category) should be
-- IP
  IF (NOT s_dl_hat_utils.f_valid_quecat(p1.lipn_que_refno,'I'))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',333);
  END IF;
--
-- Where the question relates to an involved party (QCAT domain 'IP' - involved
-- party) the application will check for a valid interested party reference number
  IF p1.lpar_per_alt_ref IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',336);
  ELSIF (NOT s_dl_hat_utils.f_exists_invpar(p1.lpar_per_alt_ref))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',336);
  END IF;
 --
 --
 -- Where a coded optional response is included
 -- within the record this will be validated against
 -- the QUESTION_OPTION_RESPONSES table
 IF p1.lipn_qor_code IS NOT NULL
 THEN
    IF (NOT s_dl_hat_utils.f_exists_qor(p1.lipn_qor_code))
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',297);
    END IF;
  END IF;


 -- Where the associated questions has a datatype of 'C'oded
 -- then a valid answer must exist in Question_permitted_respones

 IF p1.lipn_char_value IS NOT NULL
 THEN
    IF (NOT s_dl_hat_utils.f_exists_char(p1.lipn_que_refno,p1.lipn_char_value))
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',279);
    END IF;
 END IF;

-- Where the associated question has a datatype of numeric then valid
-- number must be entered or, where a QUESTION_PERMITTED_RESPONSE exists
-- then must be in the specified range

 IF p1.lipn_number_value IS NOT NULL
 THEN
    IF (NOT s_dl_hat_utils.f_exists_num(p1.lipn_que_refno,p1.lipn_number_value))
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
CURSOR c1 is
SELECT
     rowid rec_rowid,
     LIPN_DLB_BATCH_ID,
     LIPN_DL_SEQNO,
     LIPN_DL_LOAD_STATUS,
     LIPN_QUE_REFNO,
     LPAR_PER_ALT_REF
FROM  dl_hat_involved_party_answers
WHERE lipn_dlb_batch_id   = p_batch_id
AND   lipn_dl_load_status = 'C';
--
CURSOR c2 (p_lpar_per_alt_ref VARCHAR2) IS
SELECT ipa_refno
FROM parties, involved_parties
WHERE par_per_alt_ref = p_lpar_per_alt_ref
AND par_refno = ipa_par_refno;
--
i INTEGER := 0;
l_an_tab  VARCHAR2(1);
l_ipn_ipa_refno  involved_party_answers.ipn_ipa_refno%TYPE;
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_INVOLVED_PARTY_ANSWERS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_involved_party_ans.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_involved_party_ans.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lipn_dl_seqno;
i  := i +1;
l_id := p1.rec_rowid;
--
--
-- Get the involved party answers reference number.

OPEN c2 (p1.lpar_per_alt_ref);
FETCH c2 INTO l_ipn_ipa_refno;
CLOSE c2;
--
--
DELETE FROM INVOLVED_PARTY_ANSWERS
WHERE ipn_ipa_refno = l_ipn_ipa_refno
AND ipn_que_refno = p1.lipn_que_refno;
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
-- analyse the table populated by this dataload

l_an_tab := s_dl_hem_utils.dl_comp_stats('INVOLVED_PARTY_ANSWERS');

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
END s_dl_hat_involved_party_ans;
/

