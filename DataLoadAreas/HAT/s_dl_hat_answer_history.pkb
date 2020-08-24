CREATE OR REPLACE PACKAGE BODY s_dl_hat_answer_history
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Ver   WHO  WHEN       WHY
--      1.0  6.14     AJ   13/10/2017 Initial Creation for GNB Migration Project
--      1.1  6.14     AJ   20/12/2017 Further updates
--      1.2  6.14     AJ   04/01/2018 1) Removed the following as not required
--                                       lahs_ipa_refno  lahs_mrf_assessment_refno and 
--                                       lahs_hia_hin_instance_refno
--      1.3  6.14     AJ   09/01/2018 further updates done
--      1.4  6.14     AJ   11/01/2018 further updates done error code numbers added
--      1.5  6.14     AJ   12/01/2018 further updates done during testing checks 417 and 418
--                                    removed and c_dup updated to correct ORA-1858
--
--
-- ***********************************************************************
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hat_answer_history
  SET lahs_dl_load_status = p_status
  WHERE rowid = p_rowid;
--
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_answer_history');
     RAISE;
--
END set_record_status_flag;
--
-- ***********************************************************************
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
     lahs_dlb_batch_id,
     lahs_dl_seqno,
     lahs_dl_load_status,
     lahs_rec_type,               -- GEN or LAA
     lahs_app_legacy_ref,         -- M (Both)
     lahs_que_refno,              -- M (General Answers)
     lahs_lar_code,               -- Lettings Area Answers
     lahs_action_ind,             -- M D(deleted) or U(updated)
     lahs_modified_by,            -- M
     lahs_modified_date,          -- M Date Time
     lahs_date_value,             -- Date
     lahs_number_value,
     lahs_char_value,
     lahs_created_by,
     lahs_created_date,           -- Date Time
     lahs_qor_code,
     lahs_other_code,
     lahs_other_date,             -- Date
     lahs_comments
FROM  dl_hat_answer_history
WHERE lahs_dlb_batch_id   = p_batch_id
AND   lahs_dl_load_status = 'V';
--
-- *******************************
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_ANSWER_HISTORY';
cs       INTEGER;
ce	     VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_an_tab         VARCHAR2(1);
i                INTEGER := 0;
l_app_refno      answer_history.ahs_app_refno%TYPE;
--
-- *******************************
BEGIN
--
fsc_utils.proc_start('s_dl_hat_answer_history.dataload_create');
fsc_utils.debug_message( 's_dl_hat_answer_history.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lahs_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_app_refno:= NULL;
--
  SAVEPOINT SP1;
--
  l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lahs_app_legacy_ref);
--
-- General Answers
--
  IF (p1.lahs_rec_type = 'GEN')
   THEN  
     INSERT INTO answer_history
        (ahs_app_refno,
         ahs_que_refno,
         ahs_ipa_refno,
         ahs_lar_code,
         ahs_action_ind,
         ahs_modified_by,
         ahs_modified_date,
         ahs_comments,
         ahs_date_value,
         ahs_number_value,
         ahs_char_value,
         ahs_created_by,
         ahs_created_date,
         ahs_qor_code,
         ahs_other_code,
         ahs_other_date,
         ahs_mrf_assessment_refno,
         ahs_hia_hin_instance_refno)
        VALUES
        (l_app_refno,
         p1.lahs_que_refno,
         null,
         null,
         p1.lahs_action_ind,
         p1.lahs_modified_by,
         p1.lahs_modified_date,
         p1.lahs_comments,
         p1.lahs_date_value,
         p1.lahs_number_value,
         p1.lahs_char_value,
         p1.lahs_created_by,
         p1.lahs_created_date,
         p1.lahs_qor_code,
         p1.lahs_other_code,
         p1.lahs_other_date,
         null,
         null);
--
-- Lettings Area Answers
--
  ELSIF (p1.lahs_rec_type = 'LAA')
   THEN  
     INSERT INTO answer_history
        (ahs_app_refno,
         ahs_que_refno,
         ahs_ipa_refno,
         ahs_lar_code,
         ahs_action_ind,
         ahs_modified_by,
         ahs_modified_date,
         ahs_comments,
         ahs_date_value,
         ahs_number_value,
         ahs_char_value,
         ahs_created_by,
         ahs_created_date,
         ahs_qor_code,
         ahs_other_code,
         ahs_other_date,
         ahs_mrf_assessment_refno,
         ahs_hia_hin_instance_refno)
        VALUES
        (l_app_refno,
         p1.lahs_que_refno,
         null,
         p1.lahs_lar_code,
         p1.lahs_action_ind,
         p1.lahs_modified_by,
         p1.lahs_modified_date,
         p1.lahs_comments,
         p1.lahs_date_value,
         p1.lahs_number_value,
         p1.lahs_char_value,
         p1.lahs_created_by,
         p1.lahs_created_date,
         p1.lahs_qor_code,
         p1.lahs_other_code,
         p1.lahs_other_date,
         null,
         null);
--
  END IF;
--
--************************************
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
    ROLLBACK TO SP1;
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    set_record_status_flag(l_id,'O');
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
  END;
--
 END LOOP;
--
--************************************
-- Section to analyse table populated by this data load
--
l_an_tab := s_dl_hem_utils.dl_comp_stats('ANSWER_HISTORY');

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
-- *************************************************************************
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2
     ,p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT
     rowid rec_rowid,
     lahs_dlb_batch_id,
     lahs_dl_seqno,
     lahs_dl_load_status,
     lahs_rec_type,               -- GEN or LAA fail if anything else
     lahs_app_legacy_ref,         -- M (Both)
     lahs_que_refno,              -- M (General Answers)
     lahs_lar_code,               -- Lettings Area Answers
     lahs_action_ind,             -- M D(deleted) or U(updated)
     lahs_modified_by,            -- M
     lahs_modified_date,          -- M Date Time
     lahs_date_value,             -- Date
     lahs_number_value,
     lahs_char_value,
     lahs_created_by,
     lahs_created_date,           -- Date Time
     lahs_qor_code,
     lahs_other_code,
     lahs_other_date,             -- Date
     lahs_comments
FROM  dl_hat_answer_history
WHERE lahs_dlb_batch_id    = p_batch_id
AND   lahs_dl_load_status IN ('L','F','O');
--
-- *****************
CURSOR c_app_exists(p_app_legacy_ref VARCHAR2) IS
SELECT app_refno
FROM   applications
WHERE  app_legacy_ref = p_app_legacy_ref;
--
-- *****************
CURSOR c_legacy_count(p_app_legacy_ref VARCHAR2) IS
SELECT count(*)
FROM   applications
WHERE  app_legacy_ref = p_app_legacy_ref;
--
-- *****************
CURSOR c_let_ans( p_que_refno NUMBER
                 ,p_app_refno NUMBER
                 ,p_lar_code  VARCHAR2 ) IS
SELECT 'X'
FROM   lettings_area_answers
WHERE  laa_que_refno = p_que_refno
AND    laa_app_refno = p_app_refno
AND    laa_lar_code  = p_lar_code;
--
-- *****************
CURSOR c_gen_ans( p_que_refno NUMBER
                 ,p_app_refno NUMBER) IS
SELECT 'X'
FROM   general_answers
WHERE  gan_que_refno = p_que_refno
AND    gan_app_refno = p_app_refno;
--
-- *****************
CURSOR c_que_refno(p_que_refno NUMBER) IS
SELECT 'X'
FROM   questions
WHERE  que_refno = p_que_refno;
--
-- *****************
CURSOR c_lar_code(p_lar_code  VARCHAR2) IS
SELECT 'X'
FROM   lettings_areas
WHERE  lar_code = p_lar_code;
--
-- *****************
CURSOR c_qor_code( p_qor_code  VARCHAR2
                  ,p_que_refno NUMBER   ) IS
SELECT 'X'
FROM   question_optional_responses
WHERE  qor_code = p_qor_code
AND    qor_que_refno = p_que_refno;
--
-- *****************
CURSOR c_dup ( p_ahs_app_refno     NUMBER
              ,p_ahs_que_refno     NUMBER
              ,p_ahs_lar_code      VARCHAR2
              ,p_ahs_action_ind    VARCHAR2
              ,p_ahs_modified_by   VARCHAR2
              ,p_ahs_modified_date DATE
              ,p_ahs_date_value    DATE
              ,p_ahs_number_value  NUMBER
              ,p_ahs_char_value    VARCHAR2
              ,p_ahs_created_by    VARCHAR2
              ,p_ahs_created_date  DATE
              ,p_ahs_qor_code      VARCHAR2
              ,p_ahs_other_code    VARCHAR2
              ,p_ahs_other_date    DATE
              ,p_ahs_comments      VARCHAR2 ) IS
SELECT /*+ index(ANSWER_HISTORY AHI_APP_FK_I) */ 'X'
FROM  answer_history
WHERE ahs_app_refno                       = p_ahs_app_refno
AND   ahs_que_refno                       = p_ahs_que_refno
AND   nvl(ahs_lar_code,'ZZZZZZZZ')        = nvl(p_ahs_lar_code,'ZZZZZZZZ')
AND   ahs_action_ind                      = p_ahs_action_ind
AND   ahs_modified_by                     = p_ahs_modified_by
AND   ahs_modified_date                   = p_ahs_modified_date
AND   nvl(ahs_date_value,trunc(sysdate))  = nvl(p_ahs_date_value,trunc(sysdate))
AND   nvl(ahs_number_value,'99999999')    = nvl(p_ahs_number_value,'99999999')
AND   nvl(ahs_char_value,'ZZZZZZZZ')      = nvl(p_ahs_char_value,'ZZZZZZZZ')
AND   nvl(ahs_created_by,'ZZZZZZZZ')      = nvl(p_ahs_created_by,'ZZZZZZZZ')
AND   nvl(ahs_created_date,trunc(sysdate))= nvl(p_ahs_created_date,trunc(sysdate))
AND   nvl(ahs_qor_code,'ZZZZZZZZ')        = nvl(p_ahs_qor_code,'ZZZZZZZZ')
AND   nvl(ahs_other_code,'ZZZZZZZZ')      = nvl(p_ahs_other_code,'ZZZZZZZZ')
AND   nvl(ahs_other_date,trunc(sysdate))  = nvl(p_ahs_other_date,trunc(sysdate))
AND   nvl(ahs_comments,'ZZZZZZZZ')        = nvl(p_ahs_comments,'ZZZZZZZZ');
--
-- *******************************
-- constants FOR error process
--
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_ANSWER_HISTORY';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
--
l_errors        VARCHAR2(10);
l_error_ind     VARCHAR2(10);
i               INTEGER := 0;
l_app_refno     answer_history.ahs_app_refno%TYPE;
l_legacy_count  INTEGER := 0;
l_gen_exists    VARCHAR2(1);
l_laa_exists    VARCHAR2(1);
l_que_exists    VARCHAR2(1);
l_lar_exists    VARCHAR2(1);
l_qor_exists    VARCHAR2(1);
l_dup_exists    VARCHAR2(1);


BEGIN
--
fsc_utils.proc_start('s_dl_hat_answer_history.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_answer_history.dataload_validate',3 );
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
  cs := p1.lahs_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors := 'V';
  l_error_ind := 'N';
--
  l_app_refno    := NULL;
  l_legacy_count := 0;
  l_gen_exists   := NULL;
  l_laa_exists   := NULL;
  l_que_exists   := NULL;
  l_lar_exists   := NULL;
  l_qor_exists   := NULL;
  l_dup_exists   := NULL;
--
-- Check Mandatory fields first
--
  IF (p1.lahs_rec_type IS NULL) 
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',404);
-- 'The record type must be supplied'
  END IF;
--
  IF (p1.lahs_rec_type IS NOT NULL)
   THEN
    IF p1.lahs_rec_type NOT IN ('GEN','LAA')
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',405);
-- 'Record type is NOT either GEN(General Answers) or LAA(Lettings Area Answers) so cannot be processed'
    END IF;
  END IF;
--
  IF (p1.lahs_rec_type IN ('GEN','LAA'))
   THEN
    IF (p1.lahs_app_legacy_ref IS NULL) 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',406);
-- 'Application Legacy Ref must be supplied'
    END IF;
--	
    IF (p1.lahs_que_refno IS NULL) 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',407);
-- 'Question number must be supplied'
    END IF;
--	
    IF (p1.lahs_action_ind IS NULL) 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',408);
-- 'Action Indicator must be supplied'
    END IF;
--
    IF (p1.lahs_action_ind IS NOT NULL)
     THEN
      IF p1.lahs_action_ind NOT IN ('U','D')
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',409);
-- 'Action Indicator must be either a U(update) or D(delete)'
      END IF;
    END IF;
--	
    IF (p1.lahs_modified_by IS NULL) 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',410);
-- 'Modified by must be supplied'
    END IF;
--	
    IF (p1.lahs_modified_date IS NULL) 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',411);
-- 'Modified Date must be supplied'
    END IF;
--	
    IF (p1.lahs_created_by IS NULL) 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',412);
-- 'Created By must be supplied'
    END IF;
--	
    IF (p1.lahs_created_date IS NULL) 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',413);
-- 'Created Date must be supplied'
    END IF;
--
  END IF; -- end of mandatory checks for GEN and LAA
--
  IF (p1.lahs_rec_type IN ('LAA'))
   THEN
    IF (p1.lahs_lar_code IS NULL) 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',414);
-- 'Lettings Area Code must be supplied'
    END IF;
--
  END IF; -- end of mandatory checks for LAA only
--
-- Check lahs_app_legacy_ref exits on applications
-- Check that the lahs_app_legacy_ref is unique on applications table
--
  IF (p1.lahs_app_legacy_ref IS NOT NULL)
   THEN
--
    OPEN c_app_exists(p1.lahs_app_legacy_ref);
    FETCH c_app_exists INTO l_app_refno;
    CLOSE c_app_exists;
--
    IF (l_app_refno IS NULL)
     THEN
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',415);
-- 'The application cannot be found on the applications table'
    END IF; 
--
-- check that the lahs_app_legacy_ref is unique on applications table
--
    OPEN c_legacy_count(p1.lahs_app_legacy_ref);
    FETCH c_legacy_count INTO l_legacy_count;
    CLOSE c_legacy_count;
--
    IF (l_legacy_count > 1)
     THEN
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',416);
-- 'The application legacy reference supplied must be unique on the applications table'
    END IF; 
--
  END IF;
--
-- ****************************************************
-- ****************************************************
/* 
-- start of checks removed can be in answer history only if deleted (AJ 12/01/2018)
--
-- check corresponding general answer record exits
--
  IF ( p1.lahs_rec_type IN ('GEN')    AND
       p1.lahs_que_refno IS NOT NULL  AND
       l_app_refno IS NOT NULL            )
   THEN
--
    OPEN c_gen_ans(p1.lahs_que_refno, l_app_refno);
    FETCH c_gen_ans INTO l_gen_exists;
    CLOSE c_gen_ans;
--
    IF (l_gen_exists IS NULL) 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',417);
-- 'Question for the Application does not exist in General Answers table'
    END IF;
--
  END IF;
--
-- check corresponding lettings area answers record exits
--
  IF ( p1.lahs_rec_type  IN ('LAA')    AND
       p1.lahs_que_refno IS NOT NULL   AND
       l_app_refno       IS NOT NULL   AND
       p1.lahs_lar_code  IS NOT NULL       )
   THEN
--
    OPEN c_let_ans( p1.lahs_que_refno,
                    l_app_refno,
                    p1.lahs_lar_code  );
    FETCH c_let_ans INTO l_laa_exists;
    CLOSE c_let_ans;
--
    IF (l_laa_exists IS NULL) 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',418);
-- 'Question for the Application does not exist in Lettings Area Answers table'
    END IF;
--
  END IF;
-- 
*/  -- end of checks removed (AJ 12/01/2018)
-- ****************************************************
-- ****************************************************
--
-- check question exists in questions table
--
  IF (p1.lahs_que_refno  IS NOT NULL)
   THEN
--
    OPEN c_que_refno( p1.lahs_que_refno);
    FETCH c_que_refno INTO l_que_exists;
    CLOSE c_que_refno;
--
    IF (l_que_exists IS NULL) 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',419);
-- 'Question Reference does not exist in the Questions table'
    END IF;
--
  END IF;
--
-- check lar_code exists in lettings_areas table
--
  IF (p1.lahs_lar_code  IS NOT NULL)
   THEN
--
    OPEN c_lar_code( p1.lahs_lar_code);
    FETCH c_lar_code INTO l_lar_exists;
    CLOSE c_lar_code;
--
    IF (l_lar_exists IS NULL) 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',420);
-- 'Lettings Area Code does not exist in Lettings Areas table'
    END IF;
--
  END IF;
--
-- check lar_code only provided for LAA record types
--
  IF (p1.lahs_rec_type !='LAA'  AND  p1.lahs_lar_code IS NOT NULL) 
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',421);
-- 'Lettings Area Code must only be supplied for LAA record types'
  END IF;
--
-- check qor_code and que_refno combination exists in question_optional_responses table
--
  IF ( p1.lahs_qor_code  IS NOT NULL AND
       p1.lahs_que_refno IS NOT NULL     )
   THEN
--
    OPEN c_qor_code( p1.lahs_qor_code, p1.lahs_que_refno);
    FETCH c_qor_code INTO l_qor_exists;
    CLOSE c_qor_code;
--
    IF (l_qor_exists IS NULL) 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',422);
-- 'Optional Response Code and Question combination does not exist in question_optional_responses table'
    END IF;
--
  END IF;
--
-- check for duplicate records in answer history table
--
  IF ( p1.lahs_que_refno     IS NOT NULL   AND
       l_app_refno           IS NOT NULL   AND
       p1.lahs_action_ind    IS NOT NULL   AND
       p1.lahs_modified_by   IS NOT NULL   AND 
       p1.lahs_modified_date IS NOT NULL       )
   THEN
--
    OPEN c_dup( l_app_refno
               ,p1.lahs_que_refno
               ,p1.lahs_lar_code
               ,p1.lahs_action_ind
               ,p1.lahs_modified_by
               ,p1.lahs_modified_date
               ,p1.lahs_date_value
               ,p1.lahs_number_value
               ,p1.lahs_char_value
               ,p1.lahs_created_by
               ,p1.lahs_created_date
               ,p1.lahs_qor_code
               ,p1.lahs_other_code
               ,p1.lahs_other_date
               ,p1.lahs_comments   );
    FETCH c_dup INTO l_dup_exists;
    CLOSE c_dup;
--
    IF (l_dup_exists IS NOT NULL) 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',423);
-- 'A Duplicate record already exists in answer_history table'
    END IF;
--
  END IF;
--
--
-- *******************************
--
-- Now UPDATE the record count and error code
--
  IF l_errors = 'F' THEN
   l_error_ind := 'Y';
  ELSE
   l_error_ind := 'N';
  END IF;
--
-- *******************************
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
--
  END;
--
 END LOOP;
--
fsc_utils.proc_end;
COMMIT;
--
 EXCEPTION
  WHEN OTHERS THEN
   s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
-- *************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 is
SELECT
     rowid rec_rowid,
     lahs_dlb_batch_id,
     lahs_dl_seqno,
     lahs_dl_load_status,
     lahs_rec_type,               -- GEN or LAA
     lahs_app_legacy_ref,         -- M (Both)
     lahs_que_refno,              -- M (General Answers)
     lahs_lar_code,               -- Lettings Area Answers
     lahs_action_ind,             -- M D(deleted) or U(updated)
     lahs_modified_by,            -- M
     lahs_modified_date,          -- M Date Time
     lahs_date_value,             -- Date
     lahs_number_value,
     lahs_char_value,
     lahs_created_by,
     lahs_created_date,           -- Date Time
     lahs_qor_code,
     lahs_other_code,
     lahs_other_date,             -- Date
     lahs_comments
FROM  dl_hat_answer_history
WHERE lahs_dlb_batch_id   = p_batch_id
AND   lahs_dl_load_status = 'C';
--
-- *******************************
--
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_ANSWER_HISTORY';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other Variables
--
i INTEGER := 0;
l_an_tab VARCHAR2(1);
l_app_refno  applications.app_refno%TYPE;
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_answer_history.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_answer_history.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lahs_dl_seqno;
  i  := i +1;
  l_id := p1.rec_rowid;
--
  l_app_refno:= NULL;
--
  l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lahs_app_legacy_ref);
--
-- most fields are nullable so nvl done on all except 5
--
  DELETE FROM ANSWER_HISTORY
   WHERE ahs_app_refno                         = l_app_refno
     AND ahs_que_refno                         = p1.lahs_que_refno
     AND nvl(ahs_lar_code,'ZZZZZZZZ')          = nvl(p1.lahs_lar_code,'ZZZZZZZZ')
     AND ahs_action_ind                        = p1.lahs_action_ind
     AND ahs_modified_by                       = p1.lahs_modified_by
     AND ahs_modified_date                     = p1.lahs_modified_date
     AND nvl(ahs_date_value,trunc(sysdate))    = nvl(p1.lahs_date_value,trunc(sysdate))
     AND nvl(ahs_number_value,'99999999')      = nvl(p1.lahs_number_value,'99999999')
     AND nvl(ahs_char_value,'ZZZZZZZZ')        = nvl(p1.lahs_char_value,'ZZZZZZZZ')
     AND nvl(ahs_created_by,'ZZZZZZZZ')        = nvl(p1.lahs_created_by,'ZZZZZZZZ')
     AND nvl(ahs_created_date,trunc(sysdate))  = nvl(p1.lahs_created_date,trunc(sysdate))
     AND nvl(ahs_qor_code,'ZZZZZZZZ')          = nvl(p1.lahs_qor_code,'ZZZZZZZZ')
     AND nvl(ahs_other_code,'ZZZZZZZZ')        = nvl(p1.lahs_other_code,'ZZZZZZZZ')
     AND nvl(ahs_other_date,trunc(sysdate))    = nvl(p1.lahs_other_date,trunc(sysdate))
     AND nvl(ahs_comments,'ZZZZZZZZ')          = nvl(p1.lahs_comments,'ZZZZZZZZ');
--
-- *******************************
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
-- *******************************
-- Section to analyse table populated by this data load

l_an_tab := s_dl_hem_utils.dl_comp_stats('ANSWER_HISTORY');
--
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
END s_dl_hat_answer_history;
/

show errors


