--
CREATE OR REPLACE PACKAGE BODY s_dl_had_adv_case_questn_resp
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    VS   16-JAN-2009  Initial Creation.
--
--  2.0     5.15.0    VS   11-DEC-2009  Defect 2897 Fix. Disable/Enable
--                                      ACQR_BR_I in CREATE Process
--
--  3.0     5.15.0    VS   17-FEB-2010  Add TO_CHAR to cursors to make sure
--                                      indexes are used correctly
--
--                                      Changed commit 500000 to 50000
--
--  declare package variables AND constants
--
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(p_rowid  IN ROWID,
                                 p_status IN VARCHAR2)
AS
--
BEGIN
--
  UPDATE dl_had_adv_case_questn_resp
  SET    lacqr_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_had_adv_case_questn_resp');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create(p_batch_id          IN VARCHAR2,
                          p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT ROWID REC_ROWID,
       LACQR_DLB_BATCH_ID,
       LACQR_DL_SEQNO,
       LACQR_DL_LOAD_STATUS,
       LACQR_ACAS_ALTERNATE_REF,
       LACQR_CAQU_REFERENCE,
       LACQR_TYPE,
       LACQR_CQRS_CODE,
       LACQR_BOOLEAN_VALUE,
       LACQR_TEXT_VALUE,
       LACQR_DATE_VALUE,
       LACQR_NUMBER_VALUE,
       NVL(LACQR_CREATED_BY,'DATALOAD') LACQR_CREATED_BY,
       NVL(LACQR_CREATED_DATE,SYSDATE)  LACQR_CREATED_DATE,
       LACQR_ADDITIONAL_RESPONSE,
       LACQR_ACHO_LEGACY_REF,
       LACQR_REFNO
  FROM dl_had_adv_case_questn_resp
 WHERE lacqr_dlb_batch_id   = p_batch_id
   AND lacqr_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR get_acas_reference(p_acas_alt_reference VARCHAR2)
IS
SELECT acas_reference
  FROM advice_cases
 WHERE acas_alternate_reference = TO_CHAR(p_acas_alt_reference);
--
-- ***********************************************************************
--
CURSOR get_acho_reference(p_acho_reference VARCHAR2)
IS
SELECT acho_reference
  FROM advice_case_housing_options
 WHERE acho_alternative_reference = TO_CHAR(p_acho_reference);
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HAD_ADV_CASE_QUESTN_RESP';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
l_an_tab             VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
i                 INTEGER := 0;
l_exists          VARCHAR2(1);
l_acas_reference  NUMBER(10);
l_acho_reference  NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger ACQR_BR_I disable';
--
    fsc_utils.proc_start('s_dl_had_adv_case_questn_resp.dataload_create');
    fsc_utils.debug_message('s_dl_had_adv_case_questn_resp.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lacqr_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
--
-- Get acas_reference
--
          l_acas_reference := NULL;
--
           OPEN get_acas_reference(p1.lacqr_acas_alternate_ref);
          FETCH get_acas_reference INTO l_acas_reference;
          CLOSE get_acas_reference;
--
-- Get acho_reference
--
          l_acho_reference := NULL;
--
          IF (p1.lacqr_acho_legacy_ref IS NOT NULL) THEN
--
            OPEN get_acho_reference(p1.lacqr_acho_legacy_ref);
           FETCH get_acho_reference INTO l_acho_reference;
           CLOSE get_acho_reference;
--
          END IF;
--
--
-- Insert into relevent table
--
--
-- Insert into ADVICE_CASE_QUESTN_RESPONSES
--
--
          INSERT /* +APPEND */ INTO advice_case_questn_responses(ACQR_ACAS_REFERENCE,
                                                   ACQR_CAQU_REFERENCE,
                                                   ACQR_TYPE,
                                                   ACQR_CQRS_CODE,
                                                   ACQR_BOOLEAN_VALUE,
                                                   ACQR_TEXT_VALUE,
                                                   ACQR_DATE_VALUE,
                                                   ACQR_NUMBER_VALUE,
                                                   ACQR_CREATED_BY,
                                                   ACQR_CREATED_DATE,
                                                   ACQR_ADDITIONAL_RESPONSE,
                                                   ACQR_REFNO 
                                                  )
--
                                           VALUES (l_acas_reference,
                                                   p1.LACQR_CAQU_REFERENCE,
                                                   p1.LACQR_TYPE,
                                                   p1.LACQR_CQRS_CODE,
                                                   p1.LACQR_BOOLEAN_VALUE,
                                                   p1.LACQR_TEXT_VALUE,
                                                   p1.LACQR_DATE_VALUE,
                                                   p1.LACQR_NUMBER_VALUE,
                                                   p1.LACQR_CREATED_BY,
                                                   p1.LACQR_CREATED_DATE,
                                                   p1.LACQR_ADDITIONAL_RESPONSE,
                                                   p1.LACQR_REFNO
                                                  );
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
   i := i+1; 
--
   IF MOD(i,50000)=0 THEN 
     COMMIT; 
   END IF;
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
  COMMIT;
--
-- ***********************************************************************
--
-- Section to anayze the table(s) populated by this dataload
--
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_CASE_QUESTN_RESPONSES');
--
    execute immediate 'alter trigger ACQR_BR_I enable';
--
   fsc_utils.proc_END;
--
   EXCEPTION
        WHEN OTHERS THEN
           s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
           RAISE;
--
END dataload_create;
--
-- ***********************************************************************
--
--
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
                            p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT ROWID REC_ROWID,
       LACQR_DLB_BATCH_ID,
       LACQR_DL_SEQNO,
       LACQR_DL_LOAD_STATUS,
       LACQR_ACAS_ALTERNATE_REF,
       LACQR_CAQU_REFERENCE,
       LACQR_TYPE,
       LACQR_CQRS_CODE,
       LACQR_BOOLEAN_VALUE,
       LACQR_TEXT_VALUE,
       LACQR_DATE_VALUE,
       LACQR_NUMBER_VALUE,
       NVL(LACQR_CREATED_BY,'DATALOAD') LACQR_CREATED_BY,
       NVL(LACQR_CREATED_DATE,SYSDATE)  LACQR_CREATED_DATE,
       LACQR_ADDITIONAL_RESPONSE,
       LACQR_ACHO_LEGACY_REF,
       LACQR_REFNO
  FROM dl_had_adv_case_questn_resp
 WHERE lacqr_dlb_batch_id    = p_batch_id
   AND lacqr_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR chk_acas_exists(p_alternate_reference VARCHAR2) 
IS
SELECT acas_reference
  FROM advice_cases
 WHERE acas_alternate_reference = TO_CHAR(p_alternate_reference);
--
--
-- ***********************************************************************
--
CURSOR chk_caqu_exists(p_caqu_reference NUMBER) 
IS
SELECT 'X',
       caqu_type, 
       caqu_data_type
  FROM case_questions
 WHERE caqu_reference = p_caqu_reference;
--
--
-- ***********************************************************************
--
CURSOR chk_cqrs_exists(p_caqu_reference NUMBER,
                       p_cqrs_code      VARCHAR2) 
IS
SELECT 'X'
  FROM coded_question_responses
 WHERE cqrs_caqu_reference = p_caqu_reference
   AND cqrs_code           = p_cqrs_code;
--
--
-- ***********************************************************************
--
CURSOR chk_acqr_exists (p_acas_reference NUMBER,
                        p_caqu_reference NUMBER) 
IS
SELECT 'X'
  FROM advice_case_questn_responses
 WHERE acqr_acas_reference = p_acas_reference
   AND acqr_caqu_reference = p_caqu_reference;
--
--
-- ***********************************************************************
--
CURSOR chk_acho_exists(p_acho_reference VARCHAR2) 
IS
SELECT acho_reference
  FROM advice_case_housing_options
 WHERE acho_alternative_reference = TO_CHAR(p_acho_reference);
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HAD_ADV_CASE_QUESTN_RESP';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         	VARCHAR2(1);
l_acas_reference       	NUMBER(10);
l_caqu_exists           VARCHAR2(1);
l_caqu_type             VARCHAR2(1);
l_caqu_data_type        VARCHAR2(1);
l_cqrs_exists           VARCHAR2(1);
l_acqr_exists           VARCHAR2(1);
l_acho_reference       	NUMBER(10);
--
l_errors         	VARCHAR2(10);
l_error_ind      	VARCHAR2(10);
i                	INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_had_adv_case_questn_resp.dataload_validate');
    fsc_utils.debug_message('s_dl_had_adv_case_questn_resp.dataload_validate',3);
--
    cb := p_batch_id;
    cd := p_DATE;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs   := p1.lacqr_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
--
-- ***********************************************************************
--
-- Validation checks required
--
-- Check Advice Case Alt Reference LACHO_ACAS_ALTERNATE_REF has been supplied 
-- and exists on advice_cases.
--
--  
          IF (p1.lacqr_acas_alternate_ref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',113);
          ELSE
--
             l_acas_reference := NULL;
--
              OPEN chk_acas_exists(p1.lacqr_acas_alternate_ref);
             FETCH chk_acas_exists INTO l_acas_reference;
             CLOSE chk_acas_exists;
--
             IF (l_acas_reference IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',144);
             END IF;
--            
          END IF;
--
-- ***********
--
-- Check Case Question Reference LACQR_CAQU_REFERENCE has been supplied 
-- and exists on case_questions.
--
--  
          IF (p1.lacqr_caqu_reference IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',194);
          ELSE
--
             l_caqu_exists    := NULL;
             l_caqu_type      := NULL;
             l_caqu_data_type := NULL;
--
              OPEN chk_caqu_exists(p1.lacqr_caqu_reference);
             FETCH chk_caqu_exists INTO l_caqu_exists,l_caqu_type, l_caqu_data_type;
             CLOSE chk_caqu_exists;
--
             IF (l_caqu_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',195);
             END IF;
--            
          END IF;
--
-- ***********
--
-- Check Question Type LACQR_TYPE is supplied and valid
-- Valid Type of Advice Case Question Response:	'C' - Coded
--                                              'N' - Non Coded	
--                                              'Y' - Yes/No type
--
-- If the Type is (Y) Yes/No then the Yes/No response (LACQR_BOOLEAN_VALUE) 
-- must be Y or N. Coded, Text, Date and Numeric responses must not be supplied.
--
--
-- If the Type is (C) Coded then Coded response field must be populated. 
-- Yes/No, Text, Date and Numeric responses must not be supplied. It must exist 
-- on Coded Question Responses for the combination of Question Reference and 
-- Coded Response
--
--
-- If the Type is (N) Non Coded then Only one of Date, Numeric or Text Response 
-- must be populated. Coded Response and Yes/No Response must not be supplied.
-- Values supplied must agree with the Data type set up on Case Questions
--
--
          IF (p1.lacqr_type IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',196);
--
          ELSE
--
             IF (p1.lacqr_type NOT IN ('C','N','Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',197);
--
             ELSE
--
-- ******
--
                IF (p1.lacqr_type = 'Y') THEN
--
                 IF (p1.LACQR_BOOLEAN_VALUE NOT IN ('Y','N')) THEN
                  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',198);
--
                 ELSE
--
                    IF (   p1.lacqr_cqrs_code    IS NOT NULL
                        OR p1.lacqr_text_value   IS NOT NULL
                        OR p1.lacqr_date_value   IS NOT NULL
                        OR p1.lacqr_number_value IS NOT NULL) THEN
--
                     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',199);
--
                    END IF;
--
                 END IF; -- p1.lacqr_boolean_value
--
                END IF; -- p1.lacqr_type = Y
--
-- ******
--
                IF (p1.lacqr_type = 'C') THEN
--
                 IF (p1.LACQR_CQRS_CODE IS NULL) THEN
                  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',200);
--
                 ELSE
--
                    IF (   p1.lacqr_boolean_value IS NOT NULL
                        OR p1.lacqr_text_value    IS NOT NULL
                        OR p1.lacqr_date_value    IS NOT NULL
                        OR p1.lacqr_number_value  IS NOT NULL) THEN
--
                     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',201);
--
                    END IF;
--
                    IF (p1.LACQR_CAQU_REFERENCE IS NOT NULL) THEN 
--
                     l_cqrs_exists := NULL;
--
                      OPEN chk_cqrs_exists(p1.lacqr_caqu_reference, p1.lacqr_cqrs_code);
                     FETCH chk_cqrs_exists INTO l_cqrs_exists;
                     CLOSE chk_cqrs_exists;
--
                     IF (l_cqrs_exists IS NULL) THEN
                      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',202);
                     END IF;
--
                    ELSE
--
                       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',203);
--
                    END IF;

                 END IF; -- p1.lacqr_cqrs_code
--
                END IF; -- p1.lacqr_type = C
--
-- ******
--
                IF (p1.lacqr_type = 'N') THEN
--
                 IF (   p1.lacqr_cqrs_code     IS NOT NULL
                     OR p1.lacqr_boolean_value IS NOT NULL) THEN
--
                     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',204);
--
                 END IF;
--
                 IF (l_caqu_data_type IS NOT NULL) THEN
--
                  IF (l_caqu_data_type = 'D') THEN
--
                   IF(p1.lacqr_date_value IS NULL) THEN
                     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',205);
--
                   ELSIF (   p1.lacqr_text_value   IS NOT NULL
                          OR p1.lacqr_number_value IS NOT NULL) THEN
--
                       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',206);
--
                   END IF;
--
                  END IF; -- l_caqu_data_type = 'D'
--
--
                 IF (l_caqu_data_type = 'N') THEN
--
                   IF(p1.lacqr_number_value IS NULL) THEN
                     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',207);
--
                   ELSIF (   p1.lacqr_text_value IS NOT NULL
                          OR p1.lacqr_date_value IS NOT NULL) THEN
--
                       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',208);
--
                   END IF;
--
                  END IF; -- l_caqu_data_type = 'N'
--
--
                 IF (l_caqu_data_type = 'T') THEN
--
                   IF(p1.lacqr_text_value IS NULL) THEN
                     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',209);
--
                   ELSIF (   p1.lacqr_number_value IS NOT NULL
                          OR p1.lacqr_date_value    IS NOT NULL) THEN
--
                       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',210);
--
                   END IF;
--
                  END IF; -- l_caqu_data_type = 'N'
--
--
                 END IF; -- l_caqu_data_type IS NOT NULL
                 
                END IF; -- p1.lacqr_type = N
--
--
             END IF; -- p1.lacqr_type NOT IN (C/N/Y)
--
--
          END IF; -- p1.lacqr_type IS NULL
--
-- ***********
--
-- Check Housing Options Reference LACQR_ACHO_LEGACY_REF is valid if supplied
--
--  
          IF (p1.lacqr_acho_legacy_ref IS NOT NULL) THEN
--
           l_acho_reference := NULL;
--
            OPEN chk_acho_exists(p1.lacqr_acho_legacy_ref);
           FETCH chk_acho_exists INTO l_acho_reference;
           CLOSE chk_acho_exists;
--
           IF (l_acho_reference IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',185);
           END IF;
--
          END IF;

--
-- ***********
--
-- There must not already be an entry on Advice Case Question Responses 
-- for the combination of Question Reference and Advice Case reference
-- or Housing Option Advice case
-- 
--
          IF (    l_acas_reference        IS NOT NULL
              AND p1.lacqr_caqu_reference IS NOT NULL) THEN
--
           l_acqr_exists := NULL;
--
            OPEN chk_acqr_exists (l_acas_reference, p1.lacqr_caqu_reference);
           FETCH chk_acqr_exists INTO l_acqr_exists;
           CLOSE chk_acqr_exists;
--
           IF (l_acqr_exists IS NOT NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',211);
           END IF;
-- 
          END IF;
--
--
-- ***********************************************************************
--
-- All reference values supplied are valid
-- 
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
          IF (l_errors = 'F') THEN
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
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
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
    fsc_utils.proc_END;
--
    COMMIT;
--
    EXCEPTION
         WHEN OTHERS THEN
         s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
-- 
END dataload_validate;
--
--
-- ***********************************************************************
--
PROCEDURE dataload_delete(p_batch_id       IN VARCHAR2,
                          p_date           IN date) IS
--
CURSOR c1 is
SELECT ROWID REC_ROWID,
       LACQR_DLB_BATCH_ID,
       LACQR_DL_SEQNO,
       LACQR_DL_LOAD_STATUS,
       LACQR_REFNO
  FROM dl_had_adv_case_questn_resp 
 WHERE lacqr_dlb_batch_id   = p_batch_id
   AND lacqr_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAD_ADV_CASE_QUESTN_RESP';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         VARCHAR2(1);
i                INTEGER :=0;
l_an_tab         VARCHAR2(1);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_had_adv_case_questn_resp.dataload_delete');
    fsc_utils.debug_message('s_dl_had_adv_case_questn_resp.dataload_delete',3 );
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lacqr_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i + 1;
--
--
-- Delete from advice_case_questn_responses table
--
          DELETE 
            FROM advice_case_questn_responses
           WHERE acqr_refno = p1.lacqr_refno;
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
--
          IF MOD(i,5000) = 0 THEN 
           COMMIT; 
          END IF;
--
          EXCEPTION
               WHEN OTHERS THEN
               ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
               set_record_status_flag(l_id,'C');
               s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_CASE_QUESTN_RESPONSES');
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
END s_dl_had_adv_case_questn_resp;
/

