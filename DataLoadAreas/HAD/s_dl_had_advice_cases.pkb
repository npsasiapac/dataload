--
CREATE OR REPLACE PACKAGE BODY s_dl_had_advice_cases
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    VS   13-JAN-2009  Initial Creation.
--
--  2.0     5.15.0    VS   11-DEC-2009  Defect 2897 Fix. Disable/Enable
--                                      ACAS_BR_I, ACRS_BR_I in CREATE Process
--
--  3.0     5.15.0    VS   17-FEB-2010  Add TO_CHAR to cursors to make sure
--                                      indexes are used correctly
--                                      Changed commit 500000 to 50000
--  4.0     6.11      AJ   18-DEC-2015  added LACRS_ARSS_HRV_ARST_CODE which is the
--                                      stage the advice case reason is at updated
--                                      create and validate sections
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
  UPDATE dl_had_advice_cases
  SET    lacas_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_had_advice_cases');
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
       LACAS_DLB_BATCH_ID,
       LACAS_DL_SEQNO,
       LACAS_DL_LOAD_STATUS,
       LACAS_ALTERNATE_REF,
       LACAS_APPROACH_DATE,
       LACAS_SCO_CODE,
       LACAS_STATUS_DATE,
       LACAS_CORRESPONDENCE_NAME,
       NVL(LACAS_CREATED_BY,'DATALOAD') LACAS_CREATED_BY,
       NVL(LACAS_CREATED_DATE,SYSDATE)  LACAS_CREATED_DATE,
       LACAS_HOMELESS_IND,
       LACAS_HRV_ACAM_CODE,
       LACAS_HRV_CWTP_CODE,
       LACAS_HRV_ACSP_CODE,
       LACAS_EXPECTED_HOMELESS_DATE,
       LACAS_START_TIME_AT_RECEPTION,
       LACAS_END_TIME_AT_RECEPTION,
       LACAS_CASE_OPENED_DATE,
       LACAS_PREV_SCO_CODE,
       LACAS_PREV_STATUS_DATE,
       LACAS_COMMENTS,
       LACAS_AUN_CODE,
       LACRS_ARSN_CODE,
       LACRS_MAIN_IND,
       LACRS_SCO_CODE,
       LACRS_STATUS_DATE,
       NVL(LACRS_CREATED_BY,'DATALOAD') LACRS_CREATED_BY,
       NVL(LACRS_CREATED_DATE,SYSDATE)  LACRS_CREATED_DATE,
       LACRS_OUTCOME_COMMENTS,
       LACRS_PREV_SCO_CODE,
       LACRS_PREV_STATUS_DATE,
       LACRS_AUTHORISED_BY,
       LACRS_AUTHORISED_DATE,
       LACRS_ARSS_HRV_ARST_CODE
  FROM dl_had_advice_cases
 WHERE lacas_dlb_batch_id   = p_batch_id
   AND lacas_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HAD_ADVICE_CASES';
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
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger ACAS_BR_I disable';
    execute immediate 'alter trigger ACRS_BR_I disable';
--
    fsc_utils.proc_start('s_dl_had_advice_cases.dataload_create');
    fsc_utils.debug_message('s_dl_had_advice_cases.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lacas_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
--
-- Get acas_reference
--
          l_acas_reference := fsc_utils.f_dynamic_value('acas_reference_seq.NEXTVAL');
--
--
-- Insert into relevent table
--
          INSERT /* +APPEND */ INTO advice_cases(ACAS_REFERENCE,
                                   ACAS_APPROACH_DATE,
                                   ACAS_SCO_CODE,
                                   ACAS_STATUS_DATE,
                                   ACAS_CORRESPONDENCE_NAME,
                                   ACAS_CREATED_BY,
                                   ACAS_CREATED_DATE,
                                   ACAS_HOMELESS_IND,
                                   ACAS_REUSABLE_REFNO,
                                   ACAS_HRV_ACAM_CODE,
                                   ACAS_HRV_CWTP_CODE,
                                   ACAS_HRV_ACSP_CODE,
                                   ACAS_EXPECTED_HOMELESS_DATE,
                                   ACAS_ALTERNATE_REFERENCE,
                                   ACAS_START_TIME_AT_RECEPTION,
                                   ACAS_END_TIME_AT_RECEPTION,
                                   ACAS_CASE_OPENED_DATE,
                                   ACAS_PREV_SCO_CODE,
                                   ACAS_PREV_STATUS_DATE,
                                   ACAS_COMMENTS,
                                   ACAS_AUN_CODE 
                                  )
--
                            VALUES(l_acas_reference,
                                   p1.LACAS_APPROACH_DATE,
                                   p1.LACAS_SCO_CODE,
                                   p1.LACAS_STATUS_DATE,
                                   p1.LACAS_CORRESPONDENCE_NAME,
                                   p1.LACAS_CREATED_BY,
                                   p1.LACAS_CREATED_DATE,
                                   p1.LACAS_HOMELESS_IND,
                                   reusable_refno_seq.nextval,
                                   p1.LACAS_HRV_ACAM_CODE,
                                   p1.LACAS_HRV_CWTP_CODE,
                                   p1.LACAS_HRV_ACSP_CODE,
                                   p1.LACAS_EXPECTED_HOMELESS_DATE,
                                   p1.LACAS_ALTERNATE_REF,
                                   p1.LACAS_START_TIME_AT_RECEPTION,
                                   p1.LACAS_END_TIME_AT_RECEPTION,
                                   p1.LACAS_CASE_OPENED_DATE,
                                   p1.LACAS_PREV_SCO_CODE,
                                   p1.LACAS_PREV_STATUS_DATE,
                                   p1.LACAS_COMMENTS,
                                   p1.LACAS_AUN_CODE
                                  );
--
--
-- Insert into ADVICE_CASE_REASONS
--
--
          INSERT /* +APPEND */ INTO advice_case_reasons(ACRS_ACAS_REFERENCE,
                                          ACRS_ARSN_CODE,
                                          ACRS_MAIN_IND,
                                          ACRS_SCO_CODE,
                                          ACRS_STATUS_DATE,
                                          ACRS_CREATED_BY,
                                          ACRS_CREATED_DATE,
                                          ACRS_OUTCOME_COMMENTS,
                                          ACRS_PREV_SCO_CODE,
                                          ACRS_PREV_STATUS_DATE,
                                          ACRS_AUTHORISED_BY,
                                          ACRS_AUTHORISED_DATE,
                                          ACRS_ARSS_HRV_ARST_CODE
                                         )
--
                                  VALUES (l_acas_reference,
                                          p1.LACRS_ARSN_CODE,
                                          p1.LACRS_MAIN_IND,
                                          p1.LACRS_SCO_CODE,
                                          p1.LACRS_STATUS_DATE,
                                          p1.LACRS_CREATED_BY,
                                          p1.LACRS_CREATED_DATE,
                                          p1.LACRS_OUTCOME_COMMENTS,
                                          p1.LACRS_PREV_SCO_CODE,
                                          p1.LACRS_PREV_STATUS_DATE,
                                          p1.LACRS_AUTHORISED_BY,
                                          p1.LACRS_AUTHORISED_DATE,
                                          p1.LACRS_ARSS_HRV_ARST_CODE
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
-- Section to analyse the table(s) populated by this dataload
--
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_CASES');
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_CASE_REASONS');
--
    execute immediate 'alter trigger ACAS_BR_I enable';
    execute immediate 'alter trigger ACRS_BR_I enable';
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
       LACAS_DLB_BATCH_ID,
       LACAS_DL_SEQNO,
       LACAS_DL_LOAD_STATUS,
       LACAS_ALTERNATE_REF,
       LACAS_APPROACH_DATE,
       LACAS_SCO_CODE,
       LACAS_STATUS_DATE,
       LACAS_CORRESPONDENCE_NAME,
       LACAS_CREATED_BY,
       LACAS_CREATED_DATE,
       LACAS_HOMELESS_IND,
       LACAS_HRV_ACAM_CODE,
       LACAS_HRV_CWTP_CODE,
       LACAS_HRV_ACSP_CODE,
       LACAS_EXPECTED_HOMELESS_DATE,
       LACAS_START_TIME_AT_RECEPTION,
       LACAS_END_TIME_AT_RECEPTION,
       LACAS_CASE_OPENED_DATE,
       LACAS_PREV_SCO_CODE,
       LACAS_PREV_STATUS_DATE,
       LACAS_COMMENTS,
       LACAS_AUN_CODE,
       LACRS_ARSN_CODE,
       LACRS_MAIN_IND,
       LACRS_SCO_CODE,
       LACRS_STATUS_DATE,
       LACRS_CREATED_BY,
       LACRS_CREATED_DATE,
       LACRS_OUTCOME_COMMENTS,
       LACRS_PREV_SCO_CODE,
       LACRS_PREV_STATUS_DATE,
       LACRS_AUTHORISED_BY,
       LACRS_AUTHORISED_DATE,
       LACRS_ARSS_HRV_ARST_CODE
  FROM dl_had_advice_cases 
 WHERE lacas_dlb_batch_id    = p_batch_id
   AND lacas_dl_load_status in ('L','F','O');
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
CURSOR get_acas_aun_type
IS
SELECT TRIM(pva.pva_char_value)
  FROM parameter_values            pva,
       area_codes                  arc,
       parameter_definition_usages pdu
 WHERE pdu.pdu_pdf_param_type  = 'SYSTEM'
   AND arc.arc_pgp_refno       = pdu.pdu_pgp_refno
   AND pdu.pdu_pdf_name        = pva.pva_pdu_pdf_name
   AND pdu.pdu_pdf_param_type  = pva.pva_pdu_pdf_param_type
   AND pdu.pdu_pob_table_name  = pva.pva_pdu_pob_table_name
   AND pdu.pdu_pgp_refno       = pva.pva_pdu_pgp_refno
   AND pdu.pdu_display_seqno   = pva.pva_pdu_display_seqno
   AND pdu.pdu_pdf_name        = 'ADVCASE_AUN_TYPE';
--
--
-- ***********************************************************************
--
CURSOR chk_acas_aun_exists(p_acas_aun_code VARCHAR2,
                           p_acas_aun_type VARCHAR2)
IS
SELECT 'X'
  FROM admin_units
 WHERE aun_code     = p_acas_aun_code
   and aun_auy_code = p_acas_aun_type;
--
--
-- ***********************************************************************
--
CURSOR chk_arsn_exists(p_arsn_code VARCHAR2) 
IS
SELECT 'X'
  FROM advice_reasons
 WHERE arsn_code = p_arsn_code;
--
--
-- ***********************************************************************
--
CURSOR chk_acas_acrs_exists(p_acas_reference NUMBER, 
                            p_arsn_code      VARCHAR2)
IS
SELECT 'X'
  FROM advice_case_reasons
 WHERE acrs_acas_reference = p_acas_reference
   AND acrs_arsn_code      = p_arsn_code;
--
--
-- ***********************************************************************
--
CURSOR chk_sco_exists (p_sco_code      VARCHAR2)
IS
SELECT 'X'
  FROM status_codes
 WHERE sco_code = p_sco_code;
--
--
--
-- ***********************************************************************
--
CURSOR chk_arrs_arst_exists(p_arsn_code      VARCHAR2, 
                            p_arst_code      VARCHAR2)
IS
SELECT 'X'
  FROM advice_reason_stages
 WHERE arss_arsn_code      = p_arsn_code
   AND arss_hrv_arst_code  = p_arst_code;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HAD_ADVICE_CASES';
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
l_acas_aun_type         VARCHAR2(255);
l_acas_aun_exists       VARCHAR2(1);
l_arsn_exists         	VARCHAR2(1);
l_acas_acrs_exists      VARCHAR2(1);
l_arss_hrv_arst_exists  VARCHAR2(1);
--
l_errors         	VARCHAR2(10);
l_error_ind      	VARCHAR2(10);
i                	INTEGER :=0;
l_prev_sco_exists       VARCHAR2(10);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_had_advice_cases.dataload_validate');
    fsc_utils.debug_message('s_dl_had_advice_cases.dataload_validate',3);
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
          cs   := p1.lacas_dl_seqno;
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
-- Check Advice Case Alt Reference LACAS_ALTERNATE_REFERENCE has been supplied 
-- and doesn't already exists.
--
--  
          IF (p1.lacas_alternate_ref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',113);
          ELSE
--
             l_acas_reference := NULL;
--
              OPEN chk_acas_exists(p1.lacas_alternate_ref);
             FETCH chk_acas_exists INTO l_acas_reference;
             CLOSE chk_acas_exists;
--
             IF (l_acas_reference IS NOT NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',114);
             END IF;
--            
          END IF;
--
-- ***********
--
-- Check Approach date LACAS_APPROACH_DATE has been supplied
-- 
--  
          IF (p1.lacas_approach_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',115);
          END IF;
--
-- ***********
--
-- Check case status code LACAS_SCO_CODE has been supplied and is valid
--
-- If the Advice Case status is CLO then the Advice Case Reason must also have a status of CLO.
--
-- If case status code is 'OPN' then case opened date LACAS_CASE_OPENED_DATE must be supplied
--
-- If case status code is 'HLD' then previous status code LACAS_PREV_STATUS_CODE and 
-- previous status date LACAS_PREV_STATUS_DATE must be supplied
--
          IF (p1.lacas_sco_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',116);
--
          ELSIF (p1.lacas_sco_code NOT IN ('RAI','OPN','HLD','CLO')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',117);
--
          ELSIF (    p1.lacas_sco_code  = 'CLO'
                 AND p1.lacrs_sco_code != 'CLO') THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',118);
--
          ELSIF (    p1.lacas_sco_code  = 'OPN'
                 AND p1.lacas_case_opened_date IS NULL) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',119);
--
          ELSIF (    p1.lacas_sco_code  = 'HLD'
                 AND (   p1.lacas_prev_sco_code    IS NULL
                      OR p1.lacas_prev_status_date IS NULL)) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',120);
--     
          END IF;
--
-- ***********
--
-- Check Case Status date LACAS_STATUS_DATE has been supplied
--
-- 
         IF (p1.lacas_status_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',121);
         END IF;
--
-- ***********
--
-- Check Case Correspondance Name LACAS_CORRESPONDENCE_NAME has been supplied
--
-- 
         IF (p1.lacas_correspondence_name IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',122);
         END IF;
--
-- ***********
--
-- Check homeless Indicator LACAS_HOMELESS_IND is supplied and valid
-- 
-- If the Homeless Indicator is set to N then the expected homeless date 
-- LACAS_EXPECTED_HOMELESS_DATE must not be supplied. 
--
--
          IF (p1.lacas_homeless_ind IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',123);
--
          ELSIF (p1.lacas_homeless_ind NOT IN ('Y','N')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',124);
--
          END IF;
--
          IF (    p1.lacas_homeless_ind           = 'N'
              AND p1.lacas_expected_homeless_date IS NOT NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',125);
--
          END IF;
--
-- ***********
--
-- Check Advice Case Previous Status Code LACAS_PREV_SCO_CODE is valid if supplied
-- 
--  
          IF (p1.lacas_prev_sco_code IS NOT NULL) THEN
--
           l_prev_sco_exists := NULL;
--
            OPEN chk_sco_exists (p1.lacas_prev_sco_code);
           FETCH chk_sco_exists INTO l_prev_sco_exists;
           CLOSE chk_sco_exists;
--
           IF (l_prev_sco_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',141);
           END IF;
-- 
          END IF;
--
-- ***********
--
-- Check Case Admin Unit LACAS_AUN_CODE is supplied and valid
-- 
-- The admin unit code supplied must be a current valid Admin Unit and 
-- be of the type matching the parameter ‘ADVCASE_AUN_TYPE’ – Admin Unit Type 
-- for Advice Cases.
--
          IF (p1.lacas_aun_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',126);
--
          ELSE
--
             l_acas_aun_type := NULL;
--
              OPEN get_acas_aun_type;
             FETCH get_acas_aun_type INTO l_acas_aun_type;
             CLOSE get_acas_aun_type;
--
             IF (l_acas_aun_type IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',143);
-- 
             ELSE
--
                l_acas_aun_exists := NULL;
--
                 OPEN chk_acas_aun_exists (p1.lacas_aun_code, l_acas_aun_type);
                FETCH chk_acas_aun_exists INTO l_acas_aun_exists;
                CLOSE chk_acas_aun_exists;
--
                IF (l_acas_aun_exists IS NULL) THEN
                 l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',127);
                END IF;
--
             END IF; 
--
          END IF;
--
-- ***********
--
-- Check Advice Case Reason Code LACRS_ARSN_CODE is supplied and valid
-- 
--
          IF (p1.lacrs_arsn_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',128);
--
          ELSE
--
             l_arsn_exists := NULL;
--
              OPEN chk_arsn_exists (p1.lacrs_arsn_code);
             FETCH chk_arsn_exists INTO l_arsn_exists;
             CLOSE chk_arsn_exists;
--
             IF (l_arsn_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',129);
             END IF;
-- 
          END IF;
--
-- ***********
--
-- Check case reason status code LACRS_SCO_CODE has been supplied and is valid
--
-- If the Advice Reason Case status is CLO then the Advice Case must also have a status of CLO.
--
--
          IF (p1.lacrs_sco_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',130);
--
          ELSIF (p1.lacrs_sco_code NOT IN ('CUR','PEN','CLO')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',131);
--
          ELSIF (    p1.lacrs_sco_code  = 'CLO'
                 AND p1.lacas_sco_code != 'CLO') THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',132);
--        
          END IF;
--
--
-- ***********
--
-- Check Advice Case Reason Status LACRS_STATUS_DATE is supplied
-- 
--
          IF (p1.lacrs_status_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',133);
          END IF;
--
-- ***********
--
-- Check Previous Status Code LACRS_PREV_SCO_CODE is valid if supplied
-- 
--  
          IF (p1.lacrs_prev_sco_code IS NOT NULL) THEN
--
           l_prev_sco_exists := NULL;
--
            OPEN chk_sco_exists (p1.lacrs_prev_sco_code);
           FETCH chk_sco_exists INTO l_prev_sco_exists;
           CLOSE chk_sco_exists;
--
           IF (l_prev_sco_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',142);
           END IF;
-- 
          END IF;
--
-- ***********
--
-- The combination of Advice Case and Reason must not already exist on 
-- Advice Case Reason Table
-- 
--
          IF (    l_acas_reference IS NOT NULL
              AND l_arsn_exists    IS NOT NULL) THEN
--
           l_acas_acrs_exists := NULL;
--
            OPEN chk_acas_acrs_exists (l_acas_reference, p1.lacrs_arsn_code);
           FETCH chk_acas_acrs_exists INTO l_acas_acrs_exists;
           CLOSE chk_acas_acrs_exists;
--
           IF (l_acas_acrs_exists IS NOT NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',134);
           END IF;
-- 
          END IF;
--
-- ***********
--
-- Check Main Reason Indicator LACRS_MAIN_IND is supplied and valid
--
--
          IF (p1.lacrs_main_ind IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',135);
--
          ELSIF (p1.lacrs_main_ind NOT IN ('Y','N')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',136);
--
          END IF;
--
--
-- ***********************************************************************
--
-- All reference values supplied are valid
-- 
-- Case Approach Method
--
          IF (p1.lacas_hrv_acam_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',137);
--
          ELSIF (NOT s_dl_hem_utils.exists_frv('ADV_CASE_APPR_METH',p1.lacas_hrv_acam_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',138);
--
          END IF;
--
-- ***********
--
-- Casework Type
--
          IF (    p1.lacas_hrv_cwtp_code IS NOT NULL) 
            THEN
              IF (NOT s_dl_hem_utils.exists_frv('ADV_CASE_CASEWK_TYPE',p1.lacas_hrv_cwtp_code,'Y'))
              THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',139);
--
            END IF ;
--
          END IF;
--
-- ***********
--
-- Case Priority
--
          IF (    p1.lacas_hrv_acsp_code IS NOT NULL) THEN
              IF (NOT s_dl_hem_utils.exists_frv('ADV_CS_PRIORITY',p1.lacas_hrv_acsp_code,'Y')) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',140);
--
              END IF;
--
          END IF;
--
-- ***********
--
-- Check Advice Case Reason Stages Code LACRS_ARSS_HRV_ARST_CODE if supplied
-- is valid for the advice case reason LACRS_ARSN_CODE supplied 
-- 
          IF (p1.lacrs_arss_hrv_arst_code IS NOT NULL) THEN
--
             l_arss_hrv_arst_exists := NULL;
--
              OPEN chk_arrs_arst_exists (p1.lacrs_arsn_code, p1.lacrs_arss_hrv_arst_code);
             FETCH chk_arrs_arst_exists INTO l_arss_hrv_arst_exists;
             CLOSE chk_arrs_arst_exists;
--
             IF (l_arss_hrv_arst_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',817);
             END IF;
-- 
          END IF;
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
       LACAS_DLB_BATCH_ID,
       LACAS_DL_SEQNO,
       LACAS_DL_LOAD_STATUS,
       LACAS_ALTERNATE_REF,
       LACRS_ARSN_CODE
  FROM dl_had_advice_cases
 WHERE lacas_dlb_batch_id   = p_batch_id
   AND lacas_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR get_acas_reference(p_acas_alt_reference VARCHAR2)
IS
SELECT acas_reference
  FROM advice_cases
 WHERE acas_alternate_reference = TO_CHAR(p_acas_alt_reference);
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAD_ADVICE_CASES';
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
l_acas_reference NUMBER(10);
i                INTEGER :=0;
l_an_tab             VARCHAR2(1);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_had_advice_cases.dataload_delete');
    fsc_utils.debug_message('s_dl_had_advice_cases.dataload_delete',3 );
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
          cs   := p1.lacas_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i + 1;
--
-- Get acas_reference
--
          l_acas_reference := NULL;
--
           OPEN get_acas_reference(p1.lacas_alternate_ref);
          FETCH get_acas_reference INTO l_acas_reference;
          CLOSE get_acas_reference;
--
--
-- Delete from advice_case_reasons table
--
          DELETE 
            FROM advice_case_reasons
           WHERE acrs_acas_reference = l_acas_reference
             AND acrs_arsn_code      = p1.lacrs_arsn_code;
--
--
-- Delete from advice_cases table
--
          DELETE 
            FROM advice_cases
           WHERE acas_reference = l_acas_reference;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_CASES');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_CASE_REASONS');
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
END s_dl_had_advice_cases;
/

