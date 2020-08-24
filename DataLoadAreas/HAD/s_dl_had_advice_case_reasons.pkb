--
CREATE OR REPLACE PACKAGE BODY s_dl_had_advice_case_reasons
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    VS   15-JAN-2009  Initial Creation.
--
--  2.0     5.15.0    VS   11-DEC-2009  Defect 2897 Fix. Disable/Enable
--                                      ACRS_BR_I in CREATE Process
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
  UPDATE dl_had_advice_case_reasons
  SET    lacrs_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_had_advice_case_reasons');
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
       LACRS_DLB_BATCH_ID,
       LACRS_DL_SEQNO,
       LACRS_DL_LOAD_STATUS,
       LACRS_ACAS_ALTERNATE_REF,
       LACRS_ARSN_CODE,
       LACRS_MAIN_IND,
       LACRS_SCO_CODE,
       LACRS_STATUS_DATE,
       NVL(LACRS_CREATED_BY,'DATALOAD') LACRS_CREATED_BY,
       NVL(LACRS_CREATED_DATE, SYSDATE) LACRS_CREATED_DATE,
       LACRS_OUTCOME_COMMENTS,
       LACRS_PREV_SCO_CODE,
       LACRS_PREV_STATUS_DATE,
       LACRS_AUTHORISED_BY,
       LACRS_AUTHORISED_DATE,
       LAROC_OUTC_CODE,
       LAROC_PRIMARY_OUTCOME_IND,
       LAROC_CURRENT_IND,
       NVL(LAROC_CREATED_BY,'DATALOAD') LAROC_CREATED_BY,
       NVL(LAROC_CREATED_DATE,SYSDATE)  LAROC_CREATED_DATE,
       LAROC_SEQNO,
       LACRS_ARSS_HRV_ARST_CODE
  FROM dl_had_advice_case_reasons
 WHERE lacrs_dlb_batch_id   = p_batch_id
   AND lacrs_dl_load_status = 'V';
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
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HAD_ADVICE_CASE_REASONS';
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
    execute immediate 'alter trigger ACRS_BR_I disable';
--
    fsc_utils.proc_start('s_dl_had_advice_case_reasons.dataload_create');
    fsc_utils.debug_message('s_dl_had_advice_case_reasons.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lacrs_dl_seqno;
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
           OPEN get_acas_reference(p1.lacrs_acas_alternate_ref);
          FETCH get_acas_reference INTO l_acas_reference;
          CLOSE get_acas_reference;
--
--
-- Insert into relevent table
--
--
-- Insert into ADVICE_CASE_REASONS
--
--
          INSERT /* +APPEND */INTO advice_case_reasons(ACRS_ACAS_REFERENCE,
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
-- Section to anayze the table(s) populated by this dataload
--
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_CASE_REASONS');
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_CASES_OUTCOMES');
--
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
       LACRS_DLB_BATCH_ID,
       LACRS_DL_SEQNO,
       LACRS_DL_LOAD_STATUS,
       LACRS_ACAS_ALTERNATE_REF,
       LACRS_ARSN_CODE,
       LACRS_MAIN_IND,
       LACRS_SCO_CODE,
       LACRS_STATUS_DATE,
       NVL(LACRS_CREATED_BY,'DATALOAD') LACRS_CREATED_BY,
       NVL(LACRS_CREATED_DATE, SYSDATE) LACRS_CREATED_DATE,
       LACRS_OUTCOME_COMMENTS,
       LACRS_PREV_SCO_CODE,
       LACRS_PREV_STATUS_DATE,
       LACRS_AUTHORISED_BY,
       LACRS_AUTHORISED_DATE,
       LAROC_OUTC_CODE,
       LAROC_PRIMARY_OUTCOME_IND,
       LAROC_CURRENT_IND,
       NVL(LAROC_CREATED_BY,'DATALOAD') LAROC_CREATED_BY,
       NVL(LAROC_CREATED_DATE,SYSDATE)  LAROC_CREATED_DATE,
       LAROC_SEQNO,
       LACRS_ARSS_HRV_ARST_CODE
  FROM dl_had_advice_case_reasons
 WHERE lacrs_dlb_batch_id    = p_batch_id
   AND lacrs_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR chk_acas_exists(p_alternate_reference VARCHAR2) 
IS
SELECT acas_reference, acas_sco_code
  FROM advice_cases
 WHERE acas_alternate_reference = TO_CHAR(p_alternate_reference);
--
--
-- ***********************************************************************
--
CURSOR chk_acas_aun_exists(p_acas_aun_code VARCHAR2)
IS
SELECT 'X'
  FROM admin_units
 WHERE aun_code = p_acas_aun_code;
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
CURSOR chk_sco_exists(p_sco_code VARCHAR2) 
IS
SELECT 'X'
  FROM status_codes
 WHERE sco_code = p_sco_code;
--
--
-- ***********************************************************************
--
CURSOR chk_outc_exists(p_outc_code VARCHAR2) 
IS
SELECT 'X'
  FROM outcomes
 WHERE outc_code = p_outc_code;
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
ct       VARCHAR2(30) := 'DL_HAD_ADVICE_CASE_REASONS';
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
l_acas_sco_code         VARCHAR2(3);
l_acas_aun_exists       VARCHAR2(1);
l_arsn_exists         	VARCHAR2(1);
l_prev_sco_exists       VARCHAR2(1);
l_outc_exists           VARCHAR2(1);
l_acas_acrs_exists      VARCHAR2(1);
l_arss_hrv_arst_exists  VARCHAR2(1);
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
    fsc_utils.proc_start('s_dl_had_advice_case_reasons.dataload_validate');
    fsc_utils.debug_message('s_dl_had_advice_case_reasons.dataload_validate',3);
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
          cs   := p1.lacrs_dl_seqno;
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
-- Check Advice Case Alt Reference LACRS_ACAS_ALTERNATE_REF has been supplied 
-- and exists on advice_cases. Get advice case status code for use further on.
--
--  
          IF (p1.lacrs_acas_alternate_ref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',113);
          ELSE
--
             l_acas_reference := NULL;
             l_acas_sco_code  := NULL;
--
              OPEN chk_acas_exists(p1.lacrs_acas_alternate_ref);
             FETCH chk_acas_exists INTO l_acas_reference, l_acas_sco_code;
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
-- ***********
--
-- Check case reason status code LACRS_SCO_CODE has been supplied and is valid
--
-- If the Advice Case Reason status is CLO then the Advice Case must also have a status of CLO.
--
--
          IF (p1.lacrs_sco_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',130);
--
          ELSIF (p1.lacrs_sco_code NOT IN ('CUR','PEN','CLO')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',131);
--
          ELSIF (    p1.lacrs_sco_code  = 'CLO'
                 AND l_acas_sco_code   != 'CLO') THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',132);
--
          END IF;
--
-- ***********
--
-- Check Advice Case Reason Status date LACRS_STATUS_DATE has been supplied
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
-- Check Outcome Code LAROC_OUTC_CODE is valid if supplied
-- 
--  
          IF (p1.laroc_outc_code IS NOT NULL) THEN
--
           l_outc_exists := NULL;
--
            OPEN chk_outc_exists (p1.laroc_outc_code);
           FETCH chk_outc_exists INTO l_outc_exists;
           CLOSE chk_outc_exists;
--
           IF (l_outc_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',145);
           END IF;
-- 
          END IF;
--
-- ***********
--
-- Check Outcome Current Indicator LAROC_CURRENT_IND is valid if supplied
--
--
          IF (p1.laroc_current_ind IS NOT NULL) THEN
--
           IF (p1.laroc_current_ind NOT IN ('Y','N')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',146);
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
       LACRS_DLB_BATCH_ID,
       LACRS_DL_SEQNO,
       LACRS_DL_LOAD_STATUS,
       LACRS_ACAS_ALTERNATE_REF,
       LACRS_ARSN_CODE,
       LAROC_OUTC_CODE
  FROM dl_had_advice_case_reasons
 WHERE lacrs_dlb_batch_id   = p_batch_id
   AND lacrs_dl_load_status = 'C';
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
ct       VARCHAR2(30) := 'DL_HAD_ADVICE_CASE_REASONS';
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
    fsc_utils.proc_start('s_dl_had_advice_case_reasons.dataload_delete');
    fsc_utils.debug_message('s_dl_had_advice_case_reasons.dataload_delete',3 );
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
          cs   := p1.lacrs_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i + 1;
--
-- Get acas_reference
--
          l_acas_reference := NULL;
--
           OPEN get_acas_reference(p1.lacrs_acas_alternate_ref);
          FETCH get_acas_reference INTO l_acas_reference;
          CLOSE get_acas_reference;
--
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_CASE_REASONS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_REASON_OUTCOME');
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
END s_dl_had_advice_case_reasons;
/

