--
CREATE OR REPLACE PACKAGE BODY s_dl_had_adv_rsn_casewrk_evts
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    VS   04-FEB-2009  Initial Creation.
--
--  2.0     5.15.0    VS   21-APR-2009  Tidy up the Validation Process.
--                                      Especially chk_arce_exists
--
--  2.1     5.15.0    VS   29-APR-2009  Additional Validation To overcome
--                                      Trigger failure ARCE_BR_IU. Advice
--                                      case approach date cannot be > 
--                                      event_datetime.
--                                      
--                                      Also validate the duration for
--                                      format and not > 23:59
--
--  3.0     5.15.0    VS   11-DEC-2009  Defect 2897 Fix. Disable/Enable
--                                      ARCE_BR_I in CREATE Process
--
--  4.0     5.15.0    VS   08-JAN-2010  Defect 3130 Fix. larce_acas_alternate_ref
--                                      mandatory check.
--
--  5.0     5.15.0    VS   17-FEB-2010  Defect 3523 Fix. larce_acas_alternate_ref
--                                      Must be supplied in addition to 
--                                      Advice Case Housing Option Legacy Ref 
--                                      (larce_acho_reference) for events to be visible
--                                      from the front-end. Remove Validation 267. Added 
--                                      TO_CHAR for index performance change
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
  UPDATE dl_had_adv_rsn_casewrk_evts
  SET    larce_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_had_adv_rsn_casewrk_evts');
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
       LARCE_DLB_BATCH_ID,
       LARCE_DL_SEQNO,
       LARCE_DL_LOAD_STATUS,
       LARCE_ACAS_ALTERNATE_REF,
       LARCE_ACRS_ARSN_CODE,
       LARCE_ACET_CODE,
       LARCE_EVENT_DATETIME,
       LARCE_TEXT,
       LARCE_EVENT_DIRECTION_IND,
       LARCE_CLIENT_INVOLVEMENT_IND,
       LARCE_DIRECT_INTERVENTION_IND,
       NVL(lARCE_CREATED_BY, 'DATALOAD') LARCE_CREATED_BY,
       NVL(lARCE_CREATED_DATE, SYSDATE)  LARCE_CREATED_DATE,
       LARCE_DURATION,
       LARCE_REVIEW_DATE,
       LARCE_ACHO_REFERENCE,
       LARCE_AUN_CODE,
       LARCE_REFNO
  FROM dl_had_adv_rsn_casewrk_evts
 WHERE larce_dlb_batch_id   = p_batch_id
   AND larce_dl_load_status = 'V';
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
ct                   VARCHAR2(30) := 'DL_HAD_ADV_RSN_CASEWRK_EVTS';
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
    execute immediate 'alter trigger ARCE_BR_I disable';
--
    fsc_utils.proc_start('s_dl_had_adv_rsn_casewrk_evts.dataload_create');
    fsc_utils.debug_message('s_dl_had_adv_rsn_casewrk_evts.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.larce_dl_seqno;
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
          IF (p1.larce_acas_alternate_ref IS NOT NULL) THEN
--
            OPEN get_acas_reference(p1.larce_acas_alternate_ref);
           FETCH get_acas_reference INTO l_acas_reference;
           CLOSE get_acas_reference;
--
          END IF;
--
-- Get acho_reference
--
          l_acho_reference := NULL;
--
          IF (p1.larce_acho_reference IS NOT NULL) THEN
--
            OPEN get_acho_reference(p1.larce_acho_reference);
           FETCH get_acho_reference INTO l_acho_reference;
           CLOSE get_acho_reference;
--
          END IF;
--
--
-- Insert into relevent table
--
--
-- Insert into ADVICE_REASON_CASEWORK_EVENTS
--
--
          INSERT /* +APPEND */ INTO advice_reason_casework_events(ARCE_REFNO,
                                                    ARCE_ACRS_ACAS_REFERENCE,
                                                    ARCE_ACRS_ARSN_CODE,
                                                    ARCE_ACET_CODE,
                                                    ARCE_EVENT_DATETIME,
                                                    ARCE_TEXT,
                                                    ARCE_EVENT_DIRECTION_IND,
                                                    ARCE_CLIENT_INVOLVEMENT_IND,
                                                    ARCE_DIRECT_INTERVENTION_IND,
                                                    ARCE_CREATED_BY,
                                                    ARCE_CREATED_DATE,
                                                    ARCE_DURATION,
                                                    ARCE_REVIEW_DATE,
                                                    ARCE_ACHO_REFERENCE,
                                                    ARCE_AUN_CODE
                                                   )
--
                                            VALUES (p1.larce_refno,
                                                    l_acas_reference,
                                                    p1.larce_acrs_arsn_code,
                                                    p1.larce_acet_CODE,
                                                    p1.larce_event_datetime,
                                                    p1.larce_text,
                                                    p1.larce_event_direction_ind,
                                                    p1.larce_client_involvement_ind,
                                                    p1.larce_direct_intervention_ind,
                                                    p1.larce_created_by,
                                                    p1.larce_created_date,
                                                    p1.larce_duration,
                                                    p1.larce_review_date,
                                                    l_acho_reference,
                                                    p1.larce_aun_code
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_REASON_CASEWORK_EVENTS');
--
    execute immediate 'alter trigger ARCE_BR_I enable';
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
       LARCE_DLB_BATCH_ID,
       LARCE_DL_SEQNO,
       LARCE_DL_LOAD_STATUS,
       LARCE_ACAS_ALTERNATE_REF,
       LARCE_ACRS_ARSN_CODE,
       LARCE_ACET_CODE,
       LARCE_EVENT_DATETIME,
       LARCE_TEXT,
       LARCE_EVENT_DIRECTION_IND,
       LARCE_CLIENT_INVOLVEMENT_IND,
       LARCE_DIRECT_INTERVENTION_IND,
       NVL(lARCE_CREATED_BY, 'DATALOAD') LARCE_CREATED_BY,
       NVL(lARCE_CREATED_DATE, SYSDATE)  LARCE_CREATED_DATE,
       LARCE_DURATION,
       LARCE_REVIEW_DATE,
       LARCE_ACHO_REFERENCE,
       LARCE_AUN_CODE,
       LARCE_REFNO
  FROM dl_had_adv_rsn_casewrk_evts
 WHERE larce_dlb_batch_id    = p_batch_id
   AND larce_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR chk_acas_exists(p_alternate_reference VARCHAR2) 
IS
SELECT acas_reference, acas_approach_date
  FROM advice_cases
 WHERE acas_alternate_reference = TO_CHAR(p_alternate_reference);
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
CURSOR chk_acet_exists(p_acet_code VARCHAR2) 
IS
SELECT 'X'
  FROM advice_casework_event_types
 WHERE acet_code = p_acet_code;
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
CURSOR chk_aun_exists(p_aun_code VARCHAR2) 
IS
SELECT 'X'
  FROM admin_units
 WHERE aun_code = p_aun_code;
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
CURSOR chk_arce_exists(p_acas_reference NUMBER, 
                       p_arsn_code      VARCHAR2,
                       p_acho_reference NUMBER,
                       p_acet_code      VARCHAR2,
                       p_event_datetime DATE)
IS
SELECT 'X'
  FROM advice_reason_casework_events
 WHERE ARCE_ACRS_ACAS_REFERENCE = p_acas_reference
   AND ARCE_ACRS_ARSN_CODE      = p_arsn_code
   AND ARCE_ACHO_REFERENCE      = p_acho_reference
   AND ARCE_ACET_CODE           = p_acet_code
   AND ARCE_EVENT_DATETIME      = p_event_datetime;
--
--
-- ***********************************************************************
--
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HAD_ADV_RSN_CASEWRK_EVTS';
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
l_acas_approach_date    DATE;
l_acho_reference       	NUMBER(10);
l_arsn_exists           VARCHAR2(1);
l_acet_exists           VARCHAR2(1);
l_acas_acrs_exists      VARCHAR2(1);
l_arce_aun_exists       VARCHAR2(1);
l_arce_exists           VARCHAR2(1);
--
l_errors         	VARCHAR2(10);
l_error_ind      	VARCHAR2(10);
i                	INTEGER :=0;
--
l_fmt_fail              VARCHAR2(1);
--
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_had_adv_rsn_casewrk_evts.dataload_validate');
    fsc_utils.debug_message('s_dl_had_adv_rsn_casewrk_evts.dataload_validate',3);
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
          cs   := p1.larce_dl_seqno;
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
--
-- Check Advice Case Alt Reference LARCE_ACAS_ALTERNATE_REF is supplied
-- and exists on advice_cases table if supplied. Get the approach date 
-- at this time for a check later on.
--
--  
          l_acas_reference     := NULL;
          l_acas_approach_date := NULL;
--
          IF (p1.larce_acas_alternate_ref IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',113);
--
          ELSE

              OPEN chk_acas_exists(p1.larce_acas_alternate_ref);
             FETCH chk_acas_exists INTO l_acas_reference, l_acas_approach_date;
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
-- Check Advice Case Reason Code LARCE_ACRS_ARSN_CODE is valid if supplied
-- 
--
--
          l_arsn_exists := NULL;

          IF (p1.larce_acrs_arsn_code IS NOT NULL) THEN
--
            OPEN chk_arsn_exists (p1.larce_acrs_arsn_code);
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
-- Check Case Event Type Code LARCE_ACET_CODE is supplied and valid
-- 
--
--
          l_acet_exists := NULL;
--
          IF (p1.larce_acet_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',176);
--
          ELSE
--
              OPEN chk_acet_exists (p1.larce_acet_code);
             FETCH chk_acet_exists INTO l_acet_exists;
             CLOSE chk_acet_exists;
--
             IF (l_acet_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',177);
             END IF;
-- 
          END IF;
--
-- ***********
--
-- Check Event Date Time LARCE_EVENT_DATETIME is supplied
-- 
--
          IF (p1.larce_event_datetime IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',178);
          END IF;
--
-- ***********
--
-- Check Event Direction Indicator LARCE_EVENT_DIRECTION_IND is supplied and valid
-- 
--
          IF (p1.larce_event_direction_ind IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',179);
--
          ELSIF (p1.larce_event_direction_ind NOT IN ('I','O','N')) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',180);
-- 
          END IF;

--
-- ***********
--
-- Check Client Involvement Indicator LARCE_CLIENT_INVOLEMENT_IND is supplied 
-- and valid
--
--
          IF (p1.larce_client_involvement_ind IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',181);
--
          ELSIF (p1.larce_client_involvement_ind NOT IN ('Y','N')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',182);
--
          END IF;
--
--
-- ***********
--
-- Check Direct Intervention Indicator LARCE_DIRECT_INTERVENTION_IND is supplied 
-- and valid
-- LARCE_DIRECT_INTERVENTION_IND,
--
          IF (p1.larce_direct_intervention_ind IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',183);
--
          ELSIF (p1.larce_direct_intervention_ind NOT IN ('Y','N')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',184);
--
          END IF;
--
-- ***********
--
-- Check Housing Option Advice Case Reference LARCE_ACHO_REFERENCE is valid 
-- if supplied
--
--
          l_acho_reference := NULL;
-- 
          IF (p1.larce_acho_reference IS NOT NULL) THEN
--
            OPEN chk_acho_exists(p1.larce_acho_reference);
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
-- Check Admin Unit LARCE_AUN_CODE is valid if supplied
-- 
--
          l_arce_aun_exists := NULL;
--
          IF (p1.larce_aun_code IS NOT NULL) THEN
--
            OPEN chk_aun_exists (p1.larce_aun_code);
           FETCH chk_aun_exists INTO l_arce_aun_exists;
           CLOSE chk_aun_exists;
--
           IF (l_arce_aun_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',127);
           END IF;
--
          END IF;
--
-- ***********
--
-- The combination of Advice Case and Reason must exist on 
-- Advice Case Reason Table
-- 
--
          IF (    l_acas_reference IS NOT NULL
              AND l_arsn_exists    IS NOT NULL) THEN
--
           l_acas_acrs_exists := NULL;
--
            OPEN chk_acas_acrs_exists (l_acas_reference, p1.larce_acrs_arsn_code);
           FETCH chk_acas_acrs_exists INTO l_acas_acrs_exists;
           CLOSE chk_acas_acrs_exists;
--
           IF (l_acas_acrs_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',163);
           END IF;
-- 
          END IF;
--
-- ***********
--
-- Either the Advice Case Reference or the Advice Case Housing Option must be supplied.
-- *** No Longer required as larce_acas_alternate_ref is now mandatory ***
--
--          IF (    p1.larce_acas_alternate_ref IS NULL
--              AND p1.larce_acho_reference     IS NULL) THEN
--
--            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',186);
-- 
--          END IF;
--
-- Both the Advice Case Reference or the Advice Case Housing Option should not be supplied.
-- 
--
--          IF (    p1.larce_acas_alternate_ref IS NOT NULL
--              AND p1.larce_acho_reference     IS NOT NULL) THEN
--
--            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',267);
-- 
--          END IF;
--
-- Both the Advice Case reason Advice Case Housing Option should not be supplied together.
-- 
--
          IF (    p1.larce_acrs_arsn_code IS NOT NULL
              AND p1.larce_acho_reference IS NOT NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',268);
-- 
          END IF;
--
-- ***********
--
-- There must not already be a record on Advice Reason Casework Events table 
-- for the combination of:
--
-- Advice Case (if supplied)
-- Advice Case Reason (if supplied)
-- Housing Option Case (if supplied)
-- Advice Case Event Type
-- Event Date Time
--
--
--          IF (    l_acas_reference        IS NOT NULL
--              AND l_arsn_exists           IS NOT NULL
--              AND l_acho_reference        IS NOT NULL
--              AND l_acet_exists           IS NOT NULL
--              AND p1.larce_event_datetime IS NOT NULL) THEN
--
           l_arce_exists := NULL;
--
            OPEN chk_arce_exists (l_acas_reference,
                                  p1.larce_acrs_arsn_code,
                                  l_acho_reference,
                                  p1.larce_acet_code,
                                  p1.larce_event_datetime);
--
           FETCH chk_arce_exists INTO l_arce_exists;
           CLOSE chk_arce_exists;
--
           IF (l_arce_exists IS NOT NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',187);
           END IF;
-- 
--          END IF;
--
--
-- ***********
--
-- Check that the Advice Case approach date isn't greater than the 
-- event datetime. This is a check to stop trigger failure ARCE_BR_IU
-- 
--
          IF (    l_acas_approach_date    IS NOT NULL
              AND p1.larce_event_datetime IS NOT NULL) THEN
--
           IF (l_acas_approach_date > p1.larce_event_datetime) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',271);
           END IF;
-- 
          END IF;
--
-- ***********
--
-- Validate the duration. Make sure it isn't > 23:59
--
         l_fmt_fail := 'N';
--
         IF (p1.larce_duration IS NOT NULL) THEN
--
          IF LENGTH(p1.larce_duration) != 5 THEN
           l_fmt_fail := 'Y';
          END IF;
--
          IF (SUBSTR(p1.larce_duration,1,1) NOT IN ('0','1','2')) THEN
           l_fmt_fail := 'Y';
--
          ELSE
--
             IF (SUBSTR(p1.larce_duration,1,1) = '2') THEN
--
              IF (SUBSTR(p1.larce_duration,2,1) NOT IN ('0','1','2','3')) THEN
               l_fmt_fail := 'Y';
              END IF;
--
             ELSE
--
                IF (SUBSTR(p1.larce_duration,2,1) NOT IN ('0','1','2','3','4','5','6','7','8','9')) THEN
                 l_fmt_fail := 'Y';
                END IF;
--
             END IF;
--
          END IF;
--
          IF (SUBSTR(p1.larce_duration,3,1) != ':') THEN
           l_fmt_fail := 'Y';
          END IF;
--
          IF (SUBSTR(p1.larce_duration,4,1) NOT IN ('0','1','2','3','4','5')) THEN
           l_fmt_fail := 'Y';
          END IF;
--
          IF (SUBSTR(p1.larce_duration,5,1) NOT IN ('0','1','2','3','4','5','6','7','8','9')) THEN
           l_fmt_fail := 'Y';
          END IF;
--
          IF (l_fmt_fail = 'Y') THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',272);
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
       LARCE_DLB_BATCH_ID,
       LARCE_DL_SEQNO,
       LARCE_DL_LOAD_STATUS,
       LARCE_REFNO
  FROM dl_had_adv_rsn_casewrk_evts
 WHERE larce_dlb_batch_id   = p_batch_id
   AND larce_dl_load_status = 'C';
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
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'DELETE';
ct          VARCHAR2(30) := 'DL_HAD_ADV_RSN_CASEWRK_EVTS';
cs          INTEGER;
ce          VARCHAR2(200);
l_id        ROWID;
l_an_tab    VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         VARCHAR2(1);
i                INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_had_adv_rsn_casewrk_evts.dataload_delete');
    fsc_utils.debug_message('s_dl_had_adv_rsn_casewrk_evts.dataload_delete',3 );
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
          cs   := p1.larce_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i + 1;
--
--
-- Delete from advice_reason_casework_events table
--
          DELETE 
            FROM advice_reason_casework_events
           WHERE arce_refno = p1.larce_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_REASON_CASEWORK_EVENTS');
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
END s_dl_had_adv_rsn_casewrk_evts;
/

