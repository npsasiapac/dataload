CREATE OR REPLACE PACKAGE BODY s_dl_hra_group_subsidy_reviews
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY

--      1.0  5.16      VS   18-OCT-2009  Initial Version
--      1.1  6.14      AJ   27-SEP-2017  Amended lsurv_subp_asca_code so that it
--                                       checks assessment_categories, and not 
--                                       the FRV change was at v6.8 error amended from
--                                       50 to 82 in hd2 errors file
--     
-- ***********************************************************************   
--  
--  declare package variables AND constants
--
--
PROCEDURE set_record_status_flag(p_rowid  IN ROWID,
                                 p_status IN VARCHAR2)
AS
--
BEGIN
--
  UPDATE dl_hra_group_subsidy_reviews
     SET lgrsr_dl_load_status = p_status
   WHERE rowid                = p_rowid;
--
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_hra_group_subsidy_reviews');
          RAISE;
--
END set_record_status_flag;
--
-- ************************************************************************************
--
--
PROCEDURE dataload_create(p_batch_id    IN VARCHAR2,
                          p_date	IN DATE)
AS
--
CURSOR c1 
IS
SELECT ROWID REC_ROWID,
       LGRSR_DLB_BATCH_ID,
       LGRSR_DL_SEQNO,
       LGRSR_DL_LOAD_STATUS,
       LGRSR_USER_REFERENCE,
       LGRSR_EFFECTIVE_DATE,
       LGRSR_ASSESSMENT_DATE,
       LGRSR_SCO_CODE,
       LGRSR_ISSUE_ICS_REQUESTS_IND,
       LGRSR_ISSUE_INCOME_CERTIF_IND,
       NVL(LGRSR_CREATED_DATE, SYSDATE)  LGRSR_CREATED_DATE,
       NVL(LGRSR_CREATED_BY, 'DATALOAD') LGRSR_CREATED_BY,
       LGRSR_REMINDER_NUM_DAYS,
       LGRSR_TERMINATE_NUM_DAYS,
       LGRSR_LATEST_LAST_REVIEW_DATE,
       LGRSR_HRV_ASCA_CODE,
       LGRSR_AUN_CODE,
       LGRSR_TTY_CODE,
       LGRSR_MODIFIED_DATE,
       LGRSR_MODIFIED_BY,
       LGRSR_GENERATED_DATE,
       LGRSR_REFNO
  FROM dl_hra_group_subsidy_reviews
 WHERE lgrsr_dlb_batch_id   = p_batch_id
   AND lgrsr_dl_load_status = 'V';
--
-- ************************************************************************************
--
-- Constants for process_summary
--
--
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'CREATE';
ct       		VARCHAR2(30) := 'DL_HRA_GROUP_SUBSIDY_REVIEWS';
cs       		INTEGER;
ce       		VARCHAR2(200);
l_id            	ROWID;
l_an_tab		VARCHAR2(1);
i                 	INTEGER := 0;
--
BEGIN
--
    execute immediate 'alter trigger GRSR_BR_IU disable';
--
    fsc_utils.proc_start('s_dl_hra_group_subsidy_reviews.dataload_create');
    fsc_utils.debug_message('s_dl_hra_group_subsidy_reviews.dataload_create',3);
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
          cs := p1.lgrsr_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
--
          INSERT INTO GROUP_SUBSIDY_REVIEWS(GRSR_REFNO,
                                            GRSR_USER_REFERENCE,
                                            GRSR_EFFECTIVE_DATE,
                                            GRSR_ASSESSMENT_DATE,
                                            GRSR_SCO_CODE,
                                            GRSR_ISSUE_ICS_REQUESTS_IND,
                                            GRSR_ISSUE_INCOME_CERTIF_IND,
                                            GRSR_CREATED_DATE,
                                            GRSR_CREATED_BY,
                                            GRSR_REMINDER_NUM_DAYS,
                                            GRSR_TERMINATE_NUM_DAYS,
                                            GRSR_LATEST_LAST_REVIEW_DATE,
                                            GRSR_HRV_ASCA_CODE,
                                            GRSR_AUN_CODE,
                                            GRSR_TTY_CODE,
                                            GRSR_MODIFIED_DATE,
                                            GRSR_MODIFIED_BY,
                                            GRSR_GENERATED_DATE
                                           )
--
                                    VALUES (p1.LGRSR_REFNO,
                                            p1.LGRSR_USER_REFERENCE,  
                                            p1.LGRSR_EFFECTIVE_DATE,
                                            p1.LGRSR_ASSESSMENT_DATE,
                                            p1.LGRSR_SCO_CODE,
                                            p1.LGRSR_ISSUE_ICS_REQUESTS_IND,
                                            p1.LGRSR_ISSUE_INCOME_CERTIF_IND,
                                            p1.LGRSR_CREATED_DATE,
                                            p1.LGRSR_CREATED_BY,
                                            p1.LGRSR_REMINDER_NUM_DAYS,
                                            p1.LGRSR_TERMINATE_NUM_DAYS,
                                            p1.LGRSR_LATEST_LAST_REVIEW_DATE,
                                            p1.LGRSR_HRV_ASCA_CODE,
                                            p1.LGRSR_AUN_CODE,
                                            p1.LGRSR_TTY_CODE,
                                            p1.LGRSR_MODIFIED_DATE,
                                            p1.LGRSR_MODIFIED_BY,
                                            p1.LGRSR_GENERATED_DATE
                                           );
--
--
-- keep a count of the rows processed and commit after every 5000
--
          i := i+1; 
--
          IF MOD(i,5000)=0 THEN 
           COMMIT; 
          END If;
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
--
-- Section to analyze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('GROUP_SUBSIDY_REVIEWS');
--
    execute immediate 'alter trigger GRSR_BR_IU enable';
--
    fsc_utils.proc_end;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
            RAISE;
--
--
END dataload_create;
--
--
-- ************************************************************************************
--
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
                            p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT ROWID REC_ROWID,
       LGRSR_DLB_BATCH_ID,
       LGRSR_DL_SEQNO,
       LGRSR_DL_LOAD_STATUS,
       LGRSR_USER_REFERENCE,
       LGRSR_EFFECTIVE_DATE,
       LGRSR_ASSESSMENT_DATE,
       LGRSR_SCO_CODE,
       LGRSR_ISSUE_ICS_REQUESTS_IND,
       LGRSR_ISSUE_INCOME_CERTIF_IND,
       NVL(LGRSR_CREATED_DATE, SYSDATE)  LGRSR_CREATED_DATE,
       NVL(LGRSR_CREATED_BY, 'DATALOAD') LGRSR_CREATED_BY,
       LGRSR_REMINDER_NUM_DAYS,
       LGRSR_TERMINATE_NUM_DAYS,
       LGRSR_LATEST_LAST_REVIEW_DATE,
       LGRSR_HRV_ASCA_CODE,
       LGRSR_AUN_CODE,
       LGRSR_TTY_CODE,
       LGRSR_MODIFIED_DATE,
       LGRSR_MODIFIED_BY,
       LGRSR_GENERATED_DATE
  FROM dl_hra_group_subsidy_reviews
 WHERE lgrsr_dlb_batch_id   = p_batch_id
   AND lgrsr_dl_load_status in ('L','F','O');
--
-- ************************************************************************************
--
CURSOR chk_grsr_exists(p_grsr_user_reference VARCHAR2) 
IS
SELECT 'X'
  FROM group_subsidy_reviews
 WHERE grsr_user_reference = p_grsr_user_reference;
--
-- ************************************************************************************
--
CURSOR chk_sco_exists(p_sco_code     VARCHAR2) 
IS
SELECT 'X'
  FROM status_codes
 WHERE sco_code = p_sco_code;
--
-- ************************************************************************************
--
CURSOR chk_aun_exists(p_aun_code     VARCHAR2) 
IS
SELECT 'X'
  FROM admin_units
 WHERE aun_code = p_aun_code;
--
-- ************************************************************************************
--
CURSOR chk_tty_code(p_tty_code VARCHAR2) 
IS
SELECT 'X'
  FROM tenancy_types
 WHERE tty_code = p_tty_code;
--
-- ************************************************************************************
--
-- Moved check from First Ref Values to SUBSIDY_ASSESSMENT_CATEGORIES, as
-- configuration of the data has changed
--
CURSOR chk_asca_exists(p_suap_hrv_asca_code VARCHAR2)
IS
SELECT 'X'
  FROM SUBSIDY_ASSESSMENT_CATEGORIES
 WHERE ASCA_CODE = p_suap_hrv_asca_code;
--
-- ************************************************************************************
--
--
-- Constants for process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'VALIDATE';
ct       	VARCHAR2(30) := 'DL_HRA_GROUP_SUBSIDY_REVIEWS';
cs       	INTEGER;
ce       	VARCHAR2(200);
l_id            ROWID;
--
l_grsr_exists   VARCHAR2(1);
l_sco_exists    VARCHAR2(1);
l_aun_exists    VARCHAR2(1);
l_tty_exists    VARCHAR2(1);
l_asca_exists   VARCHAR2(1);
--
l_errors        VARCHAR2(10);
l_error_ind     VARCHAR2(10);
i               INTEGER :=0;
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_group_subsidy_reviews.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_group_subsidy_reviews.dataload_validate',3);
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
          cs := p1.lgrsr_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
          l_asca_exists := NULL;
--
-- ************************************************************************************
--
-- CHECK Group Subsidy Review record doesn't already exists.
--
--
          l_grsr_exists := NULL;
--
           OPEN chk_grsr_exists(p1.lgrsr_user_reference);
          FETCH chk_grsr_exists INTO l_grsr_exists;
          CLOSE chk_grsr_exists;
--
          IF (l_grsr_exists IS NOT NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',318);
          END IF;
--
-- ************************************************************************************
--
-- Check the status Code is valid 
--
          l_sco_exists := NULL;
--
           OPEN chk_sco_exists(p1.lgrsr_sco_code);
          FETCH chk_sco_exists INTO l_sco_exists;
          CLOSE chk_sco_exists;

          IF (l_sco_exists IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',249);
          END IF;
--
-- ************************************************************************************
--
-- If supplied check Aun code is valid 
--
          l_aun_exists := NULL;
--
          IF (p1.lgrsr_aun_code IS NOT NULL) THEN
--
            OPEN chk_aun_exists(p1.lgrsr_aun_code);
           FETCH chk_aun_exists INTO l_aun_exists;
           CLOSE chk_aun_exists;

           IF (l_aun_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',725);
           END IF;
--
          END IF;
--
--
-- ************************************************************************************
-- 
-- If supplied Subsidy Policy Category Code LGRSR_HRV_ASCA_CODE is valid
--
--          IF (p1.lgrsr_hrv_asca_code IS NOT NULL) THEN
--
--           IF (NOT s_dl_hem_utils.exists_frv('SUBASSCAT',p1.lgrsr_hrv_asca_code,'Y')) THEN
--              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',62);
--           END IF;
--
--        END IF;
--
-- changed from domain to table check amended to match (AJ)
--
          IF (p1.lgrsr_hrv_asca_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',49);
          ELSE
--
             l_asca_exists := NULL;
--
          OPEN chk_asca_exists(p1.lgrsr_hrv_asca_code);
         FETCH chk_asca_exists INTO l_asca_exists;
         CLOSE chk_asca_exists;
--
             IF (l_asca_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',82);
            END IF;
--
          END IF;
--
-- ********************************************
-- ************************************************************************************
--
-- If supplied check Aun code is valid 
--
          l_aun_exists := NULL;
--
          IF (p1.lgrsr_aun_code IS NOT NULL) THEN
--
            OPEN chk_aun_exists(p1.lgrsr_aun_code);
           FETCH chk_aun_exists INTO l_aun_exists;
           CLOSE chk_aun_exists;

           IF (l_aun_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',725);
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- If supplied check tenancy Type Code is valid
--
          IF (p1.lgrsr_tty_code IS NOT NULL) THEN
--
            OPEN chk_tty_code(p1.lgrsr_tty_code);
           FETCH chk_tty_code into l_tty_exists;
           CLOSE chk_tty_code;
--
           IF (l_tty_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',62);
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- Now Check the Y/N columns
--
-- Issue ICS Request Indicator
--
          IF (p1.lgrsr_issue_ics_requests_ind NOT IN ('Y','N')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',319);
          END IF;
--
-- ***************************************
--
-- Income Details Certificate Indicator
--
          IF (p1.lgrsr_issue_income_certif_ind NOT IN ('Y','N')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',320);
          END IF;
--
-- ************************************************************************************
--
-- Now UPDATE the record count AND error code
--
          IF l_errors = 'F' THEN
           l_error_ind := 'Y';
          ELSE
             l_error_ind := 'N';
          END IF;
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
          set_record_status_flag(l_id,l_errors);
--
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
    COMMIT;
--
    fsc_utils.proc_END;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
--
END dataload_validate;
--
--
-- ************************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1
IS
SELECT ROWID REC_ROWID,
       LGRSR_DLB_BATCH_ID,
       LGRSR_DL_SEQNO,
       LGRSR_DL_LOAD_STATUS,
       LGRSR_REFNO
  FROM dl_hra_group_subsidy_reviews
 WHERE lgrsr_dlb_batch_id   = p_batch_id
   AND lgrsr_dl_load_status = 'C';
--
-- ************************************************************************************
--
-- Constants for process_summary
--
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'DELETE';
ct       		VARCHAR2(30) := 'DL_HRA_GROUP_SUBSIDY_REVIEWS';
cs       		INTEGER;
ce       		VARCHAR2(200);
l_an_tab 		VARCHAR2(1);
l_id			ROWID;
--
-- Other variables
--
i                	INTEGER := 0;
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_group_subsidy_reviews.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_group_subsidy_reviews.dataload_delete',3 );
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
          cs := p1.lgrsr_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Delete record from group_subsidy_reviews
--
          DELETE 
            FROM group_subsidy_reviews
           WHERE grsr_refno = p1.lgrsr_refno;
--
--
-- keep a count of the rows processed and commit after every 1000
--
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
--
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
--
          EXCEPTION
               WHEN OTHERS THEN
                  ROLLBACK TO SP1;
                  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
                  set_record_status_flag(l_id,'C');
                  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
    COMMIT;
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('GROUP_SUBSIDY_REVIEWS');
--
    fsc_utils.proc_end;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
            RAISE;
--
END dataload_delete;
--
END s_dl_hra_group_subsidy_reviews;
/
