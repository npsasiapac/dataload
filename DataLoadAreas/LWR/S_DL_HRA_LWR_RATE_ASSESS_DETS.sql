CREATE OR REPLACE PACKAGE s_dl_hra_lwr_rate_assess_dets
AS
  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VER  DB Ver   WHO  WHEN          WHY
  --  1.0  5.17.0   VRS  27-APR-2010   INITIAL Version
  --
  --  declare package variables AND constants


  --***********************************************************************
  --  DESCRIPTION
  --
  --  1:  ...
  --  2:  ...
  --  REFERENCES FUNCTION
  --
  --
PROCEDURE set_record_status_flag(p_rowid           IN ROWID,
                                 p_status          IN VARCHAR2);
--
PROCEDURE dataload_create       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
PROCEDURE dataload_validate     (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
PROCEDURE dataload_delete       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
END s_dl_hra_lwr_rate_assess_dets;
--
/


CREATE OR REPLACE PACKAGE BODY s_dl_hra_lwr_rate_assess_dets
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.17.0    VS   29-APR-2010  Initial Creation.
--
--  2.0     5.17.0    VS   31-AUG-2010 CR101 Tidy Up. Added LRAD_RAAM_SEQ_NO
--
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
    UPDATE dl_hra_lwr_rate_assess_dets
       SET lrad_dl_load_status = p_status
     WHERE rowid               = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hra_lwr_rate_assess_dets');
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
SELECT rowid rec_rowid,
       LRAD_DLB_BATCH_ID,
       LRAD_DL_SEQNO,
       LRAD_DL_LOAD_STATUS,
       LRAD_REFNO,
       LRAD_LWRB_REFNO,
       LRAD_WRA_CURR_ASSESSMENT_REF,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LRAD_CLASS_CODE,
       LRAD_RCCO_CODE,
       LRAD_CATEGORY_RATE_AMOUNT,
       LRAD_CR_DR_INDICATOR,
       NVL(LRAD_CREATED_BY, 'DATALOAD') LRAD_CREATED_BY,
       NVL(LRAD_CREATED_DATE, SYSDATE)  LRAD_CREATED_DATE,
       LRAD_CATEGORY_START_DATE,
       LRAD_CATEGORY_END_DATE,
       LRAD_COMMENTS,
       LRAD_MODIFIED_BY,
       LRAD_MODIFIED_DATE,
       LRAD_RAAM_SEQ_NO
  FROM dl_hra_lwr_rate_assess_dets
 WHERE lrad_dlb_batch_id   = p_batch_id
   AND lrad_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR get_lwra_refno(p_lwra_lwrb_refno            NUMBER,
                      p_lwra_curr_assessment_ref    VARCHAR2,
                      p_lwra_rate_period_start_date DATE,
                      p_lwra_rate_period_end_date   DATE)
IS
SELECT lwra_refno
  FROM lwr_assessments
 WHERE lwra_lwrb_refno             = p_lwra_lwrb_refno
   AND lwra_current_assessment_ref = p_lwra_curr_assessment_ref
   AND lwra_rate_period_start_date = p_lwra_rate_period_start_date
   AND lwra_rate_period_end_date   = p_lwra_rate_period_end_date;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_RATE_ASSESS_DETS';
cs                   INTEGER;
ce	             VARCHAR2(200);
l_id                 ROWID;
l_an_tab             VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
i                    INTEGER := 0;
l_exists             VARCHAR2(1);
--
l_lwra_refno         NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger LRAD_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hra_lwr_rate_assess_dets.dataload_create');
    fsc_utils.debug_message('s_dl_hra_lwr_rate_assess_dets.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lrad_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Get LWR Assessment reference
--
          l_lwra_refno := null;
--
           OPEN get_lwra_refno(p1.lrad_lwrb_refno,
                               p1.lrad_wra_curr_assessment_ref,
                               p1.lwra_rate_period_start_date,
                               p1.lwra_rate_period_end_date);
--
          FETCH get_lwra_refno INTO l_lwra_refno;
          CLOSE get_lwra_refno;
--
--
-- Insert into LWR_RATE_ASSESSMENT_DETAILS table
--
        INSERT /* +APPEND */ INTO  lwr_rate_assessment_details(LRAD_REFNO,
                                                               LRAD_CLASS_CODE,
                                                               LRAD_LWRA_REFNO,
                                                               LRAD_RCCO_CODE,
                                                               LRAD_CATEGORY_RATE_AMOUNT,
                                                               LRAD_CR_DR_INDICATOR,
                                                               LRAD_CREATED_BY,
                                                               LRAD_CREATED_DATE,
                                                               LRAD_CATEGORY_START_DATE,
                                                               LRAD_CATEGORY_END_DATE,
                                                               LRAD_COMMENTS,
                                                               LRAD_MODIFIED_BY,
                                                               LRAD_MODIFIED_DATE
                                                              )
--
                                                       VALUES (p1.LRAD_REFNO,
                                                               p1.LRAD_CLASS_CODE,
                                                               l_lwra_refno,
                                                               p1.LRAD_RCCO_CODE,
                                                               p1.LRAD_CATEGORY_RATE_AMOUNT,
                                                               p1.LRAD_CR_DR_INDICATOR,
                                                               p1.LRAD_CREATED_BY,
                                                               p1.LRAD_CREATED_DATE,
                                                               p1.LRAD_CATEGORY_START_DATE,
                                                               p1.LRAD_CATEGORY_END_DATE,
                                                               p1.LRAD_COMMENTS,
                                                               p1.LRAD_MODIFIED_BY,
                                                               p1.LRAD_MODIFIED_DATE
                                                              );
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
          i := i+1;
--
          IF MOD(i,50000) = 0 THEN
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('LWR_RATE_ASSESSMENT_DETS');
--
    execute immediate 'alter trigger LRAD_BR_I enable';
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
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
                            p_date              IN DATE)
AS
--
CURSOR c1
IS
SELECT rowid rec_rowid,
       LRAD_DLB_BATCH_ID,
       LRAD_DL_SEQNO,
       LRAD_DL_LOAD_STATUS,
       LRAD_REFNO,
       LRAD_LWRB_REFNO,
       LRAD_WRA_CURR_ASSESSMENT_REF,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LRAD_CLASS_CODE,
       LRAD_RCCO_CODE,
       LRAD_CATEGORY_RATE_AMOUNT,
       LRAD_CR_DR_INDICATOR,
       NVL(LRAD_CREATED_BY, 'DATALOAD') LRAD_CREATED_BY,
       NVL(LRAD_CREATED_DATE, SYSDATE)  LRAD_CREATED_DATE,
       LRAD_CATEGORY_START_DATE,
       LRAD_CATEGORY_END_DATE,
       LRAD_COMMENTS,
       LRAD_MODIFIED_BY,
       LRAD_MODIFIED_DATE,
       LRAD_RAAM_SEQ_NO
  FROM dl_hra_lwr_rate_assess_dets
 WHERE lrad_dlb_batch_id   = p_batch_id
   AND lrad_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- Check Batch id doesn't already exist on lwr_batches
--
CURSOR chk_batch_id(p_lwrb_refno NUMBER)
IS
SELECT lwrb_refno
  FROM lwr_batches
 WHERE lwrb_refno = p_lwrb_refno;
--
-- ***********************************************************************
--
CURSOR get_lwra_refno(p_lwra_lwrb_refno            NUMBER,
                      p_lwra_curr_assessment_ref    VARCHAR2,
                      p_lwra_rate_period_start_date DATE,
                      p_lwra_rate_period_end_date   DATE)
IS
SELECT lwra_refno
  FROM lwr_assessments
 WHERE lwra_lwrb_refno             = p_lwra_lwrb_refno
   AND lwra_current_assessment_ref = p_lwra_curr_assessment_ref
   AND lwra_rate_period_start_date = p_lwra_rate_period_start_date
   AND lwra_rate_period_end_date   = p_lwra_rate_period_end_date;
--
-- ***********************************************************************
--
CURSOR chk_rcco_exists(p_rcco_code	VARCHAR2)
IS
SELECT 'X'
  FROM rate_categories
 WHERE rcco_code = p_rcco_code;
--
-- ***********************************************************************
--
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_RATE_ASSESS_DETS';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists             VARCHAR2(1);
l_errors             VARCHAR2(10);
l_error_ind          VARCHAR2(10);
i                    INTEGER :=0;
--
l_lwrb_refno         NUMBER(10);
l_lwra_refno         NUMBER(10);
l_rcco_exists        VARCHAR2(1);

--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_lwr_rate_assess_dets.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_lwr_rate_assess_dets.dataload_validate',3);
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
          cs   := p1.lrad_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
-- ***********************************************************************
--
-- Validation checks required
--
-- Check batch_id exists
--
          l_lwrb_refno := NULL;
--
           OPEN chk_batch_id(p1.lrad_lwrb_refno);
          FETCH chk_batch_id INTO l_lwrb_refno;
          CLOSE chk_batch_id;
--
          IF (l_lwrb_refno IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',352);
          END IF;
--
-- ***********************************************************************
--
-- Get LWR Assessment reference
--
          l_lwra_refno := null;
--
           OPEN get_lwra_refno(p1.lrad_lwrb_refno,
                               p1.lrad_wra_curr_assessment_ref,
                               p1.lwra_rate_period_start_date,
                               p1.lwra_rate_period_end_date);
--
          FETCH get_lwra_refno into l_lwra_refno;
          CLOSE get_lwra_refno;
--
          IF (l_lwra_refno IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',364);
          END IF;
--
-- ***********************************************************************
--
-- Check Assessment Details Class is valid
--
          IF (p1.lrad_class_code NOT IN ('ANRD','WFRD','IOAC')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',361);
          END IF;
--
-- ***********************************************************************
--
-- Check Rate Category Code is valid
--
          l_rcco_exists := NULL;
--
           OPEN chk_rcco_exists(p1.lrad_rcco_code);
          FETCH chk_rcco_exists INTO l_rcco_exists;
          CLOSE chk_rcco_exists;
--
          IF (l_rcco_exists IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',363);
          END IF;
--
-- ***********************************************************************
--
-- Check Assessment Detail LRAD_CATEGORY_START_DATE < LRAD_CATEGORY_END_DATE
--
          IF (    p1.lrad_category_start_date IS NOT NULL
              AND p1.lrad_category_end_date   IS NOT NULL) THEN
--
           IF (p1.lrad_category_start_date > p1.lrad_category_end_date) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',362);
           END IF;
--
--
-- Check Assessment Detail LRAD_CATEGORY_START_DATE/LRAD_CATEGORY_END_DATE is
-- between the LWR Assessment RATE_PERIOD_START_DATE/RATE_PERIOD_END_DATE
--
--
           IF ((  p1.lrad_category_start_date NOT BETWEEN p1.lwra_rate_period_start_date
                                                      AND p1.lwra_rate_period_end_date)
               OR (p1.lrad_category_end_date  NOT BETWEEN p1.lwra_rate_period_start_date
                                                      AND p1.lwra_rate_period_end_date)) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',365);
--
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check Credit Debit Indicator is valid
--
          IF (p1.lrad_cr_dr_indicator NOT IN ('CR','DR')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',360);
          END IF;
--
-- ***********************************************************************
--
-- If the Assessment detail is for an Annual Batch(ANRD) then the following must
-- be provided: Rate Category Code, Category Start Date, Category End Date,
-- Category Rate Amount, Credit/Debit Indicator
--
--
          IF (p1.lrad_class_code = 'ANRD') THEN
--
           IF (   p1.LRAD_RCCO_CODE            IS NULL
               OR p1.LRAD_CATEGORY_START_DATE  IS NULL
               OR p1.LRAD_CATEGORY_END_DATE    IS NULL
               OR p1.LRAD_CATEGORY_RATE_AMOUNT IS NULL
               OR p1.LRAD_CR_DR_INDICATOR      IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',366);
--
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- If the Assessment detail is for an Instalment Batch(ANRD) then the following must
-- be provided: Rate Category Code, Category Rate Amount, Credit/Debit Indicator
--
--
          IF (p1.lrad_class_code = 'IOAC') THEN
--
           IF (   p1.LRAD_RCCO_CODE            IS NULL
               OR p1.LRAD_CATEGORY_RATE_AMOUNT IS NULL
               OR p1.LRAD_CR_Dr_INDICATOR      IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',367);
--
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- If the Assessment detail is for an Water Batch(WFRD) then the following must
-- be provided: Rate Category Code, Category Rate Amount, Credit/Debit Indicator
--
--
          IF (p1.lrad_class_code = 'WFRD') THEN
--
           IF (   p1.LRAD_RCCO_CODE            IS NULL
               OR p1.LRAD_CATEGORY_RATE_AMOUNT IS NULL
               OR p1.LRAD_CR_Dr_INDICATOR      IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',368);
--
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
CURSOR c1
IS
SELECT rowid rec_rowid,
       LRAD_DLB_BATCH_ID,
       LRAD_DL_SEQNO,
       LRAD_DL_LOAD_STATUS,
       LRAD_REFNO
  FROM dl_hra_lwr_rate_assess_dets
 WHERE lrad_dlb_batch_id   = p_batch_id
   AND lrad_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_RATE_ASSESS_DETS';
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
l_exists         VARCHAR2(1);
i                INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_lwr_rate_assess_dets.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_lwr_rate_assess_dets.dataload_delete',3 );
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
          cs   := p1.lrad_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--
-- Delete from LWR_RATE_ASSESSMENT_DETAILS table
--
--
          DELETE
            FROM lwr_rate_assessment_details
           WHERE lrad_refno = p1.lrad_refno;
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
          IF mod(i,5000) = 0 THEN
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('LWR_RATE_ASSESSMENT_DETS');
--
    fsc_utils.proc_end;
--
    COMMIT;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
            RAISE;
--
END dataload_delete;
--
END s_dl_hra_lwr_rate_assess_dets;
/
