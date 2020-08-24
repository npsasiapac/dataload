CREATE OR REPLACE PACKAGE s_dl_hra_lwr_water_usage_dets
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
END s_dl_hra_lwr_water_usage_dets;
--
/


CREATE OR REPLACE PACKAGE BODY s_dl_hra_lwr_water_usage_dets
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.17.0    VS   01-MAY-2010  Initial Creation.
--
--  2.0     5.17.0    VS   31-AUG-2010  Tidy Up.
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
    UPDATE dl_hra_lwr_water_usage_dets
       SET lwaud_dl_load_status = p_status
     WHERE rowid                = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hra_lwr_water_usage_dets');
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
       LWAUD_DLB_BATCH_ID,
       LWAUD_DL_SEQNO,
       LWAUD_DL_LOAD_STATUS,
       LWAUD_REFNO,
       LWAUD_LWRB_REFNO,
       LWAUD_WRA_CURR_ASSESSMENT_REF,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LWAUD_BILL_PERIOD_START_DATE,
       LWAUD_BILL_PERIOD_END_DATE,
       LWAUD_CATEGORY_RATE_AMOUNT,
       LWAUD_CR_DR_INDICATOR,
       LWAUD_CHARGEABLE_WATER_USAGE,
       LWAUD_SCO_CODE,
       LWAUD_RCCO_CODE,
       LWAUD_CURRENT_METER_READING,
       LWAUD_CHARGE_RATE_LEVEL_1,
       LWAUD_CONSUMPTION_LEVEL_1,
       LWAUD_DEBIT_AMOUNT_LEVEL_1,
       NVL(LWAUD_CREATED_BY,'DATALOAD') LWAUD_CREATED_BY,
       NVL(LWAUD_CREATED_DATE,SYSDATE)  LWAUD_CREATED_DATE,
       LWAUD_WATER_METER_NUMBER,
       LWAUD_PREV_METER_READING,
       LWAUD_DAYS_SINCE_LAST_MTR_READ,
       LWAUD_CHARGE_RATE_LEVEL_2,
       LWAUD_CONSUMPTION_LEVEL_2,
       LWAUD_DEBIT_AMOUNT_LEVEL_2,
       LWAUD_CHARGE_RATE_LEVEL_3,
       LWAUD_CONSUMPTION_LEVEL_3,
       LWAUD_DEBIT_AMOUNT_LEVEL_3,
       LWAUD_CHARGE_RATE_LEVEL_4,
       LWAUD_CONSUMPTION_LEVEL_4,
       LWAUD_DEBIT_AMOUNT_LEVEL_4,
       LWAUD_REJECTED_BY,
       LWAUD_REJECTED_DATE,
       LWAUD_HRV_WURR_CODE,
       LWAUD_MODIFIED_BY,
       LWAUD_MODIFIED_DATE,
       LWAUD_COMMENTS
  FROM dl_hra_lwr_water_usage_dets
 WHERE lwaud_dlb_batch_id   = p_batch_id
   AND lwaud_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR get_lwra_refno(p_lwra_lwrb_refno             NUMBER,
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
ct                   VARCHAR2(30) := 'DL_HRA_LWR_WATER_USAGE_DETS';
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
    execute immediate 'alter trigger WAUD_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hra_lwr_water_usage_dets.dataload_create');
    fsc_utils.debug_message('s_dl_hra_lwr_water_usage_dets.dataload_create',3);
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
          cs   := p1.lwaud_dl_seqno;
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
           OPEN get_lwra_refno(p1.lwaud_lwrb_refno,
                               p1.lwaud_wra_curr_assessment_ref,
                               p1.lwra_rate_period_start_date,
                               p1.lwra_rate_period_end_date);
--
          FETCH get_lwra_refno INTO l_lwra_refno;
          CLOSE get_lwra_refno;
--
--
-- Insert into LWR_RATE_ASSESSMENT_DETAILS table
--
        INSERT /* +APPEND */ INTO  water_usage_details(WAUD_REFNO,
                                                       WAUD_LWRA_REFNO,
                                                       WAUD_BILL_PERIOD_START_DATE,
                                                       WAUD_BILL_PERIOD_END_DATE,
                                                       WAUD_CATEGORY_RATE_AMOUNT,
                                                       WAUD_CR_DR_INDICATOR,
                                                       WAUD_CHARGEABLE_WATER_USAGE,
                                                       WAUD_CURRENT_METER_READING,
                                                       WAUD_CHARGE_RATE_LEVEL_1,
                                                       WAUD_CONSUMPTION_LEVEL_1,
                                                       WAUD_DEBIT_AMOUNT_LEVEL_1,
                                                       WAUD_SCO_CODE,
                                                       WAUD_CREATED_BY,
                                                       WAUD_CREATED_DATE,
                                                       WAUD_WATER_METER_NUMBER,
                                                       WAUD_PREV_METER_READING,
                                                       WAUD_DAYS_SINCE_LAST_MTR_READ,
                                                       WAUD_CHARGE_RATE_LEVEL_2,
                                                       WAUD_CONSUMPTION_LEVEL_2,
                                                       WAUD_DEBIT_AMOUNT_LEVEL_2,
                                                       WAUD_CHARGE_RATE_LEVEL_3,
                                                       WAUD_CONSUMPTION_LEVEL_3,
                                                       WAUD_DEBIT_AMOUNT_LEVEL_3,
                                                       WAUD_CHARGE_RATE_LEVEL_4,
                                                       WAUD_CONSUMPTION_LEVEL_4,
                                                       WAUD_DEBIT_AMOUNT_LEVEL_4,
                                                       WAUD_REJECTED_BY,
                                                       WAUD_REJECTED_DATE,
                                                       WAUD_HRV_WURR_CODE,
                                                       WAUD_MODIFIED_BY,
                                                       WAUD_MODIFIED_DATE,
                                                       WAUD_COMMENTS,
                                                       WAUD_RCCO_CODE
                                                      )
--
                                               VALUES (p1.LWAUD_REFNO,
                                                       l_lwra_refno,
                                                       p1.LWAUD_BILL_PERIOD_START_DATE,
                                                       p1.LWAUD_BILL_PERIOD_END_DATE,
                                                       p1.LWAUD_CATEGORY_RATE_AMOUNT,
                                                       p1.LWAUD_CR_DR_INDICATOR,
                                                       p1.LWAUD_CHARGEABLE_WATER_USAGE,
                                                       p1.LWAUD_CURRENT_METER_READING,
                                                       p1.LWAUD_CHARGE_RATE_LEVEL_1,
                                                       p1.LWAUD_CONSUMPTION_LEVEL_1,
                                                       p1.LWAUD_DEBIT_AMOUNT_LEVEL_1,
                                                       p1.LWAUD_SCO_CODE,
                                                       p1.LWAUD_CREATED_BY,
                                                       p1.LWAUD_CREATED_DATE,
                                                       p1.LWAUD_WATER_METER_NUMBER,
                                                       p1.LWAUD_PREV_METER_READING,
                                                       p1.LWAUD_DAYS_SINCE_LAST_MTR_READ,
                                                       p1.LWAUD_CHARGE_RATE_LEVEL_2,
                                                       p1.LWAUD_CONSUMPTION_LEVEL_2,
                                                       p1.LWAUD_DEBIT_AMOUNT_LEVEL_2,
                                                       p1.LWAUD_CHARGE_RATE_LEVEL_3,
                                                       p1.LWAUD_CONSUMPTION_LEVEL_3,
                                                       p1.LWAUD_DEBIT_AMOUNT_LEVEL_3,
                                                       p1.LWAUD_CHARGE_RATE_LEVEL_4,
                                                       p1.LWAUD_CONSUMPTION_LEVEL_4,
                                                       p1.LWAUD_DEBIT_AMOUNT_LEVEL_4,
                                                       p1.LWAUD_REJECTED_BY,
                                                       p1.LWAUD_REJECTED_DATE,
                                                       p1.LWAUD_HRV_WURR_CODE,
                                                       p1.LWAUD_MODIFIED_BY,
                                                       p1.LWAUD_MODIFIED_DATE,
                                                       p1.LWAUD_COMMENTS,
                                                       p1.LWAUD_RCCO_CODE
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('WATER_USAGE_DETAILS');
--
    execute immediate 'alter trigger WAUD_BR_I enable';
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
       LWAUD_DLB_BATCH_ID,
       LWAUD_DL_SEQNO,
       LWAUD_DL_LOAD_STATUS,
       LWAUD_REFNO,
       LWAUD_LWRB_REFNO,
       LWAUD_WRA_CURR_ASSESSMENT_REF,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LWAUD_BILL_PERIOD_START_DATE,
       LWAUD_BILL_PERIOD_END_DATE,
       LWAUD_CATEGORY_RATE_AMOUNT,
       LWAUD_CR_DR_INDICATOR,
       LWAUD_CHARGEABLE_WATER_USAGE,
       LWAUD_SCO_CODE,
       LWAUD_RCCO_CODE,
       LWAUD_CURRENT_METER_READING,
       LWAUD_CHARGE_RATE_LEVEL_1,
       LWAUD_CONSUMPTION_LEVEL_1,
       LWAUD_DEBIT_AMOUNT_LEVEL_1,
       NVL(LWAUD_CREATED_BY,'DATALOAD') LWAUD_CREATED_BY,
       NVL(LWAUD_CREATED_DATE,SYSDATE)  LWAUD_CREATED_DATE,
       LWAUD_WATER_METER_NUMBER,
       LWAUD_PREV_METER_READING,
       LWAUD_DAYS_SINCE_LAST_MTR_READ,
       LWAUD_CHARGE_RATE_LEVEL_2,
       LWAUD_CONSUMPTION_LEVEL_2,
       LWAUD_DEBIT_AMOUNT_LEVEL_2,
       LWAUD_CHARGE_RATE_LEVEL_3,
       LWAUD_CONSUMPTION_LEVEL_3,
       LWAUD_DEBIT_AMOUNT_LEVEL_3,
       LWAUD_CHARGE_RATE_LEVEL_4,
       LWAUD_CONSUMPTION_LEVEL_4,
       LWAUD_DEBIT_AMOUNT_LEVEL_4,
       LWAUD_REJECTED_BY,
       LWAUD_REJECTED_DATE,
       LWAUD_HRV_WURR_CODE,
       LWAUD_MODIFIED_BY,
       LWAUD_MODIFIED_DATE,
       LWAUD_COMMENTS
  FROM dl_hra_lwr_water_usage_dets
 WHERE lwaud_dlb_batch_id   = p_batch_id
   AND lwaud_dl_load_status IN ('L','F','O');
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
CURSOR get_lwra_refno(p_lwra_lwrb_refno             NUMBER,
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
ct                   VARCHAR2(30) := 'DL_HRA_LWR_WATER_USAGE_DETS';
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
    fsc_utils.proc_start('s_dl_hra_lwr_water_usage_dets.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_lwr_water_usage_dets.dataload_validate',3);
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
          cs   := p1.lwaud_dl_seqno;
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
           OPEN chk_batch_id(p1.lwaud_lwrb_refno);
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
           OPEN get_lwra_refno(p1.lwaud_lwrb_refno,
                               p1.lwaud_wra_curr_assessment_ref,
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
-- Check LWAUD_BILL_PERIOD_START_DATE < LWAUD_BILL_PERIOD_END_DATE
--
--
          IF (p1.lwaud_bill_period_start_date > p1.lwaud_bill_period_end_date) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',371);
          END IF;
--
-- ***********************************************************************
--
-- Check Credit Debit Indicator is valid
--
          IF (p1.lwaud_cr_dr_indicator NOT IN ('CR','DR')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',360);
          END IF;
--
-- ***********************************************************************
--
-- Check Rate Category Code is valid
--
          l_rcco_exists := NULL;
--
           OPEN chk_rcco_exists(p1.lwaud_rcco_code);
          FETCH chk_rcco_exists INTO l_rcco_exists;
          CLOSE chk_rcco_exists;
--
          IF (l_rcco_exists IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',363);
          END IF;
--
-- ***********************************************************************
--
-- Check Status Code is valid
--
          IF (p1.lwaud_sco_code NOT IN ('APR','REJ','RAP')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',373);
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
       LWAUD_DLB_BATCH_ID,
       LWAUD_DL_SEQNO,
       LWAUD_DL_LOAD_STATUS,
       LWAUD_REFNO
  FROM dl_hra_lwr_water_usage_dets
 WHERE lwaud_dlb_batch_id   = p_batch_id
   AND lwaud_dl_load_status = 'C';
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
ct                   VARCHAR2(30) := 'DL_HRA_LWR_WATER_USAGE_DETS';
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
    fsc_utils.proc_start('s_dl_hra_lwr_water_usage_dets.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_lwr_water_usage_dets.dataload_delete',3 );
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
          cs   := p1.lwaud_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--
-- Delete from WATER_USAGE_DETAILS table
--
--
          DELETE
            FROM water_usage_details
           WHERE waud_refno = p1.lwaud_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('WATER_USAGE_DETAILS');
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
END s_dl_hra_lwr_water_usage_dets;
/
