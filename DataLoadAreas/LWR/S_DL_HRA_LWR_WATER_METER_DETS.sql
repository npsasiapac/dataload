CREATE OR REPLACE PACKAGE s_dl_hra_lwr_water_meter_dets
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
END s_dl_hra_lwr_water_meter_dets;
--
/


CREATE OR REPLACE PACKAGE BODY s_dl_hra_lwr_water_meter_dets
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
    UPDATE dl_hra_lwr_water_meter_dets
       SET lwmd_dl_load_status = p_status
     WHERE rowid               = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hra_lwr_water_meter_dets');
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
       LWMD_DLB_BATCH_ID,
       LWMD_DL_SEQNO,
       LWMD_DL_LOAD_STATUS,
       LWMD_REFNO,
       LWMD_LWRB_REFNO,
       LWMD_WRA_CURR_ASSESSMENT_REF,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LWMD_RCCO_CODE,
       LWMD_CURRENT_METER_READING,
       LWMD_PREVIOUS_METER_READING,
       LWMD_CHARGEABLE_WATER_USAGE,
       LWMD_CURRENT_READING_DATE,
       LWMD_PREVIOUS_READING_DATE,
       LWMD_DAYS_SINCE_LAST_READING,
       LWMD_WATER_METER_NUMBER,
       NVL(LWMD_CREATED_BY, 'DATALOAD') LWMD_CREATED_BY,
       NVL(LWMD_CREATED_DATE, SYSDATE)  LWMD_CREATED_DATE,
       LWMD_COMMENTS,
       LWMD_MODIFIED_BY,
       LWMD_MODIFIED_DATE
  FROM dl_hra_lwr_water_meter_dets
 WHERE lwmd_dlb_batch_id   = p_batch_id
   AND lwmd_dl_load_status = 'V';
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
ct                   VARCHAR2(30) := 'DL_HRA_LWR_WATER_METER_DETS';
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
    execute immediate 'alter trigger WMD_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hra_lwr_water_meter_dets.dataload_create');
    fsc_utils.debug_message('s_dl_hra_lwr_water_meter_dets.dataload_create',3);
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
          cs   := p1.lwmd_dl_seqno;
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
           OPEN get_lwra_refno(p1.lwmd_lwrb_refno,
                               p1.lwmd_wra_curr_assessment_ref,
                               p1.lwra_rate_period_start_date,
                               p1.lwra_rate_period_end_date);
--
          FETCH get_lwra_refno INTO l_lwra_refno;
          CLOSE get_lwra_refno;
--
--
--
-- Insert into LWR_ASSESSMENTS table
--
        INSERT /* +APPEND */ INTO  water_meter_details(WMD_REFNO,
                                                       WMD_LWRA_REFNO,
                                                       WMD_RCCO_CODE,
                                                       WMD_CURRENT_METER_READING,
                                                       WMD_PREVIOUS_METER_READING,
                                                       WMD_CHARGEABLE_WATER_USAGE,
                                                       WMD_CURRENT_READING_DATE,
                                                       WMD_PREVIOUS_READING_DATE,
                                                       WMD_DAYS_SINCE_LAST_READING,
                                                       WMD_WATER_METER_NUMBER,
                                                       WMD_CREATED_BY,
                                                       WMD_CREATED_DATE,
                                                       WMD_COMMENTS,
                                                       WMD_MODIFIED_BY,
                                                       WMD_MODIFIED_DATE
                                                      )
--
                                               VALUES (p1.LWMD_REFNO,
                                                       l_lwra_refno,
                                                       p1.LWMD_RCCO_CODE,
                                                       p1.LWMD_CURRENT_METER_READING,
                                                       p1.LWMD_PREVIOUS_METER_READING,
                                                       p1.LWMD_CHARGEABLE_WATER_USAGE,
                                                       p1.LWMD_CURRENT_READING_DATE,
                                                       p1.LWMD_PREVIOUS_READING_DATE,
                                                       p1.LWMD_DAYS_SINCE_LAST_READING,
                                                       p1.LWMD_WATER_METER_NUMBER,
                                                       p1.LWMD_CREATED_BY,
                                                       p1.LWMD_CREATED_DATE,
                                                       P1.LWMD_COMMENTS,
                                                       P1.LWMD_MODIFIED_BY,
                                                       P1.LWMD_MODIFIED_DATE
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('WATER_METER_DETAILS');
--
    execute immediate 'alter trigger WMD_BR_I enable';
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
       LWMD_DLB_BATCH_ID,
       LWMD_DL_SEQNO,
       LWMD_DL_LOAD_STATUS,
       LWMD_REFNO,
       LWMD_LWRB_REFNO,
       LWMD_WRA_CURR_ASSESSMENT_REF,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LWMD_RCCO_CODE,
       LWMD_CURRENT_METER_READING,
       LWMD_PREVIOUS_METER_READING,
       LWMD_CHARGEABLE_WATER_USAGE,
       LWMD_CURRENT_READING_DATE,
       LWMD_PREVIOUS_READING_DATE,
       LWMD_DAYS_SINCE_LAST_READING,
       LWMD_WATER_METER_NUMBER,
       NVL(LWMD_CREATED_BY, 'DATALOAD') LWMD_CREATED_BY,
       NVL(LWMD_CREATED_DATE, SYSDATE)  LWMD_CREATED_DATE,
       LWMD_COMMENTS,
       LWMD_MODIFIED_BY,
       LWMD_MODIFIED_DATE
  FROM dl_hra_lwr_water_meter_dets
 WHERE lwmd_dlb_batch_id   = p_batch_id
   AND lwmd_dl_load_status IN ('L','F','O');
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
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_WATER_METER_DES';
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
    fsc_utils.proc_start('s_dl_hra_lwr_water_meter_dets.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_lwr_water_meter_dets.dataload_validate',3);
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
          cs   := p1.lwmd_dl_seqno;
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
           OPEN chk_batch_id(p1.lwmd_lwrb_refno);
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
           OPEN get_lwra_refno(p1.lwmd_lwrb_refno,
                               p1.lwmd_wra_curr_assessment_ref,
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
-- Check Rate Category Code is valid
--
          l_rcco_exists := NULL;
--
           OPEN chk_rcco_exists(p1.lwmd_rcco_code);
          FETCH chk_rcco_exists INTO l_rcco_exists;
          CLOSE chk_rcco_exists;
--
          IF (l_rcco_exists IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',363);
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
         IF MOD(i,5000)=0 THEN
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
       LWMD_DLB_BATCH_ID,
       LWMD_DL_SEQNO,
       LWMD_DL_LOAD_STATUS,
       LWMD_REFNO
  FROM dl_hra_lwr_water_meter_dets
 WHERE lwmd_dlb_batch_id   = p_batch_id
   AND lwmd_dl_load_status = 'C';
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
ct                   VARCHAR2(30) := 'DL_HRA_LWR_WATER_METER_DES';
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
    fsc_utils.proc_start('s_dl_hra_lwr_water_meter_dets.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_lwr_water_meter_dets.dataload_delete',3 );
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
          cs   := p1.lwmd_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--
-- Delete from WATER_METER_DETAILS table
--
--
          DELETE
            FROM water_meter_details
           WHERE wmd_refno = p1.lwmd_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('WATER_METER_DETAILS');
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
END s_dl_hra_lwr_water_meter_dets;
/
