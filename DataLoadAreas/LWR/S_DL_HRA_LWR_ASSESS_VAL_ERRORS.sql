CREATE OR REPLACE PACKAGE s_dl_hra_lwr_assess_val_errors
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
END s_dl_hra_lwr_assess_val_errors;
--
/


CREATE OR REPLACE PACKAGE BODY s_dl_hra_lwr_assess_val_errors
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.17.0    VS   30-APR-2010  Initial Creation.
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
    UPDATE dl_hra_lwr_assess_val_errors
       SET lasve_dl_load_status = p_status
     WHERE rowid                = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hra_lwr_assess_val_errors');
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
       LASVE_DLB_BATCH_ID,
       LASVE_DL_SEQNO,
       LASVE_DL_LOAD_STATUS,
       LASVE_LWRB_REFNO,
       LASVE_WRA_CURR_ASSESSMENT_REF,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LASVE_FLVE_CODE,
       NVL(LASVE_CREATED_BY, 'DATALOAD') LASVE_CREATED_BY,
       NVL(LASVE_CREATED_DATE, SYSDATE)  LASVE_CREATED_DATE
  FROM dl_hra_lwr_assess_val_errors
 WHERE lasve_dlb_batch_id   = p_batch_id
   AND lasve_dl_load_status = 'V';
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
ct                   VARCHAR2(30) := 'DL_HRA_LWR_ASSESS_VAL_ERRORS';
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
    execute immediate 'alter trigger ASVE_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hra_lwr_assess_val_errors.dataload_create');
    fsc_utils.debug_message('s_dl_hra_lwr_assess_val_errors.dataload_create',3);
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
          cs   := p1.lasve_dl_seqno;
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
           OPEN get_lwra_refno(p1.lasve_lwrb_refno,
                               p1.lasve_wra_curr_assessment_ref,
                               p1.lwra_rate_period_start_date,
                               p1.lwra_rate_period_end_date);
--
          FETCH get_lwra_refno INTO l_lwra_refno;
          CLOSE get_lwra_refno;
--
--
-- Insert into LWR_ASSESSMENT_LAV_ERRORS table
--
        INSERT /* +APPEND */ INTO  lwr_assessment_val_errors(ASVE_LWRA_REFNO,
                                                             ASVE_FLVE_CODE,
                                                             ASVE_CREATED_BY,
                                                             ASVE_CREATED_DATE
                                                            )
--
                                                     VALUES (l_lwra_refno,
                                                             p1.LASVE_FLVE_CODE,
                                                             p1.LASVE_CREATED_BY,
                                                             p1.LASVE_CREATED_DATE
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('LWR_ASSESSMENT_VAL_ERRORS');
--
    execute immediate 'alter trigger ASVE_BR_I enable';
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
       LASVE_DLB_BATCH_ID,
       LASVE_DL_SEQNO,
       LASVE_DL_LOAD_STATUS,
       LASVE_LWRB_REFNO,
       LASVE_WRA_CURR_ASSESSMENT_REF,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LASVE_FLVE_CODE,
       NVL(LASVE_CREATED_BY, 'DATALOAD') LASVE_CREATED_BY,
       NVL(LASVE_CREATED_DATE, SYSDATE)  LASVE_CREATED_DATE
  FROM dl_hra_lwr_assess_val_errors
 WHERE lasve_dlb_batch_id   = p_batch_id
   AND lasve_dl_load_status IN ('L','F','O');
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
CURSOR chk_val_exists(p_lwra_refno	NUMBER,
                      p_asve_flve_code  VARCHAR2)
IS
SELECT 'X'
  FROM lwr_assessment_val_errors
 WHERE asve_lwra_refno = p_lwra_refno
   AND asve_flve_code  = p_asve_flve_code;
--
-- ***********************************************************************
--
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_ASSESS_VAL_ERRORS';
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
l_flve_exists        VARCHAR2(1);
l_val_exists         VARCHAR2(1);

--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_lwr_assess_val_errors.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_lwr_assess_val_errors.dataload_validate',3);
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
          cs   := p1.lasve_dl_seqno;
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
           OPEN chk_batch_id(p1.lasve_lwrb_refno);
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
           OPEN get_lwra_refno(p1.lasve_lwrb_refno,
                               p1.lasve_wra_curr_assessment_ref,
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
-- Check Validation Error Code is valid
--
--
          l_flve_exists := NULL;
--
          IF (p1.lasve_flve_code IS NOT NULL) THEN
--
           IF (NOT s_dl_hem_utils.exists_frv('LWR_VAL_ERR',p1.lasve_flve_code,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',369);
           ELSE
              l_flve_exists := 'Y';
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check to see that a record doesn't already exist for lwra_refno, flve_code
-- combination.
--
          l_val_exists := NULL;
--
          IF (    l_lwra_refno  IS NOT NULL
              AND l_flve_exists = 'Y') THEN
--
            OPEN chk_val_exists(l_lwra_refno, p1.lasve_flve_code);
           FETCH chk_val_exists INTO l_val_exists;
           CLOSE chk_val_exists;
--
           IF (l_val_exists IS NOT NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',370);
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
       LASVE_DLB_BATCH_ID,
       LASVE_DL_SEQNO,
       LASVE_DL_LOAD_STATUS,
       LASVE_LWRB_REFNO,
       LASVE_WRA_CURR_ASSESSMENT_REF,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LASVE_FLVE_CODE,
       NVL(LASVE_CREATED_BY, 'DATALOAD') LASVE_CREATED_BY,
       NVL(LASVE_CREATED_DATE, SYSDATE)  LASVE_CREATED_DATE
  FROM dl_hra_lwr_assess_val_errors
 WHERE lasve_dlb_batch_id   = p_batch_id
   AND lasve_dl_load_status = 'C';
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
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_ASSESS_VAL_ERRORS';
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
l_exists             VARCHAR2(1);
i                    INTEGER :=0;
l_lwra_refno         NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_lwr_assess_val_errors.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_lwr_assess_val_errors.dataload_delete',3 );
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
          cs   := p1.lasve_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--
-- Get LWR Assessment reference
--
          l_lwra_refno := null;
--
           OPEN get_lwra_refno(p1.lasve_lwrb_refno,
                               p1.lasve_wra_curr_assessment_ref,
                               p1.lwra_rate_period_start_date,
                               p1.lwra_rate_period_end_date);
--
          FETCH get_lwra_refno INTO l_lwra_refno;
          CLOSE get_lwra_refno;
--
--
-- Delete from LWR_ASSESSMENT_VAL_ERRORS table
--
--
          DELETE
            FROM lwr_assessment_val_errors
           WHERE asve_lwra_refno = l_lwra_refno
             AND asve_flve_code  = p1.lasve_flve_code;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('LWR_ASSESSMENT_VAL_ERRORS');
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
END s_dl_hra_lwr_assess_val_errors;
/
