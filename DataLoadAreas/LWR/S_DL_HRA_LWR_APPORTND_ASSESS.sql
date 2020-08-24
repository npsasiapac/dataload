CREATE OR REPLACE PACKAGE s_dl_hra_lwr_apportnd_assess
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
END s_dl_hra_lwr_apportnd_assess;
--
/


CREATE OR REPLACE PACKAGE BODY s_dl_hra_lwr_apportnd_assess
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
    UPDATE dl_hra_lwr_apportnd_assess
       SET laas_dl_load_status = p_status
     WHERE rowid               = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hra_lwr_apportnd_assess');
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
       LAAS_DLB_BATCH_ID,
       LAAS_DL_SEQNO,
       LAAS_DL_LOAD_STATUS,
       LAAS_LWRB_REFNO,
       LAAS_WRA_CURR_ASSESSMENT_REF,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LAAS_PRO_PROPREF,
       LAAS_AMOUNT,
       NVL(LAAS_CREATED_BY, 'DATALOAD') LAAS_CREATED_BY,
       NVL(LAAS_CREATED_DATE, SYSDATE)  LAAS_CREATED_DATE
  FROM dl_hra_lwr_apportnd_assess
 WHERE laas_dlb_batch_id   = p_batch_id
   AND laas_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR get_lwra_refno(p_batch_id                   NUMBER,
                      p_wra_curr_assessment_ref    VARCHAR2,
                      p_wra_rate_period_start_date DATE,
                      p_wra_rate_period_end_date   DATE)
IS
SELECT lwra_refno
  FROM lwr_assessments
 WHERE lwra_lwrb_refno             = p_batch_id
   AND lwra_current_assessment_ref = p_wra_curr_assessment_ref
   AND lwra_rate_period_start_date = p_wra_rate_period_start_date
   AND lwra_rate_period_end_date   = p_wra_rate_period_end_date;
--
-- ***********************************************************************
--
CURSOR get_pro_refno(p_pro_propref VARCHAR2)
IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = p_pro_propref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_APPORTND_ASSESS';
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
l_pro_refno          NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger LAAS_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hra_lwr_apportnd_assess.dataload_create');
    fsc_utils.debug_message('s_dl_hra_lwr_apportnd_assess.dataload_create',3);
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
          cs   := p1.laas_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Get LWR Assessment reference
--
          l_lwra_refno := NULL;
--
           OPEN get_lwra_refno(p1.laas_lwrb_refno,
                               p1.laas_wra_curr_assessment_ref,
                               p1.lwra_rate_period_start_date,
                               p1.lwra_rate_period_end_date);
--
          FETCH get_lwra_refno INTO l_lwra_refno;
          CLOSE get_lwra_refno;
--
-- Get Property reference
--
          l_pro_refno := NULL;
--
           OPEN get_pro_refno(p1.laas_pro_propref);
--
          FETCH get_pro_refno INTO l_pro_refno;
          CLOSE get_pro_refno;
--
--
-- Insert into LWR_APPORTIONED_ASSESSMENTS table
--
        INSERT /* +APPEND */ INTO  lwr_apportioned_assessments(LAAS_LWRA_REFNO,
                                                               LAAS_PRO_REFNO,
                                                               LAAS_AMOUNT,
                                                               LAAS_CREATED_BY,
                                                               LAAS_CREATED_DATE
                                                              )
--
                                                       VALUES (l_lwra_refno,
                                                               l_pro_refno,
                                                               p1.LAAS_AMOUNT,
                                                               p1.LAAS_CREATED_BY,
                                                               p1.LAAS_CREATED_DATE
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('LWR_APPORTIONED_ASSESSMENTS');
--
    execute immediate 'alter trigger LAAS_BR_I enable';
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
       LAAS_DLB_BATCH_ID,
       LAAS_DL_SEQNO,
       LAAS_DL_LOAD_STATUS,
       LAAS_LWRB_REFNO,
       LAAS_WRA_CURR_ASSESSMENT_REF,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LAAS_PRO_PROPREF,
       LAAS_AMOUNT,
       NVL(LAAS_CREATED_BY, 'DATALOAD') LAAS_CREATED_BY,
       NVL(LAAS_CREATED_DATE, SYSDATE)  LAAS_CREATED_DATE
  FROM dl_hra_lwr_apportnd_assess
 WHERE laas_dlb_batch_id   = p_batch_id
   AND laas_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- Check Batch id doesn't already exist on lwr_batches
--
CURSOR chk_batch_id(p_batch_id	NUMBER)
IS
SELECT lwrb_refno
  FROM lwr_batches
 WHERE lwrb_refno = p_batch_id;
--
-- ***********************************************************************
--
CURSOR get_lwra_refno(p_batch_id                   NUMBER,
                      p_wra_curr_assessment_ref    VARCHAR2,
                      p_wra_rate_period_start_date DATE,
                      p_wra_rate_period_end_date   DATE)
IS
SELECT lwra_refno
  FROM lwr_assessments
 WHERE lwra_lwrb_refno             = p_batch_id
   AND lwra_current_assessment_ref = p_wra_curr_assessment_ref
   AND lwra_rate_period_start_date = p_wra_rate_period_start_date
   AND lwra_rate_period_end_date   = p_wra_rate_period_end_date;
--
-- ***********************************************************************
--
CURSOR get_pro_refno(p_pro_propref VARCHAR2)
IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = p_pro_propref;
--
-- ***********************************************************************
--
CURSOR chk_laas_exists(p_lwra_refno NUMBER,
                       p_pro_refno  NUMBER)
IS
SELECT 'X'
  FROM lwr_apportioned_assessments
 WHERE laas_lwra_refno = p_lwra_refno
   AND laas_pro_refno  = p_pro_refno;
--
-- ***********************************************************************
--
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_APPORTND_ASSESS';
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
l_pro_refno          NUMBER(10);
l_laas_exists        VARCHAR2(1);

--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_lwr_apportnd_assess.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_lwr_apportnd_assess.dataload_validate',3);
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
          cs   := p1.laas_dl_seqno;
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
           OPEN chk_batch_id(p1.laas_lwrb_refno);
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
          l_lwra_refno := NULL;
--
           OPEN get_lwra_refno(p1.laas_lwrb_refno,
                               p1.laas_wra_curr_assessment_ref,
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
-- Check Property Reference is valid
--
          l_pro_refno := NULL;
--
           OPEN get_pro_refno(p1.laas_pro_propref);
          FETCH get_pro_refno INTO l_pro_refno;
          CLOSE get_pro_refno;
--
          IF (l_pro_refno IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',591);
          END IF;
--
-- ***********************************************************************
--
-- Check to see if there is already a record for lwra_refno/pro_refno combination
-- on LWR_APPORTIONED_ASSESSMENTS
--
          l_laas_exists := NULL;
--
          IF (    l_lwra_refno IS NOT NULL
              AND l_pro_refno  IS NOT NULL) THEN
--
            OPEN chk_laas_exists(l_lwra_refno,
                                 l_pro_refno);
--
           FETCH chk_laas_exists into l_laas_exists;
           CLOSE chk_laas_exists;
--
           IF (l_laas_exists IS NOT NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',372);
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
       LAAS_DLB_BATCH_ID,
       LAAS_DL_SEQNO,
       LAAS_DL_LOAD_STATUS,
       LAAS_LWRB_REFNO,
       LAAS_WRA_CURR_ASSESSMENT_REF,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LAAS_PRO_PROPREF,
       LAAS_AMOUNT,
       NVL(LAAS_CREATED_BY, 'DATALOAD') LAAS_CREATED_BY,
       NVL(LAAS_CREATED_DATE, SYSDATE)  LAAS_CREATED_DATE
  FROM dl_hra_lwr_apportnd_assess
 WHERE laas_dlb_batch_id   = p_batch_id
   AND laas_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- ***********************************************************************
--
CURSOR get_lwra_refno(p_batch_id                   NUMBER,
                      p_wra_curr_assessment_ref    VARCHAR2,
                      p_wra_rate_period_start_date DATE,
                      p_wra_rate_period_end_date   DATE)
IS
SELECT lwra_refno
  FROM lwr_assessments
 WHERE lwra_lwrb_refno             = p_batch_id
   AND lwra_current_assessment_ref = p_wra_curr_assessment_ref
   AND lwra_rate_period_start_date = p_wra_rate_period_start_date
   AND lwra_rate_period_end_date   = p_wra_rate_period_end_date;
--
-- ***********************************************************************
--
CURSOR get_pro_refno(p_pro_propref VARCHAR2)
IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = p_pro_propref;
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_APPORTND_ASSESS';
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
--
l_lwra_refno         NUMBER(10);
l_pro_refno          NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_lwr_apportnd_assess.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_lwr_apportnd_assess.dataload_delete',3 );
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
          cs   := p1.laas_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--
-- Get LWR Assessment reference
--
          l_lwra_refno := NULL;
--
           OPEN get_lwra_refno(p1.laas_lwrb_refno,
                               p1.laas_wra_curr_assessment_ref,
                               p1.lwra_rate_period_start_date,
                               p1.lwra_rate_period_end_date);
--
          FETCH get_lwra_refno INTO l_lwra_refno;
          CLOSE get_lwra_refno;
--
-- Get Property reference
--
          l_pro_refno := NULL;
--
           OPEN get_pro_refno(p1.laas_pro_propref);
--
          FETCH get_pro_refno INTO l_pro_refno;
          CLOSE get_pro_refno;
--
--
-- Delete from LWR_APPORTIONED_ASSESSMENTS table
--
--
          DELETE
            FROM lwr_apportioned_assessments
           WHERE laas_lwra_refno = l_lwra_refno
             AND laas_pro_refno  = l_pro_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('LWR_APPORTIONED_ASSESSMENTS');
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
END s_dl_hra_lwr_apportnd_assess;
/
