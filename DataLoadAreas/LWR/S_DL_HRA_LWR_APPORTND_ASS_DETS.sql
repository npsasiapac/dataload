CREATE OR REPLACE PACKAGE s_dl_hra_lwr_apportnd_ass_dets
AS
  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VER  DB Ver   WHO  WHEN          WHY
  --  1.0  5.17.0   VRS  30-AUG-2010   INITIAL Version
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
END s_dl_hra_lwr_apportnd_ass_dets;
--
/


CREATE OR REPLACE PACKAGE BODY s_dl_hra_lwr_apportnd_ass_dets
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.17.0    VS   31-AUG-2010  Initial Creation.
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
    UPDATE dl_hra_lwr_apportnd_ass_dets
       SET laad_dl_load_status = p_status
     WHERE rowid               = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hra_lwr_apportnd_ass_dets');
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
       LAAD_DLB_BATCH_ID,
       LAAD_DL_SEQNO,
       LAAD_DL_LOAD_STATUS,
       LAAD_REFNO,
       LAAD_LWRB_REFNO,
       LAAD_WRA_CURR_ASSESSMENT_REF,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LAAD_PRO_PROPREF,
       LAAD_AMOUNT,
       NVL(LAAD_CREATED_BY, 'DATALOAD') LAAD_CREATED_BY,
       NVL(LAAD_CREATED_DATE, SYSDATE)  LAAD_CREATED_DATE,
       LAAD_RCCO_CODE,
       LWAUD_BILL_PERIOD_START_DATE,
       LWAUD_BILL_PERIOD_END_DATE,
       LAAD_RAAM_SEQ_NO
  FROM dl_hra_lwr_apportnd_ass_dets
 WHERE laad_dlb_batch_id   = p_batch_id
   AND laad_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR get_lwra_refno(p_lwra_lwrb_refno            NUMBER,
                      p_wra_curr_assessment_ref    VARCHAR2,
                      p_wra_rate_period_start_date DATE,
                      p_wra_rate_period_end_date   DATE)
IS
SELECT lwra_refno
  FROM lwr_assessments
 WHERE lwra_lwrb_refno             = p_lwra_lwrb_refno
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
CURSOR get_waud_refno(p_waud_lwra_refno             NUMBER,
                      p_waud_rcco_code              VARCHAR2,
                      p_waud_bill_period_start_date DATE,
                      p_waud_bill_period_end_date   DATE)
IS
SELECT waud_refno
  FROM water_usage_details
 WHERE waud_lwra_refno             = p_waud_lwra_refno
   AND waud_rcco_code              = p_waud_rcco_code
   AND waud_bill_period_start_date = p_waud_bill_period_start_date
   AND waud_bill_period_end_date   = p_waud_bill_period_end_date;
--
-- ***********************************************************************
--
CURSOR get_lrad_refno(p_lrad_raam_seq_no NUMBER)
IS
SELECT lrad_refno
  FROM dl_hra_lwr_rate_assess_dets
 WHERE lrad_dl_load_status = 'C'
   AND lrad_raam_seq_no    = p_lrad_raam_seq_no;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_APPORTND_ASS_DETS';
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
l_waud_refno         NUMBER(10);
l_lrad_refno         NUMBER(10);
l_pro_refno          NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_lwr_apportnd_ass_dets.dataload_create');
    fsc_utils.debug_message('s_dl_hra_lwr_apportnd_ass_dets.dataload_create',3);
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
          cs   := p1.laad_dl_seqno;
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
           OPEN get_lwra_refno(p1.laad_lwrb_refno,
                               p1.laad_wra_curr_assessment_ref,
                               p1.lwra_rate_period_start_date,
                               p1.lwra_rate_period_end_date);
--
          FETCH get_lwra_refno INTO l_lwra_refno;
          CLOSE get_lwra_refno;
--
--
--
-- Get Property reference
--
          l_pro_refno := NULL;
--
           OPEN get_pro_refno(p1.laad_pro_propref);
--
          FETCH get_pro_refno INTO l_pro_refno;
          CLOSE get_pro_refno;
--
--
--
-- Get Water Usage Details reference
--
          l_waud_refno := NULL;
--
          IF (    l_lwra_refno                    IS NOT NULL
              AND p1.LAAD_RCCO_CODE               IS NOT NULL
              AND p1.LWAUD_BILL_PERIOD_START_DATE IS NOT NULL
              AND p1.LWAUD_BILL_PERIOD_END_DATE   IS NOT NULL) THEN
--
            OPEN get_waud_refno(l_lwra_refno,
                                p1.laad_rcco_code,
                                p1.lwaud_bill_period_start_date,
                                p1.lwaud_bill_period_end_date);
--
           FETCH get_waud_refno INTO l_waud_refno;
           CLOSE get_waud_refno;
--
          END IF;
--
--
--
-- Get Rate Assessment Details reference
--
          l_lrad_refno := NULL;
--
          IF (p1.LAAD_RAAM_SEQ_NO IS NOT NULL) THEN
--
            OPEN get_lrad_refno(p1.laad_raam_seq_no);
--
           FETCH get_lrad_refno INTO l_lrad_refno;
           CLOSE get_lrad_refno;
--
          END IF;
--
--
--
-- Insert into LWR_APPORTIONED_ASSESS_DETS table
--
        INSERT INTO lwr_apportioned_assess_dets(LAAD_REFNO,
                                                LAAD_LWRA_REFNO,
                                                LAAD_PRO_REFNO,
                                                LAAD_AMOUNT,
                                                LAAD_CREATED_DATE,
                                                LAAD_CREATED_BY,
                                                LAAD_RCCO_CODE,
                                                LAAD_WAUD_REFNO,
                                                LAAD_LRAD_REFNO
                                               )
--
                                        VALUES (p1.LAAD_REFNO,
                                                l_lwra_refno,
                                                l_pro_refno,
                                                p1.LAAD_AMOUNT,
                                                p1.LAAD_CREATED_DATE,
                                                p1.LAAD_CREATED_BY,
                                                p1.LAAD_RCCO_CODE,
                                                l_waud_refno,
                                                l_lrad_refno
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('LWR_APPORTIONED_ASSESS_DETS');
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
       LAAD_DLB_BATCH_ID,
       LAAD_DL_SEQNO,
       LAAD_DL_LOAD_STATUS,
       LAAD_REFNO,
       LAAD_LWRB_REFNO,
       LAAD_WRA_CURR_ASSESSMENT_REF,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LAAD_PRO_PROPREF,
       LAAD_AMOUNT,
       NVL(LAAD_CREATED_BY, 'DATALOAD') LAAD_CREATED_BY,
       NVL(LAAD_CREATED_DATE, SYSDATE)  LAAD_CREATED_DATE,
       LAAD_RCCO_CODE,
       LWAUD_BILL_PERIOD_START_DATE,
       LWAUD_BILL_PERIOD_END_DATE,
       LAAD_RAAM_SEQ_NO
  FROM dl_hra_lwr_apportnd_ass_dets
 WHERE laad_dlb_batch_id   = p_batch_id
   AND laad_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- Check Batch id exists on lwr_batches
--
CURSOR chk_batch_id(p_lwrb_refno NUMBER)
IS
SELECT lwrb_refno
  FROM lwr_batches
 WHERE lwrb_refno = p_lwrb_refno;
--
-- ***********************************************************************
--
-- Check Assessment exists on lwr_assessments
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
CURSOR get_pro_refno(p_pro_propref VARCHAR2)
IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = p_pro_propref;
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
-- Check Water Usage Details record exists
--
CURSOR get_waud_refno(p_waud_lwra_refno             NUMBER,
                      p_waud_rcco_code              VARCHAR2,
                      p_waud_bill_period_start_date DATE,
                      p_waud_bill_period_end_date   DATE)
IS
SELECT waud_refno
  FROM water_usage_details
 WHERE waud_lwra_refno             = p_waud_lwra_refno
   AND waud_rcco_code              = p_waud_rcco_code
   AND waud_bill_period_start_date = p_waud_bill_period_start_date
   AND waud_bill_period_end_date   = p_waud_bill_period_end_date;
--
-- ***********************************************************************
--
-- Check Rate Assessment Details record exists
--
CURSOR get_lrad_refno(p_lrad_raam_seq_no NUMBER)
IS
SELECT lrad_refno
  FROM dl_hra_lwr_rate_assess_dets
 WHERE lrad_dl_load_status = 'C'
   AND lrad_raam_seq_no    = p_lrad_raam_seq_no;
--
-- ***********************************************************************
--
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_APPORTND_ASS_DETS';
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
l_waud_refno         NUMBER(10);
l_lrad_refno         NUMBER(10);

--
l_laas_exists        VARCHAR2(1);
l_rcco_exists        VARCHAR2(1);

--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_lwr_apportnd_ass_dets.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_lwr_apportnd_ass_dets.dataload_validate',3);
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
          cs   := p1.laad_dl_seqno;
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
           OPEN chk_batch_id(p1.laad_lwrb_refno);
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
           OPEN get_lwra_refno(p1.laad_lwrb_refno,
                               p1.laad_wra_curr_assessment_ref,
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
           OPEN get_pro_refno(p1.laad_pro_propref);
          FETCH get_pro_refno INTO l_pro_refno;
          CLOSE get_pro_refno;
--
          IF (l_pro_refno IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',591);
          END IF;
--
-- ***********************************************************************
--
-- Check Rate Category Code is valid
--
          l_rcco_exists := NULL;
--
           OPEN chk_rcco_exists(p1.laad_rcco_code);
          FETCH chk_rcco_exists INTO l_rcco_exists;
          CLOSE chk_rcco_exists;
--
          IF (l_rcco_exists IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',363);
          END IF;
--
-- ***********************************************************************
--
-- Get Water Usage Details reference
--
          l_waud_refno := NULL;
--
          IF (    l_lwra_refno                    IS NOT NULL
              AND l_rcco_exists                   IS NOT NULL
              AND p1.LWAUD_BILL_PERIOD_START_DATE IS NOT NULL
              AND p1.LWAUD_BILL_PERIOD_END_DATE   IS NOT NULL) THEN
--
            OPEN get_waud_refno(l_lwra_refno,
                                p1.laad_rcco_code,
                                p1.lwaud_bill_period_start_date,
                                p1.lwaud_bill_period_end_date);
--
           FETCH get_waud_refno INTO l_waud_refno;
           CLOSE get_waud_refno;
--
           IF (l_waud_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',406);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Get Rate Assessment Details reference
--
          l_lrad_refno := NULL;
--
          IF (p1.LAAD_RAAM_SEQ_NO IS NOT NULL) THEN
--
            OPEN get_lrad_refno(p1.laad_raam_seq_no);
--
           FETCH get_lrad_refno INTO l_lrad_refno;
           CLOSE get_lrad_refno;
--
           IF (l_lrad_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',407);
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
       LAAD_DLB_BATCH_ID,
       LAAD_DL_SEQNO,
       LAAD_DL_LOAD_STATUS,
       LAAD_REFNO
  FROM dl_hra_lwr_apportnd_ass_dets
 WHERE laad_dlb_batch_id   = p_batch_id
   AND laad_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_APPORTND_ASS_DETS';
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
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_lwr_apportnd_ass_dets.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_lwr_apportnd_ass_dets.dataload_delete',3 );
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
          cs   := p1.laad_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--
-- Delete from LWR_APPORTIONED_ASSESS_DETS table
--
--
          DELETE
            FROM lwr_apportioned_assess_dets
           WHERE laad_refno = p1.laad_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('LWR_APPORTIONED_ASSESS_DETS');
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
END s_dl_hra_lwr_apportnd_ass_dets;
/
