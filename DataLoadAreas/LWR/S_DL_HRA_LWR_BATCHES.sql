CREATE OR REPLACE PACKAGE s_dl_hra_lwr_batches
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
END s_dl_hra_lwr_batches;
--
/


CREATE OR REPLACE PACKAGE BODY s_dl_hra_lwr_batches
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.17.0    VS   27-APR-2010  Initial Creation.
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
    UPDATE dl_hra_lwr_batches
       SET lwrb_dl_load_status = p_status
     WHERE rowid               = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hra_lwr_batches');
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
       LWRB_DLB_BATCH_ID,
       LWRB_DL_SEQNO,
       LWRB_DL_LOAD_STATUS,
       LWRB_REFNO,
       LWRB_BATCH_TYPE,
       LWRB_SCO_CODE,
       LWRB_LOAD_FROM_FILE_IND,
       NVL(LWRB_CREATED_BY, 'DATALOAD') LWRB_CREATED_BY,
       NVL(LWRB_CREATED_DATE, SYSDATE)  LWRB_CREATED_DATE,
       LWRB_IPP_SHORTNAME,
       LWRB_IPT_CODE,
       LWRB_AUTHORITY_BATCH_DATE,
       LWRB_AUTHORITY_BATCH_NUMBER,
       LWRB_AUTH_RECEIPT_NUMBER,
       LWRB_FILENAME,
       LWRB_LOADED_DATE,
       LWRB_LARS_FLRS_CODE,
       LWRB_LARS_YEAR,
       LWRB_INSTALMENT_NUMBER,
       LWRB_LWRB_REFNO,
       LWRB_CLAIM_TYPE,
       LWRB_CLAIM_DESCRIPTION,
       LWRB_CLOSED_BY,
       LWRB_CLOSED_DATE,
       LWRB_APPROVED_BY,
       LWRB_APPROVED_DATE,
       LWRB_CHEQUE_DATE,
       LWRB_CHEQUE_NUMBER,
       LWRB_DOCUMENT_DATE,
       LWRB_DOCUMENT_NUMBER,
       LWRB_REOPENED_BY,
       LWRB_REOPENED_DATE,
       LWRB_CANCELLED_BY,
       LWRB_CANCELLED_DATE,
       LWRB_FBCR_CODE,
       LWRB_MODIFIED_BY,
       LWRB_MODIFIED_DATE
  FROM dl_hra_lwr_batches
 WHERE lwrb_dlb_batch_id   = p_batch_id
   AND lwrb_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- Get Interested Party Reference Number
--
CURSOR c_get_ipp_refno(p_ipp_shortname  VARCHAR2,
                       p_ipp_ipt_code   VARCHAR2)
IS
SELECT ipp_refno
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_shortname
   AND ipp_ipt_code  = p_ipp_ipt_code;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_BATCHES';
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
l_ipp_refno          NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger LWRB_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hra_lwr_batches.dataload_create');
    fsc_utils.debug_message('s_dl_hra_lwr_batches.dataload_create',3);
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
          cs   := p1.lwrb_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
          l_ipp_refno := NULL;
--
-- Main processing
--
-- Open any cursors
--
           OPEN c_get_ipp_refno(p1.LWRB_IPP_SHORTNAME,
                                p1.LWRB_IPT_CODE);
--
          FETCH c_get_ipp_refno INTO l_ipp_refno;
          CLOSE c_get_ipp_refno;
--
--
-- Insert into LWR_BATCHES table
--
        INSERT /* +APPEND */ INTO  lwr_batches(LWRB_REFNO,
                                               LWRB_BATCH_TYPE,
                                               LWRB_SCO_CODE,
                                               LWRB_LOAD_FROM_FILE_IND,
                                               LWRB_CREATED_BY,
                                               LWRB_CREATED_DATE,
                                               LWRB_IPP_REFNO,
                                               LWRB_AUTHORITY_BATCH_DATE,
                                               LWRB_AUTHORITY_BATCH_NUMBER,
                                               LWRB_AUTH_RECEIPT_NUMBER,
                                               LWRB_FILENAME,
                                               LWRB_LOADED_DATE,
                                               LWRB_LARS_FLRS_CODE,
                                               LWRB_LARS_YEAR,
                                               LWRB_INSTALMENT_NUMBER,
                                               LWRB_LWRB_REFNO,
                                               LWRB_CLAIM_TYPE,
                                               LWRB_CLAIM_DESCRIPTION,
                                               LWRB_CLOSED_BY,
                                               LWRB_CLOSED_DATE,
                                               LWRB_APPROVED_BY,
                                               LWRB_APPROVED_DATE,
                                               LWRB_CHEQUE_DATE,
                                               LWRB_CHEQUE_NUMBER,
                                               LWRB_DOCUMENT_DATE,
                                               LWRB_DOCUMENT_NUMBER,
                                               LWRB_REOPENED_BY,
                                               LWRB_REOPENED_DATE,
                                               LWRB_CANCELLED_BY,
                                               LWRB_CANCELLED_DATE,
                                               LWRB_FBCR_CODE,
                                               LWRB_MODIFIED_BY,
                                               LWRB_MODIFIED_DATE
                                              )
--
                                       VALUES (p1.LWRB_REFNO,
                                               p1.LWRB_BATCH_TYPE,
                                               p1.LWRB_SCO_CODE,
                                               p1.LWRB_LOAD_FROM_FILE_IND,
                                               p1.LWRB_CREATED_BY,
                                               p1.LWRB_CREATED_DATE,
                                               l_ipp_refno,
                                               p1.LWRB_AUTHORITY_BATCH_DATE,
                                               p1.LWRB_AUTHORITY_BATCH_NUMBER,
                                               p1.LWRB_AUTH_RECEIPT_NUMBER,
                                               p1.LWRB_FILENAME,
                                               p1.LWRB_LOADED_DATE,
                                               p1.LWRB_LARS_FLRS_CODE,
                                               p1.LWRB_LARS_YEAR,
                                               p1.LWRB_INSTALMENT_NUMBER,
                                               p1.LWRB_LWRB_REFNO,
                                               p1.LWRB_CLAIM_TYPE,
                                               p1.LWRB_CLAIM_DESCRIPTION,
                                               p1.LWRB_CLOSED_BY,
                                               p1.LWRB_CLOSED_DATE,
                                               p1.LWRB_APPROVED_BY,
                                               p1.LWRB_APPROVED_DATE,
                                               p1.LWRB_CHEQUE_DATE,
                                               p1.LWRB_CHEQUE_NUMBER,
                                               p1.LWRB_DOCUMENT_DATE,
                                               p1.LWRB_DOCUMENT_NUMBER,
                                               p1.LWRB_REOPENED_BY,
                                               p1.LWRB_REOPENED_DATE,
                                               p1.LWRB_CANCELLED_BY,
                                               p1.LWRB_CANCELLED_DATE,
                                               p1.LWRB_FBCR_CODE,
                                               p1.LWRB_MODIFIED_BY,
                                               p1.LWRB_MODIFIED_DATE
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('LWR_BATCHES');
--
    execute immediate 'alter trigger LWRB_BR_I enable';
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
       LWRB_DLB_BATCH_ID,
       LWRB_DL_SEQNO,
       LWRB_DL_LOAD_STATUS,
       LWRB_REFNO,
       LWRB_BATCH_TYPE,
       LWRB_SCO_CODE,
       LWRB_LOAD_FROM_FILE_IND,
       NVL(LWRB_CREATED_BY, 'DATALOAD') LWRB_CREATED_BY,
       NVL(LWRB_CREATED_DATE, SYSDATE)  LWRB_CREATED_DATE,
       LWRB_IPP_SHORTNAME,
       LWRB_IPT_CODE,
       LWRB_AUTHORITY_BATCH_DATE,
       LWRB_AUTHORITY_BATCH_NUMBER,
       LWRB_AUTH_RECEIPT_NUMBER,
       LWRB_FILENAME,
       LWRB_LOADED_DATE,
       LWRB_LARS_FLRS_CODE,
       LWRB_LARS_YEAR,
       LWRB_INSTALMENT_NUMBER,
       LWRB_LWRB_REFNO,
       LWRB_CLAIM_TYPE,
       LWRB_CLAIM_DESCRIPTION,
       LWRB_CLOSED_BY,
       LWRB_CLOSED_DATE,
       LWRB_APPROVED_BY,
       LWRB_APPROVED_DATE,
       LWRB_CHEQUE_DATE,
       LWRB_CHEQUE_NUMBER,
       LWRB_DOCUMENT_DATE,
       LWRB_DOCUMENT_NUMBER,
       LWRB_REOPENED_BY,
       LWRB_REOPENED_DATE,
       LWRB_CANCELLED_BY,
       LWRB_CANCELLED_DATE,
       LWRB_FBCR_CODE,
       LWRB_MODIFIED_BY,
       LWRB_MODIFIED_DATE
  FROM dl_hra_lwr_batches
 WHERE lwrb_dlb_batch_id   = p_batch_id
   AND lwrb_dl_load_status IN ('L','F','O');
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
CURSOR chk_ipt_exists(p_ipt_code  VARCHAR2)
IS
SELECT 'X'
  FROM interested_party_types
 WHERE ipt_code = p_ipt_code;
--
-- ***********************************************************************
--
-- Check Interested Party exists
--
CURSOR chk_ipp_refno(p_ipp_shortname  VARCHAR2,
                     p_ipp_ipt_code   VARCHAR2)
IS
SELECT ipp_refno
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_shortname
   AND ipp_ipt_code  = p_ipp_ipt_code;
--
-- ***********************************************************************
--
-- Check Reference does not exist
--
CURSOR chk_lwr_annual_rates_sched(p_lars_flrs_code    VARCHAR2,
                                  p_lars_year         NUMBER)
IS
SELECT 'X'
  FROM lwr_annual_rates_schedules
 WHERE lars_flrs_code = p_lars_flrs_code
   AND lars_year      = p_lars_year;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_BATCHES';
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
l_lwrb_lwrb_refno    NUMBER(10);
l_ipp_refno          NUMBER(10);
l_ipt_exists         VARCHAR2(1);
l_lars_exists        VARCHAR2(1);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_lwr_batches.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_lwr_batches.dataload_validate',3);
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
          cs   := p1.lwrb_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
-- ***********************************************************************
--
-- Validation checks required
--
-- Check batch_id doesn't already exist
--
          l_lwrb_refno := NULL;
--
           OPEN chk_batch_id(p1.lwrb_refno);
          FETCH chk_batch_id INTO l_lwrb_refno;
          CLOSE chk_batch_id;
--
          IF (l_lwrb_refno IS NOT NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',339);
          END IF;
--
-- ***********************************************************************
--
-- Check Batch Type is valid
--
           IF (NOT s_dl_hem_utils.exists_frv('LWR_BATCH_TYPE',p1.lwrb_batch_type,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',340);
           END IF;
--
-- ***********************************************************************
--
-- Check Batch Status is valid
--
          IF (p1.lwrb_sco_code NOT IN ('NEW','LOD','LVF','LVA','CLO','APP','CAN','PAD')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',341);
          END IF;
--
-- ***********************************************************************
--
-- Check Load from File Indicator is valid
--
          IF (p1.lwrb_load_from_file_ind NOT IN ('Y','N')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',342);
          END IF;
--
-- ***********************************************************************
--
-- Check Interested Party ShortName LWRB_IPP_SHORTNAME/LWRB_IPT_CODE is valid if supplied
--
-- 1) Check lwrb_ipt_code has been supplied if lwrb_ipp_shortname has been supplied
-- 2) Check lwrb_ipt_code is valid if supplied
-- 3) check combination of lwrb_ipp_shortname/lwrb_ipt_code exists on interested_parties table.
--
--
          IF (    p1.lwrb_ipp_shortname IS NOT NULL
              AND p1.lwrb_ipt_code      IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',329);
--
          END IF;
--
--
          IF (p1.lwrb_ipt_code IS NOT NULL) THEN
--
           l_ipt_exists := NULL;
--
            OPEN chk_ipt_exists (p1.lwrb_ipt_code);
           FETCH chk_ipt_exists INTO l_ipt_exists;
           CLOSE chk_ipt_exists;
--
           IF (l_ipt_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',330);
           END IF;
--
          END IF;
--
--
          IF (    p1.lwrb_ipp_shortname IS NOT NULL
              AND l_ipt_exists          IS NOT NULL) THEN

           l_ipp_refno := NULL;
--
            OPEN chk_ipp_refno(p1.lwrb_ipp_shortname,
                               p1.lwrb_ipt_code);
--
           FETCH chk_ipp_refno INTO l_ipp_refno;
           CLOSE chk_ipp_refno;
--
           IF (l_ipp_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',343);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- If supplied, Validate Rates Schedule Code and Rates Schedule Year. Must exist
-- in LWR_ANNUAL_RATES_SCHEDULES table
--
          l_lars_exists := NULL;
--
          IF (    p1.lwrb_lars_flrs_code IS NOT NULL
              AND p1.lwrb_lars_year      IS NOT NULL) THEN
--
            OPEN chk_lwr_annual_rates_sched(p1.lwrb_lars_flrs_code,
                                            p1.lwrb_lars_year);
--
           FETCH chk_lwr_annual_rates_sched INTO l_lars_exists;
           CLOSE chk_lwr_annual_rates_sched;
--
           IF (l_lars_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',344);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- If the Batch type is ANN or INS then make sure values are provided for
-- LWRB_LARS_FLRS_CODE, LWRB_LARS_YEAR, LWRB_INSTALMENT_NUMBER, LWRB_LWRB_REFNO,
--
          IF (p1.lwrb_batch_type IN ('ANN','INS')) THEN
--
           IF (    p1.LWRB_LARS_FLRS_CODE    IS NULL
               AND p1.LWRB_LARS_YEAR         IS NULL
               AND p1.LWRB_INSTALMENT_NUMBER IS NULL
               AND p1.LWRB_LWRB_REFNO        IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',345);
--
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- IF Annual Batch Instalment Reference is supplied make sure it exists in LWR_BATCHES
--
          l_lwrb_lwrb_refno := NULL;
--
          IF (p1.lwrb_lwrb_refno IS NOT NULL) THEN
--
            OPEN chk_batch_id(p1.lwrb_lwrb_refno);
           FETCH chk_batch_id INTO l_lwrb_lwrb_refno;
           CLOSE chk_batch_id;
--
           IF (l_lwrb_lwrb_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',346);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- IF cancelled by/date is provided then cancellation reason must be supplied
--
          IF (    p1.lwrb_cancelled_by   IS NOT NULL
              AND p1.lwrb_cancelled_date IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',347);
--
          ELSIF (    p1.lwrb_cancelled_by   IS NULL
                 AND p1.lwrb_cancelled_date IS NOT NULL) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',348);
--
          END IF;
--
--
          IF (    p1.lwrb_cancelled_by   IS NOT NULL
              AND p1.lwrb_cancelled_date IS NOT NULL) THEN
--
           IF (p1.lwrb_fbcr_code IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',349);
           END IF;
--
          END IF;
--
--
          IF (p1.lwrb_fbcr_code IS NOT NULL) THEN
--
           IF (   p1.lwrb_cancelled_by   IS NULL
               OR p1.lwrb_cancelled_date IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',350);
--
           END IF;
--
          END IF;
--
--
          IF (p1.lwrb_fbcr_code IS NOT NULL) THEN
--
           IF (NOT s_dl_hem_utils.exists_frv('LWR_BATCH_CAN_RSN',p1.lwrb_fbcr_code,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',351);
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
       LWRB_DLB_BATCH_ID,
       LWRB_DL_SEQNO,
       LWRB_DL_LOAD_STATUS,
       LWRB_REFNO
  FROM dl_hra_lwr_batches
 WHERE lwrb_dlb_batch_id   = p_batch_id
   AND lwrb_dl_load_status = 'C';
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
ct                   VARCHAR2(30) := 'DL_HRA_LWR_BATCHES';
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
l_raud_refno     NUMBER(10);
l_rdtf_refno     NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_lwr_batches.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_lwr_batches.dataload_delete',3 );
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
          cs   := p1.lwrb_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--
-- Delete from LWR_BATCHES table
--
--
          DELETE
            FROM lwr_batches
           WHERE lwrb_refno = p1.lwrb_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('LWR_BATCHES');
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
END s_dl_hra_lwr_batches;
/
