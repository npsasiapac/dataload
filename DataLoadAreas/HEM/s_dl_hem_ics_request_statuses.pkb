--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_ics_request_statuses
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.7.0     VS   18-MAR-2013  Initial Creation.
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
    UPDATE dl_hem_ics_request_statuses
       SET lirs_dl_load_status = p_status
     WHERE rowid               = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hem_ics_request_statuses');
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
CURSOR pre_process
IS
SELECT rowid rec_rowid,
       LIRS_DLB_BATCH_ID,
       LIRS_DL_SEQNO,
       LIRS_DL_LOAD_STATUS,
       LIRS_INDR_REFERENCE,
       LIRS_SCO_CODE,
       NVL(LIRS_CREATED_DATE, SYSDATE)  LIRS_CREATED_DATE,
       NVL(LIRS_CREATED_BY, 'DATALOAD') LIRS_CREATED_BY,
       LIRS_HRV_IEC_CODE,
       LIRS_ERROR_TEXT,
       LIRS_CURRENT_STATUS_IND
  FROM dl_hem_ics_request_statuses
 WHERE lirs_dlb_batch_id        = p_batch_id
   AND lirs_dl_load_status      = 'V'
   AND lirs_sco_code           IN ('PER','UNA')
   AND lirs_current_status_ind  = 'Y';
--
-- ***********************************************************************
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LIRS_DLB_BATCH_ID,
       LIRS_DL_SEQNO,
       LIRS_DL_LOAD_STATUS,
       LIRS_INDR_REFERENCE,
       LIRS_SCO_CODE,
       NVL(LIRS_CREATED_DATE, SYSDATE)  LIRS_CREATED_DATE,
       NVL(LIRS_CREATED_BY, 'DATALOAD') LIRS_CREATED_BY,
       LIRS_HRV_IEC_CODE,
       LIRS_ERROR_TEXT,
       LIRS_CURRENT_STATUS_IND
  FROM dl_hem_ics_request_statuses
 WHERE lirs_dlb_batch_id   = p_batch_id
   AND lirs_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HEM_ICS_REQUEST_STATUSES';
cs                   INTEGER;
ce	                 VARCHAR2(200);
l_id                 ROWID;
l_an_tab             VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
i                    INTEGER := 0;
i2                   INTEGER := 0;
l_exists             VARCHAR2(1);
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_ics_request_statuses.dataload_create');
    fsc_utils.debug_message('s_dl_hem_ics_request_statuses.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
--
--
-- Pre process: Create a SEN status first all current PRE/UNA records.
--
--
    s_dl_utils.update_process_summary(cb,cp,cd,'PRE PROCESS');
--
    FOR pre IN pre_process LOOP
--
      BEGIN
--
-- Now insert into ics_request_statuses
--
          INSERT /* +APPEND */ INTO  ics_request_statuses(irs_indr_refno,
                                                          irs_sco_code,
                                                          irs_hrv_iec_code,
                                                          irs_error_text
                                                         )
--
                                                  VALUES (pre.lirs_indr_reference,
                                                          'SEN',
                                                          NULL,
                                                          NULL
                                                         );
--
-- Maintain CREATED BY/DATE
-- 
          UPDATE ics_request_statuses
             SET irs_created_date = pre.lirs_created_date - (1/1440),
                 irs_created_by   = pre.lirs_created_by
           WHERE irs_indr_refno = pre.lirs_indr_reference
             AND irs_sco_code   = 'SEN';
--
          i2 := i2+1;
--
          IF MOD(i2,50000) = 0 THEN
           COMMIT;
          END IF;
--
      END;
--
    END LOOP;
--
    COMMIT;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs   := p1.lirs_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
-- Now insert into ics_request_statuses
--
          INSERT /* +APPEND */ INTO  ics_request_statuses(irs_indr_refno,
                                                          irs_sco_code,
                                                          irs_hrv_iec_code,
                                                          irs_error_text
                                                         )
--
                                                  VALUES (p1.lirs_indr_reference,
                                                          p1.lirs_sco_code,
                                                          p1.lirs_hrv_iec_code,
                                                          p1.lirs_error_text
                                                         );
--
-- Maintain CREATED BY/DATE
-- 
          UPDATE ics_request_statuses
             SET irs_created_date = p1.lirs_created_date,
                 irs_created_by   = p1.lirs_created_by
           WHERE irs_indr_refno = p1.lirs_indr_reference
             AND irs_sco_code   = p1.lirs_sco_code;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ICS_REQUEST_STATUSES');
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
PROCEDURE dataload_validate(p_batch_id       IN VARCHAR2,
                            p_date           IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LIRS_DLB_BATCH_ID,
       LIRS_DL_SEQNO,
       LIRS_DL_LOAD_STATUS,
       LIRS_INDR_REFERENCE,
       LIRS_SCO_CODE,
       NVL(LIRS_CREATED_DATE, SYSDATE)  LIRS_CREATED_DATE,
       NVL(LIRS_CREATED_BY, 'DATALOAD') LIRS_CREATED_BY,
       LIRS_HRV_IEC_CODE,
       LIRS_ERROR_TEXT,
       LIRS_CURRENT_STATUS_IND
  FROM dl_hem_ics_request_statuses
 WHERE lirs_dlb_batch_id    = p_batch_id
   AND lirs_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR c_chk_indr_ref_exists(p_indr_reference   NUMBER) 
IS
SELECT 'X',
       indr_sco_code
  FROM income_detail_requests
 WHERE indr_refno = p_indr_reference;
--
-- ***********************************************************************
--
CURSOR c_chk_irs_exists(p_indr_reference    NUMBER,
                        p_irs_sco_code      VARCHAR2,
                        p_irs_created_date  DATE) 
IS
SELECT 'X'
  FROM ics_request_statuses
 WHERE irs_indr_refno   = p_indr_reference
   AND irs_sco_code     = p_irs_sco_code
   AND irs_created_date = p_irs_created_date;
--
-- ***********************************************************************
--
CURSOR c_chk_iec_code_exists(p_iec_code  VARCHAR2) 
IS
SELECT 'X'
  FROM first_ref_values
 WHERE frv_code        = p_iec_code
   AND frv_frd_domain  = 'ICSERRORCODE'
   AND frv_current_ind = 'Y';
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb               VARCHAR2(30);
cd               DATE;
cp               VARCHAR2(30) := 'VALIDATE';
ct               VARCHAR2(30) := 'DL_HEM_ICS_REQUEST_STATUSES';
cs               INTEGER;
ce               VARCHAR2(200);
l_id             ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_iec_exists       VARCHAR2(1);
l_indr_ref_exists  VARCHAR2(1);
l_indr_sco_code    VARCHAR2(3);
l_irs_exists       VARCHAR2(1);
l_errors           VARCHAR2(10);
l_error_ind        VARCHAR2(10);
i                  INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_ics_request_statuses.dataload_validate');
    fsc_utils.debug_message('s_dl_hem_ics_request_statuses.dataload_validate',3);
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
          cs   := p1.lirs_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';

--
-- ***********************************************************************
--
-- Income Request Detail Reference has been supplied and is valid
--
          IF (p1.lirs_indr_reference IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',327);
          ELSE
--
             l_indr_ref_exists := NULL;
--
              OPEN c_chk_indr_ref_exists(p1.lirs_indr_reference);
             FETCH c_chk_indr_ref_exists INTO l_indr_ref_exists, l_indr_sco_code;
             CLOSE c_chk_indr_ref_exists;
--
             IF (l_indr_ref_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',333);
             ELSE
--
                IF (l_indr_sco_code = 'RAI') THEN
                 l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',550); 
                END IF;
--
             END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check that there isn't already a record for Income Detail Request Reference,
-- ICS Request Statuses status code and created date combination
--
          IF (    p1.lirs_indr_reference IS NOT NULL
              AND p1.lirs_sco_code       IS NOT NULL
              AND p1.lirs_created_date   IS NOT NULL) THEN
--
           l_irs_exists := NULL;
--
            OPEN c_chk_irs_exists(p1.lirs_indr_reference, p1.lirs_sco_code, p1.lirs_created_date);
           FETCH c_chk_irs_exists INTO l_irs_exists;
           CLOSE c_chk_irs_exists;
--
           IF (l_irs_exists IS NOT NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',552);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Validate Request Status Code
--
          IF (p1.lirs_sco_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',554);
--
          ELSIF NVL(p1.lirs_sco_code, '^*#') NOT IN ('RAI','SEN','UNA','COM','TBC','CAN','ERR','CRS','RNC','CAF','PER') THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',36);
--
          END IF;
--
-- ***********************************************************************
--
-- If supplied the Request Status Error Code must exist in first ref values for domain ICSERRORCODE
--
          IF (p1.lirs_hrv_iec_code IS NOT NULL) THEN
--
            OPEN c_chk_iec_code_exists(p1.lirs_hrv_iec_code);
           FETCH c_chk_iec_code_exists INTO l_iec_exists;
--
           IF (c_chk_iec_code_exists%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',551);
           END IF;
--
           CLOSE c_chk_iec_code_exists;
--
          END IF;
--
-- ***********************************************************************
--
-- Check Error Reason Code is not null when status is 'ERR'
--
          IF (    p1.lirs_sco_code      = 'ERR'
              AND p1.lirs_hrv_iec_code IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',221);
--
          END IF;
--
-- ***********************************************************************
--
-- Check Error Text is not populated when the status code <> 'ERR'
--
          IF (    p1.lirs_hrv_iec_code IS NULL
              AND p1.lirs_error_text   IS NOT NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',222);
--
          END IF;
--
-- ***********************************************************************
--
-- Check the Current Status Indicator has been supplied and is valid
--
          IF (p1.lirs_current_status_ind IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',553);
--
          ELSIF (p1.lirs_current_status_ind NOT IN ('Y','N')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',402);
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
                          p_date           IN date) 
IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LIRS_DLB_BATCH_ID,
       LIRS_DL_SEQNO,
       LIRS_DL_LOAD_STATUS,
       LIRS_INDR_REFERENCE,
       LIRS_SCO_CODE,
       LIRS_CREATED_DATE,
       LIRS_CURRENT_STATUS_IND
  FROM dl_hem_ics_request_statuses
 WHERE lirs_dlb_batch_id   = p_batch_id
   AND lirs_dl_load_status = 'C';
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
cb               VARCHAR2(30);
cd               DATE;
cp               VARCHAR2(30) := 'DELETE';
ct               VARCHAR2(30) := 'DL_HEM_ICS_REQUEST_STATUSES';
cs               INTEGER;
ce               VARCHAR2(200);
l_id             ROWID;
l_an_tab         VARCHAR2(1);
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
    fsc_utils.proc_start('s_dl_hem_ics_request_statuses.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_ics_request_statuses.dataload_delete',3 );
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
          cs   := p1.lirs_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Delete from tables
--
          DELETE 
            FROM ics_request_statuses
           WHERE irs_indr_refno   = p1.lirs_indr_reference
             AND irs_sco_code     = p1.lirs_sco_code
             AND irs_created_date = p1.lirs_created_date;
--
-- Now Delete the SEN Status created for the current PER/UNA record.
--
          IF (    p1.lirs_current_status_ind  = 'Y'
              AND p1.lirs_sco_code           IN ('PER','UNA')) THEN
--
           DELETE 
             FROM ics_request_statuses
            WHERE irs_indr_refno   = p1.lirs_indr_reference
              AND irs_sco_code     = 'SEN'
              AND irs_created_date = p1.lirs_created_date - 1/1440
              AND irs_created_by   = 'DATALOAD';
--
          END IF;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ICS_REQUEST_STATUSES');
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
END s_dl_hem_ics_request_statuses;
/