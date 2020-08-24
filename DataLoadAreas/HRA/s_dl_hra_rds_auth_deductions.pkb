--
CREATE OR REPLACE PACKAGE BODY s_dl_hra_rds_auth_deductions
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    PH   15-JAN-2009  Initial Creation.
--
--  2.0     5.15.0    VS   04-JUN-2009  Updated and tidied code. This is
--                                      now the updated split version
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
    UPDATE dl_hra_rds_auth_deductions
       SET lraud_dl_load_status = p_status
     WHERE rowid                = p_rowid;
  --
    EXCEPTION
         WHEN OTHERS THEN
         dbms_output.put_line('Error updating status of dl_hra_rds_auth_deductions');
         RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
-- ***********************************************************************
--
PROCEDURE dataload_create(p_batch_id IN VARCHAR2,
                          p_date     IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LRAUD_DLB_BATCH_ID,
       LRAUD_DL_SEQNO,
       LRAUD_DL_LOAD_STATUS,
       LRAUD_RDSA_HA_REFERENCE,
       LRAUD_START_DATE,
       LRAUD_HRV_DEDT_CODE,
       LRAUD_CURRENT_SCO_CODE,
       LRAUD_STATUS_DATE,
       NVL(LRAUD_CREATED_BY, 'DATALOAD') LRAUD_CREATED_BY,
       NVL(LRAUD_CREATED_DATE, SYSDATE)  LRAUD_CREATED_DATE,
       LRAUD_HRV_RBEG_CODE,
       LRAUD_PENDING_SCO_CODE,
       LRAUD_END_DATE,
       LRAUD_SUSPEND_FROM_DATE,
       LRAUD_SUSPEND_TO_DATE,
       LRAUD_ACTION_SENT_DATETIME,
       LRAUD_NEXT_PAY_DATE,
       LRAUD_HRV_SUSR_CODE,
       LRAUD_HRV_TERR_CODE,
       LRAUD_REFNO
  FROM dl_hra_rds_auth_deductions
 WHERE lraud_dlb_batch_id   = p_batch_id
   AND lraud_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR c_get_rdsa_refno(p_ha_reference  VARCHAR2) 
IS
SELECT rdsa_refno
  FROM rds_authorities
 WHERE rdsa_ha_reference = p_ha_reference;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_AUTH_DEDUCTIONS';
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
i                          INTEGER := 0;
l_exists                   VARCHAR2(1);
l_rdsa_refno               rds_authorities.rdsa_refno%type;
--
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_rds_auth_deductions.dataload_create');
    fsc_utils.debug_message('s_dl_hra_rds_auth_deductions.dataload_create',3);
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
          cs   := p1.lraud_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
          l_rdsa_refno := NULL;
--
           OPEN c_get_rdsa_refno(p1.lraud_rdsa_ha_reference);
          FETCH c_get_rdsa_refno INTO l_rdsa_refno;
          CLOSE c_get_rdsa_refno;
--
-- Insert into 
--
          INSERT INTO rds_authorised_deductions(raud_rdsa_refno,
                                                raud_refno,
                                                raud_start_date,
                                                raud_hrv_dedt_code,
                                                raud_current_sco_code,
                                                raud_status_date,
                                                raud_created_by,          
                                                raud_created_date,
                                                raud_hrv_rbeg_code,
                                                raud_pending_sco_code,
                                                raud_end_date,
                                                raud_suspended_from_date,
                                                raud_suspended_to_date,
                                                raud_action_sent_datetime,
                                                raud_next_pay_date,
                                                raud_hrv_susr_code,
                                                raud_hrv_terr_code
                                               )
--
                                        VALUES (l_rdsa_refno,
                                                p1.lraud_refno,
                                                p1.lraud_start_date,
                                                p1.lraud_hrv_dedt_code,
                                                p1.lraud_current_sco_code,
                                                p1.lraud_status_date,
                                                p1.lraud_created_by,
                                                p1.lraud_created_date,
                                                p1.lraud_hrv_rbeg_code,
                                                p1.lraud_pending_sco_code,
                                                p1.lraud_end_date,
                                                p1.lraud_suspend_from_date,
                                                p1.lraud_suspend_to_date,
                                                p1.lraud_action_sent_datetime,
                                                p1.lraud_next_pay_date,
                                                p1.lraud_hrv_susr_code,
                                                p1.lraud_hrv_terr_code
                                               );
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
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
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_AUTHORISED_DEDUCTIONS');
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
PROCEDURE dataload_validate(p_batch_id IN VARCHAR2,
                            p_date     IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LRAUD_DLB_BATCH_ID,
       LRAUD_DL_SEQNO,
       LRAUD_DL_LOAD_STATUS,
       LRAUD_RDSA_HA_REFERENCE,
       LRAUD_START_DATE,
       LRAUD_HRV_DEDT_CODE,
       LRAUD_CURRENT_SCO_CODE,
       LRAUD_STATUS_DATE,
       NVL(LRAUD_CREATED_BY, 'DATALOAD') LRAUD_CREATED_BY,
       NVL(LRAUD_CREATED_DATE, SYSDATE)  LRAUD_CREATED_DATE,
       LRAUD_HRV_RBEG_CODE,
       LRAUD_PENDING_SCO_CODE,
       LRAUD_END_DATE,
       LRAUD_SUSPEND_FROM_DATE,
       LRAUD_SUSPEND_TO_DATE,
       LRAUD_ACTION_SENT_DATETIME,
       LRAUD_NEXT_PAY_DATE,
       LRAUD_HRV_SUSR_CODE,
       LRAUD_HRV_TERR_CODE,
       LRAUD_REFNO
  FROM dl_hra_rds_auth_deductions
 WHERE lraud_dlb_batch_id    = p_batch_id
   AND lraud_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR c_get_rdsa_refno(p_ha_reference  VARCHAR2) 
IS
SELECT rdsa_refno, rdsa_start_date, rdsa_end_date
  FROM rds_authorities
 WHERE rdsa_ha_reference = p_ha_reference;
--
-- ***********************************************************************
--
CURSOR c_check_raud_exists(p_rdsa_refno    NUMBER,
                           p_dedt_code     VARCHAR2,
                           p_start_date    DATE,
                           p_rbeg_code     VARCHAR2) 
IS
SELECT 'X'
  FROM rds_authorised_deductions
 WHERE raud_rdsa_refno    = p_rdsa_refno
   AND raud_hrv_dedt_code = p_dedt_code
   AND raud_start_date    = p_start_date
   AND raud_hrv_rbeg_code = p_rbeg_code;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_AUTH_DEDUCTIONS';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists                   VARCHAR2(1);
l_errors                   VARCHAR2(10);
l_error_ind                VARCHAR2(10);
i                          INTEGER :=0;
l_rdsa_refno               rds_authorities.rdsa_refno%type;
l_rdsa_start_date          rds_authorities.rdsa_start_date%type;
l_rdsa_end_date            rds_authorities.rdsa_end_date%type;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_rds_auth_deductions.dataload_validate');
    fsc_utils.debug_message( 's_dl_hra_rds_auth_deductions.dataload_validate',3);
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
          cs   := p1.lraud_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
--
-- ******************************************************************************
--
-- Validation checks required
--
-- Check the Authority Ref has been supplied and is exists
--
          IF (p1.lraud_rdsa_ha_reference IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',97);
--
          ELSE
-- 
             l_rdsa_refno      := NULL;
             l_rdsa_start_date := NULL;
             l_rdsa_end_date   := NULL;
--
              OPEN c_get_rdsa_refno(p1.lraud_rdsa_ha_reference);
             FETCH c_get_rdsa_refno INTO l_rdsa_refno, l_rdsa_start_date, l_rdsa_end_date;
             CLOSE c_get_rdsa_refno;
--
             IF (l_rdsa_refno IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',99);
             END IF;
--
          END IF;
--
--
-- ******************************************************************************
--
-- Authorised Deduction Start Date has been supplied
--
          IF (p1.lraud_start_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',289);
          END IF;
--
--
-- ******************************************************************************
--
-- Check that the Authorised Deduction Start Date is not earlier than the 
-- Authority Start Date
--
          IF (    p1.lraud_start_date IS NOT NULL
              AND l_rdsa_start_date   IS NOT NULL) THEN
--
           IF p1.lraud_start_date < nvl(l_rdsa_start_date, p1.lraud_start_date) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',301);
           END IF;
--
          END IF;
--
--
-- ******************************************************************************
--
-- Check the Deduction Type has been supplied and is valid
--
          IF (p1.lraud_hrv_dedt_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',290);
--
          ELSIF (NOT s_dl_hem_utils.exists_frv('RDS_DED_TYPE',p1.lraud_hrv_dedt_code,'N')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',103);
--
          END IF;
--
--
-- ******************************************************************************
--
-- Check Current Status Code has been supplied and is valid
--
          IF (p1.lraud_current_sco_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',248);
--
          ELSIF nvl(p1.lraud_current_sco_code, '^~#') NOT IN ( 'PND', 'CON', 'ACT', 'ERR','SUS', 'TRM', 'CAN' ) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',101);
--
          END IF;
--
--
-- ******************************************************************************
--
-- Check Status Date has been supplied
--
          IF (p1.lraud_status_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',16);
          END IF;
--
--
-- ******************************************************************************
--
-- Check that the Benefit Group has been supplied and is valid
--
          IF (p1.lraud_hrv_rbeg_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',300);
--
          ELSIF (NOT s_dl_hem_utils.exists_frv('RDS_BEN_GRP',p1.lraud_hrv_rbeg_code,'N')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',104);
--
          END IF;
--
--
-- ******************************************************************************
--
-- Check that the Pending Status Code is valid if supplied
--
          IF (p1.lraud_pending_sco_code IS NOT NULL) THEN
--
           IF p1.lraud_pending_sco_code NOT IN ('VAR', 'SUS', 'TRM','NEW') THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',102);
           END IF;
--
          END IF;
--
--
-- ******************************************************************************
--
-- If supplied the Authorised Deduction End Date is not earlier than the 
-- Authorised Deduction Start Date
--
-- If Supplied check that the Authorised Deduction End Date is not later than the 
-- Authority End Date
--
--
          IF (p1.lraud_end_date IS NOT NULL) THEN
--
           IF p1.lraud_end_date < nvl(p1.lraud_start_date, p1.lraud_end_date) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',3);
           END IF;
--
           IF p1.lraud_end_date > nvl(l_rdsa_end_date, p1.lraud_end_date) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',302);
           END IF;
--
          END IF;
--
--
-- ******************************************************************************
--
-- If supplied check the Suspension from Start Date is not earlier than start date
--
          IF (p1.lraud_suspend_from_date IS NOT NULL) THEN
--
           IF p1.lraud_suspend_from_date < nvl(p1.lraud_start_date, p1.lraud_suspend_from_date) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',93);
           END IF;
--
          END IF;
--
--
-- ******************************************************************************
--
-- If supplied the Suspension End Date is not earlier than Suspension start date
--
          IF (p1.lraud_suspend_to_date IS NOT NULL) THEN
--
           IF p1.lraud_suspend_to_date < nvl(p1.lraud_start_date, p1.lraud_suspend_from_date) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',92);
           END IF;
--
          END IF;
--
--
-- ******************************************************************************
--
-- Check unique combination
-- (RDS Authority, Deduction Type, Start Date and Benefit Group Code)
--
--
           OPEN c_check_raud_exists( l_rdsa_refno, p1.lraud_hrv_dedt_code, p1.lraud_start_date, p1.lraud_hrv_rbeg_code );
          FETCH c_check_raud_exists INTO l_exists;
--
          IF (c_check_raud_exists%FOUND) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',100);
          END IF;
--
          CLOSE c_check_raud_exists;
--
--
-- ******************************************************************************
--
-- Reference Values
--
-- ***************************
--
-- If Supplied check Suspension Reasons is valid
--
          IF (p1.lraud_hrv_susr_code IS NOT NULL) THEN
--
           IF (NOT s_dl_hem_utils.exists_frv('RDS_SUS_RSN',p1.lraud_hrv_susr_code,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',95);
           END IF;
--
          END IF;
--
-- ***************************
--
-- If supplied check Termination Reason is valid
--
          IF (p1.lraud_hrv_susr_code IS NOT NULL) THEN
--
           IF (NOT s_dl_hem_utils.exists_frv('RDS_TERM_RSN',p1.lraud_hrv_terr_code,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',96);
           END IF;
--
          END IF;
--
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
PROCEDURE dataload_delete(p_batch_id IN VARCHAR2,
                          p_date     IN date) IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       lraud_dlb_batch_id,
       lraud_dl_seqno,
       lraud_dl_load_status,
       lraud_refno
  FROM dl_hra_rds_auth_deductions
 WHERE lraud_dlb_batch_id    = p_batch_id
   AND lraud_dl_load_status  = 'C';
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
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_AUTH_DEDUCTIONS';
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
    fsc_utils.proc_start('s_dl_hra_rds_auth_deductions.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_rds_auth_deductions.dataload_delete',3 );
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
          cs   := p1.lraud_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Delete from rds_authorised_deductions
--
          DELETE 
            FROM rds_authorised_deductions
           WHERE raud_refno = p1.lraud_refno;
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
           commit; 
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_AUTHORISED_DEDUCTIONS');
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
END s_dl_hra_rds_auth_deductions;
/