--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_ics_deductions
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.7.0     VS   04-APR-2013  Initial Creation.
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
    UPDATE dl_hem_ics_deductions
       SET licd_dl_load_status = p_status
     WHERE rowid               = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hem_ics_deductions');
            RAISE;
--
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create(p_batch_id  IN VARCHAR2,
                          p_date      IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LICD_DLB_BATCH_ID,
       LICD_DL_SEQNO,
       LICD_DL_LOAD_STATUS,
       LICD_INDR_REFERENCE,
       LICD_IBP_HRV_IBEN_CODE,
       LICD_IBP_HRV_IPS_CODE,
       LICD_IBP_HRV_ICPT_CODE,
       LICD_TYPE,
       LICD_AMOUNT,
       LICD_HRV_ICDT_CODE,
       NVL(LICD_CREATED_BY, 'DATALOAD') LICD_CREATED_BY,
       NVL(LICD_CREATED_DATE, SYSDATE)  LICD_CREATED_DATE,
       LICD_DEDUCTION_DATE,
       LICD_REFNO
  FROM dl_hem_ics_deductions
 WHERE licd_dlb_batch_id   = p_batch_id
   AND licd_dl_load_status = 'V';
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
ct                   VARCHAR2(30) := 'DL_HEM_ICS_DEDUCTIONS';
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
i                    INTEGER := 0;
l_exists             VARCHAR2(1);
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_ics_deductions.dataload_create');
    fsc_utils.debug_message('s_dl_hem_ics_deductions.dataload_create',3);
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
          cs   := p1.licd_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
-- Now insert into ics_deductions
--
          INSERT /* +APPEND */ INTO  ics_deductions(ICD_REFNO,
                                                    ICD_TYPE,
                                                    ICD_INDR_REFNO,
                                                    ICD_HRV_IBEN_CODE,
                                                    ICD_AMOUNT,
                                                    ICD_HRV_ICDT_CODE,
                                                    ICD_DEDUCTION_DATE
                                                   )
--
                                            VALUES (p1.LICD_REFNO,
                                                    p1.LICD_TYPE,
                                                    p1.LICD_INDR_REFERENCE,
                                                    p1.LICD_IBP_HRV_IBEN_CODE,
                                                    p1.LICD_AMOUNT,
                                                    p1.LICD_HRV_ICDT_CODE,
                                                    p1.LICD_DEDUCTION_DATE
                                                   );
--
-- To replace the disabling of trigger ICD_BR_I
--
          UPDATE ics_deductions
             SET icd_created_by   = p1.LICD_CREATED_BY,
                 icd_created_date = p1.LICD_CREATED_DATE
           WHERE icd_refno = p1.licd_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ICS_DEDUCTIONS');
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
       LICD_DLB_BATCH_ID,
       LICD_DL_SEQNO,
       LICD_DL_LOAD_STATUS,
       LICD_INDR_REFERENCE,
       LICD_IBP_HRV_IBEN_CODE,
       LICD_IBP_HRV_IPS_CODE,
       LICD_IBP_HRV_ICPT_CODE,
       LICD_TYPE,
       LICD_AMOUNT,
       LICD_HRV_ICDT_CODE,
       LICD_DEDUCTION_DATE,
       LICD_REFNO
  FROM dl_hem_ics_deductions
 WHERE licd_dlb_batch_id   = p_batch_id
   AND licd_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR chk_indr_ref_exists(p_indr_reference   NUMBER) 
IS
SELECT 'X'
  FROM income_detail_requests
 WHERE indr_refno = p_indr_reference;
--
-- ***********************************************************************
--
CURSOR get_ibp_refno(p_indr_reference   NUMBER,
                     p_hrv_iben_code    VARCHAR2,
                     p_hrv_ips_code     VARCHAR2,
                     p_hrv_icpt_code    VARCHAR2) 
IS
SELECT ibp_refno
  FROM ics_benefit_payments
 WHERE ibp_indr_refno    = p_indr_reference
   AND ibp_hrv_iben_code = p_hrv_iben_code
   AND ibp_hrv_ips_code  = p_hrv_ips_code
   AND ibp_hrv_icpt_code = p_hrv_icpt_code;
--
-- ***********************************************************************
--
CURSOR chk_ibp_exists(p_ibp_refno       NUMBER,
                      p_indr_reference  NUMBER,
                      p_hrv_iben_code   VARCHAR2) 
IS
SELECT 'X'
  FROM ics_benefit_payments
 WHERE ibp_refno         = p_ibp_refno
   AND ibp_indr_refno    = p_indr_reference
   AND ibp_hrv_iben_code = p_hrv_iben_code;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb               VARCHAR2(30);
cd               DATE;
cp               VARCHAR2(30) := 'VALIDATE';
ct               VARCHAR2(30) := 'DL_HEM_ICS_DEDUCTIONS';
cs               INTEGER;
ce               VARCHAR2(200);
l_id             ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_icd_ref_exists         VARCHAR2(1);
l_ibp_ref_exists         VARCHAR2(1);
l_ibp_exists             VARCHAR2(1);
l_indr_ref_exists        VARCHAR2(1);
l_inf_exists             VARCHAR2(1);
--
l_icdt_valid             VARCHAR2(1);
--
l_ibp_iben_valid         VARCHAR2(1);
l_ibp_ips_valid          VARCHAR2(1);
l_ibp_icpt_valid         VARCHAR2(1);
--
l_ibp_refno              NUMBER(10);
--
l_errors                 VARCHAR2(10);
l_error_ind              VARCHAR2(10);
i                        INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_ics_deductions.dataload_validate');
    fsc_utils.debug_message('s_dl_hem_ics_deductions.dataload_validate',3);
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
          cs   := p1.licd_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
          l_icdt_valid     := 'Y';
--
          l_ibp_iben_valid := 'Y';
          l_ibp_ips_valid  := 'Y';
          l_ibp_icpt_valid := 'Y';
--
-- ***********************************************************************
--
--
-- Income Request Detail Reference has been supplied and is valid
--
          IF (p1.licd_indr_reference IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',327);
          ELSE
--
             l_indr_ref_exists := NULL;
--
              OPEN chk_indr_ref_exists(p1.licd_indr_reference);
             FETCH chk_indr_ref_exists INTO l_indr_ref_exists;
             CLOSE chk_indr_ref_exists;
--
             IF (l_indr_ref_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',333);
             END IF;
--
          END IF;
--
--
-- ***********************************************************************
--
-- Check Benefit Code has been supplied and is Valid
--
--
          IF (p1.licd_ibp_hrv_iben_code IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',559);
          ELSE
--
             IF (NOT s_dl_hem_utils.exists_frv('ICSBENEFIT',p1.licd_ibp_hrv_iben_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',378);
              l_ibp_iben_valid := 'N';
             END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check Benefit Payment Status has been supplied and is Valid
--
--
          IF (p1.licd_ibp_hrv_ips_code IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',560);
          ELSE
--
             IF (NOT s_dl_hem_utils.exists_frv('ICSPAYSTATUS',p1.licd_ibp_hrv_ips_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',379);
              l_ibp_ips_valid := 'N';
             END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check Benefit Payment Type has been supplied and is Valid
--
          IF (p1.licd_ibp_hrv_icpt_code IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',561);
          ELSE
--
             IF (NOT s_dl_hem_utils.exists_frv('ICSPAYTYPE',p1.licd_ibp_hrv_icpt_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',380);
              l_ibp_icpt_valid := 'N';
             END IF;
--
          END IF;
--
--
-- ***********************************************************************
--
-- Type Indicator must be valid
--
          IF (p1.licd_type IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',568);
          ELSE
--
             IF (p1.licd_type NOT IN ('TICD','DICD')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',396);
             END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check Deduction Amount has been supplied
--
          IF (p1.licd_amount IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',570);
          END IF;
--
-- ***********************************************************************
--
-- Check Deduction Type Code has been supplied and is Valid
--
--
          IF (p1.licd_hrv_icdt_code IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',571);
          ELSE
--
             IF (NOT s_dl_hem_utils.exists_frv('ICSDEDUCTTYPE',p1.licd_hrv_icdt_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',395);
              l_icdt_valid := 'N';
             END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check Deduction Date is only supplied when the Type Indicator is 'DICD'
--
          IF (    p1.licd_type           != 'DICD'
              AND p1.licd_deduction_date IS NOT NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',397);
--
          END IF;
--
-- ***********************************************************************
--
-- Check deduction date is only supplied when the Type Indicator is 'DIBP'
--
          IF (    p1.licd_type            = 'DICD'
              AND p1.licd_deduction_date IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',398);
--
          END IF;
--
-- ***********************************************************************
--
-- Check there is an entry in ics_benefit_payments for Detail
-- Request Reference, Benefit Code, Payment Status and Payment Type.
--
          IF (    l_ibp_iben_valid   = 'Y'
              AND l_ibp_ips_valid    = 'Y'
              AND l_ibp_icpt_valid   = 'Y'
              AND l_indr_ref_exists IS NOT NULL) THEN
--
           l_ibp_refno := NULL;
--
            OPEN get_ibp_refno(p1.licd_indr_reference,
                               p1.licd_ibp_hrv_iben_code,
                               p1.licd_ibp_hrv_ips_code,
                               p1.licd_ibp_hrv_icpt_code);
--
           FETCH get_ibp_refno INTO l_ibp_refno;
           CLOSE get_ibp_refno;
--
           IF (l_ibp_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',400);
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
                          p_date           IN date) 
IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LICD_DLB_BATCH_ID,
       LICD_DL_SEQNO,
       LICD_DL_LOAD_STATUS,
       LICD_REFNO
  FROM dl_hem_ics_deductions
 WHERE licd_dlb_batch_id   = p_batch_id
   AND licd_dl_load_status = 'C';
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
ct               VARCHAR2(30) := 'DL_HEM_ICS_DEDUCTIONS';
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
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_ics_deductions.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_ics_deductions.dataload_delete',3 );
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
          cs   := p1.licd_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Delete from tables
--
          DELETE 
            FROM ics_deductions
           WHERE icd_refno = p1.licd_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ICS_DEDUCTIONS');
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
END s_dl_hem_ics_deductions;
/