--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_ics_benefit_payments
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.7.0     VS   28-MAR-2013  Initial Creation.
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
    UPDATE dl_hem_ics_benefit_payments
       SET libp_dl_load_status = p_status
     WHERE rowid               = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hem_ics_benefit_payments');
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
       LIBP_DLB_BATCH_ID,
       LIBP_DL_SEQNO,
       LIBP_DL_LOAD_STATUS,
       LIBP_INDR_REFERENCE,
       LIBP_HRV_IBEN_CODE,
       LIBP_HRV_IPS_CODE,
       LIBP_HRV_ICPT_CODE,
       LIBP_TYPE,
       LIBP_ACTUAL_AMOUNT,
       LIBP_CANCELLED_IND,
       NVL(LIBP_CREATED_BY,'DATALOAD') LIBP_CREATED_BY,
       NVL(LIBP_CREATED_DATE,SYSDATE)  LIBP_CREATED_DATE,
       LIBP_INF_CODE,
       LIBP_GRANT_DATE,
       LIBP_LEGISLATIVE_AMOUNT,
       LIBP_MAX_RATE_IND,
       LIBP_PAYMENT_DATE,
       LIBP_NUM_OF_PAID_DAYS,
       LIBP_REFNO
  FROM dl_hem_ics_benefit_payments
 WHERE libp_dlb_batch_id   = p_batch_id
   AND libp_dl_load_status = 'V';
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
ct                   VARCHAR2(30) := 'DL_HEM_ICS_BENEFIT_PAYMENTS';
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
l_exists             VARCHAR2(1);
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_ics_benefit_payments.dataload_create');
    fsc_utils.debug_message('s_dl_hem_ics_benefit_payments.dataload_create',3);
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
          cs   := p1.libp_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
-- Now insert into ics_benefit_payments
--
          INSERT /* +APPEND */ INTO  ics_benefit_payments(IBP_REFNO,
                                                          IBP_INDR_REFNO,
                                                          IBP_HRV_IBEN_CODE,
                                                          IBP_HRV_IPS_CODE,
                                                          IBP_HRV_ICPT_CODE,
                                                          IBP_TYPE,
                                                          IBP_ACTUAL_AMOUNT,
                                                          IBP_CANCELLED_IND,
                                                          IBP_INF_CODE,
                                                          IBP_GRANT_DATE,
                                                          IBP_LEGISLATIVE_AMOUNT,
                                                          IBP_MAX_RATE_IND,
                                                          IBP_PAYMENT_DATE,
                                                          IBP_NUM_OF_PAID_DAYS
                                                         )
--
                                                  VALUES (p1.LIBP_REFNO,
                                                          p1.LIBP_INDR_REFERENCE,
                                                          p1.LIBP_HRV_IBEN_CODE,
                                                          p1.LIBP_HRV_IPS_CODE,
                                                          p1.LIBP_HRV_ICPT_CODE,
                                                          p1.LIBP_TYPE,
                                                          p1.LIBP_ACTUAL_AMOUNT,
                                                          p1.LIBP_CANCELLED_IND,
                                                          p1.LIBP_INF_CODE,
                                                          p1.LIBP_GRANT_DATE,
                                                          p1.LIBP_LEGISLATIVE_AMOUNT,
                                                          p1.LIBP_MAX_RATE_IND,
                                                          p1.LIBP_PAYMENT_DATE,
                                                          p1.LIBP_NUM_OF_PAID_DAYS
                                                         );
--
-- To replace the disabling of trigger IBP_BR_I
--
          UPDATE ics_benefit_payments
             SET ibp_created_by   = p1.LIBP_CREATED_BY,
                 ibp_created_date = p1.LIBP_CREATED_DATE
           WHERE ibp_refno = p1.libp_refno;
--      
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ICS_BENEFIT_PAYMENTS');
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
       LIBP_DLB_BATCH_ID,
       LIBP_DL_SEQNO,
       LIBP_DL_LOAD_STATUS,
       LIBP_INDR_REFERENCE,
       LIBP_HRV_IBEN_CODE,
       LIBP_HRV_IPS_CODE,
       LIBP_HRV_ICPT_CODE,
       LIBP_TYPE,
       LIBP_ACTUAL_AMOUNT,
       LIBP_CANCELLED_IND,
       LIBP_INF_CODE,
       LIBP_GRANT_DATE,
       LIBP_LEGISLATIVE_AMOUNT,
       LIBP_MAX_RATE_IND,
       LIBP_PAYMENT_DATE,
       LIBP_NUM_OF_PAID_DAYS,
       LIBP_REFNO
  FROM dl_hem_ics_benefit_payments
 WHERE libp_dlb_batch_id    = p_batch_id
   AND libp_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR chk_ibp_ref_exists(p_ibp_refno   NUMBER) 
IS
SELECT 'X'
  FROM ics_benefit_payments
 WHERE ibp_refno = p_ibp_refno;
--
-- ***********************************************************************
--
CURSOR chk_indr_ref_exists(p_indr_reference   NUMBER) 
IS
SELECT 'X'
  FROM income_detail_requests
 WHERE indr_refno = p_indr_reference;
--
-- ***********************************************************************
--
CURSOR chk_inf_code_exists(p_inf_code   VARCHAR2) 
IS
SELECT 'X'
  FROM income_frequencies
 WHERE inf_code = p_inf_code;
--
-- ***********************************************************************
--
CURSOR chk_ibp_exists(p_indr_reference   NUMBER,
                      p_hrv_iben_code    VARCHAR2,
                      p_hrv_ips_code     VARCHAR2,
                      p_hrv_icpt_code    VARCHAR2) 
IS
SELECT 'X'
  FROM ics_benefit_payments
 WHERE ibp_indr_refno    = p_indr_reference
   AND ibp_hrv_iben_code = p_hrv_iben_code
   AND ibp_hrv_ips_code  = p_hrv_ips_code
   AND ibp_hrv_icpt_code = p_hrv_icpt_code;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb               VARCHAR2(30);
cd               DATE;
cp               VARCHAR2(30) := 'VALIDATE';
ct               VARCHAR2(30) := 'DL_HEM_ICS_BENEFIT_PAYMENTS';
cs               INTEGER;
ce               VARCHAR2(200);
l_id             ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_iity_exists            VARCHAR2(1);
l_ibp_ref_exists         VARCHAR2(1);
l_indr_ref_exists        VARCHAR2(1);
l_inf_exists             VARCHAR2(1);
--
l_iben_valid             VARCHAR2(1);
l_ips_valid              VARCHAR2(1);
l_icpt_valid             VARCHAR2(1);
--
l_ibp_exists      	 VARCHAR2(1);
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
    fsc_utils.proc_start('s_dl_hem_ics_benefit_payments.dataload_validate');
    fsc_utils.debug_message('s_dl_hem_ics_benefit_payments.dataload_validate',3);
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
          cs   := p1.libp_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
          l_iben_valid := 'Y';
          l_ips_valid  := 'Y';
          l_icpt_valid := 'Y';
--
-- ***********************************************************************
--
-- Income Request Detail Reference has been supplied and is valid
--
          IF (p1.libp_indr_reference IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',327);
          ELSE
--
             l_indr_ref_exists := NULL;
--
              OPEN chk_indr_ref_exists(p1.libp_indr_reference);
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
          IF (p1.libp_hrv_iben_code IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',559);
          ELSE
--
             IF (NOT s_dl_hem_utils.exists_frv('ICSBENEFIT',p1.libp_hrv_iben_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',378);
              l_iben_valid := 'N';
             END IF;
--
          END IF;   
--
-- ***********************************************************************
--
-- Check Benefit Payment Status has been supplied and is Valid
--
--
          IF (p1.libp_hrv_ips_code IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',560);
          ELSE
--
             IF (NOT s_dl_hem_utils.exists_frv('ICSPAYSTATUS',p1.libp_hrv_ips_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',379);
              l_ips_valid := 'N';
             END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check Benefit Payment Type has been supplied and is Valid
--
          IF (p1.libp_hrv_icpt_code IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',561);
          ELSE
--
             IF (NOT s_dl_hem_utils.exists_frv('ICSPAYTYPE',p1.libp_hrv_icpt_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',380);
              l_icpt_valid := 'N';
             END IF;
--
          END IF;
--
--
-- ***********************************************************************
--
-- Benefit Payment Type Indicator must be supplied and valid
--
          IF (p1.libp_type IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',562);
--
          ELSIF (p1.libp_type NOT IN ('TIBP','DIBP')) THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',381);
          END IF;
--
-- ***********************************************************************
--
-- Actual Amount must be supplied
--
          IF (p1.libp_actual_amount IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',563);
          END IF;

--
-- ***********************************************************************
--
-- Cancelled Indicator must be supplied and valid
--
          IF (p1.libp_cancelled_ind IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',564);
--
          ELSIF (p1.libp_cancelled_ind NOT IN ('Y','N')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',382);
--
          END IF;
--
-- ***********************************************************************
--
-- Check income frequency is only supplied when the Type Indicator is 'DIBP'
--
          IF (    p1.libp_type     != 'DIBP'
              AND p1.libp_inf_code IS NOT NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',383);
--
          END IF;
--
-- ***********************************************************************
--
-- Check income frequency is supplied when the Type Indicator is 'DIBP'
--
          IF (p1.libp_type      = 'DIBP') THEN
--
           IF (p1.libp_inf_code IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',565);
--
           ELSE
--
               OPEN chk_inf_code_exists(p1.libp_inf_code);
              FETCH chk_inf_code_exists INTO l_inf_exists;
              CLOSE chk_inf_code_exists;
--
              IF (l_inf_exists IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',337);
              END IF;
--
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Max Rate Indicator must be valid
--
          IF (p1.libp_max_rate_ind NOT IN ('Y','N')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',384);
          END IF;
--
-- ***********************************************************************
--
-- Check Payment Date is only supplied when the Type Indicator is 'DIBP'
--
          IF (    p1.libp_type         != 'DIBP'
              AND p1.libp_payment_date IS NOT NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',385);
--
          END IF;
--
-- ***********************************************************************
--
-- Check Payment Date is supplied when the Type Indicator is 'DIBP'
--
          IF (    p1.libp_type          = 'DIBP'
              AND p1.libp_payment_date IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',386);
--
          END IF;
--
-- ***********************************************************************
--
-- Check Number of Paid Days is only supplied when the Type Indicator is 'DIBP'
--
          IF (    p1.libp_type             != 'DIBP'
              AND p1.libp_num_of_paid_days IS NOT NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',387);
--
          END IF;
--
-- ***********************************************************************
--
-- Check there isn't already an entry in ics_benefit_payments for Detail
-- Request Reference, Benefit Code, Payment Status and Payment Type.
--
          IF (    l_iben_valid       = 'Y'
              AND l_ips_valid        = 'Y'
              AND l_icpt_valid       = 'Y'
              AND l_indr_ref_exists IS NOT NULL) THEN
--
           l_ibp_exists := NULL;
--
            OPEN chk_ibp_exists(p1.libp_indr_reference,
                                p1.libp_hrv_iben_code,
                                p1.libp_hrv_ips_code,
                                p1.libp_hrv_icpt_code);
--
           FETCH chk_ibp_exists INTO l_ibp_exists;
           CLOSE chk_ibp_exists;
--
           IF (l_ibp_exists IS NOT NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',388);
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
       LIBP_DLB_BATCH_ID,
       LIBP_DL_SEQNO,
       LIBP_DL_LOAD_STATUS,
       LIBP_REFNO
  FROM dl_hem_ics_benefit_payments
 WHERE libp_dlb_batch_id   = p_batch_id
   AND libp_dl_load_status = 'C';
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
ct               VARCHAR2(30) := 'DL_HEM_ICS_BENEFIT_PAYMENTS';
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
    fsc_utils.proc_start('s_dl_hem_ics_benefit_payments.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_ics_benefit_payments.dataload_delete',3 );
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
          cs   := p1.libp_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Delete from tables
--
          DELETE 
            FROM ics_benefit_payments
           WHERE ibp_refno = p1.libp_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ICS_BENEFIT_COMPONENTS');
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
END s_dl_hem_ics_benefit_payments;
/

