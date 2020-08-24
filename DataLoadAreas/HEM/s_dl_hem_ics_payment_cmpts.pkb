--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_ics_payment_cmpts
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
    UPDATE dl_hem_ics_payment_cmpts
       SET licpc_dl_load_status = p_status
     WHERE rowid                = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hem_ics_payment_cmpts');
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
       LICPC_DLB_BATCH_ID,
       LICPC_DL_SEQNO,
       LICPC_DL_LOAD_STATUS,
       LICPC_IBP_INDR_REFERENCE,
       LICPC_IBP_HRV_IBEN_CODE,
       LICPC_IBP_HRV_IPS_CODE,
       LICPC_IBP_HRV_ICPT_CODE,
       LICPC_AMOUNT,
       LICPC_HRV_ICT_CODE,
       NVL(LICPC_CREATED_BY, 'DATALOAD') LICPC_CREATED_BY,
       NVL(LICPC_CREATED_DATE, SYSDATE)  LICPC_CREATED_DATE,
       LICPC_COMPONENT_PAYMENT_CODE,
       LICPC_REFNO
  FROM dl_hem_ics_payment_cmpts
 WHERE licpc_dlb_batch_id   = p_batch_id
   AND licpc_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
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
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HEM_ICS_PAYMENT_CMPTS';
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
l_ibp_refno          NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_ics_payment_cmpts.dataload_create');
    fsc_utils.debug_message('s_dl_hem_ics_payment_cmpts.dataload_create',3);
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
          cs   := p1.licpc_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- GET IBP_REFNO
--
          l_ibp_refno := NULL;
--
--
           OPEN get_ibp_refno(p1.licpc_ibp_indr_reference,
                              p1.licpc_ibp_hrv_iben_code,
                              p1.licpc_ibp_hrv_ips_code,
                              p1.licpc_ibp_hrv_icpt_code);
--
          FETCH get_ibp_refno INTO l_ibp_refno;
          CLOSE get_ibp_refno;
--
-- Now insert into ics_payment_components
--
          INSERT /* +APPEND */ INTO  ics_payment_components(ICPC_REFNO,
                                                            ICPC_IBP_REFNO,
                                                            ICPC_AMOUNT,
                                                            ICPC_HRV_ICT_CODE,
                                                            ICPC_COMPONENT_PAYMENT_CODE
                                                           )
--
                                                    VALUES (p1.LICPC_REFNO,
                                                            l_ibp_refno,
                                                            p1.LICPC_AMOUNT,
                                                            p1.LICPC_HRV_ICT_CODE,
                                                            p1.LICPC_COMPONENT_PAYMENT_CODE
                                                           );
--
-- To replace the disabling of trigger ICPC_BR_I
--
          UPDATE ics_payment_components
             SET icpc_created_by   = p1.LICPC_CREATED_BY,
                 icpc_created_date = p1.LICPC_CREATED_DATE
           WHERE icpc_refno     = p1.licpc_refno
             AND icpc_ibp_refno = l_ibp_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ICS_PAYMENT_COMPONENTS');
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
       LICPC_DLB_BATCH_ID,
       LICPC_DL_SEQNO,
       LICPC_DL_LOAD_STATUS,
       LICPC_IBP_INDR_REFERENCE,
       LICPC_IBP_HRV_IBEN_CODE,
       LICPC_IBP_HRV_IPS_CODE,
       LICPC_IBP_HRV_ICPT_CODE,
       LICPC_AMOUNT,
       LICPC_HRV_ICT_CODE,
       LICPC_COMPONENT_PAYMENT_CODE,
       LICPC_REFNO
  FROM dl_hem_ics_payment_cmpts
 WHERE licpc_dlb_batch_id   = p_batch_id
   AND licpc_dl_load_status IN ('L','F','O');
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
CURSOR chk_icpc_exists(p_icpc_hrv_ict_code            VARCHAR2,
                       p_ibp_refno                    NUMBER,
                       p_icpc_component_payment_code  VARCHAR2) 
IS
SELECT 'X'
  FROM ics_payment_components
 WHERE icpc_hrv_ict_code           = p_icpc_hrv_ict_code
   AND icpc_ibp_refno              = p_ibp_refno
   AND icpc_component_payment_code = p_icpc_component_payment_code;
--
-- ***********************************************************************
--
CURSOR chk_ibp_exists(p_indr_reference   NUMBER,
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
-- Constants for process_summary
--
cb               VARCHAR2(30);
cd               DATE;
cp               VARCHAR2(30) := 'VALIDATE';
ct               VARCHAR2(30) := 'DL_HEM_ICS_PAYMENT_CMPTS';
cs               INTEGER;
ce               VARCHAR2(200);
l_id             ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_ibp_ref_exists      VARCHAR2(1);
l_icpc_ref_exists     VARCHAR2(1);
--
l_ict_valid           VARCHAR2(1);
l_cmpt_pay_code_valid VARCHAR2(1);
--
l_icpc_exists         VARCHAR2(1);
--
l_iben_valid          VARCHAR2(1);
l_ips_valid           VARCHAR2(1);
l_icpt_valid          VARCHAR2(1);
--
l_ibp_refno           NUMBER(10);
l_indr_ref_exists     VARCHAR2(1);
--
l_errors              VARCHAR2(10);
l_error_ind           VARCHAR2(10);
i                     INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_ics_payment_cmpts.dataload_validate');
    fsc_utils.debug_message('s_dl_hem_ics_payment_cmpts.dataload_validate',3);
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
          cs   := p1.licpc_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
          l_ict_valid           := 'Y';
          l_cmpt_pay_code_valid := 'Y';
--
          l_iben_valid := 'Y';
          l_ips_valid  := 'Y';
          l_icpt_valid := 'Y';
--
-- ***********************************************************************
--
-- Income Request Detail Reference has been supplied and is valid
--
          IF (p1.licpc_ibp_indr_reference IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',327);
          ELSE
--
             l_indr_ref_exists := NULL;
--
              OPEN chk_indr_ref_exists(p1.licpc_ibp_indr_reference);
             FETCH chk_indr_ref_exists INTO l_indr_ref_exists;
             CLOSE chk_indr_ref_exists;
--
             IF (l_indr_ref_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',333);
             END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check Benefit Code has been supplied and is Valid
--
--
          IF (p1.licpc_ibp_hrv_iben_code IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',559);
          ELSE
--
             IF (NOT s_dl_hem_utils.exists_frv('ICSBENEFIT',p1.licpc_ibp_hrv_iben_code,'Y')) THEN
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
          IF (p1.licpc_ibp_hrv_ips_code IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',560);
          ELSE
--
             IF (NOT s_dl_hem_utils.exists_frv('ICSPAYSTATUS',p1.licpc_ibp_hrv_ips_code,'Y')) THEN
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
          IF (p1.licpc_ibp_hrv_icpt_code IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',561);
          ELSE
--
             IF (NOT s_dl_hem_utils.exists_frv('ICSPAYTYPE',p1.licpc_ibp_hrv_icpt_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',380);
              l_icpt_valid := 'N';
             END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check Payment Component Amount has been supplied
--
          IF (p1.licpc_amount IS NULL) THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',566);
          END IF;
--
-- ***********************************************************************
--
-- Check Component Type has been supplied and is valid
--
          IF (p1.licpc_ibp_hrv_icpt_code IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',567);
          ELSE
--
             IF (NOT s_dl_hem_utils.exists_frv('ICSCOMPONENTTYPE',p1.licpc_hrv_ict_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',391);
              l_ict_valid := 'N';
             END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Type Indicator must be valid
--
          IF (p1.licpc_component_payment_code NOT IN ('LEG','ACT')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',392);
           l_cmpt_pay_code_valid := 'N';
          END IF;
--
-- ***********************************************************************
--
-- Check there is an entry in ics_benefit_payments for Income Detail
-- Request Reference, Benefit Code, Payment Status and Payment Type.
--
          IF (    l_iben_valid       = 'Y'
              AND l_ips_valid        = 'Y'
              AND l_icpt_valid       = 'Y'
              AND l_indr_ref_exists IS NOT NULL) THEN
--
           l_ibp_refno := NULL;
--
            OPEN chk_ibp_exists(p1.licpc_ibp_indr_reference,
                                p1.licpc_ibp_hrv_iben_code,
                                p1.licpc_ibp_hrv_ips_code,
                                p1.licpc_ibp_hrv_icpt_code);
--
           FETCH chk_ibp_exists INTO l_ibp_refno;
           CLOSE chk_ibp_exists;
--
           IF (l_ibp_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',400);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check there isn't already an entry in ics_payment_components for 
-- component Type, Benefit Payment Reference and Component Payment Code.
--
          IF (    l_ibp_refno           IS NOT NULL
              AND l_ict_valid           = 'Y'
              AND l_cmpt_pay_code_valid = 'Y') THEN
--
           l_icpc_exists := NULL;
--
            OPEN chk_icpc_exists(p1.licpc_hrv_ict_code,
                                 l_ibp_refno,
                                 p1.licpc_component_payment_code);
--
           FETCH chk_icpc_exists INTO l_icpc_exists;
           CLOSE chk_icpc_exists;
--
           IF (l_icpc_exists IS NOT NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',393);
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
       LICPC_DLB_BATCH_ID,
       LICPC_DL_SEQNO,
       LICPC_DL_LOAD_STATUS,
       LICPC_REFNO
  FROM dl_hem_ics_payment_cmpts
 WHERE licpc_dlb_batch_id   = p_batch_id
   AND licpc_dl_load_status = 'C';
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
ct               VARCHAR2(30) := 'DL_HEM_ICS_PAYMENT_CMPTS';
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
    fsc_utils.proc_start('s_dl_hem_ics_payment_cmpts.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_ics_payment_cmpts.dataload_delete',3 );
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
          cs   := p1.licpc_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Delete from tables
--
          DELETE 
            FROM ics_payment_components
           WHERE icpc_refno = p1.licpc_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ICS_PAYMENT_COMPONENTS');
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
END s_dl_hem_ics_payment_cmpts;
/

