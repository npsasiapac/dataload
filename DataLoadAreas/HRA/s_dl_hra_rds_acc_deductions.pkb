--
CREATE OR REPLACE PACKAGE BODY s_dl_hra_rds_acc_deductions
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
--  3.0     5.15.0    VS   15-JAN-2010  Defect Id 3220. Disable/Enable
--                                      RACD_BR_I in CREATE Process
--
--  4.0     5.15.0    VS   28-JUN-2010  Defect Id 5159. Addition of 
--                                      Modified By/Date
--
--  5.0     5.15.0    VS   27-JUL-2010  Defect Id 5513. 'VAR' should be 
--                                      a valid Pending Action Status
--
--  6.0     6.18.0    VRS  18-DEC-2018  6.18 Changes. Addition of LRACD_NON_RELATED
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
  UPDATE dl_hra_rds_acc_deductions
     SET lracd_dl_load_status = p_status
   WHERE rowid                = p_rowid;
  --
  EXCEPTION
       WHEN OTHERS THEN
       dbms_output.put_line('Error updating status of dl_hra_rds_acc_deductions');
       RAISE;
  --
END set_record_status_flag;
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
       LRACD_DLB_BATCH_ID,
       LRACD_DL_SEQNO,
       LRACD_DL_LOAD_STATUS,
       LRACD_RDSA_HA_REFERENCE,
       LRACD_RAUD_HRV_DEDT_CODE,
       LRACD_RAUD_START_DATE,
       LRACD_RAUD_HRV_RBEG_CODE,
       LRACD_PAY_REF,
       LRACD_RADT_CODE,
       LRACD_START_DATE,
       LRACD_HRV_RBEG_CODE,
       LRACD_REQUESTED_AMOUNT,
       LRACD_FIXED_AMOUNT_IND,
       LRACD_MINOR_ARR_VARY_IND,
       LRACD_CURRENT_SCO_CODE,
       LRACD_STATUS_DATE,
       NVL(LRACD_CREATED_BY,'DATALOAD') LRACD_CREATED_BY,
       NVL(LRACD_CREATED_DATE, SYSDATE) LRACD_CREATED_DATE,
       LRACD_PENDING_SCO_CODE,
       LRACD_REQUESTED_PERCENTAGE,
       LRACD_END_DATE,
       LRACD_SUSPENDED_FROM_DATE,
       LRACD_SUSPENDED_TO_DATE,
       LRACD_ACTION_SENT_DATETIME,
       LRACD_HRV_SUSR_CODE,
       LRACD_HRV_TERR_CODE,
       LRACD_LAST_DEDUCTION_AMOUNT,
       LRACD_LAST_DEDUCTION_DATE,
       LRACD_NEXT_DEDUCTION_AMOUNT,
       LRACD_NEXT_DEDUCTION_DATE,
       LRACD_MODIFIED_BY,
       LRACD_MODIFIED_DATE,
       LRACD_MINOR_ARR_VARY_AMOUNT,
       LRACD_NET_RENT_BASIS_DEDN,
       NVL(LRACD_NON_RELATED, 'N')      LRACD_NON_RELATED,
       LRACD_REFNO
  FROM dl_hra_rds_acc_deductions
 WHERE lracd_dlb_batch_id   = p_batch_id
   AND lracd_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR c_get_raud_refno(p_ha_reference  VARCHAR2,
                        p_dedt_code     VARCHAR2,
                        p_rbeg_code     VARCHAR2,
                        p_start_date    DATE     ) 
IS
SELECT raud_refno
  FROM rds_authorised_deductions,
       rds_authorities
 WHERE rdsa_refno         = raud_rdsa_refno
   AND rdsa_ha_reference  = p_ha_reference
   AND raud_hrv_dedt_code = p_dedt_code
   AND raud_hrv_rbeg_code = p_rbeg_code
   AND raud_start_date    = p_start_date;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_ACC_DEDUCTIONS';
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
l_par_refno                parties.par_refno%type;
l_raud_refno               rds_authorised_deductions.raud_refno%type;
l_rac_accno                revenue_accounts.rac_accno%type;
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger RACD_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hra_rds_acc_deductions.dataload_create');
    fsc_utils.debug_message('s_dl_hra_rds_acc_deductions.dataload_create',3);
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
          cs   := p1.lracd_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
--
          l_raud_refno   :=NULL;
          l_rac_accno    := s_revenue_accounts2.get_rac_accno_from_pay_ref(p1.lracd_pay_ref);
--
           OPEN c_get_raud_refno(p1.lracd_rdsa_ha_reference,
                                 p1.lracd_raud_hrv_dedt_code,
                                 p1.lracd_raud_hrv_rbeg_code,
                                 p1.lracd_raud_start_date);
--
          FETCH c_get_raud_refno INTO l_raud_refno ;
          CLOSE c_get_raud_refno;
--
-- Insert into 
--
          INSERT into rds_account_deductions(racd_refno,
                                             racd_raud_refno,
                                             racd_rac_accno,
                                             racd_start_date,
                                             racd_radt_code,
                                             racd_requested_amount,
                                             racd_fixed_amount_ind,
                                             racd_minor_arr_vary_ind,
                                             racd_current_sco_code,
                                             racd_status_date,
                                             racd_created_by,
                                             racd_created_date,
                                             racd_hrv_rbeg_code,
                                             racd_pending_sco_code,
                                             racd_requested_percentage,
                                             racd_end_date,
                                             racd_suspended_from_date,
                                             racd_suspended_to_date,
                                             racd_action_sent_datetime,
                                             racd_hrv_susr_code,
                                             racd_hrv_terr_code,
                                             racd_last_deduction_amount,
                                             racd_last_deduction_date,
                                             racd_next_deduction_amount,
                                             racd_next_deduction_date, 
                                             racd_modified_by,
                                             racd_modified_date,         
                                             racd_minor_arr_vary_amount,
                                             racd_net_rent_basis_dedn,
                                             racd_non_related
                                            )
--
                                      VALUES(p1.lracd_refno,
                                             l_raud_refno,
                                             l_rac_accno,
                                             p1.lracd_start_date,
                                             p1.lracd_radt_code,
                                             p1.lracd_requested_amount,
                                             p1.lracd_fixed_amount_ind,
                                             p1.lracd_minor_arr_vary_ind,
                                             p1.lracd_current_sco_code,
                                             p1.lracd_status_date,
                                             p1.lracd_created_by,
                                             p1.lracd_created_date,
                                             p1.lracd_hrv_rbeg_code,
                                             p1.lracd_pending_sco_code,
                                             p1.lracd_requested_percentage,
                                             p1.lracd_end_date,
                                             p1.lracd_suspended_from_date,
                                             p1.lracd_suspended_to_date,
                                             p1.lracd_action_sent_datetime,
                                             p1.lracd_hrv_susr_code,
                                             p1.lracd_hrv_terr_code,
                                             p1.lracd_last_deduction_amount,
                                             p1.lracd_last_deduction_date,
                                             p1.lracd_next_deduction_amount,
                                             p1.lracd_next_deduction_date, 
                                             p1.lracd_modified_by,
                                             p1.lracd_modified_date,         
                                             p1.lracd_minor_arr_vary_amount,
                                             p1.lracd_net_rent_basis_dedn,
                                             p1.lracd_non_related
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_ACCOUNT_DEDUCTIONS');
--
    execute immediate 'alter trigger RACD_BR_I enable';
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
       LRACD_DLB_BATCH_ID,
       LRACD_DL_SEQNO,
       LRACD_DL_LOAD_STATUS,
       LRACD_RDSA_HA_REFERENCE,
       LRACD_RAUD_HRV_DEDT_CODE,
       LRACD_RAUD_START_DATE,
       LRACD_RAUD_HRV_RBEG_CODE,
       LRACD_PAY_REF,
       LRACD_RADT_CODE,
       LRACD_START_DATE,
       LRACD_HRV_RBEG_CODE,
       LRACD_REQUESTED_AMOUNT,
       LRACD_FIXED_AMOUNT_IND,
       LRACD_MINOR_ARR_VARY_IND,
       LRACD_CURRENT_SCO_CODE,
       LRACD_STATUS_DATE,
       NVL(LRACD_CREATED_BY,'DATALOAD') LRACD_CREATED_BY,
       NVL(LRACD_CREATED_DATE, SYSDATE) LRACD_CREATED_DATE,
       LRACD_PENDING_SCO_CODE,
       LRACD_REQUESTED_PERCENTAGE,
       LRACD_END_DATE,
       LRACD_SUSPENDED_FROM_DATE,
       LRACD_SUSPENDED_TO_DATE,
       LRACD_ACTION_SENT_DATETIME,
       LRACD_HRV_SUSR_CODE,
       LRACD_HRV_TERR_CODE,
       LRACD_LAST_DEDUCTION_AMOUNT,
       LRACD_LAST_DEDUCTION_DATE,
       LRACD_NEXT_DEDUCTION_AMOUNT,
       LRACD_NEXT_DEDUCTION_DATE,
       LRACD_MODIFIED_BY,
       LRACD_MODIFIED_DATE,
       LRACD_MINOR_ARR_VARY_AMOUNT,
       LRACD_NET_RENT_BASIS_DEDN,
       NVL(LRACD_NON_RELATED, 'N')      LRACD_NON_RELATED,
       LRACD_REFNO
  FROM dl_hra_rds_acc_deductions
 WHERE lracd_dlb_batch_id    = p_batch_id
   AND lracd_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR c_check_rdsa( p_ha_reference  VARCHAR2) 
IS
SELECT 'X'
  FROM rds_authorities
 WHERE rdsa_ha_reference = p_ha_reference;
--
-- ***********************************************************************
--
CURSOR c_get_raud(p_ha_reference  VARCHAR2,
                  p_dedt_code     VARCHAR2,
                  p_rbeg_code     VARCHAR2,
                  p_start_date    DATE) 
IS
SELECT raud_refno
  FROM rds_authorised_deductions,
       rds_authorities
 WHERE rdsa_refno         = raud_rdsa_refno
   AND rdsa_ha_reference  = p_ha_reference
   AND raud_hrv_dedt_code = p_dedt_code
   AND raud_hrv_rbeg_code = p_rbeg_code
   AND raud_start_date    = p_start_date;
--
-- ***********************************************************************
--
CURSOR c_check_rac(p_pay_ref  VARCHAR2) 
IS
SELECT rac_accno
  FROM revenue_accounts
 WHERE rac_pay_ref = p_pay_ref;
--
-- ***********************************************************************
--
CURSOR c_check_unique(p_raud_refno NUMBER,
                      p_rac_accno  NUMBER,
                      p_radt_code  VARCHAR2,
                      p_start_date DATE,
                      p_rbeg_code  VARCHAR2)
IS
SELECT 'X'
  FROM rds_account_deductions
 WHERE racd_raud_refno    = p_raud_refno  
   AND racd_rac_accno     = p_rac_accno
   AND racd_radt_code     = p_radt_code
   AND racd_start_date    = p_start_date
   AND racd_hrv_rbeg_code = p_rbeg_code;
--
-- ***********************************************************************
--
CURSOR c_get_radt(p_radt_code VARCHAR2) 
IS
SELECT 'X'
  FROM rds_account_deduction_types
 WHERE radt_code = p_radt_code;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_ACC_DEDUCTIONS';
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
l_par_refno                parties.par_refno%type;
l_raud_refno               rds_authorised_deductions.raud_refno%type;
l_rac_accno                revenue_accounts.rac_accno%type;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_rds_acc_deductions.dataload_validate');
    fsc_utils.debug_message( 's_dl_hra_rds_acc_deductions.dataload_validate',3);
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
          cs   := p1.lracd_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors      := 'V';
          l_error_ind   := 'N';
--
          l_raud_refno  := NULL;
          l_rac_accno   := NULL;
--
--
--
-- ***********************************************************************
--
-- Validation checks required
--
-- Check the Authority Ref has been supplied and is valid
--
          IF (p1.lracd_rdsa_ha_reference IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',97);
--
          ELSE
-- 
              OPEN c_check_rdsa(p1.lracd_rdsa_ha_reference);
             FETCH c_check_rdsa INTO l_exists;
             CLOSE c_check_rdsa;
--
             IF (l_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',99);
             END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check the Authorised Deduction Type has been supplied and is valid
--
          IF (p1.lracd_raud_hrv_dedt_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',290);
--
          ELSIF (NOT s_dl_hem_utils.exists_frv('RDS_DED_TYPE',p1.lracd_raud_hrv_dedt_code,'N')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',103);
          END IF;
--
--
-- ******************************************************************************
--
-- Authorised Deduction Start Date has been supplied
--
          IF (p1.lracd_raud_start_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',289);
          END IF;
--
--
-- ******************************************************************************
--
-- Check that the Authorised Deduction Benefit Group has been supplied and is valid
--
          IF (p1.lracd_raud_hrv_rbeg_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',300);
--
          ELSIF (NOT s_dl_hem_utils.exists_frv('RDS_BEN_GRP',p1.lracd_raud_hrv_rbeg_code,'N')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',104);
--
          END IF;
--
--
-- ******************************************************************************
--
-- Check Authorised Deductions Exist
--
           OPEN c_get_raud(p1.lracd_rdsa_ha_reference, 
                           p1.lracd_raud_hrv_dedt_code, 
                           p1.lracd_raud_hrv_rbeg_code, 
                           p1.lracd_raud_start_date);
--
          FETCH c_get_raud INTO l_raud_refno;
--
          IF c_get_raud%NOTFOUND THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',105);
          END IF;
--
          CLOSE c_get_raud;
--
--
-- ******************************************************************************
--
-- Check the Account Deduction Payment Ref has been supplied and exists
--
          IF (p1.lracd_pay_ref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',303);
--
          ELSE
--
              OPEN c_check_rac(p1.lracd_pay_ref);
             FETCH c_check_rac INTO l_rac_accno;
--
             IF (c_check_rac%NOTFOUND) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',304);
             END IF;
--
             CLOSE c_check_rac;
--
         END IF;
--
--
-- ******************************************************************************
--
-- Check the Account Deduction Type has been supplied and is valid
--
--
          IF (p1.lracd_radt_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',305);
--
          ELSE
--
              OPEN c_get_radt(p1.lracd_radt_code);
             FETCH c_get_radt INTO l_exists;
--
             IF (c_get_radt%NOTFOUND) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',107);
             END IF;
--
             CLOSE c_get_radt;
--
          END IF;
--
--
-- ******************************************************************************
--
-- Check the Deduction Start Date has been supplied
--
          IF (p1.lracd_start_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',306);
          END IF;
--
--
-- ******************************************************************************
--
-- Check that the Account Deduction Benefit Group has been supplied and is valid
--
          IF (p1.lracd_hrv_rbeg_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',307);
--
          ELSIF (NOT s_dl_hem_utils.exists_frv('RDS_BEN_GRP',p1.lracd_hrv_rbeg_code,'N')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',308);
--
          END IF;
--
--
-- ******************************************************************************
--
-- Check that the Requested amount has been supplied
--
          IF (p1.lracd_requested_amount IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',110);
          END IF;
--
--
-- ******************************************************************************
--
-- Check that the Fixed Amount Indicator has been supplied and is valid
--
          IF (p1.lracd_fixed_amount_ind IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',309);
--
          ELSIF nvl(p1.lracd_fixed_amount_ind, 'X') NOT IN ( 'Y', 'N') THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',111);
--
          END IF;
--
--
-- ******************************************************************************
--
-- Check that the Minor Arrears Indicator has been supplied and is valid
--
          IF (p1.lracd_minor_arr_vary_ind IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',310);
--
          ELSIF nvl(p1.lracd_minor_arr_vary_ind, 'X') NOT IN ( 'Y', 'N') THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',112);
--
          END IF;
--
--
-- ******************************************************************************
--
-- Check Current Status Code has been supplied and is valid
--
          IF (p1.lracd_current_sco_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',248);
--
          ELSIF nvl(p1.lracd_current_sco_code, '^~#') NOT IN ( 'CAN','CON','ERR','PND', 'ACT', 'SUS','TRM' ) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',108);
--
          END IF;
--
--
-- ******************************************************************************
--
-- Check that the Status Date has been supplied
--
          IF (p1.lracd_status_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',16);
          END IF;
--
--
-- ******************************************************************************
--
-- If supplied check that the Pending Status Code is valid
--
          IF (p1.lracd_pending_sco_code IS NOT NULL) THEN
--
           IF p1.lracd_pending_sco_code NOT IN ('NEW', 'TRM', 'SUS','VAR') THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',109);
           END IF;
--
          END IF;
--
--
-- ******************************************************************************
--
-- If supplied the Account Deduction End Date is not earlier than the Account 
-- Deduction Start Date
--
          IF (p1.lracd_end_date IS NOT NULL) THEN
--
           IF p1.lracd_end_date < nvl(p1.lracd_start_date, p1.lracd_end_date) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',3);
           END IF;
--
          END IF;
--
--
-- ******************************************************************************
--
-- If supplied check the Suspension Start Date is not earlier than the Account
-- deduction start date
--
          IF (p1.lracd_suspended_from_date IS NOT NULL) THEN
--
           IF p1.lracd_suspended_from_date < nvl(p1.lracd_start_date, p1.lracd_suspended_from_date) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',93);
           END IF;
--
          END IF;
--
--
-- ******************************************************************************
--
-- If supplied check the Suspension End Date is not earlier than Suspension start date
--
          IF (p1.lracd_suspended_to_date IS NOT NULL) THEN
--
           IF p1.lracd_suspended_to_date < nvl(p1.lracd_start_date, p1.lracd_suspended_from_date) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',92);
           END IF;
--
          END IF;
--
--
-- ******************************************************************************
--
-- Check Unique combination of of Account Payment Reference, Account Deduction Type, 
-- Account Deduction Start Date and Account Benefit Group code
--
           OPEN c_check_unique(l_raud_refno,
                               l_rac_accno,
                               p1.lracd_radt_code,
                               p1.lracd_start_date,
                               p1.lracd_hrv_rbeg_code
                              );
--
          FETCH c_check_unique INTO l_exists;
--
          IF c_check_unique%FOUND THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',106);
          END IF;
--
          CLOSE c_check_unique;
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
          IF (p1.lracd_hrv_susr_code IS NOT NULL) THEN
--
           IF (NOT s_dl_hem_utils.exists_frv('RDS_SUS_RSN',p1.lracd_hrv_susr_code,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',95);
           END IF;
--
          END IF;
--
-- ***************************
--
-- If supplied check Termination Reason is valid
--
          IF (p1.lracd_hrv_susr_code IS NOT NULL) THEN
--
           IF (NOT s_dl_hem_utils.exists_frv('RDS_TERM_RSN',p1.lracd_hrv_terr_code,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',96);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check if supplied that the non related values are Y or N only
--
          IF (p1.lracd_non_related NOT IN ( 'Y', 'N')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',944);
          END IF;
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
                          p_date     IN date) 
IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       lracd_dlb_batch_id,
       lracd_dl_seqno,
       lracd_dl_load_status,
       lracd_refno
  FROM dl_hra_rds_acc_deductions
 WHERE lracd_dlb_batch_id   = p_batch_id
   AND lracd_dl_load_status = 'C';
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
ct                   VARCHAR2(30) := 'DL_HRA_RDS_ACC_DEDUCTIONS';
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
    fsc_utils.proc_start('s_dl_hra_rds_acc_deductions.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_rds_acc_deductions.dataload_delete',3 );
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
          cs   := p1.lracd_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Delete from rds_account_deductions
--
          DELETE 
            FROM rds_account_deductions
           WHERE racd_refno = p1.lracd_refno;
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
END s_dl_hra_rds_acc_deductions;
/