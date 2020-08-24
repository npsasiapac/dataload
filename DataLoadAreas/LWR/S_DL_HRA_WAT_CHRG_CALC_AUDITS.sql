CREATE OR REPLACE PACKAGE HOU.s_dl_hra_wat_chrg_calc_audits
AS
  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VER  DB Ver   WHO  WHEN          WHY
  --
  --  1.0  5.17.0   VRS  02-SEP-2010   INITIAL Version
  --  1.1  6.18     PLL  04/06/19      Added Additional Fields

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
END s_dl_hra_wat_chrg_calc_audits;
--
/

CREATE OR REPLACE PACKAGE BODY HOU.s_dl_hra_wat_chrg_calc_audits
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.17.0    VS   02-SEP-2010  Initial Creation.
--  2.0     5.17.0    MT   16-MAR-2011  F596 - Add disable CREATED trigger.
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
    UPDATE dl_hra_wat_chrg_calc_audits
       SET lwcca_dl_load_status = p_status
     WHERE rowid                = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hra_wat_chrg_calc_audits');
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
       LWCCA_DLB_BATCH_ID,
       LWCCA_DL_SEQNO,
       LWCCA_DL_LOAD_STATUS,
       LWCCA_REFNO,
       LWCCA_RAC_PAYREF,
       LWCCA_BILL_PERIOD_START_DATE,
       LWCCA_BILL_PERIOD_END_DATE,
       LWCCA_ACTUAL_CHARGE_AMOUNT,
       LWCCA_ADJUSTED_WEEKLY_CHARGE,
       LWCCA_CHARGE_EFFECTIVE_DATE,
       LWCCA_TOTAL_ADJUSTMENT,
       LWCCA_WKLY_ADJUSTMNT_COMPONENT,
       LWCCA_TOTAL_UNADJ_PREV_CHARGES,
       LWCCA_NEW_UNADJ_WEEKLY_CHARGE,
       LWCCA_NUMBER_OF_WEEKS,
       LWCCA_SCO_CODE,
       NVL(LWCCA_CREATED_BY, 'DATALOAD') LWCCA_CREATED_BY,
       NVL(LWCCA_CREATED_DATE, SYSDATE)  LWCCA_CREATED_DATE,
       LWCCA_CHARGES_WAIVED_AMOUNT,
       LWCCA_UNADJ_WEEKLY_CHARGE_SP_1,
       LWCCA_TOTAL_UNADJ_CHARGE_SP_1,
       LWCCA_UNADJ_WEEKLY_CHARGE_SP_2,
       LWCCA_TOTAL_UNADJ_CHARGE_SP_2,
       LWCCA_RECON_ACCT_WEEKS_TO_DATE,
       LWCCA_RECON_ACCT_PCT_CHG_TODT,
       LWCCA_RECON_EXTRAPOLATED_ACT,
       LWCCA_RECON_APPLIED_CREDIT_ADJ,
       LWCCA_RECON_TOTAL_ADJUSTMENT,
       LWCCA_MODIFIED_DATE,
       LWCCA_MODIFIED_BY,
       LWCCA_TYPE,
       LWCCA_CURR_ASSESSMENT_REF      ,
       WAUD_BILL_PERIOD_START_DATE    ,
       WAUD_BILL_PERIOD_END_DATE      ,
       WAUD_RCCO_CODE                 ,
       LWCCA_ADDITIONAL_1             ,
       LWCCA_ADDITIONAL_2             ,
       LWCCA_ADDITIONAL_3             ,
       LWCCA_ADDITIONAL_4             ,
       LWCCA_ADDITIONAL_5             ,
       LWCCA_ADDITIONAL_6             ,
       LWCCA_ADDITIONAL_7             ,
       LWCCA_ADDITIONAL_8             ,
       LWCCA_ADDITIONAL_9             ,
       LWCCA_ADDITIONAL_10            ,
       LWCCA_ADDITIONAL_11            ,
       LWCCA_ADDITIONAL_12            ,
       LWCCA_ADDITIONAL_13            ,
       LWCCA_ADDITIONAL_14            ,
       LWCCA_ADDITIONAL_15            ,
       LWCCA_ADDITIONAL_16            ,
       LWCCA_ADDITIONAL_17            ,
       LWCCA_ADDITIONAL_18            ,
       LWCCA_ADDITIONAL_19            ,
       LWCCA_ADDITIONAL_20            ,
       LWCCA_ADDITIONAL_21
  FROM dl_hra_wat_chrg_calc_audits
 WHERE lwcca_dlb_batch_id   = p_batch_id
   AND lwcca_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR get_rac_accno(p_rac_pay_ref VARCHAR2)
IS
SELECT rac_accno
  FROM revenue_accounts
 WHERE rac_pay_ref = p_rac_pay_ref;
--
CURSOR get_waud_refno( cp_assessment_ref VARCHAR2
                     , cp_start_date DATE
                     , cp_end_date DATE
                     , cp_rcco_code VARCHAR2)
IS
SELECT waud_refno
FROM water_usage_details
JOIN lwr_assessments ON LWRA_REFNO = WAUD_LWRA_REFNO
WHERE TRUNC(WAUD_BILL_PERIOD_START_DATE) = TRUNC(cp_start_date)
AND TRUNC(WAUD_BILL_PERIOD_END_DATE) = TRUNC(cp_end_date)
AND WAUD_RCCO_CODE = cp_rcco_code
AND TRUNC(LWRA_RATE_PERIOD_END_DATE) >= TRUNC(cp_end_date)
AND TRUNC(LWRA_RATE_PERIOD_START_DATE) <= TRUNC(cp_start_date);
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_WAT_CHRG_CALC_AUDITS';
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
l_rac_accno          NUMBER(10);
l_waud_refno         NUMBER;
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger HOU.wcca_br_i disable';

    fsc_utils.proc_start('s_dl_hra_wat_chrg_calc_audits.dataload_create');
    fsc_utils.debug_message('s_dl_hra_wat_chrg_calc_audits.dataload_create',3);
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
          cs   := p1.lwcca_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Get Revenue Account
--
          l_rac_accno := NULL;
--
           OPEN get_rac_accno(p1.lwcca_rac_payref);
          FETCH get_rac_accno INTO l_rac_accno;
          CLOSE get_rac_accno;
-- get waud_refno
          l_waud_refno := NULL;
          IF p1.LWCCA_CURR_ASSESSMENT_REF IS NOT NULL
          AND p1.WAUD_BILL_PERIOD_START_DATE IS NOT NULL
          AND p1.WAUD_BILL_PERIOD_END_DATE IS NOT NULL
          AND p1.WAUD_RCCO_CODE IS NOT NULL
          THEN 
             OPEN get_waud_refno(p1.LWCCA_CURR_ASSESSMENT_REF
                                ,p1.WAUD_BILL_PERIOD_START_DATE
                                ,p1.WAUD_BILL_PERIOD_END_DATE
                                ,p1.WAUD_RCCO_CODE);
             FETCH get_waud_refno INTO l_waud_refno;
             CLOSE get_waud_refno;          
          END IF;
--
--
--
-- Insert into WATER_CHARGE_CALC_AUDITS table
--
        INSERT INTO water_charge_calc_audits(WCCA_REFNO,
                                             WCCA_RAC_ACCNO,
                                             WCCA_BILL_PERIOD_START_DATE,
                                             WCCA_BILL_PERIOD_END_DATE,
                                             WCCA_ACTUAL_CHARGE_AMOUNT,
                                             WCCA_ADJUSTED_WEEKLY_CHARGE,
                                             WCCA_CHARGE_EFFECTIVE_DATE,
                                             WCCA_TOTAL_ADJUSTMENT,
                                             WCCA_WKLY_ADJUSTMENT_COMPONENT,
                                             WCCA_TOTAL_UNADJ_PREV_CHARGES,
                                             WCCA_NEW_UNADJ_WEEKLY_CHARGE,
                                             WCCA_NUMBER_OF_WEEKS,
                                             WCCA_SCO_CODE,
                                             WCCA_CREATED_DATE,
                                             WCCA_CREATED_BY,
                                             WCCA_CHARGES_WAIVED_AMOUNT,
                                             WCCA_UNADJ_WEEKLY_CHARGE_SP_1,
                                             WCCA_TOTAL_UNADJ_CHARGE_SP_1,
                                             WCCA_UNADJ_WEEKLY_CHARGE_SP_2,
                                             WCCA_TOTAL_UNADJ_CHARGE_SP_2,
                                             WCCA_RECON_ACCT_WEEKS_TO_DATE,
                                             WCCA_RECON_ACCT_PCT_CHG_TODT,
                                             WCCA_RECON_EXTRAPOLATED_ACTUAL,
                                             WCCA_RECON_APPLIED_CREDIT_ADJ,
                                             WCCA_RECON_TOTAL_ADJUSTMENT,
                                             WCCA_MODIFIED_DATE,
                                             WCCA_MODIFIED_BY,
                                             WCCA_TYPE,
                                             WCCA_ADDITIONAL_1,
                                             WCCA_ADDITIONAL_2,
                                             WCCA_ADDITIONAL_3,
                                             WCCA_ADDITIONAL_4,
                                             WCCA_ADDITIONAL_5,
                                             WCCA_ADDITIONAL_6,
                                             WCCA_ADDITIONAL_7,
                                             WCCA_ADDITIONAL_8,
                                             WCCA_ADDITIONAL_9,
                                             WCCA_ADDITIONAL_10,
                                             WCCA_ADDITIONAL_11,
                                             WCCA_ADDITIONAL_12,
                                             WCCA_ADDITIONAL_13,
                                             WCCA_ADDITIONAL_14,
                                             WCCA_ADDITIONAL_15,
                                             WCCA_ADDITIONAL_16,
                                             WCCA_ADDITIONAL_17,
                                             WCCA_ADDITIONAL_18,
                                             WCCA_ADDITIONAL_19,
                                             WCCA_ADDITIONAL_20,
                                             WCCA_ADDITIONAL_21
                                            )
--
                                     VALUES (p1.LWCCA_REFNO,
                                             l_rac_accno,
                                             p1.LWCCA_BILL_PERIOD_START_DATE,
                                             p1.LWCCA_BILL_PERIOD_END_DATE,
                                             p1.LWCCA_ACTUAL_CHARGE_AMOUNT,
                                             p1.LWCCA_ADJUSTED_WEEKLY_CHARGE,
                                             p1.LWCCA_CHARGE_EFFECTIVE_DATE,
                                             p1.LWCCA_TOTAL_ADJUSTMENT,
                                             p1.LWCCA_WKLY_ADJUSTMNT_COMPONENT,
                                             p1.LWCCA_TOTAL_UNADJ_PREV_CHARGES,
                                             p1.LWCCA_NEW_UNADJ_WEEKLY_CHARGE,
                                             p1.LWCCA_NUMBER_OF_WEEKS,
                                             p1.LWCCA_SCO_CODE,
                                             p1.LWCCA_CREATED_DATE,
                                             p1.LWCCA_CREATED_BY,
                                             p1.LWCCA_CHARGES_WAIVED_AMOUNT,
                                             p1.LWCCA_UNADJ_WEEKLY_CHARGE_SP_1,
                                             p1.LWCCA_TOTAL_UNADJ_CHARGE_SP_1,
                                             p1.LWCCA_UNADJ_WEEKLY_CHARGE_SP_2,
                                             p1.LWCCA_TOTAL_UNADJ_CHARGE_SP_2,
                                             p1.LWCCA_RECON_ACCT_WEEKS_TO_DATE,
                                             p1.LWCCA_RECON_ACCT_PCT_CHG_TODT,
                                             p1.LWCCA_RECON_EXTRAPOLATED_ACT,
                                             p1.LWCCA_RECON_APPLIED_CREDIT_ADJ,
                                             p1.LWCCA_RECON_TOTAL_ADJUSTMENT,
                                             p1.LWCCA_MODIFIED_DATE,
                                             p1.LWCCA_MODIFIED_BY,
                                             p1.LWCCA_TYPE,                                             
                                             p1.LWCCA_ADDITIONAL_1             ,
                                             p1.LWCCA_ADDITIONAL_2             ,
                                             p1.LWCCA_ADDITIONAL_3             ,
                                             p1.LWCCA_ADDITIONAL_4             ,
                                             p1.LWCCA_ADDITIONAL_5             ,
                                             p1.LWCCA_ADDITIONAL_6             ,
                                             p1.LWCCA_ADDITIONAL_7             ,
                                             p1.LWCCA_ADDITIONAL_8             ,
                                             p1.LWCCA_ADDITIONAL_9             ,
                                             p1.LWCCA_ADDITIONAL_10            ,
                                             p1.LWCCA_ADDITIONAL_11            ,
                                             p1.LWCCA_ADDITIONAL_12            ,
                                             p1.LWCCA_ADDITIONAL_13            ,
                                             p1.LWCCA_ADDITIONAL_14            ,
                                             p1.LWCCA_ADDITIONAL_15            ,
                                             p1.LWCCA_ADDITIONAL_16            ,
                                             p1.LWCCA_ADDITIONAL_17            ,
                                             p1.LWCCA_ADDITIONAL_18            ,
                                             p1.LWCCA_ADDITIONAL_19            ,
                                             p1.LWCCA_ADDITIONAL_20            ,
                                             p1.LWCCA_ADDITIONAL_21
                                            );
         IF l_waud_refno IS NOT NULL
         THEN         
            INSERT INTO water_usage_detail_usages (wudu_wcca_refno, wudu_waud_refno, wudu_created_by, wudu_created_date)
            VALUES (p1.LWCCA_REFNO, l_waud_refno, p1.LWCCA_CREATED_BY, p1.LWCCA_CREATED_DATE);            
         END IF;
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
          i := i+1;
--
          IF MOD(i,10000) = 0 THEN
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('WATER_CHARGE_CALC_AUDITS');
    execute immediate 'alter trigger HOU.wcca_br_i enable';
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
       LWCCA_DLB_BATCH_ID,
       LWCCA_DL_SEQNO,
       LWCCA_DL_LOAD_STATUS,
       LWCCA_REFNO,
       LWCCA_RAC_PAYREF,
       LWCCA_BILL_PERIOD_START_DATE,
       LWCCA_BILL_PERIOD_END_DATE,
       LWCCA_ACTUAL_CHARGE_AMOUNT,
       LWCCA_ADJUSTED_WEEKLY_CHARGE,
       LWCCA_CHARGE_EFFECTIVE_DATE,
       LWCCA_TOTAL_ADJUSTMENT,
       LWCCA_WKLY_ADJUSTMNT_COMPONENT,
       LWCCA_TOTAL_UNADJ_PREV_CHARGES,
       LWCCA_NEW_UNADJ_WEEKLY_CHARGE,
       LWCCA_NUMBER_OF_WEEKS,
       LWCCA_SCO_CODE,
       NVL(LWCCA_CREATED_BY, 'DATALOAD') LWCCA_CREATED_BY,
       NVL(LWCCA_CREATED_DATE, SYSDATE)  LWCCA_CREATED_DATE,
       LWCCA_CHARGES_WAIVED_AMOUNT,
       LWCCA_UNADJ_WEEKLY_CHARGE_SP_1,
       LWCCA_TOTAL_UNADJ_CHARGE_SP_1,
       LWCCA_UNADJ_WEEKLY_CHARGE_SP_2,
       LWCCA_TOTAL_UNADJ_CHARGE_SP_2,
       LWCCA_RECON_ACCT_WEEKS_TO_DATE,
       LWCCA_RECON_ACCT_PCT_CHG_TODT,
       LWCCA_RECON_EXTRAPOLATED_ACT,
       LWCCA_RECON_APPLIED_CREDIT_ADJ,
       LWCCA_RECON_TOTAL_ADJUSTMENT,
       LWCCA_MODIFIED_DATE,
       LWCCA_MODIFIED_BY,
       LWCCA_TYPE,
       LWCCA_CURR_ASSESSMENT_REF      ,
       WAUD_BILL_PERIOD_START_DATE    ,
       WAUD_BILL_PERIOD_END_DATE      ,
       WAUD_RCCO_CODE                 ,
       LWCCA_ADDITIONAL_1             ,
       LWCCA_ADDITIONAL_2             ,
       LWCCA_ADDITIONAL_3             ,
       LWCCA_ADDITIONAL_4             ,
       LWCCA_ADDITIONAL_5             ,
       LWCCA_ADDITIONAL_6             ,
       LWCCA_ADDITIONAL_7             ,
       LWCCA_ADDITIONAL_8             ,
       LWCCA_ADDITIONAL_9             ,
       LWCCA_ADDITIONAL_10            ,
       LWCCA_ADDITIONAL_11            ,
       LWCCA_ADDITIONAL_12            ,
       LWCCA_ADDITIONAL_13            ,
       LWCCA_ADDITIONAL_14            ,
       LWCCA_ADDITIONAL_15            ,
       LWCCA_ADDITIONAL_16            ,
       LWCCA_ADDITIONAL_17            ,
       LWCCA_ADDITIONAL_18            ,
       LWCCA_ADDITIONAL_19            ,
       LWCCA_ADDITIONAL_20            ,
       LWCCA_ADDITIONAL_21
  FROM dl_hra_wat_chrg_calc_audits
 WHERE lwcca_dlb_batch_id    = p_batch_id
   AND lwcca_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR chk_rac_accno(p_rac_pay_ref VARCHAR2)
IS
SELECT rac_accno
  FROM revenue_accounts
 WHERE rac_pay_ref = p_rac_pay_ref;
--
-- ***********************************************************************
--
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HRA_WAT_CHRG_CALC_AUDITS';
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
l_rac_accno          NUMBER(10);
--
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_wat_chrg_calc_audits.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_wat_chrg_calc_audits.dataload_validate',3);
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
          cs   := p1.lwcca_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
-- ***********************************************************************
--
-- Validation checks required
--
-- Check Revenue Account Pay Reference exists
--
          l_rac_accno := NULL;
--
           OPEN chk_rac_accno(p1.lwcca_rac_payref);
          FETCH chk_rac_accno INTO l_rac_accno;
          CLOSE chk_rac_accno;
--
          IF (l_rac_accno IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',323);
          END IF;
--
-- ***********************************************************************
--
-- Check Billing Period Start Date isn't later than Billing Period End date
--
          IF (p1.lwcca_bill_period_start_date > p1.lwcca_bill_period_end_date) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',403);
          END IF;
--
-- ***********************************************************************
--
-- Check Status Code is valid
--
          IF (p1.lwcca_sco_code NOT IN ('RAP','CHG','REJ','APR','REQ','SND','RCV', 'SEN')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',404);
          END IF;

--
-- ***********************************************************************
--
-- Check Calc Type is valid
--
          IF (p1.lwcca_type NOT IN ('RECON','RECALC','OVERRIDE')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',405);
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
       LWCCA_DLB_BATCH_ID,
       LWCCA_DL_SEQNO,
       LWCCA_DL_LOAD_STATUS,
       LWCCA_REFNO
  FROM dl_hra_wat_chrg_calc_audits
 WHERE lwcca_dlb_batch_id   = p_batch_id
   AND lwcca_dl_load_status = 'C';
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
ct                   VARCHAR2(30) := 'DL_HRA_WAT_CHRG_CALC_AUDITS';
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
    fsc_utils.proc_start('s_dl_hra_wat_chrg_calc_audits.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_wat_chrg_calc_audits.dataload_delete',3 );
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
          cs   := p1.lwcca_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--
-- Delete from WATER_CHARGE_CALC_AUDITS table
--
--
          DELETE 
            FROM water_usage_detail_usages
           WHERE wudu_wcca_refno = p1.lwcca_refno;
           
          DELETE
            FROM water_charge_calc_audits
           WHERE wcca_refno = p1.lwcca_refno;
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
          IF mod(i,10000) = 0 THEN
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('WATER_CHARGE_CALC_AUDITS');
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
END s_dl_hra_wat_chrg_calc_audits;
/
