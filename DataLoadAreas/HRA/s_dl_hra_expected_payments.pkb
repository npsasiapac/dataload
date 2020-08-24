CREATE OR REPLACE PACKAGE BODY HOU.s_dl_hra_expected_payments
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.16.0    VRS  24-AUG-2009  Initial Creation.
--  2.0     6.8       PJD  04-SEP-2013  New Version
--  2.1     6.9       PJD  03-SEP-2014  Replaced SAS error codes with HD1 error codes
-- 
--  2.2     6.12      VRS  12-MAY-2016  6.12 Update
--
--  2.3     6.12      VRS  26-APR-2018  Correction to Cursor chk_pexp_refno in the VALIDATE
--                                      procedure. This was returning the pexp_refno into a VARCHAR2(1)
--                                      local variable. Have changed cursor to return 'Y' now.
--
--  NOTES:
--
--  1) Arr_refno and Pme_refno are set to NULL as NSW won't be supplying this
--
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
    UPDATE dl_hra_expected_payments
       SET lexpa_dl_load_status = p_status
     WHERE rowid                = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hra_expected_payments');
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
       lexpa_dlb_batch_id,
       lexpa_dl_seqno,
       lexpa_dl_load_status,
       lexpa_type,
       lexpa_rac_pay_ref,
       lexpa_comment,
       lexpa_payment_amount,
       lexpa_payment_due_date,
       lexpa_payment_overdue_date, 
       lexpa_unpaid_balance,
       NVL(lexpa_extracted_ind,'N')      lexpa_extracted_ind,
       lexpa_extracted_date,
       lexpa_refno
  FROM dl_hra_expected_payments
 WHERE lexpa_dlb_batch_id   = p_batch_id
   AND lexpa_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
-- Match to the payment_expectation
--
CURSOR c_pexp_refno(p_rac_accno     NUMBER,
                    p_ref_comment   VARCHAR2)
IS
SELECT pexp_refno
  FROM payment_expectations
 WHERE pexp_rac_accno = p_rac_accno
   AND NVL(pexp_comments,'~NONE~') = NVL(p_ref_comment,'~NONE~');
--
-- ***********************************************************************
--
-- Get the Account Arrears Arrangement Refno
--
--CURSOR get_arr_refno(p_rac_accno  NUMBER,
--                     p_ara_code   VARCHAR2,
--                     p_start_date DATE)
--IS
--SELECT MAX(arr_refno)
--  FROM account_arrears_arrangements
-- WHERE arr_ara_code   = p_ara_code
--   AND arr_start_date = p_start_date
--   AND arr_rac_accno  = p_rac_accno;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_EXPECTED_PAYMENTS';
cs                   INTEGER;
ce	                 VARCHAR2(200);
--
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
l_expa_refno         PLS_INTEGER;
l_pexp_refno         PLS_INTEGER;
l_rac_accno          PLS_INTEGER;
l_arr_refno          NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_expected_payments.dataload_create');
    fsc_utils.debug_message('s_dl_hra_expected_payments.dataload_create',3);
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
          cs           := p1.lexpa_dl_seqno;
          l_id         := p1.rec_rowid;
--
          l_rac_accno  := NULL;
          l_expa_refno := NULL;
          l_pexp_refno := NULL;
--
--
          l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref(p1.lexpa_rac_pay_ref );
--
--
           OPEN c_pexp_refno (l_rac_accno, p1.lexpa_comment);
          FETCH c_pexp_refno INTO l_pexp_refno;
          CLOSE c_pexp_refno;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Insert into EXPECTED_PAYMENTS table
--
--
          INSERT INTO expected_payments (EXPA_REFNO,
                                         EXPA_TYPE,
                                         EXPA_RAC_ACCNO,
                                         EXPA_PAYMENT_AMOUNT,
                                         EXPA_PAYMENT_DUE_DATE,
                                         EXPA_PAYMENT_OVERDUE_DATE,
                                         EXPA_UNPAID_BALANCE,
                                         EXPA_PEXP_REFNO,
                                         EXPA_SCO_CODE,
                                         EXPA_EXTRACTED_IND,
                                         EXPA_EXTRACTED_DATE
                                        )
--
                                 VALUES (p1.LEXPA_REFNO,
                                         p1.LEXPA_TYPE,
                                         l_rac_accno,
                                         p1.LEXPA_PAYMENT_AMOUNT,
                                         p1.LEXPA_PAYMENT_DUE_DATE,
                                         p1.LEXPA_PAYMENT_OVERDUE_DATE,
                                         p1.LEXPA_UNPAID_BALANCE,
                                         l_pexp_refno,
                                         'ACT',
                                         p1.LEXPA_EXTRACTED_IND,
                                         p1.LEXPA_EXTRACTED_DATE
                                        );
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
          i := i + 1;
--
          IF MOD(i,500000)=0 THEN
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('EXPECTED_PAYMENTS');
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
       lexpa_dlb_batch_id,
       lexpa_dl_seqno,
       lexpa_dl_load_status,
       lexpa_type,
       lexpa_rac_pay_ref,
       lexpa_comment,
       lexpa_payment_amount,
       lexpa_payment_due_date,
       lexpa_payment_overdue_date, 
       lexpa_unpaid_balance,
       NVL(lexpa_extracted_ind,'N')      lexpa_extracted_ind,
       lexpa_extracted_date
       lexpa_refno
  FROM dl_hra_expected_payments
WHERE lexpa_dlb_batch_id   = p_batch_id
  AND lexpa_dl_load_status IN ('L','F','O');
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- Match to the payment_expectation
--
-- Updated 26-APR-2018 VRS
--
CURSOR chk_pexp_refno(p_rac_accno    NUMBER,
                      p_comment_ref  VARCHAR2) 
IS
SELECT 'Y'
  FROM payment_expectations
 WHERE pexp_rac_accno = p_rac_accno
   AND NVL(pexp_comments,'~NONE~') = NVL(p_comment_ref,'~NONE~');
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
-- Check Arrears Action Code is Valid
--
--CURSOR chk_arr_code(p_arr_code VARCHAR2)
--IS
--SELECT 'X'
--  FROM arrears_actions
-- WHERE ara_code = p_arr_code;
--
-- ***********************************************************************
--
-- Check Payment Method Code is Valid
--
--CURSOR chk_pme_code(p_pme_code VARCHAR2)
--IS
--SELECT 'X'
--  FROM payment_methods
-- WHERE pme_code = p_pme_code;
--
-- ***********************************************************************
--
-- Check Account Arrears Arrangement Refno is Valid for ara_code and start
-- date.
--
--CURSOR get_arr_refno(p_rac_accno  NUMBER,
--                     p_ara_code   VARCHAR2,
--                     p_start_date DATE)
--IS
--SELECT MAX(arr_refno)
--  FROM account_arrears_arrangements
-- WHERE arr_ara_code   = p_ara_code
--   AND arr_start_date = p_start_date
--   AND arr_rac_accno  = p_rac_accno;
--
-- ***********************************************************************
--
-- Check Admin Unit Code is Valid
--
--CURSOR chk_aun_code(p_aun_code VARCHAR2)
--IS
--SELECT 'X'
--  FROM admin_units
-- WHERE aun_code = p_aun_code;
--
-- ***********************************************************************
--
-- Check Arrears Installment record is Valid for arr_refno and sequence
--
--CURSOR chk_ain_exists(p_arr_refno NUMBER,
--                      p_ain_seqno VARCHAR2)
--IS
--SELECT 'X'
--  FROM arrears_installments
-- WHERE ain_arr_refno = p_arr_refno
--   AND ain_seqno     = p_ain_seqno;
--
-- ***********************************************************************
--
-- Check Admin Period Record Exists
--
--CURSOR chk_ape_exists(p_ape_aun_code    VARCHAR2,
--                      p_ape_year        NUMBER,
--                      p_ape_period_no   VARCHAR2,
--                      p_ape_period_type VARCHAR2)
--IS
--SELECT 'X'
--  FROM admin_periods
-- WHERE APE_AYE_AUN_CODE      = p_ape_aun_code
--   AND APE_AYE_YEAR          = p_ape_year
--   AND APE_PERIOD_NO         = p_ape_period_no
--   AND APE_ADMIN_PERIOD_TYPE = p_ape_period_type;
--
-- ***********************************************************************
--
-- Check Payment Details Record Exists
--
--CURSOR chk_pde_exists(p_pme_refno            NUMBER,
--                      p_pde_aun_code         VARCHAR2,
--                      p_pde_year             NUMBER,
--                      p_pde_ppc_code         VARCHAR2,
--                      p_pde_payment_due_date DATE)
--IS
--SELECT 'X'
--  FROM payment_details
-- WHERE PDE_PME_REFNO            = p_pme_refno
--   AND PDE_PIT_PPR_AYE_AUN_CODE = p_pde_aun_code
--   AND PDE_PIT_PPR_AYE_YEAR     = p_pde_year
--   AND PDE_PIT_PPR_HRV_PPC_CODE = p_pde_ppc_code
--   AND PDE_PIT_PAYMENT_DUE_DATE = p_pde_payment_due_date;
--
-- ***********************************************************************
--
-- Check Profile Items Record Exists
--
--CURSOR chk_pit_exists(p_pit_aun_code         VARCHAR2,
--                      p_pit_year             NUMBER,
--                      p_pit_ppc_code         VARCHAR2,
--                      p_pit_payment_due_date DATE)
--IS
--SELECT 'X'
--  FROM profile_items
-- WHERE PIT_PPR_AYE_AUN_CODE = p_pit_aun_code
--   AND PIT_PPR_AYE_YEAR     = p_pit_year
--   AND PIT_PPR_HRV_PPC_CODE = p_pit_ppc_code
--   AND PIT_PAYMENT_DUE_DATE = p_pit_payment_due_date;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HRA_EXPECTED_PAYMENTS';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_expa_exists          VARCHAR2(1);
l_pexp_exists          VARCHAR2(1);
l_rac_exists           VARCHAR2(1);
l_rac_accno            NUMBER(10);  
--
l_pit_aun_code_exists  VARCHAR2(1);
l_ape_aun_code_exists  VARCHAR2(1);
l_pde_aun_code_exists  VARCHAR2(1);
--
l_ape_exists           VARCHAR2(1);
l_pde_exists           VARCHAR2(1);
l_pit_exists           VARCHAR2(1);

l_ara_exists           VARCHAR2(1);
l_ain_exists           VARCHAR2(1);
l_arr_refno            NUMBER(10);
l_pme_exists           VARCHAR2(1);
l_pme_refno            NUMBER(10);
--
l_errors               VARCHAR2(10);
l_error_ind            VARCHAR2(10);
i                      INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_expected_payments.dataload_validate');
    fsc_utils.debug_message( 's_dl_hra_expected_payments.dataload_validate',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs   := p1.lexpa_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
-- ***********************************************************************
--
-- Perform Validation on Mandatory Columns
--
-- ***********************************************************************
--
          IF (p1.lexpa_type IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',881);
          ELSIF (p1.lexpa_type NOT IN ('EXDP', 'EXNC', 'EXPR', 'EXPD', 'EXAI', 'EXHB', 'EXSP')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',771);
          END IF;
--
-- ***********************************************************************
--         
          IF (p1.lexpa_rac_pay_ref IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',159);
          ELSE
--
              l_rac_accno := NULL;
--
              l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref(p1.lexpa_rac_pay_ref);
--
              IF (l_rac_accno IS NULL) THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',117);
              END IF;
--
          END IF;
--
-- ***********************************************************************
--         
          IF (p1.lexpa_comment IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',882);
          END IF;
--
-- ***********************************************************************
--         
          IF (p1.lexpa_payment_amount IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',775);
          END IF;
--
-- ***********************************************************************
--
          IF (p1.lexpa_payment_due_date IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',774);
          END IF;
--
-- ***********************************************************************
--         
          IF (p1.lexpa_payment_overdue_date IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',773);
          END IF;
--
-- ***********************************************************************
--         
          IF (p1.lexpa_unpaid_balance IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',772);
          END IF;
--
-- ***********************************************************************
--         
          IF (p1.lexpa_extracted_ind NOT IN ('Y','N')) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',884);
          END IF;
--
--
-- ***********************************************************************
--
-- Check that the Payment Expectation reference exists
--
          l_pexp_exists := NULL;
--
           OPEN chk_pexp_refno(l_rac_accno,p1.lexpa_comment);
          FETCH chk_pexp_refno INTO l_pexp_exists;
          CLOSE chk_pexp_refno;
--
          IF (l_pexp_exists IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',770);
          END IF;
--
-- ***********************************************************************
--
-- Check Payment OverDue Date is >= Payment Due Date
--
          IF (    p1.LEXPA_PAYMENT_DUE_DATE     IS NOT NULL
              AND p1.LEXPA_PAYMENT_OVERDUE_DATE IS NOT NULL) THEN
--
           IF (p1.lexpa_payment_overdue_date < p1.lexpa_payment_due_date) THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',883);
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
    fsc_utils.proc_end;
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
       LEXPA_DLB_BATCH_ID,
       LEXPA_DL_SEQNO,
       LEXPA_DL_LOAD_STATUS,
       LEXPA_REFNO
  FROM dl_hra_expected_payments
 WHERE lexpa_dlb_batch_id   = p_batch_id
   AND lexpa_dl_load_status = 'C';
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
ct                   VARCHAR2(30) := 'DL_HRA_EXPECTED_PAYMENTS';
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
    fsc_utils.proc_start('s_dl_hra_expected_payments.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_expected_payments.dataload_delete',3 );
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
          cs   := p1.lexpa_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--
-- Delete from EXPECTED_PAYMENTS table
--
--
          DELETE
            FROM expected_payments
           WHERE expa_refno = p1.lexpa_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('EXPECTED_PAYMENTS');
--
    fsc_utils.proc_end;
    COMMIT;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
            RAISE;
--
END dataload_delete;
--
END s_dl_hra_expected_payments;
/
show errors