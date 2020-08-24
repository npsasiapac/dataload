CREATE OR REPLACE PACKAGE BODY s_dl_hem_income_details
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.5.0     VS   01-JUL-2011  Initial Creation.
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
    UPDATE dl_hem_income_details
       SET lindt_dl_load_status = p_status
     WHERE rowid                = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hem_income_details');
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
CURSOR  c1 
IS
SELECT rowid rec_rowid,
       LINDT_DLB_BATCH_ID,
       LINDT_DL_SEQNO,
       LINDT_DL_LOAD_STATUS,
       LINDT_LEGACY_REF,
       LINDT_INH_LEGACY_REF,
       LINDT_PAR_REF_TYPE,
       LINDT_INCO_CODE,
       NVL(LINDT_USE_CALCULATED_WAGE_IND,'N') LINDT_USE_CALCULATED_WAGE_IND,
       NVL(LINDT_CREATED_BY,'DATALOAD')       LINDT_CREATED_BY,
       NVL(LINDT_CREATED_DATE, SYSDATE)       LINDT_CREATED_DATE,
       LINDT_AMOUNT,
       LINDT_INF_CODE,
       LINDT_REGULAR_AMOUNT,
       LINDT_HRV_VETY_CODE,
       LINDT_EMP_PAR_REFERENCE,
       LINDT_EMPLOYMENT_START_DATE,
       LINDT_WAGE_START_DATE,
       LINDT_WAGE_END_DATE,
       LINDT_GROSS_WAGE,
       LINDT_REIMBURSEMENT_AMOUNT,
       LINDT_NUM_DAYS_WITHOUT_PAY,
       LINDT_CALCULATED_WAGE,
       LINDT_COMMENTS,
       LINDT_MODIFIED_BY,
       LINDT_MODIFIED_DATE,
       LINDT_BOARDER_PAR_REFERENCE,
       LINDT_ALTERNATIVE_AMOUNT,
       LINDT_REFNO
  FROM dl_hem_income_details
 WHERE lindt_dlb_batch_id   = p_batch_id
   AND lindt_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
--
CURSOR c_prf_refno (p_par_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_alt_ref;
--
-- ***********************************************************************
--
CURSOR c_get_inh(p_inh_legacy_ref NUMBER) 
IS
SELECT linh_refno
  FROM dl_hem_income_headers,
       income_headers
 WHERE linh_legacy_ref     = p_inh_legacy_ref
   AND linh_dl_load_status = 'C'
   AND inh_refno           = linh_refno;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HEM_INCOME_DETAILS';
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
l_boarder_par_refno        parties.par_refno%type;
l_emp_par_refno            parties.par_refno%type;
l_inh_refno                NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger INDT_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hem_income_details.dataload_create');
    fsc_utils.debug_message('s_dl_hem_income_details.dataload_create',3);
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
          cs   := p1.lindt_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
          l_emp_par_refno     := NULL;
          l_boarder_par_refno := NULL;
          l_inh_refno         := NULL;
--
          IF (p1.LINDT_PAR_REF_TYPE = 'PRF') THEN
--
           IF (p1.LINDT_EMP_PAR_REFERENCE IS NOT NULL) THEN
--
             OPEN c_prf_refno(p1.LINDT_EMP_PAR_REFERENCE);
            FETCH c_prf_refno INTO l_emp_par_refno;
            CLOSE c_prf_refno;
--
           END IF;
--
           IF (p1.LINDT_BOARDER_PAR_REFERENCE IS NOT NULL) THEN
-- 
             OPEN c_prf_refno(p1.LINDT_BOARDER_PAR_REFERENCE);
            FETCH c_prf_refno INTO l_boarder_par_refno;
            CLOSE c_prf_refno;
--
           END IF;
--
          ELSIF (p1.LINDT_PAR_REF_TYPE = 'PAR') THEN 
--
              l_emp_par_refno     := p1.LINDT_EMP_PAR_REFERENCE;
              l_boarder_par_refno := p1.LINDT_BOARDER_PAR_REFERENCE;
--
          END IF;
--
--
-- Get the income_header_refno
--
           OPEN c_get_inh(p1.LINDT_INH_LEGACY_REF);
          FETCH c_get_inh INTO l_inh_refno;
          CLOSE c_get_inh;
--
--
-- Insert int relevent table
--
          INSERT /* +APPEND */ into  income_details(INDT_REFNO,
                                                    INDT_INH_REFNO,
                                                    INDT_INCO_CODE,
                                                    INDT_USE_CALCULATED_WAGE_IND,
                                                    INDT_CREATED_BY,
                                                    INDT_CREATED_DATE,
                                                    INDT_AMOUNT,
                                                    INDT_INF_CODE,
                                                    INDT_REGULAR_AMOUNT,
                                                    INDT_HRV_VETY_CODE,
                                                    INDT_EMPLOYMENT_START_DATE,
                                                    INDT_WAGE_START_DATE,
                                                    INDT_WAGE_END_DATE,
                                                    INDT_GROSS_WAGE,
                                                    INDT_REIMBURSEMENT,
                                                    INDT_NUM_DAYS_WITHOUT_PAY,
                                                    INDT_CALCULATED_WAGE,
                                                    INDT_PAR_REFNO,
                                                    INDT_COMMENTS,
                                                    INDT_MODIFIED_BY,
                                                    INDT_MODIFIED_DATE,
                                                    INDT_DEN_HRV_IBEN_CODE,
                                                    INDT_DEN_IBEN_INF_CODE,
                                                    INDT_BOARDER_PAR_REFNO,
                                                    INDT_ALTERNATIVE_AMOUNT
                                                   )
--
                                            VALUES (p1.lindt_refno,
                                                    l_inh_refno,
                                                    p1.LINDT_INCO_CODE,
                                                    p1.LINDT_USE_CALCULATED_WAGE_IND,
                                                    p1.LINDT_CREATED_BY,
                                                    p1.LINDT_CREATED_DATE,
                                                    p1.LINDT_AMOUNT,
                                                    p1.LINDT_INF_CODE,
                                                    p1.LINDT_REGULAR_AMOUNT,
                                                    p1.LINDT_HRV_VETY_CODE,
                                                    p1.LINDT_EMPLOYMENT_START_DATE,
                                                    p1.LINDT_WAGE_START_DATE,
                                                    p1.LINDT_WAGE_END_DATE,
                                                    p1.LINDT_GROSS_WAGE,
                                                    p1.LINDT_REIMBURSEMENT_AMOUNT,
                                                    p1.LINDT_NUM_DAYS_WITHOUT_PAY,
                                                    p1.LINDT_CALCULATED_WAGE,
                                                    l_emp_par_refno,
                                                    p1.LINDT_COMMENTS,
                                                    p1.LINDT_MODIFIED_BY,
                                                    p1.LINDT_MODIFIED_DATE,
                                                    NULL,
                                                    NULL,
                                                    l_boarder_par_refno,
                                                    p1.LINDT_ALTERNATIVE_AMOUNT
                                                   );
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
          i := i+1; 
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
    execute immediate 'alter trigger INDT_BR_I enable';
-- 
    COMMIT;
--
-- ***********************************************************************
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('INCOME_DETAILS');
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
PROCEDURE dataload_validate(p_batch_id  IN VARCHAR2,
                            p_date      IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LINDT_DLB_BATCH_ID,
       LINDT_DL_SEQNO,
       LINDT_DL_LOAD_STATUS,
       LINDT_LEGACY_REF,
       LINDT_INH_LEGACY_REF,
       LINDT_PAR_REF_TYPE,
       LINDT_INCO_CODE,
       NVL(LINDT_USE_CALCULATED_WAGE_IND,'N') LINDT_USE_CALCULATED_WAGE_IND,
       NVL(LINDT_CREATED_BY,'DATALOAD')       LINDT_CREATED_BY,
       NVL(LINDT_CREATED_DATE, SYSDATE)       LINDT_CREATED_DATE,
       LINDT_AMOUNT,
       LINDT_INF_CODE,
       LINDT_REGULAR_AMOUNT,
       LINDT_HRV_VETY_CODE,
       LINDT_EMP_PAR_REFERENCE,
       LINDT_EMPLOYMENT_START_DATE,
       LINDT_WAGE_START_DATE,
       LINDT_WAGE_END_DATE,
       LINDT_GROSS_WAGE,
       LINDT_REIMBURSEMENT_AMOUNT,
       LINDT_NUM_DAYS_WITHOUT_PAY,
       LINDT_CALCULATED_WAGE,
       LINDT_COMMENTS,
       LINDT_MODIFIED_BY,
       LINDT_MODIFIED_DATE,
       LINDT_BOARDER_PAR_REFERENCE,
       LINDT_ALTERNATIVE_AMOUNT,
       LINDT_REFNO
  FROM dl_hem_income_details
 WHERE lindt_dlb_batch_id    = p_batch_id
   AND lindt_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR c_chk_inh_exists(p_inh_legacy_ref NUMBER) 
IS
SELECT inh_refno,
       inh_sco_code
  FROM dl_hem_income_headers,
       income_headers
 WHERE linh_dl_load_status = 'C'
   AND linh_legacy_ref     = p_inh_legacy_ref
   AND inh_refno           = linh_refno;
--
-- ***********************************************************************
--
CURSOR c_prf_refno(p_par_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_alt_ref;
--
-- ***********************************************************************
--
CURSOR c_par_refno(p_par_refno VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_refno;
--
-- ***********************************************************************
--
CURSOR c_chk_inco(p_inco_code  VARCHAR2) 
IS
SELECT inco_code,
       inco_wages_ind,
       inco_multiple_allowed_ind,
       inco_calc_avg_income_ind
  FROM income_codes
 WHERE inco_code = p_inco_code;
--
-- ***********************************************************************
--
CURSOR c_chk_inf(p_inf_code  VARCHAR2) 
IS
SELECT 'X'
  FROM income_frequencies
 WHERE inf_code = p_inf_code;
--
-- ***********************************************************************
--
CURSOR c_chk_vety(p_inco_code   VARCHAR2,
                  p_vety_Code   VARCHAR2) 
IS
SELECT 'X'
  FROM income_code_verifications
 WHERE incv_inco_code     = p_inco_Code
   AND incv_hrv_vety_code = p_vety_code;
--
-- ***********************************************************************
--
CURSOR chk_inco_mult_allowed_dl(p_inco_code      VARCHAR2,
                                p_inh_legacy_ref NUMBER) 
IS
SELECT count(*)
  FROM dl_hem_income_details
 WHERE lindt_inco_code      = p_inco_Code
   AND lindt_inh_legacy_ref = p_inh_legacy_ref;
--
-- ***********************************************************************
--
CURSOR chk_inco_mult_allowed(p_inco_code  VARCHAR2,
                             p_inh_refno  NUMBER) 
IS
SELECT count(*)
  FROM income_details
 WHERE indt_inco_code = p_inco_Code
   AND indt_inh_refno = p_inh_refno;
--
-- ***********************************************************************
--
CURSOR c_get_pva_value(p_pva_pdu_pdf_name VARCHAR2) 
IS
SELECT pva_char_value
  FROM parameter_values
 WHERE pva_pdu_pdf_name       = p_pva_pdu_pdf_name
   AND pva_pdu_pdf_param_type = 'SYSTEM';
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                         VARCHAR2(30);
cd                         DATE;
cp                         VARCHAR2(30) := 'VALIDATE';
ct                         VARCHAR2(30) := 'DL_HEM_INCOME_DETAILS';
cs                         INTEGER;
ce                         VARCHAR2(200);
l_id                       ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists                   VARCHAR2(1);
l_pro_refno                NUMBER(10);
l_errors                   VARCHAR2(10);
l_error_ind                VARCHAR2(10);
i                          INTEGER :=0;
l_par_refno                parties.par_refno%type;
l_emp_par_refno            parties.par_refno%type;
l_boarder_par_refno        parties.par_refno%type;
l_vety                     VARCHAR2(1);
l_inh_refno                NUMBER(10);
l_inh_sco_code             VARCHAR2(3);
--
l_inco_code                VARCHAR2(10);
l_inco_wage_ind            VARCHAR2(1);
l_inco_mult_allowed_ind    VARCHAR2(1);
l_inco_calc_avg_income_ind VARCHAR2(1);
l_mult_allowed_cnt_1       INTEGER;
l_mult_allowed_cnt_2       INTEGER;
--
l_pva_char_value_1         VARCHAR2(255);
l_pva_char_value_2         VARCHAR2(255);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_income_details.dataload_validate');
    fsc_utils.debug_message( 's_dl_hem_income_details.dataload_validate',3);
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
          cs   := p1.lindt_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
--
          l_inh_refno                := NULL;
          l_inh_sco_code             := NULL;
--
          l_emp_par_refno            := NULL;
          l_boarder_par_refno        := NULL;
--
          l_inco_code                := NULL;
          l_inco_wage_ind            := NULL;
          l_inco_mult_allowed_ind    := NULL;
          l_inco_calc_avg_income_ind := NULL;
          l_mult_allowed_cnt_1       := 0;
          l_mult_allowed_cnt_2       := 0;
--
          l_pva_char_value_1         := NULL;
          l_pva_char_value_2         := NULL;
--
--
-- ***********************************************************************
--
-- Check Income Header Legacy Reference exists in INCOME_HEADERS
--
           OPEN c_chk_inh_exists(p1.LINDT_INH_LEGACY_REF);
--
          FETCH c_chk_inh_exists INTO l_inh_refno, 
                                      l_inh_sco_code;
--
          IF (c_chk_inh_exists%NOTFOUND) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',473);
          END IF;
--
          CLOSE c_chk_inh_exists;
--
-- ************************************************************************************
--
-- Check the Object Source Type is Valid
--
          IF p1.LINDT_PAR_REF_TYPE NOT IN ('PAR', 'PRF') THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',864);
          END IF;
--
-- ************************************************************************************
--
-- If supplied, Check Employee and Border Party reference are valid
--
--
          IF (p1.LINDT_PAR_REF_TYPE = 'PAR') THEN
--
           IF (p1.LINDT_EMP_PAR_REFERENCE IS NOT NULL) THEN
--
             OPEN c_par_refno(p1.LINDT_EMP_PAR_REFERENCE);
            FETCH c_par_refno INTO l_emp_par_refno;
--
            IF (c_par_refno%NOTFOUND) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',019);
            END IF;
--
            CLOSE c_par_refno;
--
           END IF;
--
           IF (p1.LINDT_BOARDER_PAR_REFERENCE IS NOT NULL) THEN
--
             OPEN c_par_refno(p1.LINDT_BOARDER_PAR_REFERENCE);
            FETCH c_par_refno INTO l_boarder_par_refno;
--
            IF (c_par_refno%NOTFOUND) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',477);
            END IF;
--
            CLOSE c_par_refno;
--
           END IF;
--
          END IF;
--
-- *********************
--
          IF (p1.LINDT_PAR_REF_TYPE = 'PRF') THEN
--
           IF (p1.LINDT_EMP_PAR_REFERENCE IS NOT NULL) THEN
--
             OPEN c_prf_refno(p1.LINDT_EMP_PAR_REFERENCE);
            FETCH c_prf_refno INTO l_emp_par_refno;
--
            IF (c_prf_refno%NOTFOUND) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',019);
            END IF;
--
            CLOSE c_prf_refno;
--
           END IF;
--
           IF (p1.LINDT_BOARDER_PAR_REFERENCE IS NOT NULL) THEN
--
             OPEN c_prf_refno(p1.LINDT_BOARDER_PAR_REFERENCE);
            FETCH c_prf_refno INTO l_boarder_par_refno;
--
            IF (c_prf_refno%NOTFOUND) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',477);
            END IF;
--
            CLOSE c_prf_refno;
--
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- Income Code must exist on the Income Codes Table
--
           OPEN c_chk_inco(p1.lindt_inco_code);
          FETCH c_chk_inco INTO l_inco_code,
                                l_inco_wage_ind,
                                l_inco_mult_allowed_ind,
                                l_inco_calc_avg_income_ind;
--
          IF (c_chk_inco%NOTFOUND) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',18);
          END IF;
--
          CLOSE c_chk_inco;
--
-- ************************************************************************************
--
-- Income Frequency Code must exist on the Income Frequencies Table
--
           OPEN c_chk_inf(p1.lindt_inf_code);
          FETCH c_chk_inf INTO l_exists;
--
          IF (c_chk_inf%NOTFOUND) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',337);
          END IF;
--
          CLOSE c_chk_inf;
--
-- ************************************************************************************
--
-- Use Calculated Wage Indicator Y or N
--
          IF p1.lindt_use_calculated_wage_ind NOT IN ( 'Y', 'N' ) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',22);
          END IF;
--
-- ************************************************************************************
--
-- Check Income Code Verifications                                   
--
          IF (    p1.lindt_inco_code     IS NOT NULL
              AND p1.lindt_hrv_vety_code IS NOT NULL) THEN
--
            OPEN c_chk_vety(p1.lindt_inco_code,
                            p1.lindt_hrv_vety_code);
--
           FETCH c_chk_vety into l_vety;
-- 
           IF (c_chk_vety%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',265); 
           END IF;
--
           CLOSE c_chk_vety;
--
          END IF;
--
-- ************************************************************************************
--
-- If supplied, the wage end date must not be before the wage start date
--
          IF (p1.lindt_wage_end_date IS NOT NULL) THEN
--
           IF (p1.lindt_wage_end_date < nvl(p1.lindt_wage_start_date, p1.lindt_wage_end_date)) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',20);
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- If calculated Wage is supplied then Wage Start Date, 
-- Wage End Date and Gross Wage must be supplied
--
          IF (p1.lindt_calculated_wage IS NOT NULL) THEN
--
           IF (   p1.lindt_wage_start_date IS NULL
               OR p1.lindt_wage_end_date   IS NULL
               OR p1.lindt_gross_wage      IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',21);
--
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- If the Income Code is the same as that assigned to the ‘DEEMED_INCOME_CODE’ 
-- system parameter then the Income Detail may only be loaded where the associated 
-- Income Header has a status of ‘VER’.
--
           OPEN c_get_pva_value('DEEMED_INCOME_CODE');
          FETCH c_get_pva_value INTO l_pva_char_value_1;
          CLOSE c_get_pva_value;
--
          IF (    p1.lindt_inco_code  = l_pva_char_value_1
              AND l_inh_sco_code     != 'VER') THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',478);
--
          END IF;
--
--
-- ************************************************************************************
--
-- If the Income Code has ‘Multiple’s Allowed’ set to ‘N’ 
-- then only one instance of Income Detail, for that Income Code, must be loaded 
-- against an Income Header. Check INCOME_HEADERS as well as DL_HEM_INCOME_HEADERS
--
--
          IF (l_inco_mult_allowed_ind = 'N') THEN
--
            OPEN chk_inco_mult_allowed_dl(p1.lindt_inco_code,
                                          p1.lindt_inh_legacy_ref);
--
           FETCH chk_inco_mult_allowed_dl INTO l_mult_allowed_cnt_1;
           CLOSE chk_inco_mult_allowed_dl;
--
--
            OPEN chk_inco_mult_allowed(p1.lindt_inco_code,
                                       l_inh_refno);
--
           FETCH chk_inco_mult_allowed INTO l_mult_allowed_cnt_2;
           CLOSE chk_inco_mult_allowed;
--
           IF (l_mult_allowed_cnt_1 + l_mult_allowed_cnt_2 > 1) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',479);
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- If the Income Code has ‘Wages’ set to ‘Y’ then Employment Start Date 
-- and Employer must be supplied. If set to ‘N’ these fields must not be supplied. 
--
          IF (l_inco_wage_ind = 'Y') THEN
--
           IF (   p1.LINDT_EMP_PAR_REFERENCE     IS NULL
               OR p1.LINDT_EMPLOYMENT_START_DATE IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',480);
--
           END IF;
--
          ELSIF (l_inco_wage_ind = 'N') THEN
--
              IF (   p1.LINDT_EMP_PAR_REFERENCE     IS NOT NULL
                  OR p1.LINDT_EMPLOYMENT_START_DATE IS NOT NULL) THEN
--
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',481);
--
              END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- If Income Code has ‘Calculate Average Income’ set to ‘M’ or ‘O’ then 
-- Income Start, Income End, Gross Amount, Allowance Amount and Number of Days 
-- without Pay may be supplied. If set to ‘N’ these fields must not be supplied.
--
          IF (l_inco_calc_avg_income_ind IN ('M','O')) THEN
--
           IF (   p1.LINDT_WAGE_START_DATE      IS NULL
               OR p1.LINDT_WAGE_END_DATE        IS NULL
               OR p1.LINDT_GROSS_WAGE           IS NULL
               OR p1.LINDT_REIMBURSEMENT_AMOUNT IS NULL
               OR p1.LINDT_NUM_DAYS_WITHOUT_PAY IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',482);
--
           END IF;
--
          ELSIF (l_inco_calc_avg_income_ind NOT IN ('M','O')) THEN
--
              IF (   p1.LINDT_WAGE_START_DATE      IS NOT NULL
                  OR p1.LINDT_WAGE_END_DATE        IS NOT NULL
                  OR p1.LINDT_GROSS_WAGE           IS NOT NULL
                  OR p1.LINDT_REIMBURSEMENT_AMOUNT IS NOT NULL
                  OR p1.LINDT_NUM_DAYS_WITHOUT_PAY IS NOT NULL) THEN
--
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',483);
--
              END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- All reference values are valid
--
--  Income Verification Type
--
          IF (NOT s_dl_hem_utils.exists_frv('VERIFIEDTYPE',p1.lindt_hrv_vety_code,'Y')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',23);
          END IF;
--
-- ************************************************************************************
--
-- If Alternative Amount has been supplied then Weekly Amount should be set 
-- to Alternative Amount
--
-- If Alternative Amount has not been supplied but a Calculated Wage has been 
-- supplied and Calculated Wage Indicator is set to ‘Y’ then Weekly Amount should be 
-- set to Calculated Wage
--
--
          IF (p1.lindt_alternative_amount IS NOT NULL) THEN
--
           IF (p1.lindt_regular_amount != p1.lindt_alternative_amount) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',484);
           END IF;
--
          ELSE
--
             IF (    p1.lindt_calculated_wage          IS NOT NULL
                 AND p1.lindt_use_calculated_wage_ind  = 'Y'
                 AND p1.lindt_regular_amount          != p1.lindt_calculated_wage) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',485);
--
             END IF;

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
PROCEDURE dataload_delete(p_batch_id IN VARCHAR2,
                          p_date     IN date) 
IS
--
CURSOR c1 is
SELECT rowid rec_rowid,
       LINDT_DLB_BATCH_ID,
       LINDT_DL_SEQNO,
       LINDT_DL_LOAD_STATUS,
       LINDT_REFNO
  FROM dl_hem_income_details
 WHERE lindt_dlb_batch_id   = p_batch_id
   AND lindt_dl_load_status = 'C';
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
cb               VARCHAR2(30);
cd               DATE;
cp               VARCHAR2(30) := 'DELETE';
ct               VARCHAR2(30) := 'DL_HEM_INCOME_DETAILS';
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
    fsc_utils.proc_start('s_dl_hem_income_details.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_income_details.dataload_delete',3 );
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
          cs   := p1.lindt_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Delete from income_details table
--
          DELETE 
            FROM income_details
           WHERE indt_refno = p1.lindt_refno;
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
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('INCOME_DETAILS');
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
END s_dl_hem_income_details;
/

