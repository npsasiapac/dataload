CREATE OR REPLACE PACKAGE BODY hou.s_dl_hra_payment_expectations
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.16.0    VRS  18-AUG-2009  Initial Creation.
--
--  2.0     5.16.0    MT   29-OCT-2009  Revised creation to set e acho_refno 
--                                      matched against the acho_alternative_reference
--                                      Corrected the checks on trans types.
--
--  3.0     6.8.0     VRS  08-AUG-2013  6.8.0 Changes + Functionality.
--  3.1     6.9.0     PJD  03-SEP-2014  Replace SAS error codes with HDL error codes
--
--  3.2     6.12      VRS  03-MAY-2016  6.12 Changes + Functionality + error code 
--                                      corrections.
--  
--  3.3     6.12      VRS  27-APR-2018  Core Reference not generated correctly. Changing to Umesh Joshi's
--                                      correction (Ref #3.3)
--                                      Incorrect l_core_ref := LPAD(s_revenue_accounts2.get_pay_ref_from_accno(l_pexp_rac_accno) ,TO_NUMBER(l_payrefln) ,'0') || LPAD(l_core_ref_version,2,'0');
--                                      Correct   l_core_ref := LPAD(s_revenue_accounts2.get_pay_ref_from_accno(l_pexp_rac_accno) || LPAD(l_core_ref_version,2,'0') ,TO_NUMBER(l_payrefln) ,'0');   -- Ref# 3.3
--
--  3.4     6.18      VRS  04-DEC-2018  6.18 Changes to accommodate RDS_ACCOUNT_DEDUCTIONS functionality
--                                      for Queensland
--  
--                                      Incorrect HD1 877 Error code assigned when checking Payment Expectation Type supplied. This has
--                                      been updated to 758
--  3.5     6.18      PML  30-APR-2019  Added PEXP_MAX_EXPA_DUE_DATE load.
--  3.6     6.18      PML  07-JUN-2019  Changed RDS loader details
--  NOTES:
--
--  1) Arr_refno and Pme_refno are set to NULL as NSW won't be supplying this
--
--  2) l_pexp_direct_debit_ind is derived from payment_expectation_type setup
--
--  declare package variables AND constants
--
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag (p_rowid  IN ROWID, 
                                  p_status IN VARCHAR2)
AS
--
BEGIN
--
    UPDATE dl_hra_payment_expectations
       SET lpexp_dl_load_status = p_status
     WHERE ROWID = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            DBMS_OUTPUT.put_line('Error updating status of dl_hra_payment_expectations');
            RAISE;
--
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create (p_batch_id IN VARCHAR2, 
                           p_date     IN DATE)
AS
--
CURSOR c1
IS
SELECT ROWID rec_rowid,
       LPEXP_DLB_BATCH_ID,
       LPEXP_DL_SEQNO,
       LPEXP_DL_LOAD_STATUS,
       LPEXP_RAC_PAY_REF,
       LPEXP_HRV_PEXT_CODE,
       LPEXP_DUE_DATE_OF_FIRST_PAY,
       LPEXP_AMOUNT,
       LPEXP_TRS_TRT_CODE,
       LPEXP_TRS_CODE,
       LPEXP_TRT_CODE,
       LPEXP_PRIORITY,
       NVL(LPEXP_ALLOCATE_TO_FUTURE_PAY,'N')                  LPEXP_ALLOCATE_TO_FUTURE_PAY,
       LPEXP_FREQUENCY,
       LPEXP_END_DATE,
       LPEXP_COMMENTS,
       'ACT'                                                  LPEXP_SCO_CODE,
       LPEXP_ACHO_ALT_REFERENCE,
       LPEXP_AUTHORISED_BY,
       LPEXP_AUTHORISED_DATE,
       LPEXP_EVENT_DATE,
       LPEXP_REQUESTED_PERCENTAGE,
       LPEXP_PAR_TYPE,
       LPEXP_PAR_REFERENCE,
       LPEXP_FIXED_AMOUNT_COMPONENT,
       LPEXP_PERCENTAGE_COMPONENT,
       LPEXP_DEN_ACCOUNT_CHARGE,
       LPEXP_MAX_EXPA_DUE_DATE,
       LPEXP_EXTERNAL_REF,
       LPEXP_BDE_BTY_CODE,
       LPEXP_BDE_BANK_NAME,
       LPEXP_BAD_ACCOUNT_NO,
       LPEXP_BAD_ACCOUNT_NAME,
       LPEXP_BAD_SORT_CODE,
       LPEXP_BDE_BRANCH_NAME,
       NVL(LPEXP_BAD_START_DATE, LPEXP_DUE_DATE_OF_FIRST_PAY) LPEXP_BAD_START_DATE,
       LPEXP_AUN_BAD_ACCOUNT_NO,
       NVL(LPEXP_PAR_ORG_IND,'P')                             LPEXP_PAR_ORG_IND,
       LPEXP_BAD_PAR_REFERENCE,
       LPEXP_LINK_TO_RACD,
       LPEXP_RACD_RAUD_REFNO,
       LPEXP_RACD_RADT_CODE,
       LPEXP_RACD_START_DATE,
       LPEXP_RACD_HRV_RBEG_CODE,
       LPEXP_REFNO
  FROM dl_hra_payment_expectations
 WHERE lpexp_dlb_batch_id   = p_batch_id 
   AND lpexp_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR get_rac_accno(p_rac_pay_ref VARCHAR2)
IS
SELECT rac_accno
  FROM revenue_accounts
 WHERE rac_pay_ref = p_rac_pay_ref;
--
-- ***********************************************************************
--
CURSOR get_acho_reference(p_pexp_acho_alt_reference VARCHAR2)
IS
SELECT acho_reference
  FROM advice_case_housing_options
 WHERE acho_alternative_reference = p_pexp_acho_alt_reference;
--
-- ***********************************************************************
--
CURSOR get_pext_tolerance_days(p_pext_code VARCHAR2)
IS
SELECT NVL(pext_default_tolerance_days,0),
       NVL(pext_direct_debit_ind,'N')
  FROM payment_expectation_types
 WHERE pext_code = p_pext_code;
--
-- ***********************************************************************
--
CURSOR c_get_par(p_par_per_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_per_alt_ref;
--
-- ***********************************************************************
--
CURSOR c_get_prf(p_par_per_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_per_alt_ref;
--
-- *********************************************************************
--
CURSOR c_org_refno(p_par_alt_ref VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_com_short_name = p_par_alt_ref
    OR par_org_short_name = p_par_alt_ref;
--
-- *********************************************************************
--
CURSOR c_bde_exists(p_pexp_bank_name   VARCHAR2, 
                    p_pexp_branch_name VARCHAR2)
IS
SELECT bde_refno
  FROM bank_details
 WHERE UPPER(bde_bank_name)            = UPPER(p_pexp_bank_name)
   AND NVL(UPPER(bde_branch_name),'~') = NVL(UPPER(p_pexp_branch_name),'~');
--
-- *********************************************************************
--
CURSOR c_bde_refno
IS
SELECT bde_refno_seq.nextval
  FROM dual;
--
-- *********************************************************************
--
CURSOR c_bad_refno
IS
SELECT bad_refno_seq.nextval
  FROM dual;
--
-- *********************************************************************
--
CURSOR c_get_tenant_par_refno(p_pexp_rac_pay_ref VARCHAR2) 
IS
SELECT hop_par_refno
  FROM household_persons,
       tenancy_instances,
       revenue_accounts 
 WHERE hop_refno           = tin_hop_refno
   AND tin_tcy_refno       = rac_tcy_refno
   AND tin_main_tenant_ind = 'Y'
   AND rac_pay_ref         = p_pexp_rac_pay_ref;
--
-- ***********************************************************************
--
-- Get Organisation Bank Account No
-- 
CURSOR get_org_aun_acct(p_pexp_rac_pay_ref VARCHAR2)
IS
SELECT aub.aub_bad_refno, 
       aub.aub_aun_code, 
       aub.aub_start_date
  FROM bank_account_details     bad,
       admin_unit_bank_accounts aub,
       revenue_accounts         rac 
 WHERE bad.bad_refno               = aub.aub_bad_refno
   AND bad.bad_type                = 'ORG'
   AND rac.rac_pay_ref             = p_pexp_rac_pay_ref
   AND aub.aub_aun_code            = rac.rac_aun_code
   AND aub.aub_default_account_ind = 'Y'
   AND SYSDATE BETWEEN aub.aub_start_date
                   AND NVL(aub.aub_end_date, SYSDATE + 1);
--
-- *********************************************************************
--
CURSOR get_org_tar_acct(p_pexp_aun_bad_account_no VARCHAR2) 
IS
SELECT aub.aub_bad_refno,
       aub.aub_aun_code,
       aub.aub_start_date
  FROM admin_unit_bank_accounts aub,
       bank_account_details     bad
 WHERE bad.bad_type       = 'ORG'
   AND aub.aub_bad_refno  = bad.bad_refno
   AND bad.bad_account_no = nvl(p_pexp_aun_bad_account_no,bad.bad_account_no);
--
-- *********************************************************************
--
CURSOR get_core_ref_version(p_rac_accno  NUMBER)
IS
SELECT TO_NUMBER(MAX(SUBSTR(TRIM(ddi_core_reference),-2)))
  FROM direct_debit_instructions
 WHERE ddi_rac_accno  =  p_rac_accno;
--
-- *********************************************************************
--
CURSOR c_get_racd_refno(p_racd_raud_refno    NUMBER,
                        p_racd_rac_accno     VARCHAR2,
                        p_racd_radt_code     VARCHAR2,
                        p_racd_start_date    DATE,
                        p_racd_hrv_rbeg_code VARCHAR2)
IS
SELECT lracd_refno
FROM DL_HRA_RDS_ACC_DEDUCTIONS -- PL CHANGED TO USE dataload tables
WHERE LRACD_DL_LOAD_STATUS = 'C'
AND LRACD_RDSA_HA_REFERENCE = p_racd_raud_refno
AND LRACD_START_DATE = p_racd_start_date
AND NVL(LRACD_HRV_RBEG_CODE,'!X!') = NVL(p_racd_hrv_rbeg_code,'!X!')
AND LRACD_PAY_REF = p_racd_rac_accno
AND LRACD_RADT_CODE = p_racd_radt_code;
/*

CURSOR c_get_racd_refno(p_racd_raud_refno    NUMBER,
                        p_racd_rac_accno     NUMBER,
                        p_racd_radt_code     VARCHAR2,
                        p_racd_start_date    DATE,
                        p_racd_hrv_rbeg_code VARCHAR2)
IS
SELECT racd_refno
  FROM rds_account_deductions
 WHERE racd_raud_refno               = p_racd_raud_refno
   AND racd_rac_accno                = p_racd_rac_accno
   AND racd_radt_code                = p_racd_radt_code
   AND racd_start_date               = p_racd_start_date
   AND NVL(racd_hrv_rbeg_code,'!X!') = NVL(p_racd_hrv_rbeg_code,'!X!');
   */
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb         VARCHAR2(30);
cd         DATE;
cp         VARCHAR2(30)  := 'CREATE';
ct         VARCHAR2(30)  := 'DL_HRA_PAYMENT_EXPECTATIONS';
cs         INTEGER;
ce         VARCHAR2(200);
l_id       ROWID;
l_an_tab   VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
i                               INTEGER := 0;
l_exists                        VARCHAR2 (1);
l_pexp_rac_accno                NUMBER(10);
l_pexp_acho_reference           NUMBER(10);
l_pexp_default_tolerance_days   NUMBER(2);
l_pexp_direct_debit_ind         VARCHAR2(1);
--
l_bde_refno                     NUMBER(10);
l_bad_refno                     NUMBER(10);
l_org_target_bad_refno          NUMBER(10);
l_org_target_acct_no            NUMBER(10);
l_org_aun_code                  VARCHAR2(20);
l_org_start_date                DATE;
l_source_par_refno              NUMBER(10);
l_par_refno                     NUMBER(10);
--
l_coresrce                      VARCHAR2(20) := fsc_utils.get_sys_param('CORESRCE');
l_payrefln                      VARCHAR2(20) := fsc_utils.get_sys_param('PAYREFLIN');
--l_reuse_ddis                    VARCHAR2(1)  := fsc_utils.get_sys_param('REUSE_DDIS');
l_core_ref                      VARCHAR2(18);
l_core_ref_version              NUMBER(2);
--
l_racd_refno                    NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start ('s_dl_hra_payment_expectations.dataload_create');
    fsc_utils.debug_message ('s_dl_hra_payment_expectations.dataload_create', 3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary (cb, cp, cd, 'RUNNING');

--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lpexp_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT sp1;

--
-- Main processing
--
          l_pexp_rac_accno        := NULL;
          l_pexp_acho_reference   := NULL;
          l_org_target_acct_no    := NULL;
          l_org_target_bad_refno  := NULL;
          l_bde_refno             := NULL;
          l_bad_refno             := NULL;
          l_racd_refno            := NULL;
--
--
-- Open any cursors
--
--
-- Get Rac_accno
--
           OPEN get_rac_accno(p1.lpexp_rac_pay_ref);
          FETCH get_rac_accno INTO l_pexp_rac_accno;
          CLOSE get_rac_accno;
--
--
-- Get Tolerance Days and Direct debit ind from expectation type
--
          l_pexp_default_tolerance_days := NULL;
--
           OPEN get_pext_tolerance_days(p1.lpexp_hrv_pext_code);
          FETCH get_pext_tolerance_days INTO l_pexp_default_tolerance_days, l_pexp_direct_debit_ind;
          CLOSE get_pext_tolerance_days;
--
--
-- Get advice case housing options reference 
--
          IF (p1.lpexp_acho_alt_reference IS NOT NULL) THEN
--
            OPEN get_acho_reference(p1.lpexp_acho_alt_reference);
           FETCH get_acho_reference INTO l_pexp_acho_reference;
           CLOSE get_acho_reference;
--
          END IF;
--
--
-- Get par_refno
--
          l_par_refno := NULL;
--
          IF (p1.lpexp_par_reference IS NOT NULL) THEN
--
           IF (p1.lpexp_par_type = 'PAR') THEN
--
             OPEN c_get_par(p1.lpexp_par_reference);
            FETCH c_get_par INTO l_par_refno;
            CLOSE c_get_par;
--
           ELSE
--
               OPEN c_get_prf(p1.lpexp_par_reference);
              FETCH c_get_prf INTO l_par_refno;
              CLOSE c_get_prf;
--
           END IF;
--
          END IF;
--
-- 04-DEC-2018 6.18 Change
-- BEGIN
--
          IF (NVL(p1.lpexp_link_to_racd,'N') = 'Y') THEN
--          
            OPEN c_get_racd_refno(p1.LPEXP_RACD_RAUD_REFNO,
                                  p1.lpexp_rac_pay_ref, -- PL Change
                                  p1.LPEXP_RACD_RADT_CODE,
                                  p1.LPEXP_RACD_START_DATE,
                                  p1.LPEXP_RACD_HRV_RBEG_CODE);
--
           FETCH c_get_racd_refno INTO l_racd_refno;
           CLOSE c_get_racd_refno;
--
         END IF;
--
-- END
-- 04-DEC-2018 6.18 Change
--

--
-- Insert into PAYMENT_EXPECTATIONS table
--
          INSERT INTO payment_expectations(pexp_refno, 
                                           pexp_rac_accno,
                                           pexp_hrv_pext_code, 
                                           pexp_due_date_of_first_pay,
                                           pexp_amount,
                                           pexp_trs_trt_code, 
                                           pexp_trs_code,
                                           pexp_trt_code, 
                                           pexp_priority,
                                           pexp_allocate_to_future_pay,
                                           pexp_payment_days_tolerance, 
                                           pexp_frequency,
                                           pexp_end_date,
                                           pexp_comments,
                                           pexp_arr_refno,
                                           pexp_pme_refno,
                                           pexp_sco_code, 
                                           pexp_acho_reference,
                                           pexp_authorised_by, 
                                           pexp_authorised_date,
                                           pexp_event_date,
                                           pexp_requested_percentage,
                                           pexp_par_refno,
                                           pexp_fixed_amount_component, 
                                           pexp_percentage_component,
                                           pexp_den_account_charge,
                                           pexp_max_expa_due_date,
                                           pexp_racd_refno
                                          )
                              VALUES      (p1.lpexp_refno, 
                                           l_pexp_rac_accno,
                                           p1.lpexp_hrv_pext_code, 
                                           p1.lpexp_due_date_of_first_pay,
                                           p1.lpexp_amount, 
                                           p1.lpexp_trs_trt_code, 
                                           p1.lpexp_trs_code,
                                           p1.lpexp_trt_code, 
                                           p1.lpexp_priority,
                                           p1.lpexp_allocate_to_future_pay,
                                           l_pexp_default_tolerance_days, 
                                           p1.lpexp_frequency,
                                           p1.lpexp_end_date, 
                                           p1.lpexp_comments, 
                                           NULL,     -- arr_refno
                                           NULL,     -- pme_refno
                                           p1.lpexp_sco_code,
                                           l_pexp_acho_reference,
                                           p1.lpexp_authorised_by, 
                                           p1.lpexp_authorised_date,
                                           p1.lpexp_event_date,
                                           p1.lpexp_requested_percentage,
                                           l_par_refno,
                                           p1.lpexp_fixed_amount_component, 
                                           p1.lpexp_percentage_component,
                                           p1.lpexp_den_account_charge,
                                           p1.lpexp_max_expa_due_date,
                                           l_racd_refno
                                          );
--
-- ADDED 19 Jan 2017
--
          s_credit_allocations.del_credit_alloc_for_rac(l_pexp_rac_accno);
--
-- Update created by to DATALOAD
--
          UPDATE payment_expectations
             SET pexp_created_by = 'DATALOAD'
           WHERE pexp_refno = p1.lpexp_refno;
--
--
-- **********************************************************************************************
--
-- Now deal with the Bank Detail/Account Creation
--
--
-- Organisation Target Bank Account
--
--
          IF (p1.LPEXP_AUN_BAD_ACCOUNT_NO IS NULL) THEN
--
-- Get default org bank account details
--
            OPEN get_org_aun_acct(p1.LPEXP_RAC_PAY_REF);
           FETCH get_org_aun_acct INTO l_org_target_bad_refno, l_org_aun_code, l_org_start_date;
           CLOSE get_org_aun_acct;
--
          ELSE
--
-- Get org bank addount details based on target account no supplied
--
              OPEN get_org_tar_acct(p1.LPEXP_AUN_BAD_ACCOUNT_NO);
             FETCH get_org_tar_acct INTO l_org_target_bad_refno, l_org_aun_code, l_org_start_date;
             CLOSE get_org_tar_acct;
--
          END IF;
--
-- 
--
-- Now we need to get insert into bank_details,
-- bank account details, party_bank_accounts and
-- as appropriate;
--
--
          IF (p1.lpexp_bad_account_no IS NOT NULL) THEN
--
           IF (p1.lpexp_bad_par_reference IS NULL) THEN
--
-- get main tenant to link this to
--
            l_source_par_refno := NULL;
--
             OPEN c_get_tenant_par_refno(p1.lpexp_rac_pay_ref);
            FETCH c_get_tenant_par_refno into l_source_par_refno;
            CLOSE c_get_tenant_par_refno;
--
           ELSE
--
              IF (p1.lpexp_par_org_ind = 'P') THEN
--
-- If a party has been supplied then...
-- is it a Person
--
               IF (p1.lpexp_par_type = 'PAR') THEN
--
                 OPEN c_get_par(p1.lpexp_bad_par_reference);
                FETCH c_get_par INTO l_source_par_refno;
                CLOSE c_get_par;
--
               ELSE
--
                   OPEN c_get_prf(p1.lpexp_bad_par_reference);
                  FETCH c_get_prf INTO l_source_par_refno;
                  CLOSE c_get_prf;
--
               END IF;
--
              ELSE
--
-- or else it must be an organisation
--
                  OPEN c_org_refno(p1.lpexp_bad_par_reference);
                 FETCH c_org_refno INTO l_source_par_refno;
                 CLOSE c_org_refno;
--
              END IF; -- lpexp_par_org_ind

           END IF; -- had the lpexp_bad_par_reference been supplied  
--
--
-- So does the bank detail already exist
--
           l_bde_refno := NULL;
--
           IF (    p1.lpexp_bde_bank_name   IS NOT NULL
               AND p1.lpexp_bde_branch_name IS NOT NULL) THEN
--
             OPEN c_bde_exists(p1.lpexp_bde_bank_name, p1.lpexp_bde_branch_name);
            FETCH c_bde_exists into l_bde_refno;
            CLOSE c_bde_exists;
--
            IF (l_bde_refno IS NULL) THEN
--
              OPEN c_bde_refno;
             FETCH c_bde_refno into l_bde_refno;
             CLOSE c_bde_refno;
--
             INSERT INTO BANK_DETAILS(bde_refno,
                                      bde_bank_name,
                                      bde_created_by,
                                      bde_created_date,
                                      bde_branch_name,
                                      bde_bty_code)
-- 
                               VALUES(l_bde_refno,
                                      p1.lpexp_bde_bank_name,
                                      'DATALOAD',
                                      SYSDATE,
                                      p1.lpexp_bde_branch_name,
                                      p1.lpexp_bde_bty_code); 
--
            END IF;
--
           END IF;
--
-- Now insert into bank_account_details
--
--
           l_bad_refno := NULL; 
--
            OPEN c_bad_refno;
           FETCH c_bad_refno into l_bad_refno;
           CLOSE c_bad_refno;
--     
           INSERT INTO BANK_ACCOUNT_DETAILS(bad_refno,
                                            bad_type,
                                            bad_sort_code,
                                            bad_bde_refno,
                                            bad_account_no,
                                            bad_account_name,
                                            bad_start_date,
                                            bad_created_by,
                                            bad_created_date,
                                            bad_end_date,
                                            bad_user_alt_ref
                                           )
--
                                     VALUES(l_bad_refno,
                                            'CUS',
                                            p1.lpexp_bad_sort_code,
                                            l_bde_refno,
                                            p1.lpexp_bad_account_no,
                                            p1.lpexp_bad_account_name,
                                            p1.lpexp_bad_start_date, -- SET to lpexp_due_date_of_first_pay IF NULL
                                            'DATALOAD',
                                            SYSDATE,
                                            NULL,
                                            NULL);
-- 
-- and now into party bank_accounts 
--
--
           INSERT INTO PARTY_BANK_ACCOUNTS(pba_par_refno,
                                           pba_bad_refno,
                                           pba_start_date,
                                           pba_created_date,
                                           pba_created_by,
                                           pba_end_date)
-- 
                                    VALUES(l_source_par_refno,
                                           l_bad_refno,
                                           p1.lpexp_bad_start_date, -- SET to lpexp_due_date_of_first_pay IF NULL
                                           SYSDATE,
                                           'DATALOAD',
                                           NULL);
--
          END IF; -- p1.lpexp_bad_account_no IS NOT NULL
--
--
-- **********************************************************************************************
--
-- Now sort out the Direct Debit Instruction creation
-- What should happen is that the user defines CREATE DDI IND against the expectation type and if set to Y 
-- we should go ahead and create a new DDI usage.
--
-- Because we have to create a new DDI usage we should check to see what is the latest core ref version and
-- create the new core ref accordingly.
--
--
          IF (l_pexp_direct_debit_ind = 'Y')  THEN
--
-- get core reference version
--
            OPEN get_core_ref_version(l_pexp_rac_accno);
           FETCH get_core_ref_version INTO l_core_ref_version;
           CLOSE get_core_ref_version;
--          
           l_core_ref_version := NVL(l_core_ref_version,0) + 1;
--
-- generate core reference first
--
--          
           l_core_ref := NULL;
--
           IF l_coresrce = 'ACCNO' THEN
            l_core_ref := LPAD(TO_CHAR(l_pexp_rac_accno) ,8 ,'0') || LPAD(l_core_ref_version,2,'0');
           ELSE
              l_core_ref := LPAD(s_revenue_accounts2.get_pay_ref_from_accno(l_pexp_rac_accno) || LPAD(l_core_ref_version,2,'0') ,TO_NUMBER(l_payrefln) ,'0');   -- Ref# 3.3
           END IF;
--
--
-- Create the Direct Debit Instruction
--
           s_direct_debit_instructions.create_dd_instruction(p_ddi_core_reference       => l_core_ref,
                                                             p_ddi_requestor_name       => NULL,
                                                             p_ddi_requestor_telephone  => NULL,
                                                             p_ddi_rac_accno            => l_pexp_rac_accno,
                                                             p_ddi_sco_code             => 'NEW',
                                                             p_ddi_created_date         => SYSDATE,
                                                             p_ddi_created_by           => 'DATALOAD');
--
-- Create the DDI Account 
--
           s_ddi_accounts.create_ddi_account(p_pba_par_refno      => l_source_par_refno,
                                             p_pba_bad_refno      => l_bad_refno,
                                             p_pba_start_date     => p1.lpexp_bad_start_date,
                                             p_aub_aun_code       => l_org_aun_code,
                                             p_aub_bad_refno      => l_org_target_bad_refno,
                                             p_aub_start_date     => l_org_start_date,
                                             p_ddi_core_reference => l_core_ref,
                                             p_sco_code           => 'RAI',
                                             p_created_date       => SYSDATE,
                                             p_created_by         => 'DATALOAD');
--
-- Create the DDI Usage
--
           s_ddi_usages.create_ddi_usage(p_core_ref       => l_core_ref,
                                         p_pme_aar_refno  => p1.lpexp_refno,
                                         p_pme_arr_flag   => 'PEXT',
                                         p_created_by     => 'DATALOAD',
                                         p_created_date   => SYSDATE);
--
--
          END IF;
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
          i := i + 1;
--
          IF MOD (i, 50000) = 0 THEN
           COMMIT;
          END IF;
--
          s_dl_process_summary.update_processed_count (cb, cp, cd, 'N');
          set_record_status_flag (l_id, 'C');
--
          EXCEPTION
               WHEN OTHERS THEN
                  ROLLBACK TO sp1;
                  ce := s_dl_errors.record_error (cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
                  set_record_status_flag (l_id, 'O');
                  s_dl_process_summary.update_processed_count (cb, cp, cd, 'Y');
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
    l_an_tab := s_dl_hem_utils.dl_comp_stats('PAYMENT_EXPECTATIONS');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('PARTY_BANK_ACCOUNT');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('BANK_DETAILS');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('BANK_ACCOUNT_DETAILS');
--
    fsc_utils.proc_end;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_process_summary.update_summary (cb, cp, cd, 'FAILED');
            RAISE;
--
END dataload_create;
--
-- ***********************************************************************
--
PROCEDURE dataload_validate (p_batch_id IN VARCHAR2, p_date IN DATE)
AS
--
CURSOR c1
IS
SELECT ROWID rec_rowid,
       LPEXP_DLB_BATCH_ID,
       LPEXP_DL_SEQNO,
       LPEXP_DL_LOAD_STATUS,
       LPEXP_RAC_PAY_REF,
       LPEXP_HRV_PEXT_CODE,
       LPEXP_DUE_DATE_OF_FIRST_PAY,
       LPEXP_AMOUNT,
       LPEXP_TRS_TRT_CODE,
       LPEXP_TRS_CODE,
       LPEXP_TRT_CODE,
       LPEXP_PRIORITY,
       NVL(LPEXP_ALLOCATE_TO_FUTURE_PAY,'N')                  LPEXP_ALLOCATE_TO_FUTURE_PAY,
       LPEXP_FREQUENCY,
       LPEXP_END_DATE,
       LPEXP_COMMENTS,
       'ACT'                                                  LPEXP_SCO_CODE,
       LPEXP_ACHO_ALT_REFERENCE,
       LPEXP_AUTHORISED_BY,
       LPEXP_AUTHORISED_DATE,
       LPEXP_EVENT_DATE,
       LPEXP_REQUESTED_PERCENTAGE,
       LPEXP_PAR_TYPE,
       LPEXP_PAR_REFERENCE,
       LPEXP_FIXED_AMOUNT_COMPONENT,
       LPEXP_PERCENTAGE_COMPONENT,
       LPEXP_DEN_ACCOUNT_CHARGE,
       LPEXP_MAX_EXPA_DUE_DATE,
       LPEXP_EXTERNAL_REF,
       LPEXP_BDE_BTY_CODE,
       LPEXP_BDE_BANK_NAME,
       LPEXP_BAD_ACCOUNT_NO,
       LPEXP_BAD_ACCOUNT_NAME,
       LPEXP_BAD_SORT_CODE,
       LPEXP_BDE_BRANCH_NAME,
       NVL(LPEXP_BAD_START_DATE, LPEXP_DUE_DATE_OF_FIRST_PAY) LPEXP_BAD_START_DATE,
       LPEXP_AUN_BAD_ACCOUNT_NO,
       NVL(LPEXP_PAR_ORG_IND,'P')                             LPEXP_PAR_ORG_IND,
       LPEXP_BAD_PAR_REFERENCE,
       LPEXP_LINK_TO_RACD,
       LPEXP_RACD_RAUD_REFNO,
       LPEXP_RACD_RADT_CODE,
       LPEXP_RACD_START_DATE,
       LPEXP_RACD_HRV_RBEG_CODE,
       LPEXP_REFNO
  FROM dl_hra_payment_expectations
 WHERE lpexp_dlb_batch_id   = p_batch_id 
   AND lpexp_dl_load_status IN ('L', 'F', 'O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
-- Check Payment Expectation Type Exists
--
CURSOR chk_pext_exists (p_pexp_hrv_pext_code VARCHAR2)
IS
SELECT pext.*
  FROM payment_expectation_types pext
 WHERE pext_code = p_pexp_hrv_pext_code;
--
-- ***********************************************************************
-- Check Revenue Account Payment Reference is VALID
--
CURSOR chk_rac_pay_ref_exists (p_rac_pay_ref VARCHAR2)
IS
SELECT rac_accno
  FROM revenue_accounts
 WHERE rac_pay_ref = p_rac_pay_ref;
--
-- ***********************************************************************
--
-- Check Transaction Type Code is Valid
--
CURSOR chk_trt_code (p_trt_code VARCHAR2)
IS
SELECT 'X'
  FROM transaction_types
 WHERE trt_code = p_trt_code;
--
-- ***********************************************************************
--
-- Check Transaction Type Code/Subtype code is Valid
--
CURSOR chk_trt_trs_code (p_trt_code VARCHAR2, 
                         p_trs_code VARCHAR2)
IS
SELECT 'X'
  FROM transaction_subtypes
 WHERE trs_trt_code = p_trt_code 
   AND trs_code     = p_trs_code;
--
-- ***********************************************************************
--
CURSOR chk_acho_exists (p_acho_alt_reference VARCHAR2)
IS
SELECT 'X'
  FROM advice_case_housing_options
 WHERE acho_alternative_reference = p_acho_alt_reference;
--
-- ***********************************************************************
--
-- Check Organisation Bank Account exists
-- 
CURSOR chk_org_acct_no_exists(p_pexp_rac_pay_ref VARCHAR2)
IS
SELECT bad.bad_account_no
  FROM bank_account_details     bad,
       admin_unit_bank_accounts aub,
       revenue_accounts         rac 
 WHERE bad.bad_refno               = aub.aub_bad_refno
   AND bad.bad_type                = 'ORG'
   AND rac.rac_pay_ref             = p_pexp_rac_pay_ref
   AND aub.aub_aun_code            = rac.rac_aun_code
   AND aub.aub_default_account_ind = 'Y'
   AND SYSDATE BETWEEN aub.aub_start_date
                   AND NVL(aub.aub_end_date, SYSDATE + 1);
--
-- ***********************************************************************
--
CURSOR c_get_par(p_par_per_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_per_alt_ref;
--
-- ***********************************************************************
--
CURSOR c_get_prf(p_par_per_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_per_alt_ref;
--
-- *********************************************************************
--
CURSOR c_org_refno(p_par_per_alt_ref VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_com_short_name = p_par_per_alt_ref
    OR par_org_short_name = p_par_per_alt_ref;
--
-- ***********************************************************************
-- 
CURSOR chk_bty_branch_man_flag(p_pexp_bde_bank_name VARCHAR2)
IS
SELECT bty.bty_branch_code_mandatory_flag
  FROM bank_details bde,
       bank_types   bty
 WHERE bty.bty_code                       = bde.bde_bty_code
   AND UPPER(bde.bde_bank_name)           = UPPER(p_pexp_bde_bank_name)
   AND bty.BTY_BRANCH_CODE_MANDATORY_FLAG = 'MAN'
   AND rownum = 1;
--
-- ***********************************************************************
--
CURSOR chk_bty_code(p_bde_bty_code VARCHAR2) 
IS
SELECT 'X'
  FROM bank_types
 WHERE bty_code = p_bde_bty_code;
--
-- *********************************************************************
-- 
CURSOR chk_racd_raud_refno_exists(p_racd_raud_refno NUMBER)
IS
SELECT lracd_refno
FROM DL_HRA_RDS_ACC_DEDUCTIONS -- PL CHANGED TO USE dataload tables
WHERE LRACD_DL_LOAD_STATUS = 'C'
AND LRACD_RDSA_HA_REFERENCE = p_racd_raud_refno;

/* SELECT raud_refno
  FROM rds_authorised_deductions
 WHERE raud_refno = p_racd_raud_refno;*/
--
-- *********************************************************************
--
CURSOR chk_racd_refno_exists(p_racd_raud_refno    NUMBER,
                             p_racd_rac_accno     VARCHAR2,
                             p_racd_radt_code     VARCHAR2,
                             p_racd_start_date    DATE,
                             p_racd_hrv_rbeg_code VARCHAR2)
IS
SELECT lracd_refno
FROM DL_HRA_RDS_ACC_DEDUCTIONS -- PL CHANGED TO USE dataload tables
WHERE LRACD_DL_LOAD_STATUS = 'C'
AND LRACD_RDSA_HA_REFERENCE = p_racd_raud_refno
AND LRACD_START_DATE = p_racd_start_date
AND NVL(LRACD_HRV_RBEG_CODE,'!X!') = NVL(p_racd_hrv_rbeg_code,'!X!')
AND LRACD_PAY_REF = p_racd_rac_accno
AND LRACD_RADT_CODE = p_racd_radt_code;
/*
CURSOR chk_racd_refno_exists(p_racd_raud_refno    NUMBER,
                             p_racd_rac_accno     NUMBER,
                             p_racd_radt_code     VARCHAR2,
                             p_racd_start_date    DATE,
                             p_racd_hrv_rbeg_code VARCHAR2)
IS
SELECT racd_refno
  FROM rds_account_deductions
 WHERE racd_raud_refno               = p_racd_raud_refno
   AND racd_rac_accno                = p_racd_rac_accno
   AND racd_radt_code                = p_racd_radt_code
   AND racd_start_date               = p_racd_start_date
   AND NVL(racd_hrv_rbeg_code,'!X!') = NVL(p_racd_hrv_rbeg_code,'!X!');
*/
--
-- *********************************************************************
--
CURSOR chk_radt_code_exists(p_racd_radt_code VARCHAR2)
IS
SELECT radt_code,
       radt_pext_code
  FROM rds_account_deduction_types
 WHERE radt_code = p_racd_radt_code;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                 VARCHAR2 (30);
cd                 DATE;
cp                 VARCHAR2 (30)  := 'VALIDATE';
ct                 VARCHAR2 (30)  := 'DL_HRA_PAYMENT_EXPECTATIONS';
cs                 INTEGER;
ce                 VARCHAR2 (200);
l_id               ROWID;
--
-- ***********************************************************************
--
-- Other variables
--
l_pext_exists         VARCHAR2(1);
l_pext_rec            PAYMENT_EXPECTATION_TYPES%ROWTYPE;
l_rac_accno           NUMBER(10);
l_trt_exists          VARCHAR2(1);
l_trs_trt_exists      VARCHAR2(1);
l_par_refno           NUMBER(10);
l_bad_par_refno       NUMBER(10);
l_aun_bad_acct_no     VARCHAR2(30);
l_branch_code_man     VARCHAR2(3);
l_bty_exists          VARCHAR2(1);
--
l_arr_exists          VARCHAR2(1);
l_pme_exists          VARCHAR2(1);
--
l_acho_exists         VARCHAR2(1);
l_sco_exists          VARCHAR2(1);
--
l_errors              VARCHAR2(10);
l_error_ind           VARCHAR2(10);
i                     INTEGER := 0;
--
l_racd_refno          NUMBER(10);
l_racd_raud_refno     NUMBER(10);
l_racd_radt_code      VARCHAR2(15);
l_radt_pexp_code      VARCHAR2(10);

--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start ('s_dl_hra_payment_expectations.dataload_validate');
    fsc_utils.debug_message ('s_dl_hra_payment_expectations.dataload_validate', 3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary (cb, cp, cd, 'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs   := p1.lpexp_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
-- ***********************************************************************
--
-- Perform Validation Checks
--
-- Check that the Revenue Account Payment Reference has been supplied and is VALID
--
          IF (p1.lpexp_rac_pay_ref IS NULL) THEN
           l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD1',159);
--
          ELSE
--
             l_rac_accno := NULL;
--
              OPEN chk_rac_pay_ref_exists (p1.lpexp_rac_pay_ref);
             FETCH chk_rac_pay_ref_exists INTO l_rac_accno;
             CLOSE chk_rac_pay_ref_exists;
--
             IF (l_rac_accno IS NULL) THEN
              l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2',323);
             END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- If Payment Expectation Type has been supplied and is valid
--
          IF (p1.lpexp_hrv_pext_code IS NULL) THEN
           l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs,'HD1',757);
--
          ELSE
--
             l_pext_exists := NULL;
--
              OPEN chk_pext_exists (p1.lpexp_hrv_pext_code);
             FETCH chk_pext_exists INTO l_pext_rec;
             CLOSE chk_pext_exists;
--
             IF (l_pext_rec.pext_code IS NULL) THEN
              l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs,'HD1',758);
             END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- If Date of first Payment has been supplied
--
          IF (p1.lpexp_due_date_of_first_pay IS NULL) THEN
           l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs ,'HD1',762);
          END IF;
--
-- ***********************************************************************
--
-- If Payment Amount has been supplied
--
          IF (p1.lpexp_amount IS NULL) THEN
           l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs ,'HD1',775);
          END IF;
--
-- ***********************************************************************
--
-- If Transaction Type is valid if supplied
--
          IF (p1.lpexp_trt_code IS NOT NULL) THEN
--
           l_trt_exists := NULL;
--
            OPEN chk_trt_code (p1.lpexp_trt_code);
           FETCH chk_trt_code INTO l_trt_exists;
           CLOSE chk_trt_code;
--
           IF (l_trt_exists IS NULL) THEN
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HDL', 130);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- If Transaction Type/SubType is valid
--
          IF (p1.lpexp_trs_trt_code IS NOT NULL AND p1.lpexp_trs_code IS NOT NULL) THEN
--
           l_trs_trt_exists := NULL;
--
            OPEN chk_trt_trs_code (p1.lpexp_trs_trt_code, p1.lpexp_trs_code);
           FETCH chk_trt_trs_code INTO l_trs_trt_exists;
           CLOSE chk_trt_trs_code;
--
           IF (l_trs_trt_exists IS NULL) THEN
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HDL', 132);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Transaction types/subtypes
-- 
-- 1)  they can't all be null
--
         IF   (p1.lpexp_trt_code     IS NULL 
           AND p1.lpexp_trs_code     IS NULL 
           AND p1.lpexp_trs_trt_code IS NULL) THEN 
--
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',767);
-- 
         END IF;
--
--  2) if subtype is being used then both fields must be completed
--

        IF (  (p1.lpexp_trs_trt_code IS NOT NULL AND p1.lpexp_trs_code IS NULL)
            OR
              (p1.lpexp_trs_trt_code IS NULL AND p1.lpexp_trs_code IS NOT NULL)) THEN 
--
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',768);
--
        END IF;
--
-- 3) if trans type is used then the 2 subtype fields must be null
--
       IF (p1.lpexp_trt_code IS NOT NULL 
	       AND (p1.lpexp_trs_code IS NOT NULL OR p1.lpexp_trs_trt_code IS NOT NULL)) THEN
--
	    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',769);
--
       END IF;
--
-- ***********************************************************************
--
-- If Priority has been supplied
--
          IF (p1.lpexp_priority IS NULL) THEN
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD1',764);
--
          ELSIF (p1.lpexp_priority < 1 OR p1.lpexp_priority > 9999) THEN
              l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD1',878);
--
          END IF;
--
-- ***********************************************************************
--
-- If Allocate to future pay is valid
--            
         IF ( p1.lpexp_allocate_to_future_pay NOT IN ('Y','N') ) THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',761);       
         END IF;
--
-- ***********************************************************************
--

-- Check tolerance days is supplied
--
--         IF (p1.lpexp_payment_days_tolerance IS NULL) THEN
--          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',766);
--         END IF;
--
-- ***********************************************************************
--
-- Check Frequency has been supplied and is Valid
--
          IF (p1.lpexp_frequency IS NULL) THEN
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs,'HD1',759);
--
          ELSIF (p1.lpexp_frequency NOT IN ('MONTHLY', 'QUARTERLY', 'ONE OFF', 'WEEKLY', 'FORTNIGHTLY', '4WEEKLY')) THEN
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs,'HD1',760);
--
          END IF;
--
-- ***********************************************************************
--
-- Check END DATE is not earlier than the FIRST PAYMENT DATE
--
          IF (p1.lpexp_end_date IS NOT NULL) THEN
--
           IF (p1.lpexp_end_date < p1.lpexp_due_date_of_first_pay) THEN
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HDL', 542);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
--
-- If Supplied, Check that the Housing Option Reference is valid
--
--
          IF (p1.lpexp_acho_alt_reference IS NOT NULL) THEN
--
           l_acho_exists := NULL;
--
            OPEN chk_acho_exists (p1.lpexp_acho_alt_reference);
           FETCH chk_acho_exists INTO l_acho_exists;
           CLOSE chk_acho_exists;
--
           IF (l_acho_exists IS NULL) THEN
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2', 185);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check that LPEXP_REQUESTED_PERCENTAGE is supplied if the 
-- payment_expectation_types.PEXT_RECALC_ON_RENT_CHANGE is set to Y
--
--
          IF (l_pext_rec.pext_recalc_on_rent_change = 'Y') THEN
--
           IF (p1.lpexp_requested_percentage IS NULL) THEN
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2', 782);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check party reference type is valid. This must be supplied if the 
-- payment_expectation_types.PEXT_DIRECT_DEBIT_IND is set to Y
--
          IF (p1.lpexp_par_type IS NOT NULL) THEN 
--
           IF (p1.lpexp_par_type NOT IN ('PAR','PRF')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',581);
           END IF;
--
          END IF;
--
          IF (l_pext_rec.pext_direct_debit_ind = 'Y') THEN
--
           IF (p1.lpexp_par_type IS NULL) THEN
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2', 581);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check that LPEXP_PAR_REFERENCE is supplied and valid if the 
-- payment_expectation_types.PEXT_DIRECT_DEBIT_IND is set to Y
--
--
          IF (l_pext_rec.pext_direct_debit_ind = 'Y') THEN
--
           IF (p1.lpexp_par_reference IS NULL) THEN
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2', 582);
--
           ELSE
--
              IF (p1.lpexp_par_type = 'PAR') THEN
--
                OPEN c_get_par(p1.lpexp_par_reference);
               FETCH c_get_par INTO l_par_refno;
--
               IF (c_get_par%NOTFOUND) THEN
                l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',583);
               END IF;
--
               CLOSE c_get_par;
--
              ELSE
--
                  OPEN c_get_prf(p1.lpexp_par_reference);
                 FETCH c_get_prf INTO l_par_refno;
--
                 IF (c_get_prf%NOTFOUND) THEN
                  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',583);
                 END IF;
--
                 CLOSE c_get_prf;
--
              END IF;
--
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check that at lease one of LPEXP_FIXED_AMOUNT_COMPONENT or 
-- LPEXP_PERCENTAGE_COMPONENT is supplied based on the payment expectation
-- type setup pext_percentage_allowed_ind and pext_flat_rate_allowed_ind
--
--
          IF (l_pext_rec.pext_percentage_allowed_ind = 'Y' AND l_pext_rec.pext_flat_rate_allowed_ind = 'Y') THEN
--
           IF (    p1.lpexp_fixed_amount_component IS NULL 
               AND p1.lpexp_percentage_component   IS NULL) THEN
--
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2', 783);
--
           END IF;
--
          ELSIF(l_pext_rec.pext_percentage_allowed_ind = 'Y' AND l_pext_rec.pext_flat_rate_allowed_ind = 'N') THEN
--
              IF (p1.lpexp_percentage_component IS NULL) THEN
               l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2', 783);
              END IF;
--
          ELSIF(l_pext_rec.pext_percentage_allowed_ind = 'N' AND l_pext_rec.pext_flat_rate_allowed_ind = 'Y') THEN
--
              IF (p1.lpexp_fixed_amount_component IS NULL) THEN
               l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2', 783);
              END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check that Account Charge LPEXP_DEN_ACCOUNT_CHARGE is supplied if
-- LPEXP_REQUESTED_PERCENTAGE is supplied.
--
          IF (p1.lpexp_requested_percentage IS NOT NULL) THEN
--
           IF (p1.lpexp_den_account_charge IS NULL) THEN
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2', 784);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check that LPEXP_BAD_ACCOUNT_NO is supplied if the 
-- payment_expectation_types.PEXT_DIRECT_DEBIT_IND is set to Y
--
--
          IF (l_pext_rec.pext_direct_debit_ind = 'Y') THEN
--
           IF (p1.lpexp_bad_account_no IS NULL) THEN
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD1', 879);
--
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check that Bank Name, Bank Account Name and Bank Sort Code is supplied if
-- the Bank Account No is supplied.
--
          IF (p1.lpexp_bad_account_no IS NOT NULL) THEN
--
           IF (   p1.lpexp_bde_bank_name    IS NULL
               OR p1.lpexp_bde_bty_code     IS NULL
               OR p1.lpexp_bad_account_name IS NULL
               OR p1.lpexp_bad_sort_code    IS NULL) THEN
--
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HDL', 109);
--
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- If the Bank Type Code is supplied check it is a valid code.
--
          l_bty_exists := NULL;
--
          IF (p1.lpexp_bde_bty_code IS NOT NULL) THEN
-- 
            OPEN chk_bty_code(p1.lpexp_bde_bty_code);
           FETCH chk_bty_code INTO l_bty_exists;
           CLOSE chk_bty_code;
--
           IF (l_bty_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',866); 
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check that a default Organisation Bank Account Number can be derived for
-- the revenue account admin unit code (RAC_AUN_CODE) if one hasn't been supplied
--
          IF (p1.lpexp_aun_bad_account_no IS NULL) THEN
--
           l_aun_bad_acct_no := NULL;
--
            OPEN chk_org_acct_no_exists(p1.lpexp_rac_pay_ref);
           FETCH chk_org_acct_no_exists INTO l_aun_bad_acct_no;
           CLOSE chk_org_acct_no_exists;
--
           IF (l_aun_bad_acct_no IS NULL) THEN
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HDL', 867);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check that Source Bank Account Party Reference LPEXP_BAD_PAR_REFERENCE 
-- is valid if supplied
--
--
          IF (p1.lpexp_bad_par_reference IS NOT NULL) THEN
--
           IF (p1.lpexp_par_org_ind = 'P') THEN
--
-- If a party has been supplied then its a Person
--
            IF (p1.lpexp_par_type = 'PAR') THEN
--
              OPEN c_get_par(p1.lpexp_bad_par_reference);
             FETCH c_get_par INTO l_bad_par_refno;
--
             IF (c_get_par%NOTFOUND) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',880);
             END IF;
--
             CLOSE c_get_par;
--
            ELSE
--
                OPEN c_get_prf(p1.lpexp_bad_par_reference);
               FETCH c_get_prf INTO l_bad_par_refno;
--
               IF (c_get_prf%NOTFOUND) THEN
                l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',880);
               END IF;
--
               CLOSE c_get_prf;
--
            END IF; -- p1.lpexp_par_type = 'PAR'
--
           ELSE
--
-- else it must be an organisation
-- 
               OPEN c_org_refno(p1.lpexp_bad_par_reference);
              FETCH c_org_refno INTO l_bad_par_refno;
--
              IF (c_org_refno%NOTFOUND) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',332);
              END IF;
--
              CLOSE c_org_refno;
--
           END IF; -- p1.lpexp_par_org_ind = 'P'
--   
          END IF; -- p1.lpexp_bad_par_reference IS NOT NULL
--
-- ***********************************************************************
--
-- If the bty_branch_code_mandatory_flag is set to 'MAN' and the Bank Account 
-- Number is supplied, then the bank branch name field is Mandatory
--
          IF (p1.lpexp_bad_account_no IS NOT NULL) THEN
--
           l_branch_code_man := NULL;
--
            OPEN chk_bty_branch_man_flag(p1.lpexp_bde_bank_name);
           FETCH chk_bty_branch_man_flag INTO l_branch_code_man;
           CLOSE chk_bty_branch_man_flag;
--
           IF (l_branch_code_man = 'MAN' AND p1.lpexp_bde_branch_name IS NULL) THEN
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HDL', 697);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- If the Bank Account Number is supplied, then the Direct Debit Instruction
-- ind field is Mandatory
--
--          IF (p1.lpexp_bad_account_no IS NOT NULL) THEN
--
--           IF (p1.lpexp_ddi_create_ind IS NULL) THEN
--            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2', 785);
--           END IF;
--
--          END IF;
--
-- ***********************************************************************
--
-- Check that the Party/Organisation Ind is P or O only if supplied
--
          IF (p1.lpexp_par_org_ind IS NOT NULL) THEN
--
           IF (p1.lpexp_par_org_ind NOT IN ('P','O')) THEN
            l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2', 786);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- 04-DEC-2018 6.18 Change Additional Validation checks for new columns 
-- BEGIN
--
          IF (NVL(p1.lpexp_link_to_racd,'N') NOT IN ('Y','N')) THEN
           l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2', 945);
          END IF;
--
-- ***********************************************************************
--

-- Check is LPEXP_RACD_RAUD_REFNO exists in RDS_AUTHORISED_DEDUCTIONS
--
--
          IF (NVL(p1.lpexp_link_to_racd,'N') = 'Y') THEN
--
           IF (p1.lpexp_racd_raud_refno IS NOT NULL) THEN
--
            l_racd_raud_refno := NULL;
--
             OPEN chk_racd_raud_refno_exists(p1.lpexp_racd_raud_refno);
            FETCH chk_racd_raud_refno_exists INTO l_racd_raud_refno;
            CLOSE chk_racd_raud_refno_exists;
--
            IF (l_racd_raud_refno IS NULL) THEN
             l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2', 939);
            END IF;
--
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check is LPEXP_RACD_RADT_CODE exists in RDS_ACCOUNT_DEDUCTION_TYPES
--
          IF (NVL(p1.lpexp_link_to_racd,'N') = 'Y') THEN
--
           IF (p1.lpexp_racd_radt_code IS NOT NULL) THEN
--
            l_racd_radt_code := NULL;
            l_radt_pexp_code := NULL;
--
             OPEN chk_radt_code_exists(p1.lpexp_racd_radt_code);
            FETCH chk_radt_code_exists INTO l_racd_radt_code, l_radt_pexp_code;
            CLOSE chk_radt_code_exists;
--
            IF (l_racd_radt_code IS NULL) THEN
             l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2', 940);
--
            ELSE
--
               IF (NVL(l_radt_pexp_code,'!X!') != p1.LPEXP_HRV_PEXT_CODE) THEN
                l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2', 943);
               END IF;
--
            END IF;
--
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check is LPEXP_RACD_HRV_RBEG_CODE exists in first_ref_values is supplied
--
          IF (NVL(p1.lpexp_link_to_racd,'N') = 'Y') THEN
--
           IF (p1.lpexp_racd_hrv_rbeg_code IS NOT NULL) THEN
--
            IF(NOT s_dl_hem_utils.exists_frv('RDS_BEN_GRP',p1.lpexp_racd_hrv_rbeg_code)) THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',308);
            END IF;
--
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check if all the columns have been supplied if it is to be linked to RDS_ACCOUNT_DEDUCTIONS
--
          IF (NVL(p1.lpexp_link_to_racd,'N') = 'Y') THEN
--
           IF (   p1.lpexp_racd_raud_refno    IS NULL
               OR p1.lpexp_racd_radt_code     IS NULL
               OR p1.lpexp_racd_start_date    IS NULL
               OR p1.lpexp_racd_hrv_rbeg_code IS NULL) THEN
--            
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',941);
--
           ELSE
--
              l_racd_refno := NULL;
--
               OPEN chk_racd_refno_exists(p1.lpexp_racd_raud_refno,
                                          p1.lpexp_rac_pay_ref,  -- PL Change
                                          p1.lpexp_racd_radt_code,
                                          p1.lpexp_racd_start_date,
                                          p1.lpexp_racd_hrv_rbeg_code);
--
              FETCH chk_racd_refno_exists INTO l_racd_refno;
              CLOSE chk_racd_refno_exists;
--
              IF (l_racd_refno IS NULL) THEN
               l_errors := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'HD2', 942);
              END IF;
--
           END IF;
--
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
           s_dl_process_summary.update_processed_count (cb, cp, cd, l_error_ind);
           set_record_status_flag (l_id, l_errors);
--
-- keep a count of the rows processed and commit after every 1000
--
           i := i + 1;
--
           IF MOD (i, 1000) = 0 THEN
            COMMIT;
           END IF;
--
           EXCEPTION
                WHEN OTHERS THEN
                   ce := s_dl_errors.record_error (cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
                   s_dl_process_summary.update_processed_count (cb, cp, cd, 'Y');
                   set_record_status_flag (l_id, 'O');
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
            s_dl_utils.update_process_summary (cb, cp, cd, 'FAILED');
--
END dataload_validate;
--
--
-- ***********************************************************************
--
PROCEDURE dataload_delete (p_batch_id IN VARCHAR2, 
                           p_date     IN DATE)
IS
--
CURSOR c1
IS
SELECT ROWID rec_rowid,
       lpexp_dlb_batch_id,
       lpexp_dl_seqno,
       lpexp_dl_load_status,
       lpexp_refno
  FROM dl_hra_payment_expectations
 WHERE lpexp_dlb_batch_id   = p_batch_id 
   AND lpexp_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb         VARCHAR2 (30);
cd         DATE;
cp         VARCHAR2 (30)  := 'DELETE';
ct         VARCHAR2 (30)  := 'DL_HRA_PAYMENT_EXPECTATIONS';
cs         INTEGER;
ce         VARCHAR2 (200);
l_id       ROWID;
l_an_tab   VARCHAR2 (1);
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists   VARCHAR2 (1);
i          INTEGER        := 0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start ('s_dl_hra_payment_expectations.dataload_delete');
    fsc_utils.debug_message ('s_dl_hra_payment_expectations.dataload_delete', 3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary (cb, cp, cd, 'RUNNING');
--
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lpexp_dl_seqno;
          l_id := p1.rec_rowid;
          i := i + 1;
--
--
-- Delete from PAYMENT_EXPECTATIONS table
--
--
          DELETE 
            FROM payment_expectations
           WHERE pexp_refno = p1.lpexp_refno;
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
--
          s_dl_process_summary.update_processed_count (cb, cp, cd, 'N');
          set_record_status_flag (l_id, 'V');
--
          IF MOD (i, 5000) = 0 THEN
           COMMIT;
          END IF;
--
          EXCEPTION
               WHEN OTHERS THEN
                  ce := s_dl_errors.record_error (cb, cp, cd, ct, cs, 'ORA', SQLCODE);
                  set_record_status_flag (l_id, 'C');
                  s_dl_process_summary.update_processed_count (cb, cp, cd, 'Y');
--
      END;
--
    END LOOP;
--
--
-- Section to analyze the table(s) populated by this dataload
--
    l_an_tab := s_dl_hem_utils.dl_comp_stats ('PAYMENT_EXPECTATIONS');
--
    fsc_utils.proc_end;
    COMMIT;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_utils.update_process_summary (cb, cp, cd, 'FAILED');
            RAISE;
--
END dataload_delete;
--
END s_dl_hra_payment_expectations;
/

show errors