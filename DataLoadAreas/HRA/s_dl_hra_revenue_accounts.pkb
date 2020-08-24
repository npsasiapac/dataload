CREATE OR REPLACE PACKAGE BODY s_dl_hra_revenue_accounts
AS
-- ***********************************************************************

--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERS  DB Ver   WHO  WHEN       WHY
--   1.0           MTR  29/11/00   Product Dataload
--   1.1  5.1.4    PJD  03/02/02   Added cursor c_year instead of 
--                                 previous code 
--                                 Removed use of l_aun_code 
--                                 variable as it was not set
--                                 Bank Type Error is Now 866
--
--   1.2  5.1.4    PJD  27/02/02   Changed validation on
--                                 bde_bty_code.
--                                 Put insert into payment methods 
--                                 after contracts
--                                 Changed validation of Org bank
--                                 acc to allow for NCTs
--                                 Added insert into
--                                 pm_destination_accounts
--
--   1.3  5.1.4    PJD  06/03/02   Added NVLs to various IND
--                                 colums on insert
--
--   1.4  5.1.4    PJD  27/03/02   Correction to insert to Bank
--                                 Details
-- 
--   1.5  5.1.5    PH   23/04/02   Amended main cursor on delete
--                                 to join tables using pay_ref
--                                 not alt_ref.
--   1.6  5.1.6    PJD  20/05/02   Improve performance of
--                                 c_get_tenant in the create proc
--   1.7  5.1.6    PH   19/06/02   Amended Delete from Bank
--                                 Account Details
--   1.8  5.1.6    PJD  21/06/02   Minor corection to validation 
--                                 of error 868
--   1.9  5.1.6    PH   01/07/02   Amended second exception on delete so it
--                                 updates s_dl_errors
--
--   2.0  5.2.0    PH   11/07/02   Set l_bde_refno to null on create.
--                                 Software Release 5.2.0
--                                 Also added rac_day_due into Create Proc
--
--   2.1  5.2.0   PJD   19/08/02   Made payment methods optional in the 
--                                 create and validate procedure.  
--   2.2  5.2.0   PJD   05/09/02   Removed reference to HRR product area 
--                                 as now obsolete
--   2.3  5.2.0   PJD   28/10/02   Corrected the above amendment 
--   3.0  5.3.0   PJD   04/02/03   Default 'Created By' field to DATALOAD
--   3.1  5.3.0   PJD   26/03/03   Alter c_bde_exists cursor to allow for 
--                                 null branch name.
--   3.2  5.3.0   PJD   16/04/03   Validation - reset l_dummy between records.
--   3.3  5.4.0   PH    06/07/03   Validation - Changed curs c_null_aub_aun_code
--                                 from exists to in as more efficient
--   3.4  5.4.0   PH    11/07/03   Amended Validation in property/tenancy to 
--                                 cater for account type SER. These not linked
--                                 to tenancy.
--   3.5  5.4.0   PH    04/09/03   Changed cursor c_class on create to cater for
--                                 account type SER.
--   3.6  5.4.0   PH    08/10/03   Corrected error code on escalation
--                                 policy check from 863 to 403.
--   3.7  5.4.0   PH    08/10/03   Changed cursors c_aub_aun_code and 
--                                 c_null_aub_aun_code on validate and create
--                                 to use admin_groupings_self for efficiency.
--   3.8. 5.4.0   PJD   10/11/03   Res_ind now set by cursor within this package
--                                 as previous call to ext proc not reliable. 
--   3.9  5.4.0   PJD   05/12/03   Bank Account Start Date will now default to 
--                                 the payment method start date where required
--                                 but not supplied.
--   4.0  5.5.0   IR    14/04/04   Added creation of lease account details.  
--                                 Created cursors c_get_lea_pro_propref and 
--                                 c_lea_pro_refno
--   4.1  5.6.0   VRS   17/09/04   Corrected field name lrac_las_lea_pro_refno.
--                                 Should now be lrac_las_lea_pro_propref.
--   4.2  5.6.0   PH    10/11/04   Amended validate on regular payment methods. 
--                                 Now pass in pme_start as you Cannot have a 
--                                 pme record without relevant payment_profile 
--                                 for the period.
--   4.3  5.6.0   PH    07/12/04   Amended cursor c_class on Create process as 
--                                 MWO account types should have a class of MWO
--   4.4  5.6.0   PH    12/01/05   Amended Validate on HDL111 to exclude 
--                                 MWO accounts. 
--                                 For 570 Release defaulted new field
--                                 rac_sco_code to 'ACT'. May need to be
--                                 added into the spec at 580
--   4.5  5.8.0   MB    01/06/05   Amended Validate cursor c_year to set 
--                                 parameter p_date = p_start_date to match 
--                                 cursor definition
--   4.6  5.10.0  PH    20/10/06   Commented out Cursor to derive rac_class_code
--                                 now use value supplied in table. Also added
--                                 additional validate on Class Code and 
--                                 combination with account type.
--   4.7  5.10.0  PH    25/10/06   Validate added on Report Prop Ref, this 
--                                 should never be null. Should be a valid 
--                                 propref or ~NCT~. Also added check on 
--                                 lrac_las_lea_pro_propref for SER and MWO 
--                                 accounts.
--
--   4.8  5.10.0 VRS    20/04/07   Validating lrac_las_lea_pro_propref causes 
--                                 validation fail with numeric value to large 
--                                 error and leave cursor open, causing all 
--                                 records to fail validation with ora-6502 and
--                                 ora-6511 oracle error.
-- 
--   4.9  5.10.0 PH     30/04/07   Added in new check to make sure that the 
--                                 account is in same REN admin unit as Parent 
--                                 Property.
--
--   5.0  5.10.0 VRS    11/05/07   Validation for constraint RAC_LAS_FK. 
--                                 RAC_LAS_LEA_PRO_REFNO,
--                                 RAC_LAS_LEA_START_DATE, RAC_LAS_START_DATE. 
--                                 to stop create failures based on this 
--                                 constraint. Identified at Nottingham.
--                                 Plus General Code Tidy up
--   5.1  5.12.0 PH     19/06/07   Added insert into Invoice Parties table for
--                                 Customer Liability Accounts.
--   5.2  5.12.0 IR     06/08/07   changed l_class_code to look at 
--                                 rac_class_code instead of rac_hrv_ate_code
--   6.0  5.13.0 PH     05/02/08   Added validate on lrac_las_start_date and 
--                                 value of system parameter 'LIALEALAS'. Only 
--                                 check pay method start if pay method supplied
--                                 Now includes its own 
--                                 set_record_status_flag procedure.
--   6.1  5.13.0 PH     04/06/08   Amended validate resetting variables l_year
--                                 and l_ppr within the loop. Also amended
--                                 c_year cursor so that it gets the earliest
--                                 admin year for the date supplied.
--   6.1  5.13.0 PH     20/11/08   Amended validate on Bank Details, now checks
--                                 bank types table.
--   6.1  5.13.0 PJD    20/01/09   Amended validate on valid assigment start 
--                                 date to only check if rac_lea_las_start_Date
--                                 is not null.
--   6.3  5.13.0 PH     06/04/09   Added additional deletes to 
--                                 aa_party_bank_accounts and ddi_accounts
--   6.4  5.15.1 PH     04/11/09   Amended validate on HDL111 to run off class
--                                 code rather that account type
--   6.9  6.9    PJD    10/04/14   Make Payment Method details optional
--   6.11 6.11   AJ     27/07/15   Updated for v6.11 multi-language changes
--                                 in insert bank_details and validate bank details
--   6.11 6.11   AJ     28/07/15   Minor changes to v6.11 multi-language checks
--   6.12 6.13   AJ     13/12/16   Validate error HDL_109 and HDL_868 Bank and Party
--                                 used more than once so amended duplicates to HD3_017,018,019,020
--   6.13 6.13   AJ     14/12/16   Validate errors HD3_017,018,019,020 already used in copy done
--                                 by Vish so amended to 074(017),075(018),076(019)and 077(020)  
--   6.16 6.16   MJK    22/01/18   Allow the creation of accounts with the class code of 'LOA'
--   7.0b  6.16   AJ  09+10/04/18  1)set rac_res_ind to "Y"(l_pro_pty_res)if not found or or not supplied
--                                 2)added LRAC_RES_IND field to the data load file to be used for setting
--                                   the residential Indicator for records where field 7 is set to "~NCT~"
--                                 3)The epo_code will be loaded if supplied against SPA accounts
--                                   further validation is also need to allow for this change.
--
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hra_revenue_accounts
  SET lrac_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_revenue_accounts');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
  --                                    
  --  declare package variables AND constants


PROCEDURE dataload_create(p_batch_id          IN VARCHAR2,
                          p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2)
IS
SELECT rowid rec_rowid,
       lrac_dlb_batch_id,
       lrac_dl_seqno,
       lrac_dl_load_status,
       lrac_accno,lrac_pay_ref, 
       lrac_hrv_ate_code, 
       lrac_class_code,
       lrac_start_date, 
       lrac_recoverable_ind,
       lrac_vat_ind, 
       lrac_archived_ind, 
       lrac_dispute_ind,
       lrac_verify_ind, 
       lrac_res_ind, 
       lrac_suspend_statement_ind,
       lrac_aun_code, 
       lrac_hb_new_acc_ind, 
       lrac_model_ind,
       lrac_tcy_refno, 
       lrac_created_by, 
       lrac_created_date,
       lrac_reusable_refno, 
       lrac_report_pro_refno, 
       lrac_hrv_ade_code,
       lrac_modified_by, 
       lrac_modified_date, 
       lrac_par_alt_ref,
       lrac_tcy_new_refno, 
       lrac_check_digit, 
       lrac_debit_to_date,
       lrac_rebate, 
       lrac_statement_to_date,
       lrac_statement_bal,
       lrac_alt_ref, 
       lrac_end_date, 
       lrac_hb_claim_no,
       lrac_text,
       lrac_ipp_refno,
       lrac_bhe_code,
       lrac_budget_start_date, 
       lrac_wor_ordno,
       lrac_src_code, 
       lrac_hde_claim_no, 
       lrac_pro_refno,
       lrac_review_date, 
       lrac_verify_date, 
       lrac_verify_count,
       lrac_verify_text, 
       lrac_model_date,
       lrac_model_count,
       lrac_model_text, 
       lrac_credit_budget_aun, 
       lrac_bah_seqno_hbo,
       lrac_next_bal_date, 
       lrac_terminated_date, 
       lrac_review_code,
       lrac_arrears_text, 
       lrac_terminated_by,
       lrac_dcd_date,
       lrac_last_aba_date,
       lrac_last_aba_balance,
       lrac_lco_code,
       lrac_rtb_ref, 
       lrac_s125_offer_date, 
       lrac_lease_years,
       lrac_initial_start_date,
       lrac_hrv_initrsn, 
       lrac_reference_start_date,
       lrac_hrv_refrsn,
       lrac_prev_report_pro_refno, 
       lrac_epo_code,
       lrac_pme_pmy_code,
       lrac_pme_start_date,
       lrac_pme_hrv_ppc_code,
       lrac_pme_first_dd_taken_ind,
       lrac_bde_bank_name,
       lrac_bde_branch_name,
       lrac_bad_account_no,
       lrac_bad_account_name,
       lrac_bad_sort_code,
       lrac_bad_start_date,
       lrac_aun_bad_account_no,
       lrac_par_org_ind,
       lrac_bad_par_per_alt_ref,
       lrac_pct_amount,
       lrac_pct_percentage,
       lrac_bde_bty_code,
       lrac_tcy_alt_ref,
       lrac_day_due,
       lrac_las_lea_start_date,
       lrac_las_start_date,
       lrac_las_lea_pro_propref,
       lrac_bde_bank_name_mlang,
       lrac_bde_branch_name_mlang
  FROM dl_hra_revenue_accounts
 WHERE lrac_dlb_batch_id   = p_batch_id
   AND lrac_dl_load_status = 'V';
--
--
-- *********************************************************************
--
CURSOR c_pro_refno(l_pro_propref VARCHAR2)
IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = l_pro_propref;
--
--
-- *********************************************************************
--
CURSOR c_lea_pro_refno(l_lea_pro_propref VARCHAR2)
IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = l_lea_pro_propref;
--
--
-- *********************************************************************
--
CURSOR c_par_refno(l_par_alt_ref VARCHAR2)
IS
SELECT par_refno
  FROM   parties
 WHERE  par_per_alt_ref = l_par_alt_ref;
--
--
-- *********************************************************************
--
CURSOR c_class (p_rac_hrv_ate_code VARCHAR2,
                p_par_refno        VARCHAR2)
IS
SELECT decode(p_rac_hrv_ate_code,
             'REN','REN',
             'SER','SER',
             'MWO','MWO'
             ,decode(p_par_refno
                    ,null,'TSA'
                    ,'SPA'
                    )
            )
  FROM dual;
--
--
-- *********************************************************************
--
CURSOR c_rac_accno
IS
SELECT rac_accno_seq.nextval
  FROM dual;
--
--
-- *********************************************************************
--
CURSOR c_org_refno(l_par_alt_ref VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_com_short_name = l_par_alt_ref
    OR par_org_short_name = l_par_alt_ref;
--
--
-- *********************************************************************
--
CURSOR c_pme_refno
IS
SELECT pme_refno_seq.nextval
  FROM dual;
--
--
-- *********************************************************************
--
CURSOR c_pct_refno
IS
SELECT pct_refno_seq.nextval
  FROM dual;
--
--
-- *********************************************************************
--
CURSOR c_pmt (p_pmy_code VARCHAR2) 
IS
SELECT pmy_regular_payment_ind,
       pmy_extract_ind
  FROM payment_method_types
 WHERE pmy_code = p_pmy_code;
--
--
-- *********************************************************************
--
CURSOR c_bde_exists(p_bank_name VARCHAR2, p_branch_name VARCHAR2)
IS
SELECT bde_refno
  FROM bank_details
 WHERE bde_bank_name   = p_bank_name
   AND nvl(bde_branch_name,'~') = nvl(p_branch_name,'~');
--
--
-- *********************************************************************
--
CURSOR c_bde_refno
IS
SELECT bde_refno_seq.nextval
  FROM dual;
--
--
-- *********************************************************************
--
CURSOR c_bad_refno
IS
SELECT bad_refno_seq.nextval
  FROM dual;
--
--
-- *********************************************************************
--
CURSOR c_get_tenant (p_tcy_refno NUMBER) 
IS
SELECT hop_par_refno
  FROM household_persons,
       tenancy_instances 
 WHERE hop_refno     = tin_hop_refno
   AND tin_tcy_refno = p_tcy_refno
 ORDER BY nvl(tin_end_date,sysdate) desc,
          tin_start_date, 
          tin_hop_refno;
--
--
-- *********************************************************************
--
CURSOR c_aub_aun_code(p_aun_bad_account_no VARCHAR2,
                      p_pro_refno          NUMBER,
                     p_rac_aun_code       VARCHAR2) 
IS
SELECT aub_aun_code,aub_bad_refno,aub_start_date
  FROM admin_unit_bank_accounts a,
       bank_account_details     b
 WHERE b.bad_type = 'ORG'
   AND a.aub_bad_refno  = b.bad_refno
   AND b.bad_account_no = nvl(p_aun_bad_account_no,b.bad_account_no)
   AND   ((p_pro_refno IS NULL
           AND a.aub_aun_code = p_rac_aun_code
       ) 
       OR
       (p_pro_refno IS NOT NULL
        AND
        EXISTS (SELECT NULL
                  FROM admin_properties p,
                       admin_groupings_self g
                 WHERE a.aub_aun_code          = g.agr_aun_code_parent
                   AND p.apr_aun_code          = g.agr_aun_code_child    
                   AND p.apr_pro_refno         = p_pro_refno
               )
        )
       )
ORDER BY aub_default_account_ind DESC;  -- Y before N
--
--
-- *********************************************************************
--
CURSOR c_res_ind (p_pro_refno NUMBER) 
IS
SELECT pro_hou_residential_ind
  FROM properties
 WHERE pro_refno = p_pro_refno;
--
--
-- *********************************************************************
--
-- Constants for process_summary
--
cb              VARCHAR2(30);
cd              DATE;
cp              VARCHAR2(30) := 'CREATE';
ct              VARCHAR2(30) := 'DL_HRA_REVENUE_ACCOUNTS';
cs              PLS_INTEGER;
ce              VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_exists                VARCHAR2(1);
l_priority              arrears_master_statuses.ams_priority%TYPE;
l_pro_pty_res           properties.pro_hou_residential_ind%TYPE;
l_tcy_refno             tenancies.tcy_refno%TYPE;
l_acc_type              admin_units.aun_auy_code%TYPE;
l_class_code            revenue_accounts.rac_class_code%TYPE;
l_pro_refno             properties.pro_refno%TYPE;
l_lea_pro_refno         properties.pro_refno%TYPE;
l_par_refno             parties.par_refno%TYPE;
l_pme_refno             payment_methods.pme_refno%TYPE;
l_pct_refno             payment_contracts.pct_refno%TYPE;
l_pay_par_refno         parties.par_refno%TYPE;
l_rac_accno             revenue_Accounts.rac_accno%TYPE;
l_epo_code              escalation_policies.epo_code%type;
l_bad_refno             NUMBER(8);
l_bde_refno             NUMBER(8);
l_is_configured         VARCHAR(1);
l_reusable_refno        revenue_accounts.rac_reusable_refno%TYPE;
l_an_tab                VARCHAR2(1);
i                       PLS_INTEGER := 0;
l_aub_aun_code          VARCHAR2(20);
l_aub_bad_refno         NUMBER(10);
l_aub_start_date        DATE;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_revenue_accounts.dataload_create');
fsc_utils.debug_message( 's_dl_hra_revenue_accounts.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1(p_batch_id) LOOP
--
BEGIN
--
cs := p1.lrac_dl_seqno;
l_id := p1.rec_rowid;
SAVEPOINT SP1;
--
IF (p1.lrac_accno IS NULL) 
THEN
  --
   OPEN c_rac_accno;
  FETCH c_rac_accno INTO l_rac_accno;
  CLOSE c_rac_accno;
  --
ELSE 
  --
  l_rac_accno := p1.lrac_accno;
  --
END IF;
--
l_tcy_refno     := NULL;
l_pro_refno     := NULL;
l_par_refno     := NULL;
l_pay_par_refno := NULL;
l_epo_code      := NULL;
l_lea_pro_refno := NULL;
--
-- If not a non-council tenant sundry debtor account, get the
-- residential property indicator and tenancy account number.
--
IF (p1.lrac_report_pro_refno != '~NCT~') 
THEN
  --
   OPEN c_pro_refno(p1.lrac_report_pro_refno);
  FETCH c_pro_refno INTO l_pro_refno;
  CLOSE c_pro_refno;
  --
  l_pro_pty_res := NULL;
  --
   OPEN  c_res_ind(l_pro_refno);
  FETCH c_res_ind INTO l_pro_pty_res;
  CLOSE c_res_ind;
  --
  l_pro_pty_res := nvl(l_pro_pty_res,'Y'); -- default "Y" on table (AJ)
  --
  l_tcy_refno   := s_tenancies2.get_refno_from_alt_ref(p1.lrac_tcy_alt_ref);
  --
  l_epo_code    := p1.lrac_epo_code;
  --
ELSE
  --
  -- this is for where set to NCT so SPA(Sundry Party Accounts)accounts in the main
  --
   OPEN c_par_refno(p1.lrac_par_alt_ref);
  FETCH c_par_refno INTO l_par_refno;
  CLOSE c_par_refno;
  --
  IF (p1.lrac_res_ind IS NULL)
    THEN  
     l_pro_pty_res := 'Y'; -- default "Y" on table (AJ)
   ELSE
     l_pro_pty_res := p1.lrac_res_ind;
  END IF;
  --
  --
  --
  l_epo_code    := p1.lrac_epo_code;
  --
END IF;
--
-- If a Leaseholder Account then get the pro refno
--
IF (p1.lrac_las_lea_pro_propref is not null) 
THEN
  --
   OPEN  c_lea_pro_refno(p1.lrac_las_lea_pro_propref);
  FETCH c_lea_pro_refno into l_lea_pro_refno;
  CLOSE c_lea_pro_refno;
  --
END IF;
--
-- Get the REUSABLE_REFNO
--
l_reusable_refno := fsc_utils.f_dynamic_value('reusable_refno_seq.NEXTVAL');
--
l_class_code := p1.lrac_class_code;
--
-- insert a row into REVENUE_ACCOUNTS
--
INSERT INTO revenue_accounts(rac_accno, 
                             rac_pay_ref, 
                             rac_hrv_ate_code,
                             rac_class_code, 
                             rac_start_date, 
                             rac_recoverable_ind,
                             rac_vat_ind, 
                             rac_archived_ind, 
                             rac_dispute_ind,
                             rac_verify_ind, 
                             rac_res_ind, 
                             rac_suspend_statement_ind,
                             rac_aun_code,
                             rac_hb_new_acc_ind, 
                             rac_model_ind,
                             rac_tcy_refno, 
                             rac_created_by, 
                             rac_created_date,
                             rac_reusable_refno,
                             rac_report_pro_refno, 
                             rac_hrv_ade_code,
                             rac_modified_by, 
                             rac_modified_date, 
                             rac_par_refno,
                             rac_tcy_new_refno,
                             rac_check_digit, 
                             rac_debit_to_date,
                             rac_rebate, 
                             rac_statement_to_date, 
                             rac_statement_bal,
                             rac_alt_ref,
                             rac_end_date, 
                             rac_hb_claim_no,
                             rac_text, 
                             rac_ipp_refno, 
                             rac_bhe_code,
                             rac_budget_start_date, 
                             rac_wor_ordno,
                             rac_src_code,
                             rac_hde_claim_no, 
                             rac_pro_refno, 
                             rac_review_date,
                             rac_verify_date, 
                             rac_verify_count, 
                             rac_verify_text,
                             rac_model_date, 
                             rac_model_count, 
                             rac_model_text,
                             rac_credit_budget_aun,
                             rac_bah_seqno_hbo,
                             rac_next_bal_date,
                             rac_terminated_date, 
                             rac_review_code, 
                             rac_arrears_text,
                             rac_terminated_by, 
                             rac_dcd_date, 
                             rac_last_aba_date,
                             rac_last_aba_balance, 
                             rac_lco_code, 
                             rac_rtb_ref,
                             rac_s125_offer_date, 
                             rac_lease_years,
                             rac_initial_start_date,
                             rac_hrv_initrsn, 
                             rac_reference_start_date, 
                             rac_hrv_refrsn,
                             rac_prev_report_pro_refno, 
                             rac_epo_code, 
                             rac_day_due,
                             rac_las_lea_start_date, 
                             rac_las_start_date, 
                             rac_las_lea_pro_refno,
                             rac_sco_code
                            )VALUES
                            (l_rac_accno, 
                             p1.lrac_pay_ref, 
                             p1.lrac_hrv_ate_code,
                             l_class_code, 
                             p1.lrac_start_date, 
                             nvl(p1.lrac_recoverable_ind,'Y'),
                             nvl(p1.lrac_vat_ind,'N'), 
                             nvl(p1.lrac_archived_ind,'N'), 
                             nvl(p1.lrac_dispute_ind,'N'),
                             nvl(p1.lrac_verify_ind,'Y'), 
                             l_pro_pty_res, 
                             nvl(p1.lrac_suspend_statement_ind,'N'),
                             p1.lrac_aun_code, 
                             nvl(p1.lrac_hb_new_acc_ind,'N'), 
                             nvl(p1.lrac_model_ind,'Y'),
                             l_tcy_refno,   
                             'DATALOAD',
                             trunc(sysdate),
                             l_reusable_refno, 
                             l_pro_refno, 
                             p1.lrac_hrv_ade_code,
                             p1.lrac_modified_by, 
                             p1.lrac_modified_date, 
                             l_par_refno,
                             p1.lrac_tcy_new_refno, 
                             p1.lrac_check_digit, 
                             p1.lrac_debit_to_date,
                             p1.lrac_rebate, 
                             p1.lrac_statement_to_date, 
                             p1.lrac_statement_bal,
                             p1.lrac_alt_ref, 
                             p1.lrac_end_date, 
                             p1.lrac_hb_claim_no,
                             p1.lrac_text, 
                             p1.lrac_ipp_refno, 
                             p1.lrac_bhe_code,
                             p1.lrac_budget_start_date, 
                             p1.lrac_wor_ordno,
                             p1.lrac_src_code,
                             p1.lrac_hde_claim_no, 
                             p1.lrac_pro_refno, 
                             p1.lrac_review_date,
                             p1.lrac_verify_date, 
                             p1.lrac_verify_count,
                             p1.lrac_verify_text,
                             p1.lrac_model_date, 
                             p1.lrac_model_count, 
                             p1.lrac_model_text,
                             p1.lrac_credit_budget_aun, 
                             p1.lrac_bah_seqno_hbo,
                             p1.lrac_next_bal_date,
                             p1.lrac_terminated_date, 
                             p1.lrac_review_code, 
                             p1.lrac_arrears_text,
                             p1.lrac_terminated_by, 
                             p1.lrac_dcd_date, 
                             p1.lrac_last_aba_date,
                             p1.lrac_last_aba_balance, 
                             p1.lrac_lco_code, 
                             p1.lrac_rtb_ref,
                             p1.lrac_s125_offer_date, 
                             p1.lrac_lease_years,
                             p1.lrac_initial_start_date,
                             p1.lrac_hrv_initrsn, 
                             p1.lrac_reference_start_date, 
                             p1.lrac_hrv_refrsn,
                             p1.lrac_prev_report_pro_refno, 
                             l_epo_code,
                             p1.lrac_day_due,
                             p1.lrac_las_lea_start_date, 
                             p1.lrac_las_start_date, 
                             l_lea_pro_refno,
                             'ACT');
--
-- If not a non-council tenant sundry debtor account, assign a
-- summary account, otherwise insert a row into NCT_AUN_LINK and
-- assign a summary account
--
BEGIN
  --
  IF (l_class_code != 'SPA') 
  THEN
    --
    hra_rencon.p_assign_rac_sac(l_rac_accno, p1.lrac_hrv_ate_code
                               , trunc(sysdate));
    --
  ELSE
    --
    s_nct_aun_link.create_nct_aun_link_dml(l_rac_accno
                                          , p1.lrac_aun_code, 'REN');
    --
    hra_rencon.p_assign_nct_sac(l_rac_accno, p1.lrac_hrv_ate_code
                               , trunc(sysdate));
    --
  END IF;
  --
  EXCEPTION
  WHEN OTHERS 
  THEN 
    NULL;
  --
END;
--
--       
-- If bank details have been supplied, get the next sequence number
-- and insert into BANK_DETAILS and BANK_HOLDINGS
--
IF (    p1.lrac_bad_account_no IS NOT NULL
    AND p1.lrac_pme_pmy_code   IS NOT NULL 
   ) 
THEN
  --
  -- Need to see if a party has been supplied on the record
  --
  l_pay_par_refno := l_par_refno;
  --
  -- 
  IF (p1.lrac_bad_par_per_alt_ref IS NULL) 
  THEN
    --
    -- is it a sundry party account - in which case use that party
    --
    IF (l_pay_par_refno IS NULL) 
    THEN
      --
       OPEN c_get_tenant(l_tcy_refno);
      FETCH c_get_tenant into l_pay_par_refno;
      CLOSE c_get_tenant;
      --
    END IF;
    --
  ELSE
    --
    -- If a party has been supplied then...
    -- is it a Person
    --
    IF (NVL(p1.lrac_par_org_ind,'P') = 'P') 
    THEN
      --
       OPEN c_par_refno(p1.lrac_bad_par_per_alt_ref);
      FETCH c_par_refno INTO l_pay_par_refno;
      CLOSE c_par_refno;
      --
    ELSE
      --
      -- or else it must be an organisation
 
       OPEN c_org_refno(p1.lrac_bad_par_per_alt_ref);
      FETCH c_org_refno INTO l_pay_par_refno;
      CLOSE c_org_refno;
      --
    END IF;
    --
  END IF; -- had the bad_par_per_alt_ref been supplied  
  --
  --
  --
  -- If the Payment Contract Details have been supplied
  -- then create a payment contract
  --
  --
  IF (   p1.lrac_pct_amount     IS NOT NULL
      OR p1.lrac_pct_percentage IS NOT NULL ) 
  THEN
    --
    --
    l_pct_refno := NULL;
 
     OPEN c_pct_refno;
    FETCH c_pct_refno into l_pct_refno;
    CLOSE c_pct_refno;
    --
    INSERT INTO payment_contracts (pct_refno,
                                   pct_rac_accno,
                                   pct_par_refno,
                                   pct_start_date,
                                   pct_status,
                                   pct_created_date,
                                   pct_created_by,
                                   pct_amount,
                                   pct_percentage
                                  ) VALUES
                                  (l_pct_refno,
                                   l_rac_accno,
                                   l_pay_par_refno,
                                   p1.lrac_pme_start_date,
                                   'A',
                                   sysdate,
                                   'DATALOAD',
                                   p1.lrac_pct_amount,
                                   p1.lrac_pct_percentage);
    -- 
  END IF;
  --
  --   insert a row into PAYMENT_METHODS
  --
   OPEN c_pme_refno;  
  FETCH c_pme_refno into l_pme_refno;
  CLOSE c_pme_refno;
  --
  INSERT into payment_methods(pme_refno,
                              pme_pmy_code,
                              pme_start_date,
                              pme_first_dd_taken_ind,
                              pme_pct_refno,
                              pme_rac_accno,
                              pme_hrv_ppc_code
                             ) VALUES
                             (l_pme_refno,
                              p1.lrac_pme_pmy_code,
                              p1.lrac_pme_start_date,
                              nvl(p1.lrac_pme_first_dd_taken_ind,'N'),
                              l_pct_refno,
                              l_rac_accno,
                              p1.lrac_pme_hrv_ppc_code);
  --
  -- Now we need to get insert into bank_details,
  -- bank account details, party_bank_accounts and
  -- party bank acct pymt_mthds as appropriate;
  --
  -- So does the bank detail already exist
  --
  l_exists    := NULL;
  l_bde_refno := NULL;
  --
   OPEN c_bde_exists(p1.lrac_bde_bank_name, p1.lrac_bde_branch_name);
  FETCH c_bde_exists into l_bde_refno;
  CLOSE c_bde_exists;
  --
  IF (l_bde_refno  IS NULL) 
  THEN
    --
     OPEN c_bde_refno;
    FETCH c_bde_refno into l_bde_refno;
    CLOSE c_bde_refno;
--
-- used when mlang not supplied
--
    IF (p1.lrac_bde_bank_name_mlang   IS NULL) 
    THEN
    --
    INSERT INTO BANK_DETAILS(bde_refno,
                             bde_bank_name,
                             bde_created_by,
                             bde_created_date,
                             bde_branch_name,
                             bde_bty_code
                            ) VALUES 
                            (l_bde_refno,
                             p1.lrac_bde_bank_name,
                             'DATALOAD',
                             sysdate,
                             p1.lrac_bde_branch_name,
                             p1.lrac_bde_bty_code);
    --
    END IF;
    --
    IF (p1.lrac_bde_bank_name_mlang IS NOT NULL)
    THEN
--
-- used when mlang supplied
--
    INSERT INTO BANK_DETAILS(bde_refno,
                             bde_bank_name,
                             bde_created_by,
                             bde_created_date,
                             bde_branch_name,
                             bde_bty_code,
                             bde_bank_name_mlang,
                             bde_branch_name_mlang
                            ) VALUES 
                            (l_bde_refno,
                             p1.lrac_bde_bank_name,
                             'DATALOAD',
                             sysdate,
                             p1.lrac_bde_branch_name,
                             p1.lrac_bde_bty_code,
                             p1.lrac_bde_bank_name_mlang,
                             p1.lrac_bde_branch_name_mlang);
    --
    END IF;
    --  
  END IF;
  --
  -- Now insert into bank_account_details
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
                                  ) VALUES
                                  (l_bad_refno,
                                   'CUS',
                                   p1.lrac_bad_sort_code,
                                   l_bde_refno,
                                   p1.lrac_bad_account_no,
                                   p1.lrac_bad_account_name,
                                   nvl(p1.lrac_bad_start_date
                                      ,p1.lrac_pme_start_date),
                                   'DATALOAD',
                                   sysdate,
                                   NULL,
                                   NULL);
  -- 
  -- and now into party bank_accounts 
  --
  INSERT INTO PARTY_BANK_ACCOUNTS(pba_par_refno,
                                  pba_bad_refno,
                                  pba_start_date,
                                  pba_created_date,
                                  pba_created_by,
                                  pba_end_date
                                 ) VALUES
                                 (l_pay_par_refno,
                                  l_bad_refno,
                                  p1.lrac_pme_start_date,
                                  sysdate,
                                  'DATALOAD',
                                  NULL);
  -- 
  --
  -- and now into PARTY BANK ACCT PYMT MTHDS
  --
  --
  INSERT INTO PARTY_BANK_ACCT_PYMT_MTHDS(pbp_pba_par_refno,
                                         pbp_pba_bad_refno,
                                         pbp_pba_start_date,
                                         pbp_pme_refno,
                                         pbp_start_date,
                                         pbp_created_date,
                                         pbp_created_by,
                                         pbp_end_date
                                        ) VALUES
                                        (l_pay_par_refno,
                                         l_bad_refno,
                                         p1.lrac_pme_start_date,
                                         l_pme_refno,
                                         p1.lrac_pme_start_date,
                                         sysdate,
                                         'DATALOAD',
                                         NULL);
  -- 
  --
  -- and finally do an insert into pm_destination_accounts
  --
  --
  l_aub_aun_code   := NULL;
  l_aub_bad_refno  := NULL;
  l_aub_start_date := NULL;
  --
   OPEN  c_aub_aun_code(p1.lrac_aun_bad_account_no,l_pro_refno,p1.lrac_aun_code);
  FETCH c_aub_aun_code INTO l_aub_aun_code,l_aub_bad_refno,l_aub_start_date;
  CLOSE c_aub_aun_code;
  --
  --
  IF (l_aub_bad_refno IS NOT NULL) 
  THEN
    --
    INSERT INTO pm_destination_accounts(pda_aub_aun_code,
                                        pda_aub_bad_refno,
                                        pda_aub_start_date,
                                        pda_pme_refno,
                                        pda_start_date,
                                        pda_created_date,
                                        pda_created_by
                                        ) VALUES
                                        (l_aub_aun_code,
                                        l_aub_bad_refno,
                                        l_aub_start_date,
                                        l_pme_refno,
                                        p1.lrac_pme_start_date,
                                        sysdate,
                                        'DATALOAD');
    -- 
  END IF;
  --
ELSE
  --
  --   insert a row into PAYMENT_METHODS
  --
  IF (p1.lrac_pme_pmy_code IS NOT NULL) 
  THEN
    --
     OPEN c_pme_refno;  
    FETCH c_pme_refno into l_pme_refno;
    CLOSE c_pme_refno;
    --
    INSERT into payment_methods(pme_refno,
                                pme_pmy_code,
                                pme_start_date,
                                pme_first_dd_taken_ind,
                                pme_pct_refno,
                                pme_rac_accno,
                                pme_hrv_ppc_code
                               ) VALUES
                               (l_pme_refno,
                                p1.lrac_pme_pmy_code,
                                p1.lrac_pme_start_date,
                                nvl(p1.lrac_pme_first_dd_taken_ind,'N'),
                                null,
                                l_rac_accno,
                                p1.lrac_pme_hrv_ppc_code);
    --
  END IF; -- have payment details been supplied.
  -- 
  --
END IF; -- p1.lrac_bad_account_no IS NOT NULL
--
-- If it is a rent account, put the account number on
-- TENANCY_HOLDINGS
--
--
IF (p1.lrac_hrv_ate_code = 'REN') 
THEN
  --
  UPDATE tenancy_holdings
     SET tho_rac_accno   = l_rac_accno
   WHERE tho_tcy_refno   = l_tcy_refno
     AND tho_pro_refno   = l_pro_refno;
   --
END IF;
--
-- New code 19/06/07
-- If its a customer liability invoice then create
-- the entry for Invoice Parties based on who is 
-- currently in Lease Parties, starting on the
-- same dates as supplied in this load.
--
IF (    p1.lrac_hrv_ate_code = 'SER'
    AND l_class_code = 'LIA'
   )
THEN
  INSERT into sc_invoice_parties
        (scip_start_date
        ,scip_end_date
        ,scip_modified_by
        ,scip_created_by
        ,scip_created_date
        ,scip_modified_date
        ,scip_par_refno
        ,scip_rac_accno
        )
      SELECT   
         p1.lrac_start_date
        ,p1.lrac_end_date
        ,null
        ,'DATALOAD'
        ,sysdate
        ,null
        ,lpt_par_refno
        ,l_rac_accno
      FROM   lease_parties
      WHERE  lpt_las_lea_pro_refno  = l_lea_pro_refno
      AND    lpt_las_lea_start_date = p1.lrac_las_lea_start_date
      AND    lpt_las_start_date     = p1.lrac_las_start_date;
END IF;
--
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; 
--
IF MOD(i,1000)=0 
THEN 
  COMMIT; 
END IF;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'C');
--
EXCEPTION
WHEN OTHERS 
THEN
  ROLLBACK to SP1;
  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
  set_record_status_flag(l_id,'O');
  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
  --
  --
END;
--
END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('REVENUE_ACCOUNTS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANCY_HOLDINGS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PAYMENT_METHODS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PARTY_BANK_ACCT_PYMT_MTHDS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PARTY_BANK_ACCOUNT');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('BANK_DETAILS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('BANK_ACCOUNT_DETAILS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUMMARY_RAC_ACCOUNTS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('NCT_AUN_LINK');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PAYMENT_CONTRACTS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PARTY_BANK_ACCT_PYMT_MTHDS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PARTY_BANK_ACCOUNTS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PM_DESTINATION_ACCOUNTS');
--
--
fsc_utils.proc_end;
COMMIT;
--
--
EXCEPTION
WHEN OTHERS 
THEN
  s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
--
--
END dataload_create;
--
--
-- **************************************************************************************************
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       lrac_dlb_batch_id,
       lrac_dl_seqno,
       lrac_dl_load_status,
       lrac_accno,
       lrac_pay_ref, 
       lrac_hrv_ate_code, 
       lrac_class_code,
       lrac_start_date, 
       lrac_recoverable_ind,
       lrac_vat_ind,
       lrac_archived_ind, 
       lrac_dispute_ind,
       lrac_verify_ind,
       lrac_res_ind, 
       lrac_suspend_statement_ind,
       lrac_aun_code,
       lrac_hb_new_acc_ind, 
       lrac_model_ind, 
       lrac_tcy_refno,
       lrac_created_by, 
       lrac_created_date,
       lrac_reusable_refno,
       lrac_report_pro_refno, 
       lrac_hrv_ade_code, 
       lrac_modified_by,
       lrac_modified_date,
       lrac_par_alt_ref, 
       lrac_tcy_new_refno,
       lrac_check_digit, 
       lrac_debit_to_date, 
       lrac_rebate,
       lrac_statement_to_date,
       lrac_statement_bal, 
       lrac_alt_ref,
       lrac_end_date, 
       lrac_hb_claim_no,  
       lrac_text,
       lrac_ipp_refno, 
       lrac_bhe_code,
       lrac_budget_start_date,
       lrac_wor_ordno, 
       lrac_src_code, 
       lrac_hde_claim_no,
       lrac_pro_refno, 
       lrac_review_date,
       lrac_verify_date,
       lrac_verify_count, 
       lrac_verify_text, 
       lrac_model_date,
       lrac_model_count, 
       lrac_model_text,
       lrac_credit_budget_aun,
       lrac_bah_seqno_hbo, 
       lrac_next_bal_date, 
       lrac_terminated_date,
       lrac_review_code,
       lrac_arrears_text, 
       lrac_terminated_by,
       lrac_dcd_date, 
       lrac_last_aba_date, 
       lrac_last_aba_balance,
       lrac_lco_code, 
       lrac_rtb_ref, 
       lrac_s125_offer_date,
       lrac_lease_years, 
       lrac_initial_start_date,
       lrac_hrv_initrsn,
       lrac_reference_start_date,
       lrac_hrv_refrsn,
       lrac_prev_report_pro_refno,
       lrac_epo_code,
       lrac_pme_pmy_code, 
       lrac_pme_start_date,
       lrac_pme_hrv_ppc_code,
       lrac_pme_first_dd_taken_ind,
       lrac_bde_bank_name,
       lrac_bde_branch_name,
       lrac_bad_account_no,
       lrac_bad_account_name, 
       lrac_bad_sort_code, 
       lrac_bad_start_date,
       lrac_aun_bad_account_no, 
       lrac_par_org_ind,
       lrac_bad_par_per_alt_ref,
       lrac_pct_amount,
       lrac_pct_percentage,
       lrac_bde_bty_code,
       lrac_tcy_alt_ref,
       lrac_day_due,
       lrac_las_lea_start_date,
       lrac_las_start_date,
       lrac_las_lea_pro_propref,
       lrac_bde_bank_name_mlang,
       lrac_bde_branch_name_mlang
  FROM dl_hra_revenue_accounts
 WHERE lrac_dlb_batch_id       = p_batch_id  
   AND lrac_dl_load_status  in ('L','F','O');
--
--
-- *********************************************************************
--
CURSOR c_ppr(c_lrac_aun_code            VARCHAR2,
             c_lrac_pme_hrv_ppc_code    VARCHAR2,
             c_year                     VARCHAR2)
IS
SELECT ppr_aye_year
  FROM payment_profiles
 WHERE ppr_aye_aun_code = c_lrac_aun_code
   AND ppr_aye_year     = c_year
   AND ppr_hrv_ppc_code = c_lrac_pme_hrv_ppc_code;
--
-- *********************************************************************
--
CURSOR c_ams(c_lrac_ams_code VARCHAR2)
IS
SELECT ams_priority
  FROM arrears_master_statuses
 WHERE ams_code = c_lrac_ams_code;
--
-- *********************************************************************
--
CURSOR c_get_pro_refno(c_pro_propref VARCHAR2)
IS
SELECT pro_refno 
  FROM properties
 WHERE pro_propref = c_pro_propref;
--
-- *********************************************************************
--
CURSOR c_get_lea_pro_refno(c_lea_pro_propref VARCHAR2)
IS
SELECT pro_refno 
  FROM properties
 WHERE pro_propref = c_lea_pro_propref;
--
-- *********************************************************************
--
CURSOR c_par_refno(l_par_alt_ref VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = l_par_alt_ref;
--
-- *********************************************************************
--
CURSOR c_epo_code(c_lrac_epo_code VARCHAR2)
IS
SELECT 'x'
  FROM escalation_policies
 WHERE epo_code = c_lrac_epo_code;
--
-- *********************************************************************
--
CURSOR c_aun(c_lrac_aun_code VARCHAR2)
IS
SELECT aun_current_ind
  FROM admin_units
 WHERE aun_code = c_lrac_aun_code;
--
-- *********************************************************************
--
CURSOR c_is_pro_refno_in_tenancy(c_tcy_refno    NUMBER,
                                 c_pro_refno    NUMBER,
                                 c_start_date   DATE)
IS
SELECT 'X'
  FROM tenancy_holdings
 WHERE tho_tcy_refno = c_tcy_refno
   AND tho_pro_refno = c_pro_refno
   AND c_start_date BETWEEN tho_start_date
                        AND nvl(tho_end_date,c_start_date);
--
-- *********************************************************************
--
CURSOR c_pmy_dets(p_pmy_code VARCHAR2) 
IS
SELECT PMY_CODE,
       PMY_REGULAR_PAYMENT_IND
  FROM payment_method_types
 WHERE pmy_code = p_pmy_code;
--
-- *********************************************************************
--
CURSOR c_pay_par_refno(l_par_alt_ref VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = l_par_alt_ref;
--
-- *********************************************************************
--
CURSOR c_pay_org_refno(l_par_alt_ref VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_com_short_name = l_par_alt_ref
    OR par_org_short_name = l_par_alt_ref;
--
-- *********************************************************************
--
CURSOR c_aub_aun_code(p_aun_bad_account_no VARCHAR2,
                      p_pro_refno          NUMBER) 
IS
SELECT 'X' 
  FROM bank_account_details
 WHERE bad_type = 'ORG'
   AND bad_account_no = p_aun_bad_account_no
   AND (p_pro_refno IS NULL 
        OR EXISTS (SELECT NULL
                     FROM admin_properties,
                          admin_groupings_self,
                          admin_unit_bank_accounts
                    WHERE aub_bad_refno      = bad_refno
                      AND aub_aun_code       = agr_aun_code_parent
                      AND agr_aun_code_child = apr_aun_code
                      AND apr_pro_refno      = p_pro_refno));
--
-- *********************************************************************
--
CURSOR c_null_aub_aun_code(p_pro_refno NUMBER) 
IS
SELECT 'X' 
  FROM bank_account_details
 WHERE bad_type = 'ORG'
   AND (p_pro_refno IS NULL
        OR bad_refno IN (SELECT aub_bad_refno
                           FROM admin_properties,
                                admin_groupings_self,
                                admin_unit_bank_accounts
                          WHERE aub_aun_code       = agr_aun_code_parent
                            AND agr_aun_code_child = apr_aun_code
                            AND apr_pro_refno      = p_pro_refno));
--
-- *********************************************************************
--
CURSOR c_bty_code(p_bde_bty_code VARCHAR2) 
IS
SELECT bty_code
      ,bty_branch_code_mandatory_flag
  FROM bank_types
 WHERE bty_code = p_bde_bty_code;
--
-- *********************************************************************
--
CURSOR c_year(p_aun_code VARCHAR2,
              p_start_date DATE) 
IS
SELECT aye_year
FROM   admin_years
WHERE  p_aun_code   = aye_aun_code
AND    aye_end_date > p_start_date 
ORDER BY aye_start_date;
--
-- *********************************************************************
--
CURSOR c_pro_ren(p_pro_refno NUMBER) 
IS
SELECT agr_aun_code_parent
  FROM admin_groupings_self,
       admin_properties
 WHERE apr_pro_refno       = p_pro_refno
   AND apr_aun_code        = agr_aun_code_child
   AND agr_auy_code_parent = 'REN';
--
-- *********************************************************************
--
CURSOR c_pro_ser(p_pro_propref VARCHAR2) 
IS
SELECT agr_aun_code_parent
  FROM admin_groupings_self,
       admin_properties,
       properties
 WHERE pro_propref         = p_pro_propref
   AND apr_pro_refno       = pro_refno
   AND apr_aun_code        = agr_aun_code_child
   AND agr_auy_code_parent = 'SER';
--
-- *********************************************************************
--
CURSOR chk_las_exists(p_las_lea_pro_refno   NUMBER,
                      p_las_lea_start_date  DATE,
                      p_las_start_date      DATE) 
IS
SELECT 'X'
  FROM lease_assignments
 WHERE las_lea_pro_refno  = p_las_lea_pro_refno
   AND las_lea_start_date = p_las_lea_start_date
   AND las_start_date     = p_las_start_date;
--
-- *********************************************************************
--
--
-- Constants for process_summary
--
--
cb                      VARCHAR2(30);
cd                      DATE;
cp                      VARCHAR2(30) := 'VALIDATE';
ct                      VARCHAR2(30) := 'DL_HRA_REVENUE_ACCOUNTS';
cs                      PLS_INTEGER;
ce                      VARCHAR2(200);
l_id     ROWID;
--
l_exists                VARCHAR2(1);
l_las_exists            VARCHAR2(1);
l_lea_pro_refno         NUMBER(10);
l_pro_refno             NUMBER(10);
l_par_refno             NUMBER(10);
l_pay_par_refno         NUMBER(10);
l_errors                VARCHAR2(10);
l_error_ind             VARCHAR2(10);
i                       PLS_INTEGER :=0;
-- Other Variables
r_rac                   revenue_accounts%ROWTYPE;
l_ppr                   NUMBER(4);
l_year                  NUMBER(4);
l_priority              NUMBER(8);
l_aun_current           VARCHAR2(1);
-- l_aun_code           VARCHAR2(20);
l_rac_accno             VARCHAR2(8);
l_tcy_refno             tenancies.tcy_refno%TYPE;
l_lrac_report_pro_refno NUMBER(25);
l_dummy                 VARCHAR(1);
epo_found               VARCHAR(1);
l_pmy_code              VARCHAR2(2);
l_pmy_reg_pay           VARCHAR2(1);
--
l_pro_ren_au            VARCHAR2(20);
l_pro_ser_au            VARCHAR2(20);
l_lialealas             VARCHAR2(255);
l_bty_code              bank_types.bty_code%type;
l_branch_mand           bank_types.bty_branch_code_mandatory_flag%type;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_revenue_accounts.dataload_validate');
fsc_utils.debug_message( 's_dl_hra_revenue_accounts.dataload_validate',3);
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
  cs := p1.lrac_dl_seqno;
  l_id := p1.rec_rowid;
  --
  l_errors := 'V';
  l_error_ind := 'N';
  --
  --  
  -- get the pro_refno to be used in later processing
  --
  l_pro_refno := NULL;
  --
  -- added so only checks when pro_propref supplied (AJ)
  IF(p1.lrac_report_pro_refno != '~NCT~') 
  THEN
   --
   OPEN  c_get_pro_refno(p1.lrac_report_pro_refno);
   FETCH c_get_pro_refno INTO l_pro_refno;
   CLOSE c_get_pro_refno;
   --
  END IF;
  --
  -- Check the revenue account does not already exist on
  -- REVENUE_ACCOUNTS
  --
  l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref
                                     ( p1.lrac_pay_ref );
  --
  IF (l_rac_accno IS NOT NULL) 
  THEN
    --
    r_rac := s_revenue_accounts2.rec_rac ( l_rac_accno );
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',100);
    --
  END IF;
  --
  -- Check that the admin unit code exists on ADMIN_UNITS, is
  -- current and is a rents admin unit
  --
  IF (s_admin_units.is_admin_unit(p1.lrac_aun_code)) 
  THEN
    --
     OPEN c_aun(p1.lrac_aun_code);
    FETCH c_aun INTO l_aun_current;
    CLOSE c_aun;
    --
    IF (l_aun_current != 'Y') 
    THEN
      --
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',051);
      --
    END IF;
    --
    IF (s_admin_units.get_aun_auy_code(p1.lrac_aun_code) 
                      NOT IN ('REN','SER')) 
    THEN
      --
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',129);
      --
    END IF;
    --
  ELSE
    --
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
    --
  END IF;
  --
  --
  -- *********************************************************************
  --
  -- Check the Y/N columns
  --
  -- First direct debit taken
  --
  IF (nvl(p1.lrac_pme_first_dd_taken_ind,'N') NOT IN ('Y','N')) 
  THEN
    --
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',101);
    --
  END IF;
  --
  --
  -- Recoverable Ind
  --
  IF (nvl(p1.lrac_recoverable_ind,'Y') NOT IN ('Y','N')) 
  THEN
    --
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',885);
    --
  END IF;
  --
  --
  -- VAT IND
  --
  IF (nvl(p1.lrac_vat_ind,'N') NOT IN ('Y','N')) 
  THEN
    --
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',886);
    --
  END IF;
  --
  --
  -- Dispute Ind
  --
  IF (nvl(p1.lrac_dispute_ind,'N') NOT IN ('Y','N')) 
  THEN
    --
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',887);
    --
  END IF;
  --
  --
  -- Suspend Statement Ind
  --
  IF (nvl(p1.lrac_suspend_statement_ind,'N') NOT IN ('Y','N')) 
  THEN
    --
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',888);
    --
  END IF;
  --
  -- Check mlang bank name supplied if branch supplied
  --
  IF (     p1.lrac_bde_bank_name_mlang   IS NULL
       AND p1.lrac_bde_branch_name_mlang IS NOT NULL    ) 
  THEN
    --
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',074);
    -- 
  END IF;
  --
  -- Check the escalation policy code exists
  --
  IF (p1.lrac_epo_code IS NOT NULL) 
  THEN
    --
     OPEN c_epo_code(p1.lrac_epo_code);
    FETCH c_epo_code INTO epo_found;
    IF (c_epo_code%NOTFOUND) 
    THEN
      --
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',403);
      --
    END IF;
    --
    CLOSE c_epo_code;
    --
	-- Additional validation on class codes from RAC_AR_IU
    -- only accounts of 'REN','TSA','SPA','REP','MWO','SER','SLA' can have
    -- escalation policies
    -- (NOTE:  this list needs to match the one in s_escalation.p_escalation_policy)
    -- Cannot do more checking as do not have gross or balance indicators at this point of the load
	--
    IF NVL(p1.lrac_class_code,'XXX') NOT IN ('REN','TSA','SPA','REP','MWO','SER','SLA','LOA')
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',575);
    END IF;
    --
  END IF;
  --
  -- *********************************************************************
  --
  -- Check all the reference value columns
  --
  -- Account type
  --
  --
  IF (NOT s_fsc_ref_data.validate_codes('RAC_TYPE', p1.lrac_hrv_ate_code, 'Y'))
  THEN
    --
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',102);
    --
  END IF;
  --
  -- Class Code
  --
  -- Additional Validate on Class Code as removed the decode
  -- from the create. Must be in....
  -- 'REN', 'TSA', 'SPA', 'SLA', 'SUS', 'REP', 'MWO', 'LIA', 'SER', 'LOA'
  -- Also need to validate combination of REN and REN,
  -- SER and 'MWO', 'LIA', 'SER'
  -- the others are at sites discretion.
  --
  --
  IF (p1.lrac_class_code NOT IN ('REN', 'TSA', 'SPA', 'SLA', 'SUS'
                                , 'REP', 'MWO', 'LIA', 'SER', 'LOA')) 
  THEN
    --
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',217);
    --
  END IF;
  --
  --
  IF (    p1.lrac_hrv_ate_code = 'REN'
      AND p1.lrac_class_code  != 'REN') 
  THEN
    --
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',218);
    --
  END IF;
  --
  --
  IF (    p1.lrac_hrv_ate_code = 'SER'
      AND p1.lrac_class_code NOT IN ('SER', 'MWO', 'LIA')) 
  THEN
    --
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',219);
    --
  END IF;
  --
  -- *********************************************************************
  --
  -- Payment method
  --
  IF (p1.lrac_pme_pmy_code IS NOT NULL) 
  THEN
    --
    l_pmy_code    := NULL;
    l_pmy_reg_pay := NULL;
    --
     OPEN c_pmy_dets(p1.lrac_pme_pmy_code);
    FETCH c_pmy_dets into l_pmy_code,l_pmy_reg_pay;
    CLOSE c_pmy_dets;
    --
    IF (l_pmy_code IS NULL) 
    THEN
      --
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',103);
      --
    END IF;
    --
    -- If the payment method is by standing order or direct debit, check
    -- that a corresponding entry exists in PAYMENT_PROFILES
    --
    -- If the payment method is by a reg payment method, check
    -- that a corresponding entry exists in PAYMENT_PROFILES
    -- Amended to include the payment method start as we need
    -- to make sure the profile exists for the period of the method
    --
    IF (l_pmy_reg_pay ='Y') 
    THEN
      --
      l_year := NULL;
      l_ppr  := NULL;
      --
       OPEN c_year(p1.lrac_aun_code, p1.lrac_pme_start_date);
      FETCH c_year INTO l_year;
      CLOSE c_year;
      --
       OPEN c_ppr(p1.lrac_aun_code,p1.lrac_pme_hrv_ppc_code,l_year);
      FETCH c_ppr INTO l_ppr;
      CLOSE c_ppr;
      --
      IF (l_ppr IS NULL) 
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',104);
      END IF;
      --
      --
      -- *********************************************************************
      --
      --
      -- If a bank account number has been supplied, check that all the
      -- other bank details have been supplied, otherwise check that no
      -- bank details have been supplied.
      -- PJD removed validation of bad_start_date from following statement
      -- as it will now default to the pme_start_Date
      --
      -- Move the other regular pay checks to before the bank details
      -- as it's possible to supply a bank name and no branch name
      -- depending on system build. Therefore need to open this cursor
      -- first
      --
      -- *********************************************************************
      --
      -- OTHER REG PAY CHECKS  ADDED
      --
      -- Check the bank type is valid - if supplied
      --
      l_bty_code        := NULL;
      l_branch_mand     := NULL;
      --
      IF (p1.lrac_bde_bty_code IS NOT NULL) 
      THEN
        -- 
         OPEN c_bty_code(p1.lrac_bde_bty_code);
        FETCH c_bty_code into l_bty_code, l_branch_mand;
        --
        IF (c_bty_code%NOTFOUND) 
        THEN
          --
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',866); 
          --
        END IF;
        --
        CLOSE c_bty_code;
        --
      END IF;
      --
      IF (p1.lrac_bad_account_no IS NOT NULL) 
      THEN
        --
        IF l_branch_mand = 'MAN'
        THEN
          IF ((   p1.lrac_bad_account_name IS NULL 
               OR p1.lrac_bad_sort_code    IS NULL 
               OR p1.lrac_bde_branch_name  IS NULL 
               OR p1.lrac_bde_bank_name    IS NULL 
                OR p1.lrac_bde_bty_code    IS NULL )) 
          THEN
            --
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',075);
            -- 
          END IF;
          --
          -- added for mlang check
          --
          IF (     p1.lrac_bde_bank_name_mlang   IS NOT NULL
               AND p1.lrac_bde_branch_name_mlang IS NULL    ) 
          THEN
            --
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',109);
            -- 
          END IF;
          --
        ELSE
          --
          IF ((   p1.lrac_bad_account_name IS NULL 
               OR p1.lrac_bad_sort_code    IS NULL 
               OR p1.lrac_bde_bank_name    IS NULL 
               OR p1.lrac_bde_bty_code     IS NULL )) 
          THEN
            --
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',076);
            -- 
          END IF;
          --
        END IF;  /* l_branch_mand = 'MAN'  */
        --
      ELSE
        --
        IF ((   p1.lrac_bad_account_name   IS NOT NULL
             OR p1.lrac_bad_sort_code      IS NOT NULL
             OR p1.lrac_bad_start_date     IS NOT NULL
             OR p1.lrac_bde_branch_name    IS NOT NULL
             OR p1.lrac_bde_bank_name      IS NOT NULL
             OR p1.lrac_bde_bty_code       IS NOT NULL
             OR p1.lrac_aun_bad_account_no IS NOT NULL)) 
        THEN
          -- 
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',110);
          --
        END IF;
        --
      END IF;
      --
      -- *********************************************************************
      --
      -- Check the organisation bank account is valid
      --
      IF (p1.lrac_aun_bad_account_no IS NOT NULL) 
      THEN
        --
        l_exists := NULL;
        --
         OPEN c_aub_aun_code(p1.lrac_aun_bad_account_no,l_pro_refno);
        FETCH c_aub_aun_code INTO l_exists;
        CLOSE c_aub_aun_code;
        --
        IF (l_exists IS NULL) 
        THEN 
          --
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',867);
          --
        END IF;
        --
      ELSE
        --
        l_exists := NULL;
        --
         OPEN c_null_aub_aun_code(l_pro_refno);
        FETCH c_null_aub_aun_code INTO l_exists;
        CLOSE c_null_aub_aun_code;
        --
        IF (l_exists IS NULL) 
        THEN 
          --
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',867);
          --
        END IF;
        --
      END IF;
      --
      IF (p1.lrac_bad_par_per_alt_ref IS NOT NULL) 
      THEN
        --
        l_pay_par_refno := NULL;
        --
        -- Check the payment par_per_alt_ref is valid (if supplied)
        --
        IF (nvl(p1.lrac_par_org_ind,'P') = 'P' ) 
        THEN   -- Person
          --
           OPEN  c_pay_par_refno(p1.lrac_bad_par_per_alt_ref);
          FETCH c_pay_par_refno into l_pay_par_refno;
          CLOSE c_pay_par_refno;
          --
          IF (l_pay_par_refno IS NULL) 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',868);
          END IF;
          --
        ELSE   -- organisation
          --
           OPEN  c_pay_org_refno(p1.lrac_bad_par_per_alt_ref);
          FETCH c_pay_org_refno into l_pay_par_refno;
          CLOSE c_pay_org_refno;
          --
          IF (l_pay_par_refno IS NULL) 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',868);
          END IF; -- par_refno is null
          --
        END IF; -- organisation ind
        --
      END IF; -- bad par per alt ref is not null
      --
    END IF; -- regular payment
    --
  END IF; -- has payment method info been supplied.
  --
  --
  --
  -- *********************************************************************
  --   
  --
  -- Check payment method start date is not before account start date
  -- Added new NVL clause to ensure NULL pme_start_date will fail
  -- Added in if payment method supplied
  --
  IF p1.lrac_pme_pmy_code IS NOT NULL
  THEN
    IF (nvl(p1.lrac_pme_start_date,p1.lrac_start_date -1) < p1.lrac_start_date)
    THEN
      --
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',105);
      --
    END IF;
  END IF;
  --
  -- *********************************************************************
  --
  -- Check account end date not before account start date
  --
  IF (p1.lrac_end_date IS NOT NULL AND p1.lrac_end_date < p1.lrac_start_date) 
  THEN
    --
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',106);
    --
  END IF;
  --
  --
  -- *********************************************************************
  --
  -- Check last statement date not in the future
  --
  --  IF (p1.lrac_statement_to_date > sysdate)
  --  THEN
  --    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',107);
  --  END IF;
  --
  --
  -- *********************************************************************
  --
  --
  -- Check last statement date is not before account start date
  --
  IF (p1.lrac_statement_to_date < p1.lrac_start_date) 
  THEN
    --
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',108);
    --
  END IF;
  --
  --
  -- *********************************************************************
  --
  --
  -- For non 'NCT' accounts
  -- Check a tenancy exists for this account and, if so, check the
  -- account does not start before the tenancy and the report property
  -- reference exists on TCY_HOLDINGS . Check report_propref matches
  -- rents admin unit.
  -- Amended 11/08/03 P Hearty. Amended as account type SER will not
  -- be linked to a tenancy therefore this validate needed changing.
  -- Amended 12/01/05 P Hearty, excluded MWO accounts from this check.
  -- Amended 04/11/09 P Hearty, runs off class code not ate_code
  --
  --
  IF (    p1.lrac_report_pro_refno != '~NCT~'
      AND p1.lrac_class_code not in ( 'LIA','SLA', 'SER' , 'MWO' )
     )
  THEN
    --
    l_tcy_refno := s_tenancies2.get_refno_from_alt_ref(p1.lrac_tcy_alt_ref);
    --
    IF (l_tcy_refno IS NOT NULL) 
    THEN
      --
      IF (p1.lrac_start_date < s_tenancies.get_actual_start_date(l_tcy_refno)) 
      THEN
        --
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',112);
        --
      END IF;
      --
      --
      IF (NOT s_properties.chk_prop_in_rents_au(l_pro_refno)) 
      THEN
        --
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',124);
        --
      END IF;
      --
      l_dummy := NULL;
      --
      OPEN c_is_pro_refno_in_tenancy( l_tcy_refno,l_pro_refno
                                    , p1.lrac_start_date);
      FETCH c_is_pro_refno_in_tenancy INTO l_dummy;
      CLOSE  c_is_pro_refno_in_tenancy;
      --
      IF (l_dummy IS NULL) 
      THEN
        --
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',113);
        --    
      END IF;
      --
    ELSE
      --
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',111);
      --
    END IF;
    --
  ELSIF p1.lrac_class_code   in ( 'LIA','SLA','SER', 'MWO' ) 
  THEN
    --
    -- Make sure the property exists for the lease prop ref supplied
    --
    --
    l_lea_pro_refno := NULL;
    --
     OPEN c_get_lea_pro_refno(p1.lrac_las_lea_pro_propref );
    FETCH c_get_lea_pro_refno INTO l_lea_pro_refno;
    --
    IF (c_get_lea_pro_refno%NOTFOUND) 
    THEN
      --
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',222);
      --
    END IF;
    --
    CLOSE c_get_lea_pro_refno;
    --
    --
    -- Check to see that there is a valid Lease Assignment. This is to overcome
    --  constraint failure during the CREATE process for RAC_LAS_FK
    -- Altered the following to only apply if lrac_las_start_date has been 
    -- supplied to allow for the introduction of the LIALEALAS parameter.
    --
    --
    IF (    l_lea_pro_refno IS NOT NULL
        AND p1.lrac_las_start_date IS NOT NULL) 
    THEN
      --
      l_las_exists := NULL;

       OPEN chk_las_exists(l_lea_pro_refno
                         ,p1.lrac_las_lea_start_date, p1.lrac_las_start_date);
      FETCH chk_las_exists INTO l_las_exists;
      CLOSE chk_las_exists;
      --
      IF (l_las_exists IS NULL) 
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',249);
      END IF;
      --
    END IF; -- l_lea_pro_refno IS NOT NULL
    --
    -- Check lrac_las_start_date and value of system parameter LIALEALAS.
    -- If it's LEASE then lrac_las_start_date should be null
    -- If it's ASSIGNMENT then lrac_las_start_date should be populated
    --
    -- Get LIALEALAS system parameter
    -- 
    l_lialealas := NULL;
    --
    l_lialealas := fsc_utils.get_sys_param('LIALEALAS');
    --
    IF (    l_lialealas = 'LEASE'
        AND p1.lrac_las_start_date IS NOT NULL
       )
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',523);
      --
    ELSIF (    l_lialealas = 'ASSIGNMENT'
           AND p1.lrac_las_start_date IS NULL)
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',524);
    END IF;
    --
  ELSE
    --
    l_par_refno := null;
    --
     OPEN c_par_refno(p1.lrac_par_alt_ref);
    FETCH c_par_refno into l_par_refno;
    CLOSE c_par_refno;
    --
    IF (l_par_refno IS NULL) 
    THEN
      --
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',077);
      --
    END IF;
    --
  END IF;
  --
  --
  --If ~NCT~ Account then lrac_par_alt_ref should be populated with pers alt ref
  --
  --
  l_par_refno := null;
  --
  IF(p1.lrac_report_pro_refno = '~NCT~') 
  THEN
    --
     OPEN c_par_refno(p1.lrac_par_alt_ref);
    FETCH c_par_refno into l_par_refno;
    CLOSE c_par_refno;
    --
    IF (l_par_refno IS NULL) 
    THEN
      --
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',243);
      --
    END IF;
    --
  END IF;
  --
  -- Check the debit to date is a valid admin period end date
  --
  /*
  IF (NOT s_admin_periods.f_exists_ape_end(p1.lrac_aun_code,'DEB'
                                          ,p1.lrac_debit_to_date))
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',114);
  END IF;
  --
  */
  --
  --
  -- New validation... For every account the prop ref should be
  -- be populated either with a value or with ~NCT~
  --
  IF (p1.lrac_report_pro_refno IS NULL) 
  THEN
    --
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',221);
    --
  END IF;
  --
  -- New validation.. for current accounts check it's in same REN
  -- admin unit as Parent Property. For Leaseholders check it's
  -- in the same SER admin unit
  --
  --
  l_pro_ren_au := NULL;
  l_pro_ser_au := NULL;
  --
  IF (p1.lrac_end_date IS NULL) 
  THEN
    --
    -- process the REN AU for the REN TSA and REP Accounts first
    --
    IF p1.lrac_class_code IN ('REN', 'TSA', 'REP') 
    THEN
      --
       OPEN c_pro_ren(l_pro_refno);
      FETCH c_pro_ren INTO l_pro_ren_au;
      --
      IF nvl(l_pro_ren_au, '~#A') != nvl(p1.lrac_aun_code, 'Z~#') 
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',247);
      END IF;
      --
      CLOSE c_pro_ren;
      --
      -- Process the SER AU for the Leaseholder Accounts
      --
    ELSIF p1.lrac_class_code IN ('SLA', 'MWO', 'LIA', 'SER') 
    THEN
      --
       OPEN c_pro_ser(p1.lrac_las_lea_pro_propref );
      FETCH c_pro_ser INTO l_pro_ser_au;
      --
      IF nvl(l_pro_ser_au, '~#A') != nvl(p1.lrac_aun_code,  'Z~#') 
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',248);
      END IF;
      --
    CLOSE c_pro_ser;
    --
    END IF;  -- lrac_class_code
    --
  END IF;    -- lrac_end_date is null
  --
  -- check lrac_res_ind is Y or N if supplied
  --
  IF (p1.lrac_res_ind IS NOT NULL)
   THEN
    IF (p1.lrac_res_ind NOT IN ('Y','N'))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',574);
    END IF;
  END IF;
  --
  --
  --
  --
  -- Now UPDATE the record count AND error code
  --
  IF l_errors = 'F' 
  THEN
    l_error_ind := 'Y';
  ELSE
    l_error_ind := 'N';
  END IF;
  --
  --
  -- keep a count of the rows processed and commit after every 1000
  --
  i := i+1; 
  --
  IF MOD(i,1000)=0 
  THEN 
    COMMIT; 
  END IF;
  --
  s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind); 
  set_record_status_flag(l_id,l_errors);
  --
  EXCEPTION
  WHEN OTHERS 
  THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
    set_record_status_flag(l_id,'O');
    --
END;
--
END LOOP;
--
COMMIT;
--
fsc_utils.proc_END;
--
EXCEPTION
WHEN OTHERS 
THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
-- ****************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 
IS
SELECT a1.rowid rec_rowid,
       a1.lrac_dlb_batch_id,
       a1.lrac_dl_seqno,
       a1.lrac_dl_load_status,
       p1.rac_accno,
       a1.lrac_pay_ref,
       a1.lrac_par_org_ind,
       a1.lrac_bad_par_per_alt_ref,
       a1.lrac_pme_pmy_code,
       a1.lrac_pme_start_date
  FROM revenue_accounts p1,
       dl_hra_revenue_accounts a1
 WHERE p1.rac_pay_ref      = a1.lrac_pay_ref
   AND lrac_dlb_batch_id   = p_batch_id
   AND lrac_dl_load_status = 'C';
--
--
-- *********************************************************************
--
CURSOR c_rac_details(p_pay_ref  VARCHAR2) 
IS
SELECT rac_accno,
       rac_aun_code,
       rac_class_code,
       rac_par_refno
  FROM revenue_accounts
 WHERE rac_pay_ref = p_pay_ref;
--
--
-- *********************************************************************
--
CURSOR c_par_refno(l_par_alt_ref        VARCHAR2)
IS
SELECT par_refno
FROM   parties
WHERE  par_per_alt_ref = l_par_alt_ref;
--
--
-- *********************************************************************
--
CURSOR c_org_refno(l_par_alt_ref        VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_com_short_name = l_par_alt_ref
    OR par_org_short_name = l_par_alt_ref;
--
--
-- *********************************************************************
--
CURSOR c_get_pme_refno (p_rac_accno     NUMBER,
                        p_pmy_code      VARCHAR2,
                        p_start_date    DATE)
IS
SELECT pme_refno
  FROM payment_methods
 WHERE pme_rac_accno  = p_rac_accno
   AND pme_pmy_code   = p_pmy_code
   AND pme_start_date = p_start_date;
--
--
-- *********************************************************************
--
CURSOR c_get_pba_details (l_par_refno   NUMBER, 
                          l_pme_refno   NUMBER)
IS
SELECT pbp_pba_bad_refno
  FROM party_bank_acct_pymt_mthds
 WHERE pbp_pba_par_refno = l_par_refno
   AND pbp_pme_refno     = l_pme_refno;
--
--
-- *********************************************************************
--
CURSOR c_get_tenant (p_rac_accno        NUMBER) 
IS
SELECT hop_par_refno
  FROM household_persons,  
       tenancy_instances, 
       revenue_accounts
 WHERE hop_refno     = tin_hop_refno
   AND tin_tcy_refno = rac_tcy_refno
   AND rac_accno     = p_rac_accno
 ORDER BY nvl(tin_end_date,sysdate) desc,
          tin_start_date, 
          tin_hop_refno;
--
--
-- *********************************************************************
--
-- Constants for process_summary
--
--
cb              VARCHAR2(30);
cd              DATE;
cp              VARCHAR2(30) := 'DELETE';
ct              VARCHAR2(30) := 'DL_HRA_REVENUE_ACCOUNTS';
cs              PLS_INTEGER;
ce              VARCHAR2(200);
l_id            ROWID;
--
-- Other variables
--
l_rac_accno     revenue_accounts.rac_accno%TYPE;
l_par_refno     parties.par_refno%TYPE;
l_pme_refno     payment_methods.pme_refno%TYPE;
l_bad_refno     bank_details.bde_refno%TYPE;
l_rac_aun_code  revenue_accounts.rac_aun_code%TYPE;
l_class_code    revenue_accounts.rac_class_code%TYPE;
i               PLS_INTEGER := 0;
l_an_tab        VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_revenue_accounts.dataload_delete');
fsc_utils.debug_message( 's_dl_hra_revenue_accounts.dataload_delete',3 );
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
cs := p1.lrac_dl_seqno;
l_id := p1.rec_rowid;
SAVEPOINT SP1;
i := i +1;
--
--
 OPEN c_rac_details(p1.lrac_pay_ref);
FETCH c_rac_details into l_rac_accno,l_rac_aun_code,l_class_code,l_par_refno;
CLOSE c_rac_details;
-- 
-- Now need to get the Par refno
--
IF (p1.lrac_bad_par_per_alt_ref IS NULL) 
THEN
  --
  -- is it a sundry party account - in which case use that party
  --
  IF (l_par_refno IS NULL) 
  THEN
    --
     OPEN c_get_tenant(l_rac_accno);
    FETCH c_get_tenant into l_par_refno;
    CLOSE c_get_tenant;
    --
  END IF;
  --
ELSE
  --
  -- If a party has been supplied then...-- is it a Person
  --
  IF (NVL(p1.lrac_par_org_ind,'P') = 'P') 
  THEN
    --
     OPEN c_par_refno(p1.lrac_bad_par_per_alt_ref);
    FETCH c_par_refno INTO l_par_refno;
    CLOSE c_par_refno;
    --
  ELSE
    --
    -- or else it must be an organisation
    --
     OPEN c_org_refno(p1.lrac_bad_par_per_alt_ref);
    FETCH c_org_refno INTO l_par_refno;
    CLOSE c_org_refno;
    --
  END IF;
  --
END IF; -- had the bad_par_per_alt_ref been supplied  
--
-- Get the PME refno
--
l_pme_refno := NULL;
--      
IF p1.lrac_pme_pmy_code IS NOT NULL
THEN
   OPEN c_get_pme_refno(l_rac_accno,p1.lrac_pme_pmy_code
                       ,p1.lrac_pme_start_date);
  FETCH c_get_pme_refno into l_pme_refno;
  CLOSE c_get_pme_refno;
END IF;
--
-- Now Get the pba details
--
l_bad_refno := NULL;
--
 OPEN c_get_pba_details(l_par_refno,l_pme_refno); 
FETCH c_get_pba_details into l_bad_refno;
CLOSE c_get_pba_details;
--
IF l_bad_refno IS NOT NULL
THEN
  DELETE 
   FROM party_bank_acct_pymt_mthds
  WHERE pbp_pba_par_refno  = l_par_refno
    AND pbp_pba_bad_refno  = l_bad_refno
    AND pbp_pme_refno      = l_pme_refno
    AND pbp_pba_start_date = p1.lrac_pme_start_date;
END IF;
--
DELETE 
  FROM payment_contracts
 WHERE  pct_rac_accno  = l_rac_accno
   AND  pct_par_refno  = l_par_refno
   AND  pct_start_date = p1.lrac_pme_start_date;
--
IF l_pme_refno IS NOT NULL
THEN
  DELETE 
    FROM payment_methods
   WHERE pme_refno = l_pme_refno;
END IF;
--
DELETE 
  FROM summary_rac_accounts
 WHERE sra_rac_accno = l_rac_accno;
--
DELETE 
  FROM nct_aun_links
 WHERE nal_rac_accno = l_rac_accno;
--
DELETE 
  FROM summary_rents
 WHERE sre_rac_accno = l_rac_accno;
--
UPDATE tenancy_holdings
   SET tho_rac_accno = NULL
 WHERE tho_rac_accno = l_rac_accno;
--
DELETE 
  FROM revenue_accounts
 WHERE rac_accno = l_rac_accno;
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; 
--
IF MOD(i,1000)=0 
THEN 
  COMMIT; 
END IF;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'V');
--
EXCEPTION
WHEN OTHERS 
THEN
  ROLLBACK TO SP1;
  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
  set_record_status_flag(l_id,'C');
  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
--
END LOOP;
--
-- Now tidy up the bank account details and bank details tables
--
DELETE 
  FROM aa_party_bank_accounts
 WHERE EXISTS (SELECT NULL
               FROM party_bank_accounts
               WHERE apb_pba_par_refno  = pba_par_refno
                 AND apb_pba_bad_refno  = pba_bad_refno
                 AND apb_pba_start_date = pba_start_date
                 AND NOT EXISTS (SELECT NULL 
                                 FROM party_bank_acct_pymt_mthds
                                 WHERE pbp_pba_par_refno  = pba_par_refno
                                   AND pbp_pba_bad_refno  = pba_bad_refno
                                   AND pbp_pba_start_date = pba_start_date));
--
DELETE 
  FROM ddi_accounts
 WHERE EXISTS (SELECT NULL
               FROM party_bank_accounts
               WHERE dda_pba_par_refno  = pba_par_refno
                 AND dda_pba_bad_refno  = pba_bad_refno
                 AND dda_pba_start_date = pba_start_date
                 AND NOT EXISTS (SELECT NULL 
                                 FROM party_bank_acct_pymt_mthds
                                 WHERE pbp_pba_par_refno  = pba_par_refno
                                   AND pbp_pba_bad_refno  = pba_bad_refno
                                   AND pbp_pba_start_date = pba_start_date));
--
DELETE 
  FROM party_bank_accounts
 WHERE NOT EXISTS (SELECT NULL 
                   FROM party_bank_acct_pymt_mthds
                   WHERE pbp_pba_par_refno  = pba_par_refno
                     AND pbp_pba_bad_refno  = pba_bad_refno
                     AND pbp_pba_start_date = pba_start_date);
--
DELETE 
  FROM bank_account_details
 WHERE NOT EXISTS (SELECT NULL 
                   FROM party_bank_accounts
                   WHERE pba_bad_refno = bad_refno)
                     AND   bad_type = 'CUS';
--
DELETE 
  FROM bank_details
 WHERE NOT EXISTS (SELECT NULL 
                   FROM bank_account_details
                   WHERE bad_bde_refno = bde_refno);
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('REVENUE_ACCOUNTS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANCY_HOLDINGS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PAYMENT_METHODS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('BANK_DETAILS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('BANK_ACCOUNT_DETAILS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUMMARY_RAC_ACCOUNTS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('NCT_AUN_LINK');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PAYMENT_CONTRACTS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PARTY_BANK_ACCT_PYMT_MTHDS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PARTY_BANK_ACCOUNTS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PM_DESTINATION_ACCOUNTS');
--
--
    fsc_utils.proc_end;
    commit;
--
    EXCEPTION
         WHEN OTHERS THEN
         ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
         set_record_status_flag(l_id,'O');
         s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_delete;
--
--
END s_dl_hra_revenue_accounts;
/
