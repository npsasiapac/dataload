--
CREATE OR REPLACE PACKAGE BODY s_dl_hra_subsidy_reviews
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    VS   06-JAN-2009  Initial Creation.
--  1.1     5.15.0    VS   05-MAY-2009  Disable trigger SURV_BR_IU on
--                                      CREATE process to stop the supplied
--                                      created_by and created_date from
--                                      being over written.
--  1.1     5.15.0    VS   18-MAY-2009  Use the supplied Application Legacy
--                                      Reference and Subsidy Review Legacy
--                                      Reference to populate Primary/Foreign
--                                      key fields.
--                                      Addition of LSURV_ASSESSED_SELB_CODE
--                                      for HHOLD Threshold Income
--  1.2     5.16.0    IR   21-SEP-2009  Added LSURV_ACHO_LEGACY_REF for
--                                      SAS data
--  1.3     5.16.0    IR   29-SEP-2009  Amended validation on tenancy reference
--                                      and subsidy application.
--  1.4     5.16.0    VS   20-OCT-2009  Amended Code for Group Subsidy Review Change--
--  1.5     5.16.0    VS   21-OCT-2009  Added status Code 'INA' for Subsidy Review
--                                      validation.
--  1.6     5.16.0    MT   25-NOV-2009  Revised validation on tenancy class
--                                      from "SAS" to "PRS" to align with check
--                                      constraint.  MQC2684
--  1.7     5.16.0    VS   27-SEP-2010  Added order by clause in the CREATE process
--                                      so that the highest Subsidy Review in subsidy
--                                      application is flagged as current -
--                                      ORDER BY  ORDER BY LSURV_SUAP_LEGACY_REF ASC,
--                                                         LSURV_LEGACY_REF ASC;
--                                      MQC2536
--  1.8     6.4.0     PH   29-JUN-2011 Amended the code so that we use the
--                                     subsidy review sequence for surv_seqno
--                                     not the user ref supplied. Also need to
--                                     go to dl table for application to get the
--                                     suap_refno using alt ref as this is not
--                                     currently held on main table.
--                                     Removed disable/enable of trigger and
--                                     perform a post insert update instead
--                                     Added new fields.
--  1.9     6.5.1     MT   01-NOV-2011 Add logic for new columns CAP_TYPE and
--                                     AMOUNT as per spec V20.
--  2.0     6.12.0    AJ  21-AUG-2015  Amended error codes 493(now 813) 494(now 814)
--                                     and 495(now 815) as numbers already used in
--                                     standard current version of hd2_errs_in.sql
--  2.1     6.14      AJ  27=SEP-2017  Amended lsurv_subp_asca_code so that it
--                                     checks assessment_categories, and not 
--                                     the FRV change was at v6.8 error amended from
--                                     50 to 82 in hd2 errors file
--  2.3     6.9/14    AJ  11-OCT-2017  1)Reinstated changes added in the LSURV_PAY_MARKET_RENT_IND
--                                     2)Main table (subsidy reviews) now holds the legacy reference
--                                       amended code to insert this value and also validate that its unique.
--                                     3)Subsidy Applications Legacy Ref now held in subsidy applications
--                                       table also therefore add call but kept call to dl table just
--                                       in case saup_legacy_ref has not been loaded
--                                     4)Amended order by in create process, now use lsurv_dl_seqno and 
--                                       updated document to advise sites to put records in
--                                       correct order within data file.
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
  UPDATE dl_hra_subsidy_reviews
  SET    lsurv_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_subsidy_review');
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
       lsurv_dlb_batch_id,
       lsurv_dl_seqno,
       lsurv_dl_load_status,
       lsurv_suap_legacy_ref,
       lsurv_legacy_ref,
       lsurv_tcy_app_alt_ref,
       lsurv_suap_start_date,
       lsurv_class_code,
       lsurv_effective_date,
       lsurv_assessment_date,
       lsurv_den_eligible_ind,
       lsurv_subp_asca_code,
       lsurv_subp_seq,
       lsurv_sco_code,
       lsurv_hsrr_code,
       nvl(lsurv_created_date, sysdate)    lsurv_created_date,
       nvl(lsurv_created_by, 'DATALOAD')   lsurv_created_by,
       lsurv_end_date,
       lsurv_authorised_date,
       lsurv_authorised_by,
       lsurv_default_percentage,
       lsurv_den_sub_unrounded_amt,
       lsurv_den_hhold_assess_inc,
       lsurv_den_subsidy_amount,
       lsurv_den_calc_rent_payable,
       lsurv_den_tcy_market_rent,
       lsurv_assessed_selb_code,
       lsurv_hscr_code,
       lsurv_acho_legacy_ref,
       lsurv_grsr_user_reference,
       lsurv_refno,
       lsurv_hsrs_code,
       lsurv_hty_code,
       lsurv_review_initiated_date,
       lsurv_details_received_date,
       lsurv_cap_amount,
       lsurv_cap_type,
       lsurv_suda_legacy_ref,
       DECODE(lsurv_pay_market_rent_ind,'Y','Y','N') lsurv_pay_market_rent_ind
  FROM dl_hra_subsidy_reviews
 WHERE lsurv_dlb_batch_id    = p_batch_id
   AND lsurv_dl_load_status  = 'V'
 ORDER BY lsurv_dl_seqno;
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR get_acho_ref(p_acho_ref   VARCHAR2)
IS
SELECT acho_reference
  FROM advice_case_housing_options
 WHERE acho_alternative_reference = p_acho_ref;
--
-- ************************************************************************
--
-- get Group Subsidy Reviews Refno
--
CURSOR get_grsr_refno(p_grsr_user_reference  VARCHAR2)
IS
SELECT grsr_refno
  FROM group_subsidy_reviews
 WHERE grsr_user_reference = p_grsr_user_reference;
--
-- ************************************************************************
--
-- get subsidy review Refno, held on the DL table
--
CURSOR get_suap_refno(p_suap_legagcy_ref  VARCHAR2)
IS
SELECT suap_reference
  FROM subsidy_applications
      ,dl_hra_subsidy_applications
 WHERE suap_reference   = lsuap_reference
   AND lsuap_legacy_ref = p_suap_legagcy_ref;
--
-- get subsidy applications refno from applications table
-- as now held on table
--
CURSOR get_suap_refno2(p_suap_legagcy_ref  VARCHAR2)
IS
SELECT suap_reference
  FROM subsidy_applications
 WHERE suap_legacy_ref = p_suap_legagcy_ref;
--
--
CURSOR get_suda_refno(p_suda_legacy_ref  VARCHAR2)
IS
SELECT suda_refno
  FROM subsidy_debt_assessments
      ,dl_hra_subsidy_debt_assmnts
 WHERE suda_refno       = lsuda_refno
   AND lsuda_legacy_ref = p_suda_legacy_ref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_SUBSIDY_REVIEWS';
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
i                          INTEGER := 0;
l_acho_reference           NUMBER(10);
l_grsr_refno               NUMBER(10);
l_suap_reference           NUMBER(10);
l_suda_refno               NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_subsidy_reviews.dataload_create');
    fsc_utils.debug_message('s_dl_hra_subsidy_reviews.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lsurv_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
--
          l_acho_reference := NULL;
          l_suap_reference := NULL;
          l_grsr_refno     := NULL;
          l_suda_refno     := NULL;
--
          IF (p1.lsurv_acho_legacy_ref IS NOT NULL) THEN
--
            OPEN get_acho_ref(p1.lsurv_acho_legacy_ref);
           FETCH get_acho_ref into l_acho_reference;
           CLOSE get_acho_ref;
--
          END IF;
--
          IF (p1.lsurv_grsr_user_reference IS NOT NULL) THEN
--
            OPEN get_grsr_refno(p1.lsurv_grsr_user_reference);
           FETCH get_grsr_refno into l_grsr_refno;
           CLOSE get_grsr_refno;
--
          END IF;
--
-- get the survey application ref subsidy_applications table
--
            OPEN get_suap_refno2(p1.lsurv_suap_legacy_ref);
           FETCH get_suap_refno2 into l_suap_reference;
           CLOSE get_suap_refno2;
--
-- get the survey application ref from dl_hat_subsidy_applications table
-- if suap_legacy_ref has not been loaded into subsidy applications record
--
        IF l_suap_reference IS NULL
         THEN
--		 
            OPEN get_suap_refno(p1.lsurv_suap_legacy_ref);
           FETCH get_suap_refno into l_suap_reference;
           CLOSE get_suap_refno;
--
        END IF;
--
-- subsidy_debt_assessments ref
--
            OPEN get_suda_refno(p1.lsurv_suda_legacy_ref);
           FETCH get_suda_refno into l_suda_refno;
           CLOSE get_suda_refno;
--
-- Insert int relevent table
--
          INSERT /* +APPEND */ INTO subsidy_reviews(surv_refno,
                                      surv_class_code,
                                      surv_suap_reference,
                                      surv_reusable_refno,
                                      surv_effective_date,
                                      surv_assessment_date,
                                      surv_den_eligible_ind,
                                      surv_subp_hrv_asca_code,
                                      surv_subp_seq,
                                      surv_sco_code,
                                      surv_hsrr_code,
                                      surv_created_date,
                                      surv_created_by,
                                      surv_end_date,
                                      surv_authorised_date,
                                      surv_authorised_by,
                                      surv_default_percentage,
                                      surv_den_sub_unrounded_amt,
                                      surv_den_hhold_assess_inc,
                                      surv_den_subsidy_amount,
                                      surv_den_calc_rent_payable,
                                      surv_grsr_refno,
                                      surv_den_tcy_market_rent,
                                      surv_assessed_selb_code,
                                      surv_hscr_code,
                                      surv_acho_reference,
                                      surv_hsrs_code,
                                      surv_hty_code,
                                      surv_review_initiated_date,
                                      surv_details_received_date,
                                      surv_cap_amount,
                                      surv_cap_type,
                                      surv_suda_refno,
                                      surv_legacy_ref,
                                      surv_pay_market_rent_ind
                                     )
                              VALUES(p1.lsurv_refno,
                                     p1.lsurv_class_code,
                                     l_suap_reference,
                                     reusable_refno_seq.nextval,
                                     p1.lsurv_effective_date,
                                     p1.lsurv_assessment_date,
                                     p1.lsurv_den_eligible_ind,
                                     p1.lsurv_subp_asca_code,
                                     p1.lsurv_subp_seq,
                                     p1.lsurv_sco_code,
                                     p1.lsurv_hsrr_code,
                                     p1.lsurv_created_date,
                                     p1.lsurv_created_by,
                                     p1.lsurv_end_date,
                                     p1.lsurv_authorised_date,
                                     p1.lsurv_authorised_by,
                                     p1.lsurv_default_percentage,
                                     p1.lsurv_den_sub_unrounded_amt,
                                     p1.lsurv_den_hhold_assess_inc,
                                     p1.lsurv_den_subsidy_amount,
                                     p1.lsurv_den_calc_rent_payable,
                                     l_grsr_refno,
                                     p1.lsurv_den_tcy_market_rent,
                                     p1.lsurv_assessed_selb_code,
                                     p1.lsurv_hscr_code,
                                     l_acho_reference,
                                     p1.lsurv_hsrs_code,
                                     p1.lsurv_hty_code,
                                     p1.lsurv_review_initiated_date,
                                     p1.lsurv_details_received_date,
                                     p1.lsurv_cap_amount,
                                     p1.lsurv_cap_type,
                                     l_suda_refno,
                                     p1.lsurv_legacy_ref,
                                     p1.lsurv_pay_market_rent_ind
                                    );
--
-- Now update the record to set the correct created by and created date
-- to overcome the trigger
--
         UPDATE   subsidy_reviews
            SET   surv_created_date = p1.lsurv_created_date
                , surv_created_by   = p1.lsurv_created_by
          WHERE   surv_refno        = p1.lsurv_refno;
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
  COMMIT;
--
-- ***********************************************************************
--
-- Section to analyse the table(s) populated by this dataload
--
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUBSIDY_REVIEWS');
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
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
                            p_date              IN DATE)
AS
--
CURSOR c1
IS
SELECT rowid rec_rowid,
       lsurv_dlb_batch_id,
       lsurv_dl_seqno,
       lsurv_dl_load_status,
       lsurv_suap_legacy_ref,
       lsurv_legacy_ref,
       lsurv_tcy_app_alt_ref,
       lsurv_suap_start_date,
       lsurv_class_code,
       lsurv_effective_date,
       lsurv_assessment_date,
       lsurv_den_eligible_ind,
       lsurv_subp_asca_code,
       lsurv_subp_seq,
       lsurv_sco_code,
       lsurv_hsrr_code,
       lsurv_created_date,
       lsurv_created_by,
       lsurv_end_date,
       lsurv_authorised_date,
       lsurv_authorised_by,
       lsurv_default_percentage,
       lsurv_den_sub_unrounded_amt,
       lsurv_den_hhold_assess_inc,
       lsurv_den_calc_rent_payable,
       lsurv_den_tcy_market_rent,
       lsurv_assessed_selb_code,
       lsurv_hscr_code,
       decode(TO_CHAR(lsurv_effective_date,'fmDAY'),'MONDAY',   'MON',
                                                    'TUESDAY',  'TUE',
                                                    'WEDNESDAY','WED',
                                                    'THURSDAY', 'THU',
                                                    'FRIDAY',   'FRI',
                                                    'SATURDAY', 'SAT',
                                                    'SUNDAY',   'SUN') lsurv_effective_start_day,
       lsurv_acho_legacy_ref,
       lsurv_grsr_user_reference,
       lsurv_hsrs_code,
       lsurv_hty_code,
       lsurv_review_initiated_date,
       lsurv_details_received_date,
       lsurv_cap_amount,
       lsurv_cap_type,
       lsurv_suda_legacy_ref,
       DECODE(lsurv_pay_market_rent_ind,'Y','Y','N') lsurv_pay_market_rent_ind
  FROM dl_hra_subsidy_reviews
 WHERE lsurv_dlb_batch_id    = p_batch_id
   AND lsurv_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR chk_tcy_exists( p_tcy_alt_ref      VARCHAR2
                     , p_suap_legacy_ref  VARCHAR2
                     , p_suap_start_date  DATE )
IS
SELECT tcy_refno
  FROM tenancies
      ,subsidy_applications
      ,dl_hra_subsidy_applications
 WHERE suap_reference   = lsuap_reference
   AND lsuap_legacy_ref = p_suap_legacy_ref
   AND suap_start_date  = p_suap_start_date
   AND tcy_alt_ref      = p_tcy_alt_ref
   AND tcy_refno        = suap_tcy_refno;
--
-- ***********************************************************************
--
CURSOR chk_tcy_exists2( p_tcy_alt_ref      VARCHAR2
                      , p_suap_legacy_ref  VARCHAR2
                      , p_suap_start_date  DATE )
IS
SELECT tcy_refno
  FROM tenancies
      ,subsidy_applications
 WHERE suap_legacy_ref  = p_suap_legacy_ref
   AND suap_start_date  = p_suap_start_date
   AND tcy_alt_ref      = p_tcy_alt_ref
   AND tcy_refno        = suap_tcy_refno;
--
-- ***********************************************************************
--
CURSOR chk_app_exists( p_app_legacy_ref   VARCHAR2
                     , p_suap_legacy_ref  VARCHAR2
                     , p_suap_start_date  DATE )
IS
SELECT app_refno
  FROM applications
      ,subsidy_applications
      ,dl_hra_subsidy_applications
 WHERE suap_reference   = lsuap_reference
   AND lsuap_legacy_ref = p_suap_legacy_ref
   AND suap_start_date  = p_suap_start_date
   AND app_legacy_ref   = p_app_legacy_ref
   AND app_refno        = suap_app_refno;
--
--
-- ***********************************************************************
--
CURSOR chk_app_exists2( p_app_legacy_ref   VARCHAR2
                      , p_suap_legacy_ref  VARCHAR2
                      , p_suap_start_date  DATE )
IS
SELECT app_refno
  FROM applications
      ,subsidy_applications
 WHERE suap_legacy_ref  = p_suap_legacy_ref
   AND suap_start_date  = p_suap_start_date
   AND app_legacy_ref   = p_app_legacy_ref
   AND app_refno        = suap_app_refno;
--
--
-- ***********************************************************************
--
--
CURSOR chk_surv_exists(p_surv_legacy_ref    VARCHAR2)
IS
SELECT 'X'
  FROM subsidy_reviews
      ,dl_hra_subsidy_reviews
 WHERE surv_refno       = lsurv_refno
   AND lsurv_legacy_ref = p_surv_legacy_ref;
--
-- ***********************************************************************
--
--
CURSOR chk_surv_exists2(p_surv_legacy_ref    VARCHAR2)
IS
SELECT 'X'
  FROM subsidy_reviews
 WHERE surv_legacy_ref = p_surv_legacy_ref;
--
-- ***********************************************************************
--
CURSOR get_suda_refno(p_suda_legacy_ref  VARCHAR2)
IS
SELECT suda_refno
  FROM subsidy_debt_assessments
      ,dl_hra_subsidy_debt_assmnts
 WHERE suda_refno       = lsuda_refno
   AND lsuda_legacy_ref = p_suda_legacy_ref;
--
-- ***********************************************************************
--
--
CURSOR get_admin_year_dets(p_tcy_refno       NUMBER,
                           p_suap_start_date DATE)
IS
SELECT b.aye_rent_week_start
  FROM revenue_accounts a,
       admin_years      b
 WHERE a.rac_tcy_refno = p_tcy_refno
   AND SYSDATE BETWEEN a.rac_start_date
                   AND NVL(a.rac_end_date,SYSDATE + 1)
   AND b.aye_aun_code  = a.rac_aun_code
   AND p_suap_start_date BETWEEN b.aye_start_date
                             AND b.aye_end_date;
--
-- ***********************************************************************
--
-- NB Does this require the effective date passing in as well?????
--
CURSOR chk_pol_cat_exists(p_subp_hrv_asca_code VARCHAR2,
                          p_subp_seq           NUMBER)
IS
SELECT 'X'
  FROM subsidy_policies
 WHERE subp_hrv_asca_code  = p_subp_hrv_asca_code
   AND subp_seq            = p_subp_seq
   AND subp_authorised_ind = 'Y';
--
-- ************************************************************************
--
-- Check Advice Case Housing Option exists where legcy reference is supplied
--
CURSOR chk_acho_exists(p_acho_ref           VARCHAR2)
IS
SELECT 'X'
  FROM advice_case_housing_options
 WHERE acho_alternative_reference = p_acho_ref;
--
-- ************************************************************************
--
-- Check Group Subsidy Reviews Reference exists if supplied
--
CURSOR chk_grsr_exists(p_grsr_user_reference  VARCHAR2)
IS
SELECT 'X'
  FROM group_subsidy_reviews
 WHERE grsr_user_reference = p_grsr_user_reference;
--
--
-- ************************************************************************
-- Subsidy Review Reasons
--
CURSOR c_hsrr ( p_hsrr_code   VARCHAR2 ) IS
SELECT 'X'
FROM   subsidy_review_reasons
WHERE  srrn_code  = p_hsrr_code;
--
-- ************************************************************************
-- Subsidy Stage and  Review Reasons
--
CURSOR c_stage ( p_hsrs_code   VARCHAR2
               , p_hsrr_code   VARCHAR2 ) IS
SELECT 'X'
FROM   subsidy_reason_stages
WHERE  srst_hsrs_code  = p_hsrs_code
AND    srst_srrn_code  = p_hsrr_code;
--
-- ************************************************************************
--
-- Check Household Type
--
CURSOR c_hty ( p_hty_code    VARCHAR2 ) IS
SELECT 'X'
FROM   hhold_types
WHERE  hty_code = p_hty_code;
--
-- ***********************************************************************
--
-- Moved check from First Ref Values to SUBSIDY_ASSESSMENT_CATEGORIES, as
-- configuration of the data has changed
--
CURSOR chk_asca_exists(p_suap_hrv_asca_code VARCHAR2)
IS
SELECT 'X'
FROM subsidy_assessment_categories
WHERE asca_code = p_suap_hrv_asca_code;
--
-- ***********************************************************************
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_SUBSIDY_REVIEWS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         	    VARCHAR2(1);
l_tcy_refno      	    NUMBER(10);
l_app_refno             NUMBER(10);
l_force_subsidy_period  VARCHAR2(1);
l_rent_week_start       VARCHAR2(3);
l_suap_exists    	    VARCHAR2(1);
l_surv_exists    	    VARCHAR2(1);
l_acho_exists           VARCHAR2(1);
l_grsr_exists           VARCHAR2(1);
l_sub_pol_exists        VARCHAR2(1);
l_asca_exists           VARCHAR2(1);
--
l_errors         	    VARCHAR2(10);
l_error_ind      	    VARCHAR2(10);
i                	    INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
--
    fsc_utils.proc_start('s_dl_hra_subsidy_reviews.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_subsidy_reviews.dataload_validate',3);
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
          cs   := p1.lsurv_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
--
-- ***********************************************************************
--
-- Validation checks required
--
-- Check Tenancy Refno SUAP_TCY_REFNO has been supplied and exists
-- Amended these validation checks as follows...
--
-- If the Class Code is TENANCY, then the Subsidy Application must have a
-- tenancy and that tenancy must match the value supplied for the
-- Tenancy/Application Alternative Reference.
--
-- If the Class Code is APPLICANT, then the Subsidy Application must have an
-- Application and that Application must match the value supplied for the
-- Tenancy/Application Alternative Reference

-- If the Class Code is PRS, then the Subsidy Application must have a Advice
-- Case Housing Option and that Advice Case Housing Option must match
-- the value supplied for theAdvice Case Housing Option Reference
--
-- If the Class Code is PRS, then the Subsidy Review should not have a
-- Subsidy Application.
--
   l_tcy_refno          := NULL;
   l_app_refno          := NULL;
   l_acho_exists        := NULL;
   l_asca_exists        := NULL;
--
    IF p1.lsurv_class_code  = 'TENANCY'
     THEN
--
-- get from subsidy_applications table
--
      OPEN chk_tcy_exists2( p1.lsurv_tcy_app_alt_ref, p1.lsurv_suap_legacy_ref
                          , p1.lsurv_suap_start_date );
       FETCH chk_tcy_exists2 INTO l_tcy_refno;
      CLOSE chk_tcy_exists2;
--
-- get the tenancy ref from dl_hat_subsidy_applications table
-- if suap_legacy_ref has not been loaded into subsidy applications record
--
      IF (l_tcy_refno IS NULL)
       THEN
--
         OPEN chk_tcy_exists( p1.lsurv_tcy_app_alt_ref, p1.lsurv_suap_legacy_ref
                            , p1.lsurv_suap_start_date );
         FETCH chk_tcy_exists INTO l_tcy_refno;
         CLOSE chk_tcy_exists;
--
      END IF;
--
        IF (l_tcy_refno IS NULL)
         THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',41);
        END IF;
--
    ELSIF p1.lsurv_class_code  = 'APPLICANT'
     THEN
--
-- get from subsidy_applications table
--
      OPEN chk_app_exists2( p1.lsurv_tcy_app_alt_ref, p1.lsurv_suap_legacy_ref
                          , p1.lsurv_suap_start_date );
       FETCH chk_app_exists2 INTO l_app_refno;
      CLOSE chk_app_exists2;
--
-- get the app ref from dl_hat_subsidy_applications table
-- if suap_legacy_ref has not been loaded into subsidy applications record
--
      IF (l_app_refno IS NULL)
       THEN
--
         OPEN chk_app_exists( p1.lsurv_tcy_app_alt_ref, p1.lsurv_suap_legacy_ref
                            , p1.lsurv_suap_start_date );
         FETCH chk_app_exists INTO l_app_refno;
         CLOSE chk_app_exists;
--
      END IF;
--
        IF (l_app_refno IS NULL)
         THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',233);
        END IF;
--
    ELSIF p1.lsurv_class_code  = 'PRS'
     THEN
--
      OPEN chk_acho_exists(p1.lsurv_acho_legacy_ref );
       FETCH chk_acho_exists INTO l_acho_exists;
      CLOSE chk_acho_exists;
--
        IF (l_acho_exists IS NULL)
         THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',185);
        END IF;
--
        IF p1.lsurv_suap_legacy_ref IS NOT NULL
         THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',464);
        END IF;
--
    END IF;   /*   IF p1.lsurv_class_code  = 'TENANCY'  */
--
-- ********************************************
--
-- If the System Parameter is set to force the Effective Date of the
-- Subsidy Review to coincide with the Rent Week Start Day, 'FORCE_SUB_PERIOD'
-- set to Y', then the following check will take place.
--
-- The Effective Date must be set to a date which falls on the day of the week
-- identified by ADMIN YEAR Rent Week Start for the ADMIN YEAR which spans the
-- Effective Date and is for the Rents Admin Unit that the associated Rent
-- Account is in.
--
-- *********
--
          IF (    l_tcy_refno             IS NOT NULL
              AND p1.lsurv_effective_date IS NOT NULL) THEN
--
           l_force_subsidy_period := NULL;
--
           l_force_subsidy_period := s_parameter_values.get_param('FORCE_SUBSIDY_PERIOD','SYSTEM');
--
           IF (l_force_subsidy_period = 'Y') THEN
--
            l_rent_week_start := NULL;
--
             OPEN get_admin_year_dets(l_tcy_refno,p1.lsurv_effective_date);
            FETCH get_admin_year_dets INTO l_rent_week_start;
            CLOSE get_admin_year_dets;
--
            IF (l_rent_week_start != p1.lsurv_effective_start_day) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',53);
            END IF;
--
           END IF; -- l_force_subsidy_period
--
          END IF;
--
-- ********************************************
--
-- Check Subsidy Review Legacy Reference has been supplied and doesn't already
-- exist
--
--
          IF (p1.lsurv_legacy_ref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',274);
--
          ELSE
--
             l_surv_exists := NULL;
--
-- checking the subsidy_reviews table directly
--
              OPEN chk_surv_exists2 (p1.lsurv_legacy_ref);
             FETCH chk_surv_exists2 INTO l_surv_exists;
             CLOSE chk_surv_exists2;
--
-- get the survey application ref from dl_hat_subsidy_reviews table
-- if suap_legacy_ref has not been loaded into subsidy reviews table
--
        IF (l_surv_exists IS NULL)
         THEN
--	
              OPEN chk_surv_exists (p1.lsurv_legacy_ref);
             FETCH chk_surv_exists INTO l_surv_exists;
             CLOSE chk_surv_exists;
--
        END IF;
--
             IF (l_surv_exists IS NOT NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',275);
             END IF;
--
          END IF;
--
--
-- ********************************************
--
-- Check Class Code SURV_CLASS_CODE has been supplied and is valid
--
--
          IF (p1.lsurv_class_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',54);
--
          ELSIF (p1.lsurv_class_code NOT IN ('TENANCY','APPLICANT', 'PRS')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',55);
--
          END IF;
--
--
-- ********************************************
--
-- Check Review Effective date SURV_EFFECTIVE_DATE has been supplied and is valid
--
-- Application Review Effective Date must not be before Subsidy Application Start Date
--
--
          IF (p1.lsurv_effective_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',56);
--
          ELSIF (p1.lsurv_effective_date < p1.lsurv_suap_start_date) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',57);
--
          END IF;
--
--
-- ********************************************
--
-- Check Review Assessment date SURV_ASSESSMENT_DATE has been supplied
--
--
          IF (p1.lsurv_assessment_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',58);
          END IF;
--
--
-- ********************************************
--
-- Check Eligible Indicator SURV_DEN_ELIGIBLE_IND has been supplied and is valid
--
--
          IF (p1.lsurv_den_eligible_ind IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',59);
--
          ELSIF (p1.lsurv_den_eligible_ind NOT IN ('Y','N')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',60);
--
          END IF;
--
--
-- ********************************************
--
-- Subsidy Policy Category Code SURV_SUBP_ASCA_CODE has been supplied and is valid
--
--          IF (p1.lsurv_subp_asca_code IS NULL) THEN
--           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',61);
--
--          ELSIF (NOT s_dl_hem_utils.exists_frv('SUBASSCAT',p1.lsurv_subp_asca_code,'Y')) THEN
--              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',62);
--
--          END IF;
--
-- changed from domain to table check amended to match (AJ)
--
          IF (p1.lsurv_subp_asca_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',49);
          ELSE
--
             l_asca_exists := NULL;
--
          OPEN chk_asca_exists(p1.lsurv_subp_asca_code);
         FETCH chk_asca_exists INTO l_asca_exists;
         CLOSE chk_asca_exists;
--
             IF (l_asca_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',82);
            END IF;
--
          END IF;
--
-- ********************************************
--
-- Subsidy Policy Sequence SURV_SUBP_SEQ has been supplied
--
          IF (p1.lsurv_subp_seq IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',63);
          END IF;
--
--
-- ********************************************
--
-- Check Subsidy Policy Category Code SURV_SUBP_ASCA_CODE and
-- Subsidy Policy Sequence exists on Subsidy Policies table
--
          IF (    p1.lsurv_subp_asca_code IS NOT NULL
              AND p1.lsurv_subp_seq       IS NOT NULL) THEN
--
           l_sub_pol_exists := NULL;
--
            OPEN chk_pol_cat_exists (p1.lsurv_subp_asca_code,p1.lsurv_subp_seq);
           FETCH chk_pol_cat_exists INTO l_sub_pol_exists;
           CLOSE chk_pol_cat_exists;
--
           IF (l_sub_pol_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',64);
           END IF;
--
          END IF;
--
--
-- ********************************************
--
-- The status code SUAP_SCO_CODE has been supplied and is valid
--
--
          IF (p1.lsurv_sco_code IS NULL)
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',47);
--
          ELSIF (p1.lsurv_pay_market_rent_ind = 'Y'
                 AND
                 p1.lsurv_sco_code NOT IN ('INA','INE')) 
           THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',336);
--
          ELSIF (p1.lsurv_pay_market_rent_ind = 'N'
                 AND
                 p1.lsurv_sco_code NOT IN 
                         ('RAI','ASS','AUT','ACT','CAN','INA','INE')) 
          THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',337);
--
          END IF;
--
-- If Status is CAN then the cancellation Reason Code must be supplied
--
          IF (    p1.lsurv_sco_code = 'CAN'
              AND p1.lsurv_hscr_code IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',65);
--
          END IF;
--
-- If Status is AUT then the Authorised Date and By must be supplied
--
          IF (        p1.lsurv_sco_code         = 'AUT'
              AND (   p1.lsurv_authorised_by   IS NULL
                   OR p1.lsurv_authorised_date IS NULL )) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',66);
--
          END IF;
--
--
-- ********************************************
--
-- If Supplied the Application end date must be later than the Assessment date
-- and Effective Date
--
--
          IF (p1.lsurv_end_date IS NOT NULL) THEN
--
           IF (p1.lsurv_end_date < p1.lsurv_assessment_date) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',67);
           END IF;
--
           IF (p1.lsurv_end_date < p1.lsurv_effective_date) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',68);
           END IF;
--
          END IF;
--
--
-- ********************************************
--
-- Subsidy HHold Threshold Income Code SURV_ASSESSED_SELB_CODE is valid if supplied
--
          IF (p1.lsurv_assessed_selb_code IS NOT NULL) THEN
--
           IF (p1.lsurv_assessed_selb_code NOT IN ('HIGH','INELIG','LOWER','MODERATE')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',287);
           END IF;
--
          END IF;
--
--
-- ********************************************
--
-- All reference values supplied are valid
--
-- Subsidy Review Reason - Amended, no longer a domain, it's a table
--
   IF (p1.lsurv_hsrr_code IS NULL)
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',69);
--
   ELSE
--
    OPEN c_hsrr ( p1.lsurv_hsrr_code );
     FETCH c_hsrr INTO l_exists;
      IF c_hsrr%NOTFOUND
       THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',70);
      END IF;
    CLOSE c_hsrr;
--
   END IF;   /*   IF (p1.lsurv_hsrr_code IS NULL)   */
--
-- Subsidy Cancellation Reason
--
          IF (p1.lsurv_hscr_code IS NOT NULL) THEN
--
           IF (NOT s_dl_hem_utils.exists_frv('SUBSIDY CANC REASONS',p1.lsurv_hscr_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',71);
           END IF;
--
          END IF;
--
--
-- ***********************************************************************
--
-- Check group subsidy reviews exists where supplied
--
         IF (p1.lsurv_grsr_user_reference IS NOT NULL) THEN
--
            l_grsr_exists := NULL;
--
            OPEN chk_grsr_exists(p1.lsurv_grsr_user_reference);
           FETCH chk_grsr_exists into l_grsr_exists;
           CLOSE chk_grsr_exists;
--
           IF (l_grsr_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',317);
           END IF;
--
         END IF;
--
-- ***********************************************************************
--
-- Check Subsidy Stage
--
    IF (NOT s_dl_hem_utils.exists_frv('SUBSIDY STAGE',p1.lsurv_hsrs_code,'N'))
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',465);
    END IF;
--
-- ***********************************************************************
--
-- Check Subsidy Stage and Subsidy Reason
--
    OPEN c_stage ( p1.lsurv_hsrs_code, p1.lsurv_hsrr_code );
     FETCH c_stage INTO l_exists;
      IF c_stage%NOTFOUND
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',466);
      END IF;
    CLOSE c_stage;
--
-- ***********************************************************************
--
-- Check Household Type
--
    IF p1.lsurv_hty_code IS NOT NULL
     THEN
       OPEN c_hty ( p1.lsurv_hty_code );
        FETCH c_hty INTO l_exists;
         IF c_hty%NOTFOUND
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',467);
         END IF;
       CLOSE c_hty;
    END IF;   /*   IF p1.lsurv_hty_code IS NOT NULL   */
--
-- ***********************************************************************
--
-- Check Cap Type and Amount - v20 spec
--
    IF p1.lsurv_cap_amount is not null and p1.lsurv_cap_type is null
     THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',814);
    END IF;

    IF p1.lsurv_cap_type is not null and p1.lsurv_cap_amount is null
     THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',815);
    END IF;

    IF p1.lsurv_cap_type NOT IN ('ABATING','NON ABATING')
     THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',816);
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
--
END dataload_validate;
--
--
-- ***********************************************************************
--
PROCEDURE dataload_delete(p_batch_id       IN VARCHAR2,
                          p_date           IN date) IS
--
CURSOR c1 is
SELECT rowid REC_ROWID,
       LSURV_DLB_BATCH_ID,
       LSURV_DL_SEQNO,
       LSURV_DL_LOAD_STATUS,
       LSURV_LEGACY_REF,
       LSURV_REFNO
  FROM dl_hra_subsidy_reviews
 WHERE lsurv_dlb_batch_id   = p_batch_id
   AND lsurv_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRA_SUBSIDY_REVIEWS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
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
    fsc_utils.proc_start('s_dl_hra_subsidy_reviews.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_subsidy_reviews.dataload_delete',3 );
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
          cs   := p1.lsurv_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i + 1;
--
-- Delete from table
--
          DELETE
            FROM subsidy_reviews
           WHERE surv_refno = p1.lsurv_refno;
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
          IF MOD(i,5000) = 0 THEN
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
-- Section to analyse the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUBSIDY_REVIEWS');
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
END s_dl_hra_subsidy_reviews;
/
