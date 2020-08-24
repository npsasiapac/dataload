CREATE OR REPLACE PACKAGE BODY s_dl_hpp_pp_applications
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      1.0  5.1.6     PH   14-APR-2002  Initial Creation
--      2.0  5.2.0     SB   29-AUG-2002  Switched created date and created by
--                                       in insert statement.
--                                       Changed validation 812 etc. to allow
--                                       for NULL values
--      2.1  5.5.0     PH   08-MAR-2004  Amended domain for validate on sale type
--                                       from SALE_TYPE to SALES_TYP
--      2.2  5.5.0     PH   01-APR-2004  Added new field LPAPP_DSE_CODE
--                     PJD  01-APR-2004  Allow for supply of tcy_refno
--                                       rather than alt ref
--                                       Default HB Flags
--                                       Made lpapp_papr_code non mandatory
--      2.3  5.5.0     PH   20-APR-2004  Added insert into tenancy_histories
--                                       from what is held in tenancy_holdings
--      2.4  5.5.0     DH   06-MAY-2004  Changed ipt_hrv_fiy_code from 'LAND' to
--                                       'PPLL'
--      2.5  5.6.0     PH   13-AUG-2004  Commented out IPP_xxx codes as these
--                                       have been removed at 560 and not
--                                       sure what we do with these.
--      2.6  5.6.0     PH   13-OCT-2004  Removed IPP fields from insert into
--                                       pp_applications as these should now
--                                       be inserted to interested_party_usages
--                                       Have therefore added 4 new inserts into
--                                       this table if data supplied.
--      2.7  5.7.0     PH   12-JAN-2005  Added new field for 570 release
--                                       LPAPP_MAX_DISCOUNT_AMOUNT no validation.
--      2.8  5.7.0     PJD  16-JAN-2005  Changes to Delete Proc
--						     If error encountered then record status
--                                       remains at C.
--                                       Address Usages and Address deletes only
--                                       take place if no 'unlinked' addresses
--                                       existed before process run.
--      2.9  5.7.0     PH   10-MAR-2005  Amended Validate on Application Type.
--                                       No longer held in reference domains, now
--                                       in pp_application_types table.
--      3.0  5.7.0     PH   25-APR-2005  Amended Insert into tenancy_histories, uses
--                                       thi_start_date rather than tho_start_date.
--      3.1  5.8.0     MB   05-AUG-2005  Added extra NULLs in call to dl_hem_utils.
--					 insert_address
--      3.2  5.9.0     VST  06-JAN-2006  Added extra NULLs in call to dl_hem_utils.
--					 insert_address
--      3.3  5.9.0     PH   17-JAN-2006  Corrected compilation errors, too many fields
--                                       into insert_address
--      3.4  5.9.0     PH   07-JUL-2006  Create now updates properties plus additional
--                                       field on pp_applications, amended delete too.
--                                       Change done after address changes but 
--                                       backpatched to 5.9.0 code.
--      4.0  5.10.0    PH   08-MAY-2006  Removed references to Addresses, these
--                                       should be loaded in Addresses.
--      5.0  5.13.0    PH   06-FEB-2008  Now includes its own 
--                                       set_record_status_flag procedure.
--
--
-- ***********************************************************************
--
--  declare package variables and constants
--
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hpp_pp_applications
  SET lpapp_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hpp_pp_applications');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lpapp_dlb_batch_id
,lpapp_dl_seqno
,lpapp_dl_load_status
,lpapp_pro_propref
,lpapp_displayed_reference
,lpapp_tcy_alt_ref
,lpapp_paty_code
,lpapp_past_code
,lpapp_landlord_ipp_shortname
,lpapp_application_date
,lpapp_corr_name
,lpapp_comments
,lpapp_cost_floor_amount
,lpapp_discounted_price
,lpapp_calculated_discount
,lpapp_papr_code
,lpapp_sco_code
,lpapp_sco_changed_date
,lpapp_held_date
,lpapp_admitted_date
,lpapp_completed_date
,lpapp_former_status_code
,nvl(lpapp_rtm_hb_entitled_ind,'N')  lpapp_rtm_hb_entitled_ind
,nvl(lpapp_rtm_hb_withdrawn_ind,'N') lpapp_rtm_hb_withdrawn_ind
,lpapp_rtm_equity
,lpapp_mortgage_accno
,lpapp_legal_comments
,lpapp_lender_ipp_shortname
,lpapp_solicitor_ipp_shortname
,lpapp_slt_code
,lpapp_palt_code
,lpapp_title_ref
,lpapp_land_reg_comments
,lpapp_insurer_ipp_shortname
,lpapp_insurance_no
,lpapp_insurance_valuation
,lpapp_mortgage_amount
,lpapp_lease_years
,lpapp_years_as_tenant
,lpapp_period_start
,lpapp_ref_period_end
,lpapp_rtm_weekly_rent
,lpapp_rtm_multiplier
,lpapp_rtm_min_init_amount
,lpapp_rtm_max_init_amount
,lpapp_rtm_init_discount
,lpapp_rtm_landlord_share
,lpapp_current_landlord_ind
,lpapp_discount_amount
,lpapp_prev_discount
,lpapp_rtm_calculate_discount
,lpapp_rtm_calc_disc_percent
,lpapp_rtm_price_after_discount
,lpapp_rtm_min_icp_amount
,lpapp_rtm_dfc_at_purchase
,lpapp_rtm_dfc_percent
,lpapp_rtm_adj_weekly_rent
,lpapp_rtm_act_icp_amount
,lpapp_rtm_loan_period
,lpapp_rtm_num_rent_weeks
,lpapp_dse_code
,lpapp_max_discount_amount
FROM DL_HPP_PP_APPLICATIONS
WHERE lpapp_dlb_batch_id    = p_batch_id
AND   lpapp_dl_load_status = 'V';
--
CURSOR c_papp_refno IS
SELECT papp_refno_seq.nextval
FROM   dual;
--
CURSOR c_pro_refno(p_propref varchar2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
-- Cursor to find tcy_refno
--
CURSOR c_tcy(p_tcy_alt_ref varchar2, p_propref varchar2) IS
SELECT tho_tcy_refno, tho_start_date
FROM   tenancies, tenancy_holdings, properties
WHERE  tho_pro_refno = pro_refno
AND    tho_tcy_refno = tcy_refno
AND    tcy_alt_ref   = p_tcy_alt_ref
AND    pro_propref   = p_propref;
--
CURSOR c_tcy2(p_tcy_alt_ref varchar2, p_propref varchar2) IS
SELECT tho_tcy_refno, tho_start_date
FROM   tenancies, tenancy_holdings, properties
WHERE  tho_pro_refno = pro_refno
AND    tho_tcy_refno = tcy_refno
AND    tcy_refno     = to_number(p_tcy_alt_ref)
AND    pro_propref   = p_propref;
--
-- Cursor to generate the ppap_rtb_seq
-- Looks to see how many earlier applications
-- there are for a property
--
CURSOR c_count_seq(p_propref varchar2, p_app_date date) IS
SELECT count(*)
FROM   pp_applications, properties
WHERE  papp_tho_pro_refno = pro_refno
AND    pro_propref        = p_propref
AND    nvl(papp_application_date, p_app_date-1) < p_app_date;
--
CURSOR c_land(p_land_short varchar2) IS
SELECT ipp_refno
FROM   interested_parties, interested_party_types
WHERE  ipt_code         = ipp_ipt_code
AND    ipt_current_ind  = 'Y'
AND    ipp_shortname    = p_land_short
AND    ipt_hrv_fiy_code = 'PPLL';
--
CURSOR c_lend(p_land_short varchar2) IS
SELECT ipp_refno
FROM   interested_parties, interested_party_types
WHERE  ipt_code         = ipp_ipt_code
AND    ipt_current_ind  = 'Y'
AND    ipp_shortname    = p_land_short
AND    ipt_hrv_fiy_code = 'LEND';
--
CURSOR c_sol(p_land_short varchar2) IS
SELECT ipp_refno
FROM   interested_parties, interested_party_types
WHERE  ipt_code         = ipp_ipt_code
AND    ipt_current_ind  = 'Y'
AND    ipp_shortname    = p_land_short
AND    ipt_hrv_fiy_code = 'SOL';
--
CURSOR c_ins(p_land_short varchar2) IS
SELECT ipp_refno
FROM   interested_parties, interested_party_types
WHERE  ipt_code         = ipp_ipt_code
AND    ipt_current_ind  = 'Y'
AND    ipp_shortname    = p_land_short
AND    ipt_hrv_fiy_code = 'INSU';
--
CURSOR c_get_tcy_name (p_tcy_refno number) IS
SELECT tcy_correspond_name
FROM   tenancies
WHERE  tcy_refno        = p_tcy_refno;
--
CURSOR c_get_hop_refno (p_tcy_refno number) IS
SELECT tin_hop_refno, tin_start_date
FROM   tenancy_instances
WHERE  tin_tcy_refno       = p_tcy_refno
AND    tin_main_tenant_ind = 'Y';
--
CURSOR c_get_pro_adr (p_pro_refno number) IS
SELECT adr_line_all
FROM   address_usages,
       addresses
where  adr_refno           = aus_adr_refno
and    aus_aut_fao_code    = 'PRO'
and    aus_aut_far_code    = 'PHYSICAL'
and    aus_pro_refno       = p_pro_refno
and    sysdate between aus_start_date
                   and nvl(aus_end_date, sysdate);
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HPP_PP_APPLICATIONS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
l_pro_refno            number;
i                      integer := 0;
l_papp_refno           number(10);
l_count_seq            number;
l_tcy_refno            number;
l_tho_start_date       date;
l_dummy                VARCHAR2(1);
l_mode                 VARCHAR2(10);
l_land_ipp             NUMBER(10);
l_lend_ipp             NUMBER(10);
l_sol_ipp              NUMBER(10);
l_ins_ipp              NUMBER(10);
l_answer               VARCHAR(1);
l_tcy_name             VARCHAR2(240);
l_hop_refno            NUMBER(10);
l_thi_start_date       DATE;
l_pro_adr              VARCHAR2(240);
l_verified_ind         VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_applications.dataload_create');
fsc_utils.debug_message( 's_dl_hpp_pp_applications.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the 'Use Tcy Refno Question'
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lpapp_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- get the next papp_refno
--
l_papp_refno := null;
OPEN c_papp_refno;
FETCH c_papp_refno INTO l_papp_refno;
CLOSE c_papp_refno;
--
-- get the pro_refno
--
l_pro_refno := null;
--
OPEN  c_pro_refno(p1.lpapp_pro_propref);
FETCH c_pro_refno INTO l_pro_refno;
CLOSE c_pro_refno;
--
-- get the tcy_refno
--
l_tcy_refno      := null;
l_tho_start_date := null;
--
IF nvl(l_answer,'N') != 'Y'
THEN
  OPEN  c_tcy(p1.lpapp_tcy_alt_ref, p1.lpapp_pro_propref);
  FETCH c_tcy INTO l_tcy_refno, l_tho_start_date;
  CLOSE c_tcy;
END IF;--
--
IF nvl(l_answer,'N') = 'Y'
THEN
  OPEN  c_tcy2(p1.lpapp_tcy_alt_ref, p1.lpapp_pro_propref);
  FETCH c_tcy2 INTO l_tcy_refno, l_tho_start_date;
  CLOSE c_tcy2;
END IF;--
--
-- get ipp refno's
--
l_land_ipp := null;
l_lend_ipp := null;
l_sol_ipp  := null;
l_ins_ipp  := null;
--
  OPEN c_land(p1.lpapp_landlord_ipp_shortname);
   FETCH c_land INTO l_land_ipp;
  CLOSE c_land;
--
  OPEN c_lend(p1.lpapp_lender_ipp_shortname);
   FETCH c_lend INTO l_lend_ipp;
  CLOSE c_lend;
--
  OPEN c_sol(p1.lpapp_solicitor_ipp_shortname);
    FETCH c_sol INTO l_sol_ipp;
  CLOSE c_sol;
--
  OPEN c_ins(p1.lpapp_insurer_ipp_shortname);
    FETCH c_ins INTO l_ins_ipp;
  CLOSE c_ins;
--
-- This updates the ppap_rtb_seq for applications
-- which are later than the one we are loading
-- therefore keeps the sequence.
--
l_count_seq := 0;
--
OPEN  c_count_seq(p1.lpapp_pro_propref, p1.lpapp_application_date);
FETCH c_count_seq INTO l_count_seq;
CLOSE c_count_seq;
--
l_count_seq := l_count_seq+1;
--
UPDATE pp_applications
SET    papp_rtb_seq       = papp_rtb_seq+1
WHERE  papp_tho_pro_refno = l_pro_refno;
--
-- Now do the Insert
--
      INSERT INTO pp_applications
         (papp_refno
         ,papp_displayed_reference
         ,papp_created_date
         ,papp_created_by
         ,papp_application_date
         ,papp_corr_name
         ,papp_comments
         ,papp_cost_floor_amount
         ,papp_discounted_price
         ,papp_calculated_discount
         ,papp_held_date
         ,papp_admitted_date
         ,papp_completed_date
         ,papp_rtm_hb_entitled_ind
         ,papp_rtm_hb_withdrawn_ind
         ,papp_rtm_equity
         ,papp_mortgage_accno
         ,papp_legal_comments
         ,papp_title_ref
         ,papp_land_reg_comments
         ,papp_insurance_no
         ,papp_insurance_valuation
         ,papp_mortgage_amount
         ,papp_lease_years
         ,papp_years_as_tenant
         ,papp_ref_period_end
         ,papp_period_start
         ,papp_rtm_weekly_rent
         ,papp_rtm_multiplier
         ,papp_rtm_min_init_amount
         ,papp_rtm_max_init_amount
         ,papp_rtm_init_discount
         ,papp_rtm_landlord_share
         ,papp_current_landlord_ind
         ,papp_rtb_seq
         ,papp_discount_amount
         ,papp_prev_discount
         ,papp_rtm_calculate_discount
         ,papp_rtm_calc_discount_percent
         ,papp_rtm_price_after_discount
         ,papp_rtm_min_icp_amount
         ,papp_rtm_dfc_at_purchase
         ,papp_rtm_dfc_percent
         ,papp_rtm_adj_weekly_rent
         ,papp_rtm_act_icp_amount
         ,papp_rtm_loan_period
         ,papp_rtm_num_rent_weeks
         ,papp_modified_date
         ,papp_modified_by
         ,papp_sco_changed_date
         ,papp_tho_tcy_refno
         ,papp_tho_start_date
         ,papp_tho_pro_refno
         ,papp_papr_code
         ,papp_slt_code
         ,papp_palt_code
         ,papp_past_code
         ,papp_paty_code
         ,papp_sco_code
         ,papp_former_status_code
         ,papp_dse_code
         ,papp_max_discount_amount
         ,papp_pro_rtb_refno
         )
      VALUES
         (l_papp_refno
         ,p1.lpapp_displayed_reference
         ,trunc(sysdate)
         ,'DATALOAD'
         ,p1.lpapp_application_date
         ,p1.lpapp_corr_name
         ,p1.lpapp_comments
         ,p1.lpapp_cost_floor_amount
         ,p1.lpapp_discounted_price
         ,p1.lpapp_calculated_discount
         ,p1.lpapp_held_date
         ,p1.lpapp_admitted_date
         ,p1.lpapp_completed_date
         ,p1.lpapp_rtm_hb_entitled_ind
         ,p1.lpapp_rtm_hb_withdrawn_ind
         ,p1.lpapp_rtm_equity
         ,p1.lpapp_mortgage_accno
         ,p1.lpapp_legal_comments
         ,p1.lpapp_title_ref
         ,p1.lpapp_land_reg_comments
         ,p1.lpapp_insurance_no
         ,p1.lpapp_insurance_valuation
         ,p1.lpapp_mortgage_amount
         ,p1.lpapp_lease_years
         ,p1.lpapp_years_as_tenant
         ,p1.lpapp_ref_period_end
         ,p1.lpapp_period_start
         ,p1.lpapp_rtm_weekly_rent
         ,p1.lpapp_rtm_multiplier
         ,p1.lpapp_rtm_min_init_amount
         ,p1.lpapp_rtm_max_init_amount
         ,p1.lpapp_rtm_init_discount
         ,p1.lpapp_rtm_landlord_share
         ,p1.lpapp_current_landlord_ind
         ,l_count_seq
         ,p1.lpapp_discount_amount
         ,p1.lpapp_prev_discount
         ,p1.lpapp_rtm_calculate_discount
         ,p1.lpapp_rtm_calc_disc_percent
         ,p1.lpapp_rtm_price_after_discount
         ,p1.lpapp_rtm_min_icp_amount
         ,p1.lpapp_rtm_dfc_at_purchase
         ,p1.lpapp_rtm_dfc_percent
         ,p1.lpapp_rtm_adj_weekly_rent
         ,p1.lpapp_rtm_act_icp_amount
         ,p1.lpapp_rtm_loan_period
         ,p1.lpapp_rtm_num_rent_weeks
         ,trunc(sysdate)
         ,'DATALOAD'
         ,p1.lpapp_sco_changed_date
         ,l_tcy_refno
         ,l_tho_start_date
         ,l_pro_refno
         ,p1.lpapp_papr_code
         ,p1.lpapp_slt_code
         ,p1.lpapp_palt_code
         ,p1.lpapp_past_code
         ,p1.lpapp_paty_code
         ,p1.lpapp_sco_code
         ,p1.lpapp_former_status_code
         ,p1.lpapp_dse_code
         ,p1.lpapp_max_discount_amount
         ,l_papp_refno
         );
--
-- 560 Change on Interested Parties. Now insert into
-- interested_party_usages for Landlord, Lender,
-- Solicitor and Insurer.
--
-- Earlier cursor obtained ipp_refnos
--
  IF l_land_ipp is not null
   THEN
      INSERT INTO interested_party_usages
         (ipus_refno
         ,ipus_ipp_refno
         ,ipus_start_date
         ,ipus_app_refno
         ,ipus_vin_refno
         ,ipus_tcy_refno
         ,ipus_end_date
         ,ipus_comments
         ,ipus_apl_refno
         ,ipus_papp_refno
         ,ipus_scs_refno
         ,ipus_cnt_reference
         ,ipus_aun_code
         ,ipus_pro_refno
         ,ipus_psl_refno
         ,ipus_pval_papp_refno
         ,ipus_pval_seqno
         ,ipus_schd_refno
         )
      VALUES
         (ipus_refno_seq.nextval
         ,l_land_ipp
         ,p1.lpapp_application_date
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         ,l_papp_refno
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         );
  END IF;
--
  IF l_lend_ipp is not null
   THEN
      INSERT INTO interested_party_usages
         (ipus_refno
         ,ipus_ipp_refno
         ,ipus_start_date
         ,ipus_app_refno
         ,ipus_vin_refno
         ,ipus_tcy_refno
         ,ipus_end_date
         ,ipus_comments
         ,ipus_apl_refno
         ,ipus_papp_refno
         ,ipus_scs_refno
         ,ipus_cnt_reference
         ,ipus_aun_code
         ,ipus_pro_refno
         ,ipus_psl_refno
         ,ipus_pval_papp_refno
         ,ipus_pval_seqno
         ,ipus_schd_refno
         )
      VALUES
         (ipus_refno_seq.nextval
         ,l_lend_ipp
         ,p1.lpapp_application_date
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         ,l_papp_refno
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         );
  END IF;
--
  IF l_sol_ipp  is not null
   THEN
      INSERT INTO interested_party_usages
         (ipus_refno
         ,ipus_ipp_refno
         ,ipus_start_date
         ,ipus_app_refno
         ,ipus_vin_refno
         ,ipus_tcy_refno
         ,ipus_end_date
         ,ipus_comments
         ,ipus_apl_refno
         ,ipus_papp_refno
         ,ipus_scs_refno
         ,ipus_cnt_reference
         ,ipus_aun_code
         ,ipus_pro_refno
         ,ipus_psl_refno
         ,ipus_pval_papp_refno
         ,ipus_pval_seqno
         ,ipus_schd_refno
         )
      VALUES
         (ipus_refno_seq.nextval
         ,l_sol_ipp
         ,p1.lpapp_application_date
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         ,l_papp_refno
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         );
  END IF;
--
  IF l_ins_ipp  is not null
   THEN
      INSERT INTO interested_party_usages
         (ipus_refno
         ,ipus_ipp_refno
         ,ipus_start_date
         ,ipus_app_refno
         ,ipus_vin_refno
         ,ipus_tcy_refno
         ,ipus_end_date
         ,ipus_comments
         ,ipus_apl_refno
         ,ipus_papp_refno
         ,ipus_scs_refno
         ,ipus_cnt_reference
         ,ipus_aun_code
         ,ipus_pro_refno
         ,ipus_psl_refno
         ,ipus_pval_papp_refno
         ,ipus_pval_seqno
         ,ipus_schd_refno
         )
      VALUES
         (ipus_refno_seq.nextval
         ,l_ins_ipp
         ,p1.lpapp_application_date
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         ,l_papp_refno
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         );
  END IF;
--
--
-- Now do the insert into tenancy_histories based on
-- what is in tenancy_holdings for the tenancy
--
l_tcy_name     := null;
l_hop_refno    := null;
l_pro_adr      := null;
l_verified_ind := null;
--
   OPEN c_get_tcy_name(l_tcy_refno);
    FETCH c_get_tcy_name INTO l_tcy_name;
   CLOSE c_get_tcy_name;
--
   OPEN c_get_hop_refno(l_tcy_refno);
    FETCH c_get_hop_refno into l_hop_refno, l_thi_start_date;
   CLOSE c_get_hop_refno;
--
   OPEN c_get_pro_adr(l_pro_refno);
    FETCH c_get_pro_adr INTO l_pro_adr;
   CLOSE c_get_pro_adr;
--
   IF p1.lpapp_discounted_price is not null
    THEN l_verified_ind := 'Y';
     ELSE l_verified_ind := 'N';
   END IF;
--
      INSERT INTO tenancy_histories
         (thi_period_start
         ,thi_period_end
         ,thi_comments
         ,thi_verified_ind
         ,thi_verified_by
         ,thi_landlord_free_text
         ,thi_tenant_name
         ,thi_tenant_address
         ,thi_papa_papp_refno
         ,thi_papa_tin_start_date
         ,thi_papa_tin_tcy_refno
         ,thi_papa_hop_refno
         ,thi_tht_code
         ,thi_tin_start_date
         ,thi_tin_tcy_refno
         ,thi_tin_hop_refno
         ,thi_type
         ,thi_current_landlord_ind
         )
      VALUES
         (l_tho_start_date
         ,p1.lpapp_application_date
         ,null
         ,l_verified_ind
         ,decode(l_verified_ind, 'Y', 'DATALOAD', null)
         ,null
         ,l_tcy_name
         ,l_pro_adr
         ,l_papp_refno
         ,l_tho_start_date
         ,l_tcy_refno
         ,l_hop_refno
         ,'1'
         ,l_thi_start_date
         ,l_tcy_refno
         ,l_hop_refno
         ,'GEN'
         ,'Y'
         );
--
--
--
-- New update to properties
--
    update properties
    set    pro_hou_rtb_refno = l_papp_refno
    where  pro_refno         = l_pro_refno;
--
-- keep a count of the rows processed and commit after every 5000
--
i := i+1; IF MOD(i,5000)=0 THEN COMMIT; END If;
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
 END;
--
END LOOP;
COMMIT;
--
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PP_APPLICATIONS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANCY_HISTORIES');
--
fsc_utils.proc_end;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
     RAISE;
--
END dataload_create;
--
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lpapp_dlb_batch_id
,lpapp_dl_seqno
,lpapp_dl_load_status
,lpapp_pro_propref
,lpapp_displayed_reference
,lpapp_tcy_alt_ref
,lpapp_paty_code
,lpapp_past_code
,lpapp_landlord_ipp_shortname
,lpapp_application_date
,lpapp_corr_name
,lpapp_comments
,lpapp_cost_floor_amount
,lpapp_discounted_price
,lpapp_calculated_discount
,lpapp_papr_code
,lpapp_sco_code
,lpapp_sco_changed_date
,lpapp_held_date
,lpapp_admitted_date
,lpapp_completed_date
,lpapp_former_status_code
,nvl(lpapp_rtm_hb_entitled_ind,'N')   lpapp_rtm_hb_entitled_ind
,nvl(lpapp_rtm_hb_withdrawn_ind,'N') lpapp_rtm_hb_withdrawn_ind
,lpapp_rtm_equity
,lpapp_mortgage_accno
,lpapp_legal_comments
,lpapp_lender_ipp_shortname
,lpapp_solicitor_ipp_shortname
,lpapp_slt_code
,lpapp_palt_code
,lpapp_title_ref
,lpapp_land_reg_comments
,lpapp_insurer_ipp_shortname
,lpapp_insurance_no
,lpapp_insurance_valuation
,lpapp_mortgage_amount
,lpapp_lease_years
,lpapp_years_as_tenant
,lpapp_period_start
,lpapp_ref_period_end
,lpapp_rtm_weekly_rent
,lpapp_rtm_multiplier
,lpapp_rtm_min_init_amount
,lpapp_rtm_max_init_amount
,lpapp_rtm_init_discount
,lpapp_rtm_landlord_share
,lpapp_current_landlord_ind
,lpapp_discount_amount
,lpapp_prev_discount
,lpapp_rtm_calculate_discount
,lpapp_rtm_calc_disc_percent
,lpapp_rtm_price_after_discount
,lpapp_rtm_min_icp_amount
,lpapp_rtm_dfc_at_purchase
,lpapp_rtm_dfc_percent
,lpapp_rtm_adj_weekly_rent
,lpapp_rtm_act_icp_amount
,lpapp_rtm_loan_period
,lpapp_rtm_num_rent_weeks
,lpapp_dse_code
FROM  DL_HPP_PP_APPLICATIONS
WHERE lpapp_dlb_batch_id      = p_batch_id
AND   lpapp_dl_load_status   in ('L','F','O');
--
-- Cursor for pro_refno
--
CURSOR c_pro_refno(p_pro_propref VARCHAR2)
IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_pro_propref;
--
-- Cursor for tenancy check
--
CURSOR c_tcy_refno(p_tcy_ref varchar2, p_pro_refno number) IS
SELECT tcy_refno
FROM   tenancies,
       tenancy_holdings
WHERE  tcy_refno     = tho_tcy_refno
AND    tcy_alt_ref   = p_tcy_ref
AND    tho_pro_refno = p_pro_refno;
--
--
CURSOR c_tcy_refno2(p_tcy_ref varchar2, p_pro_refno number) IS
SELECT tcy_refno
FROM   tenancies,
       tenancy_holdings
WHERE  tcy_refno     = tho_tcy_refno
AND    tcy_refno     = to_number(p_tcy_ref)
AND    tho_pro_refno = p_pro_refno;
--
-- Cursor to check for multiple applications with a status of COM
--
CURSOR c_multiple(p_papp_refno  varchar2
                 ,p_propref     varchar2
                 ,p_tcy_ref varchar2) IS
SELECT 'X'
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_refno
AND    papp_tho_pro_refno       = p_propref
AND    papp_tho_tcy_refno       = p_tcy_ref
AND    papp_sco_code            = 'COM';
--
CURSOR c_land(p_land_short varchar2) IS
SELECT 'X'
FROM   interested_parties, interested_party_types
WHERE  ipt_code         = ipp_ipt_code
AND    ipt_current_ind  = 'Y'
AND    ipp_shortname    = p_land_short
AND    ipt_hrv_fiy_code = 'LAND';
--
CURSOR c_lend(p_land_short varchar2) IS
SELECT 'X'
FROM   interested_parties, interested_party_types
WHERE  ipt_code         = ipp_ipt_code
AND    ipt_current_ind  = 'Y'
AND    ipp_shortname    = p_land_short
AND    ipt_hrv_fiy_code = 'LEND';
--
CURSOR c_sol(p_land_short varchar2) IS
SELECT 'X'
FROM   interested_parties, interested_party_types
WHERE  ipt_code         = ipp_ipt_code
AND    ipt_current_ind  = 'Y'
AND    ipp_shortname    = p_land_short
AND    ipt_hrv_fiy_code = 'SOL';
--
CURSOR c_ins(p_land_short varchar2) IS
SELECT 'X'
FROM   interested_parties, interested_party_types
WHERE  ipt_code         = ipp_ipt_code
AND    ipt_current_ind  = 'Y'
AND    ipp_shortname    = p_land_short
AND    ipt_hrv_fiy_code = 'INSU';
--
CURSOR c_sco(p_sco varchar2) IS
SELECT 'X'
FROM   status_codes
WHERE  sco_code = p_sco;
--
CURSOR c_paty_code(p_paty_code varchar2) IS
SELECT 'X'
FROM   pp_application_types
WHERE  paty_code   =  p_paty_code;
--
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HPP_PP_APPLICATIONS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists            VARCHAR2(1);
l_pro_refno         NUMBER(10);
l_errors            VARCHAR2(10);
l_error_ind         VARCHAR2(10);
i                   INTEGER :=0;
l_street_index_code VARCHAR2(12);
l_adr_refno         INTEGER;
l_mode              VARCHAR2(10);
l_tcy_refno         NUMBER(10);
l_answer            VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_applications.dataload_validate');
fsc_utils.debug_message( 's_dl_hpp_pp_applications.dataload_validate',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the 'Use Tcy Refno Question'
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lpapp_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- Check the Links to Other Tables
--
-- Check the property exists on properties
--
l_pro_refno := null;
--
    IF (not s_dl_hem_utils.exists_propref(p1.lpapp_pro_propref))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
    END IF;
--
-- Check the tenancy exists
--
-- get the pro_refno first
--
l_pro_refno := null;
--
  OPEN c_pro_refno(p1.lpapp_pro_propref);
   FETCH c_pro_refno INTO l_pro_refno;
  CLOSE c_pro_refno;
--
IF NVL(l_answer,'N') != 'Y'
THEN  OPEN c_tcy_refno(p1.lpapp_tcy_alt_ref, l_pro_refno);
   FETCH c_tcy_refno INTO l_tcy_refno;
    IF c_tcy_refno%NOTFOUND
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',080);
    END IF;
  CLOSE c_tcy_refno;
END IF;
--
IF NVL(l_answer,'N') = 'Y'
THEN  OPEN c_tcy_refno2(p1.lpapp_tcy_alt_ref, l_pro_refno);
   FETCH c_tcy_refno2 INTO l_tcy_refno;
    IF c_tcy_refno2%NOTFOUND
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',080);
    END IF;
  CLOSE c_tcy_refno2;
END IF;
--
-- Check mandatory Columns
--
-- Displayed Reference
--
   IF p1.lpapp_displayed_reference IS NULL
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',507);
   END IF;
--
-- Correspond Name
--
   IF p1.lpapp_corr_name IS NULL
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',508);
   END IF;
--
-- Status Changed Date
--
   IF p1.lpapp_sco_changed_date IS NULL
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',509);
   END IF;
--
-- Check all Y/N columns
--
    IF (NOT s_dl_hem_utils.yorn(p1.lpapp_rtm_hb_entitled_ind))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',800);
    END IF;
--
    IF (NOT s_dl_hem_utils.yorn(p1.lpapp_rtm_hb_withdrawn_ind))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',801);
    END IF;
--
    IF (NOT s_dl_hem_utils.yornornull(p1.lpapp_current_landlord_ind))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',802);
    END IF;
--
-- Check to see if multiple applications with a status of COMP
--
  IF p1.lpapp_sco_code = 'COM'
   THEN
--
l_pro_refno := null;
l_tcy_refno := null;
--
   OPEN c_pro_refno(p1.lpapp_pro_propref);
    FETCH c_pro_refno INTO l_pro_refno;
   CLOSE c_pro_refno;
--
   OPEN c_tcy_refno(p1.lpapp_tcy_alt_ref, l_pro_refno);
    FETCH c_tcy_refno INTO l_tcy_refno;
   CLOSE c_tcy_refno;
--
    OPEN  c_multiple(p1.lpapp_displayed_reference, l_pro_refno, l_tcy_refno);
     FETCH c_multiple into l_exists;
      IF c_multiple%found
       THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',821);
      END IF;
    CLOSE c_multiple;
  END IF;
--
-- Check Reference Values are valid
--
-- Application Type
-- Amended 10-MAR-2005 from FRV check to pp_application_type table
--
   OPEN c_paty_code(p1.lpapp_paty_code);
     FETCH c_paty_code into l_exists;
      IF c_paty_code%notfound
       THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',809);
      END IF;
   CLOSE c_paty_code;
--
-- Application Status
--
   IF (NOT s_dl_hem_utils.exists_frv('PP_APP_STA', p1.lpapp_past_code, 'N'))
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',810);
   END IF;
--
-- Application Priority Code
--
   IF p1.lpapp_papr_code  IS NOT NULL
   THEN
     IF (NOT s_dl_hem_utils.exists_frv('PP_APP_PRIO', p1.lpapp_papr_code, 'N'))
      THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',811);
     END IF;
   END IF;
--
-- Check landlord reference
--
   IF p1.lpapp_landlord_ipp_shortname IS NOT NULL
    THEN
     OPEN c_land(p1.lpapp_landlord_ipp_shortname);
      FETCH c_land into l_exists;
        IF c_land%notfound
         THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',812);
        END IF;
     CLOSE c_land;
   END IF;
--
-- Check Lender reference
--
   IF p1.lpapp_lender_ipp_shortname IS NOT NULL
    THEN
     OPEN c_lend(p1.lpapp_lender_ipp_shortname);
      FETCH c_lend into l_exists;
       IF c_lend%notfound
        THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',813);
      END IF;
    CLOSE c_lend;
   END IF;
--
-- Check Solicitor reference
--
   IF p1.lpapp_solicitor_ipp_shortname IS NOT NULL
    THEN
     OPEN c_sol(p1.lpapp_solicitor_ipp_shortname);
      FETCH c_sol into l_exists;
       IF c_sol%notfound
        THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',814);
       END IF;
     CLOSE c_sol;
   END IF;
--
-- Check insurer reference
--
   IF p1.lpapp_insurer_ipp_shortname IS NOT NULL
    THEN
     OPEN c_ins(p1.lpapp_insurer_ipp_shortname);
      FETCH c_ins into l_exists;
       IF c_ins%notfound
        THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',815);
       END IF;
    CLOSE c_ins;
   END IF;
--
-- Check the status code -Need to check that cursor is correct
--
   OPEN c_sco(p1.lpapp_sco_code);
    FETCH c_sco into l_exists;
     IF c_sco%notfound
      THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',816);
     END IF;
   CLOSE c_sco;
--
-- Check Former Status Code
--
   IF p1.lpapp_former_status_code IS NOT NULL
    THEN
     OPEN c_sco(p1.lpapp_former_status_code);
      FETCH c_sco into l_exists;
       IF c_sco%notfound
        THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',817);
       END IF;
     CLOSE c_sco;
   END IF;
--
-- Check the Sale Type
--
   IF (NOT s_dl_hem_utils.exists_frv('SALES_TYP', p1.lpapp_slt_code, 'Y'))
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',819);
   END IF;
--
-- Check the Lease Type
--
   IF (NOT s_dl_hem_utils.exists_frv('LEASE_TYPE', p1.lpapp_palt_code, 'Y'))
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',820);
   END IF;
--
--
--
-- Now UPDATE the record count AND error code
IF l_errors = 'F' THEN
  l_error_ind := 'Y';
ELSE
  l_error_ind := 'N';
END IF;
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
set_record_status_flag(l_id,l_errors);
--
   EXCEPTION
      WHEN OTHERS THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
END;
--
END LOOP;
COMMIT;
--
fsc_utils.proc_END;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lpapp_dlb_batch_id
,lpapp_dl_seqno
,lpapp_dl_load_status
,lpapp_pro_propref
,lpapp_displayed_reference
,lpapp_tcy_alt_ref
FROM DL_HPP_PP_APPLICATIONS
WHERE lpapp_dlb_batch_id     = p_batch_id
  AND lpapp_dl_load_status   = 'C';
--
--
CURSOR c_pro_refno(p_propref varchar2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
CURSOR c_papp(p_papp varchar2) IS
SELECT papp_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp;
--
--
-- Constants for process_summary
cb           VARCHAR2(30);
cd           DATE;
cp           VARCHAR2(30) := 'DELETE';
ct           VARCHAR2(30) := 'DL_HPP_PP_APPLICATIONS';
cs           INTEGER;
ce           VARCHAR2(200);
l_id     ROWID;
l_an_tab     VARCHAR2(1);
l_sa VARCHAR2(1) := 'N'; -- do stand alone addresses exist
--
i integer := 0;
l_pro_refno  INTEGER;
l_papp_refno INTEGER;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_applications.dataload_delete');
fsc_utils.debug_message( 's_dl_hpp_pp_applications.dataload_delete',3 );
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
cs := p1.lpapp_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- Get the papp_refno
--
l_papp_refno := null;
--
OPEN c_papp(p1.lpapp_displayed_reference);
 FETCH c_papp into l_papp_refno;
CLOSE c_papp;
--
DELETE FROM address_usages
WHERE  aus_papp_refno = l_papp_refno;
--
-- get the pro_refno
--
l_pro_refno := null;
--
OPEN c_pro_refno(p1.lpapp_pro_propref);
 FETCH c_pro_refno into l_pro_refno;
CLOSE c_pro_refno;
--
--
-- New update to properties
--
   UPDATE properties
   SET    pro_hou_rtb_refno = null
   WHERE  pro_refno         = l_pro_refno;
--
DELETE FROM pp_applications
WHERE papp_displayed_reference = p1.lpapp_displayed_reference
AND   papp_tho_pro_refno       = l_pro_refno;
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'V');
--
EXCEPTION
   WHEN OTHERS THEN
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'C');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
--
END LOOP;
--
COMMIT;
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PP_APPLICATIONS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADDRESS_USAGES');
--
fsc_utils.proc_end;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_delete;
--
--
END s_dl_hpp_pp_applications;
/

