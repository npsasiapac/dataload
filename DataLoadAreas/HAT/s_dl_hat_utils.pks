CREATE OR REPLACE PACKAGE s_dl_hat_utils
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Ver WHO  WHEN       WHY
--      1.0        RJ   03/04/2001  Dataload
--      2.0 5.4.0  PJD  07/11/2003  added function f_exists_app_refno
--
-- ***********************************************************************
FUNCTION f_correspondence_name
( p_lapp_legacy_ref IN VARCHAR2)
RETURN VARCHAR2;
--
FUNCTION f_exists_tenacy_ref
       (p_tcy_alt_ref IN VARCHAR2) RETURN BOOLEAN;
--
FUNCTION f_exists_tenacy_ref
       (p_tcy_refno IN NUMBER) RETURN BOOLEAN;
--
FUNCTION f_exists_applic_list_entries
       (p_lapp_legacy_ref IN VARCHAR2) RETURN BOOLEAN;
--
FUNCTION f_exists_dl_hat_inv_parties
       (p_lapp_legacy_ref IN VARCHAR2) RETURN BOOLEAN;
--
FUNCTION f_exists_interested_party
      (p_lipt_code IN VARCHAR2)
      RETURN BOOLEAN;
--
FUNCTION f_exists_application(p_lale_alt_ref IN VARCHAR2)
  RETURN BOOLEAN;
--
FUNCTION f_exists_rld(p_lapp_legacy_ref IN VARCHAR2,
                      p_rli_code IN VARCHAR2,
                      p_rls_code IN VARCHAR2)
  RETURN BOOLEAN;
--
FUNCTION f_exists_hid(p_lhin_instance_refno IN VARCHAR2,
                      p_rls_code IN VARCHAR2)
  RETURN BOOLEAN;
--
FUNCTION f_exists_rlicode(p_rli_code IN VARCHAR2)
  RETURN BOOLEAN;
--
FUNCTION f_exists_rsdcode(p_rsd_code IN VARCHAR2)
  RETURN BOOLEAN;
--
FUNCTION f_application_type(p_lale_alt_ref IN VARCHAR2,
                            p_rli_type IN VARCHAR2)
   RETURN BOOLEAN;
--
FUNCTION f_exists_auncode(p_aun_code IN VARCHAR2)
  RETURN BOOLEAN;
--
FUNCTION f_exists_auntype(p_aun_code IN VARCHAR2)
  RETURN BOOLEAN;
--
FUNCTION f_exists_applistentry(p_lleh_alt_ref IN VARCHAR2,
                               p_rli_code IN VARCHAR2)
  RETURN BOOLEAN;
--
FUNCTION f_exists_lstcode(p_lst_code IN VARCHAR2)
RETURN BOOLEAN;
--
FUNCTION f_exists_dl_application(p_lipa_legacy_ref IN VARCHAR2)
RETURN BOOLEAN;
--
FUNCTION f_app_received (p_lipa_legacy_ref IN VARCHAR2)
RETURN DATE;
--
FUNCTION f_exists_genans(p_lgan_que_refno IN NUMBER,p_lapp_legacy_ref IN VARCHAR2)
RETURN BOOLEAN;
--
FUNCTION f_exists_hiaans(p_lhia_que_refno IN NUMBER,p_hin_instance_refno IN VARCHAR2)
RETURN BOOLEAN;
--
FUNCTION f_exists_quenum(p_que_refno IN NUMBER)
RETURN BOOLEAN;
--
FUNCTION f_valid_quecat(p_que_refno IN NUMBER,p_ans_type IN VARCHAR2)
RETURN BOOLEAN;
----
FUNCTION f_exists_qor(p_qor_code IN VARCHAR2)
RETURN BOOLEAN;
--
FUNCTION f_exists_char(p_que_refno IN NUMBER,p_char_value IN VARCHAR2)
RETURN BOOLEAN;
--
FUNCTION f_exists_num(p_que_refno IN NUMBER,p_number_value IN NUMBER)
RETURN BOOLEAN;
--
FUNCTION f_exists_invpans(p_lpar_per_alt_ref IN VARCHAR2,p_lipn_que_refno IN NUMBER)
RETURN BOOLEAN;
--
FUNCTION f_exists_invpar(p_lpar_per_alt_ref IN VARCHAR2)
RETURN BOOLEAN;
--
FUNCTION f_exists_menans(p_lman_que_refno IN NUMBER,
                         p_lman_mrf_assessment_refno IN VARCHAR2)
RETURN BOOLEAN;
--
FUNCTION f_exists_letans(p_llaa_lar_code IN VARCHAR2,p_lapp_legacy_ref IN VARCHAR2,
  p_llaa_que_refno IN NUMBER)
RETURN BOOLEAN;
--
FUNCTION f_valid_larcode(p_llaa_lar_code IN VARCHAR2)
RETURN BOOLEAN;
--
FUNCTION f_valid_child_larcode(p_llaa_lar_code IN VARCHAR2)
RETURN BOOLEAN;
--
FUNCTION f_dates_required(p_lale_lst_code IN VARCHAR2)
RETURN VARCHAR2;
--
FUNCTION f_exists_address_usages(p_laus_aut_fao_code IN VARCHAR2,
                                 p_laus_aut_far_code IN VARCHAR2,
                                 p_laus_start_date IN DATE,
                                 p_laus_app_refno IN NUMBER)
RETURN BOOLEAN;
--
FUNCTION f_exists_address_usage_types(p_laus_aut_fao_code IN VARCHAR2,
                                      p_laus_aut_far_code IN VARCHAR2)
RETURN BOOLEAN;

FUNCTION app_refno_for_app_legacy_ref(p_app_legacy_ref IN VARCHAR2)
RETURN NUMBER;

FUNCTION f_exists_menref(p_lmrf_assessment_legacy_ref IN VARCHAR2)
RETURN BOOLEAN;

FUNCTION f_exists_medassref(p_lman_mrf_assessment_refno IN VARCHAR2,p_app_refno IN NUMBER)
RETURN BOOLEAN;

FUNCTION f_exists_refdate(p_lmrf_referral_date IN DATE,p_app_refno IN NUMBER)
RETURN BOOLEAN;

FUNCTION f_exists_app_refno(p_app_refno IN INTEGER)
RETURN BOOLEAN;


END  s_dl_hat_utils;
/

