CREATE OR REPLACE PACKAGE BODY s_dl_hat_utils AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Ver   WHO  WHEN        WHY
--    1.0            RJ   03/04/2001  Dataload
--    2.0   5.2.0    PJD  18/10/2002  Correction to f_app_received to use 
--                                    applications rather than
--                                    dl_hat_applications
--    3.0   5.4.0    PJD  07/11/2003  Added function f_exists_app_refno
--    4.0   5.4.0    MH   21/11/2003  Changed f_exists_interested_party
--                                    as cursor incorrect
--    4.1   5.5.0    PH   25/02/2004  Corrected compilation error.
--    4.2   5.5.0    MH   06/05/2004  Med Ref cursor comparing wrong value.
--    4.3   5.15.0   MB   03/12/2009  Added f_exists_hiaan, f_exists_hid  
--                                    and amended f_valid_quecat for HI ques
--    4.4   6.10     AJ   06/03/2015  Notes against f_exists_num and 
--                                    f_exists_char only
--    4.5   6.18     PJD  19/12/2018  Allow for names upto 250 chars in 
--                                    f_correspondence_name
--
-- ***********************************************************************


FUNCTION f_correspondence_name
(p_lapp_legacy_ref IN VARCHAR2
)
RETURN VARCHAR2 IS
--
correspondence_name VARCHAR2(40) DEFAULT NULL;

CURSOR c_correspondence_name IS
  SELECT SUBSTR(RTRIM(lpar_per_forename)||' '||RTRIM(lpar_per_surname),0,250)
  FROM   dl_hat_involved_parties
  WHERE  lapp_legacy_ref = p_lapp_legacy_ref;
--
BEGIN
--
OPEN  c_correspondence_name;
FETCH c_correspondence_name
INTO  correspondence_name;
CLOSE c_correspondence_name;
--
RETURN (correspondence_name);
--
END;

FUNCTION f_exists_tenacy_ref
       (p_tcy_alt_ref IN VARCHAR2) RETURN BOOLEAN
 IS
  CURSOR c_tcy_ref(p_tcy_alt_ref VARCHAR2) IS
  SELECT 'X'
  FROM  tenancies
  WHERE  tcy_alt_ref = p_tcy_alt_ref;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_tcy_ref(p_tcy_alt_ref);
    FETCH c_tcy_ref INTO l_exists;
    IF c_tcy_ref%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_tcy_ref;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_tenacy_ref;

FUNCTION f_exists_tenacy_ref
       (p_tcy_refno IN NUMBER) RETURN BOOLEAN
 IS

  CURSOR c_tcy_ref(p_tcy_refno NUMBER)    IS
  SELECT 'X'
  FROM  tenancies
  WHERE  tcy_refno = p_tcy_refno;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_tcy_ref(p_tcy_refno);
    FETCH c_tcy_ref INTO l_exists;
    IF c_tcy_ref%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_tcy_ref;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_tenacy_ref;

FUNCTION f_exists_applic_list_entries
       (p_lapp_legacy_ref IN VARCHAR2) RETURN BOOLEAN
 IS
  CURSOR c_alt_ref (p_lapp_legacy_ref VARCHAR2) IS
  SELECT 'X' FROM applic_list_entries
  WHERE ale_alt_ref = p_lapp_legacy_ref;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_alt_ref(p_lapp_legacy_ref);
    FETCH c_alt_ref INTO l_exists;
    IF c_alt_ref%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_alt_ref;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_applic_list_entries;

FUNCTION f_exists_dl_hat_inv_parties
       (p_lapp_legacy_ref IN VARCHAR2) RETURN BOOLEAN
 IS

 CURSOR c_lega_ref (p_lapp_legacy_ref VARCHAR2) IS
 SELECT 'X' FROM dl_hat_involved_parties
 WHERE lapp_legacy_ref = p_lapp_legacy_ref
 AND lipa_main_applicant_ind = 'Y';

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_lega_ref(p_lapp_legacy_ref);
    FETCH c_lega_ref INTO l_exists;
    IF c_lega_ref%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_lega_ref;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_dl_hat_inv_parties;

FUNCTION f_exists_interested_party
      (p_lipt_code IN VARCHAR2) RETURN BOOLEAN
      IS

 CURSOR c_ipt_code(p_lipt_code IN VARCHAR2) IS
SELECT 'X'
FROM interested_parties
WHERE ipp_shortname = p_lipt_code
  AND EXISTS (select null from interested_party_types
              WHERE ipt_code = ipp_ipt_code
                AND ipt_hrv_fiy_code = 'HLCW');


  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_ipt_code(p_lipt_code);
    FETCH c_ipt_code INTO l_exists;
    IF c_ipt_code%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_ipt_code;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_interested_party;

FUNCTION f_exists_application
       (p_lale_alt_ref IN VARCHAR2) RETURN BOOLEAN
 IS

 CURSOR c_app_lega_ref (p_lale_alt_ref VARCHAR2) IS
 SELECT 'X' FROM applications
 WHERE app_legacy_ref = p_lale_alt_ref;


  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_app_lega_ref (p_lale_alt_ref);
    FETCH c_app_lega_ref INTO l_exists;
    IF c_app_lega_ref%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_app_lega_ref;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_application;
--
FUNCTION f_exists_rld(p_lapp_legacy_ref IN VARCHAR2,
                      p_rli_code IN VARCHAR2,
                      p_rls_code IN VARCHAR2)
  RETURN BOOLEAN
  IS

  CURSOR c_als_ale_refno (p_lapp_legacy_ref IN VARCHAR2,p_rli_code VARCHAR2,
                      p_rls_code VARCHAR2) IS
  SELECT 'X' FROM applic_list_stage_decisions, applic_list_entries
  WHERE als_ale_app_refno = ale_app_refno
  AND   ale_alt_ref = p_lapp_legacy_ref
  AND   als_ale_rli_code = p_rli_code
  AND   als_rls_code = p_rls_code;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_als_ale_refno(p_lapp_legacy_ref,p_rli_code,p_rls_code);
    FETCH c_als_ale_refno INTO l_exists;
    IF c_als_ale_refno%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_als_ale_refno;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_rld;
--
--
FUNCTION f_exists_hid(p_lhin_instance_refno IN VARCHAR2,
                      p_rls_code IN VARCHAR2)
  RETURN BOOLEAN
  IS

  CURSOR c_hid (p_lhin_instance_refno IN VARCHAR2, p_rls_code VARCHAR2) IS
  SELECT 'X' FROM hless_ins_stage_decisions
  WHERE hid_hin_instance_refno = p_lhin_instance_refno
  AND   hid_rls_code = p_rls_code;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_hid(p_lhin_instance_refno,p_rls_code);
    FETCH c_hid INTO l_exists;
    IF c_hid%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_hid;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_hid;
--
--
FUNCTION f_exists_rlicode(p_rli_code IN VARCHAR2)
  RETURN BOOLEAN
IS
  CURSOR c_rli_code (p_rli_code VARCHAR2) IS
  SELECT 'X' FROM rehousing_lists
  WHERE rli_code = p_rli_code ;


  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_rli_code(p_rli_code);
    FETCH c_rli_code INTO l_exists;
    IF c_rli_code%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_rli_code;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_rlicode;

FUNCTION f_exists_rsdcode(p_rsd_code IN VARCHAR2)
  RETURN BOOLEAN
IS
  CURSOR c_rsd_code (p_rsd_code VARCHAR2) IS
  SELECT 'X' FROM rehousing_list_stage_decisions
  WHERE rsd_hrv_lsd_code = p_rsd_code ;


  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_rsd_code(p_rsd_code);
    FETCH c_rsd_code INTO l_exists;
    IF c_rsd_code%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_rsd_code;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_rsdcode;

FUNCTION f_application_type(p_lale_alt_ref IN VARCHAR2,p_rli_type IN VARCHAR2)
   RETURN BOOLEAN
   IS
  CURSOR c_hml_date (p_lale_alt_ref IN VARCHAR2) IS
  SELECT  app_expected_hless_date,
          app_tcy_refno FROM applications
  WHERE app_legacy_ref = p_lale_alt_ref;

  l_app_expected_hless_date applications.app_expected_hless_date %TYPE;
  l_app_tcy_refno  applications.app_tcy_refno %TYPE;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN c_hml_date(p_lale_alt_ref);
    FETCH c_hml_date INTO l_app_expected_hless_date,
                          l_app_tcy_refno;

    IF p_rli_type = 'H' AND l_app_expected_hless_date IS NOT NULL
    THEN
       l_result := TRUE;
    END IF;

    IF p_rli_type = 'S' AND l_app_expected_hless_date IS NULL
    THEN
       l_result := TRUE;
    END IF;

    IF p_rli_type = 'T'
    THEN
       l_result := f_exists_tenacy_ref(l_app_tcy_refno);
    END IF;

    CLOSE c_hml_date;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_application_type;

FUNCTION f_exists_auncode(p_aun_code IN VARCHAR2)
  RETURN BOOLEAN
IS
 CURSOR c_aun_code (p_aun_code VARCHAR2) IS
  SELECT 'X' FROM admin_units
  WHERE aun_code = p_aun_code ;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_aun_code(p_aun_code);
    FETCH c_aun_code INTO l_exists;
    IF c_aun_code%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_aun_code;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_auncode;

FUNCTION f_exists_auntype(p_aun_code IN VARCHAR2)
  RETURN BOOLEAN
IS
 CURSOR c_aun_code (p_aun_code VARCHAR2) IS
  SELECT 'X' FROM admin_units
  WHERE aun_code = p_aun_code
  AND  aun_auy_code = 'OFF';

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_aun_code(p_aun_code);
    FETCH c_aun_code INTO l_exists;
    IF c_aun_code%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_aun_code;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_auntype;

FUNCTION f_exists_applistentry(p_lleh_alt_ref IN VARCHAR2,
                               p_rli_code IN VARCHAR2)
  RETURN BOOLEAN
IS
 CURSOR c_ale (p_lleh_alt_ref IN VARCHAR2, p_rli_code VARCHAR2) IS
  SELECT 'X' FROM applic_list_entries
  WHERE ale_alt_ref  = p_lleh_alt_ref
  AND  ale_rli_code = p_rli_code;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_ale(p_lleh_alt_ref,p_rli_code);
    FETCH c_ale INTO l_exists;
    IF c_ale%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_ale;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_applistentry;

FUNCTION f_exists_lstcode(p_lst_code IN VARCHAR2)
RETURN BOOLEAN
IS
 CURSOR c_lst_code (p_lst_code VARCHAR2) IS
  SELECT 'X' FROM list_statuses
  WHERE  lst_code = p_lst_code;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_lst_code(p_lst_code);
    FETCH c_lst_code INTO l_exists;
    IF c_lst_code%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_lst_code;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_lstcode;

FUNCTION f_exists_dl_application(p_lipa_legacy_ref IN VARCHAR2)
RETURN BOOLEAN
IS
  CURSOR c_dl_app (p_lipa_legacy_ref VARCHAR2) IS
  SELECT 'X' FROM dl_hat_applications
  WHERE  lapp_legacy_ref = p_lipa_legacy_ref;

  CURSOR c_dl_hml_app (p_lipa_legacy_ref VARCHAR2) IS
  SELECT 'X' FROM dl_hat_hml_applications
  WHERE  lapp_legacy_ref = p_lipa_legacy_ref;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_dl_app(p_lipa_legacy_ref);
    FETCH c_dl_app INTO l_exists;
    IF c_dl_app%FOUND THEN
       l_result := TRUE;
    ELSE
       OPEN c_dl_hml_app (p_lipa_legacy_ref);
       FETCH c_dl_hml_app INTO l_exists;
       IF c_dl_hml_app%FOUND THEN
       l_result := TRUE;
       END IF;
       CLOSE c_dl_hml_app;
    END IF;
    CLOSE c_dl_app;
    RETURN(l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_dl_application;
--
FUNCTION f_app_received (p_lipa_legacy_ref IN VARCHAR2)
RETURN DATE
IS
  CURSOR c_rec_date (p_lipa_legacy_ref VARCHAR2) IS
  SELECT app_received_date 
    FROM applications
  WHERE  app_legacy_ref = p_lipa_legacy_ref;
 --
  l_result BOOLEAN := FALSE;
  l_received_date DATE := NULL;
BEGIN

    OPEN  c_rec_date(p_lipa_legacy_ref);
    FETCH c_rec_date INTO l_received_date;
    CLOSE c_rec_date;
    RETURN(l_received_date);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_app_received;

FUNCTION f_exists_genans(p_lgan_que_refno IN NUMBER,p_lapp_legacy_ref IN VARCHAR2)
RETURN BOOLEAN
IS

 CURSOR c_gan_refno (p_lgan_que_refno NUMBER,p_lapp_legacy_ref IN VARCHAR2) IS
 SELECT 'X' FROM general_answers,applications
 WHERE gan_que_refno = p_lgan_que_refno
 AND  gan_app_refno = app_refno
 AND app_legacy_ref = p_lapp_legacy_ref;


  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_gan_refno(p_lgan_que_refno,p_lapp_legacy_ref);
    FETCH c_gan_refno INTO l_exists;
    IF c_gan_refno%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_gan_refno;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_genans;
--

FUNCTION f_exists_hiaans(p_lhia_que_refno IN NUMBER,p_hin_instance_refno IN VARCHAR2)
RETURN BOOLEAN
IS

 CURSOR c_hia_refno (p_lhia_que_refno NUMBER,p_hin_instance_refno IN VARCHAR2) IS
 SELECT 'X' FROM hless_ins_answers
 WHERE hia_que_refno = p_lhia_que_refno
 AND  hia_hin_instance_refno = p_hin_instance_refno;


  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_hia_refno(p_lhia_que_refno,p_hin_instance_refno);
    FETCH c_hia_refno INTO l_exists;
    IF c_hia_refno%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_hia_refno;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_hiaans;
--
FUNCTION f_exists_quenum(p_que_refno IN NUMBER)
RETURN BOOLEAN
IS

 CURSOR c_que_refno (p_que_refno NUMBER) IS
 SELECT 'X' FROM questions
 WHERE que_refno = p_que_refno;


  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_que_refno(p_que_refno);
    FETCH c_que_refno INTO l_exists;
    IF c_que_refno%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_que_refno;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_quenum;
--
FUNCTION f_valid_quecat(p_que_refno IN NUMBER,p_ans_type IN VARCHAR2)
RETURN BOOLEAN
  IS

 CURSOR c_que_cat (p_que_refno NUMBER) IS
 SELECT que_qgr_question_category FROM questions
 WHERE que_refno = p_que_refno;


  l_que_qgr_question_category questions.que_qgr_question_category%TYPE;
  l_result BOOLEAN := FALSE;
BEGIN
    -- p_ans_type can be G for General Answers
    -- I for Involved party answers
    -- M for Medical answers
    -- L for letting area answers
	-- H for Hless ins answers

    OPEN  c_que_cat(p_que_refno);
    FETCH c_que_cat INTO l_que_qgr_question_category;
    IF c_que_cat%FOUND THEN

       IF l_que_qgr_question_category IN ('CC','HR','OS','GA','SG','MD','DQ')
       AND p_ans_type = 'G'
       THEN
          l_result := TRUE;
       END IF;

       IF l_que_qgr_question_category = 'IP'
       AND p_ans_type = 'I'
       THEN
          l_result := TRUE;
       END IF;

       IF l_que_qgr_question_category = 'MA'
       AND p_ans_type = 'M'
       THEN
          l_result := TRUE;
       END IF;

       IF l_que_qgr_question_category = 'AP'
       AND p_ans_type = 'L'
       THEN
          l_result := TRUE;
       END IF;
	   
		IF l_que_qgr_question_category = 'HI'
       AND p_ans_type = 'H'
       THEN
          l_result := TRUE;
       END IF;

    END IF;
    CLOSE c_que_cat;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_valid_quecat;

FUNCTION f_exists_qor(p_qor_code IN VARCHAR2)
RETURN BOOLEAN
IS
CURSOR c_qor_code (p_qor_code VARCHAR2) IS
 SELECT 'X' FROM  question_optional_responses
 WHERE qor_code = p_qor_code;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_qor_code(p_qor_code);
    FETCH c_qor_code INTO l_exists;
    IF c_qor_code%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_qor_code;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_qor;
--
-- Column QPR_CURRENT_IND added to question_permitted_responses at v6.10
-- Not needed in check AJ 06Mar2015
--
FUNCTION f_exists_char(p_que_refno IN NUMBER,p_char_value IN VARCHAR2)
RETURN BOOLEAN
IS
 CURSOR c_datatype (p_que_refno NUMBER) IS
 SELECT que_datatype FROM questions
 WHERE que_refno = p_que_refno;

 CURSOR c_char_value (p_que_refno NUMBER,p_char_value VARCHAR2) IS
 SELECT 'X' FROM question_permitted_responses
 WHERE qpr_que_refno = p_que_refno
 AND qpr_code = p_char_value;

  l_exists VARCHAR2(1) := NULL;
  l_datatype questions.que_datatype%TYPE;
  l_result BOOLEAN := FALSE;
BEGIN
    OPEN  c_datatype(p_que_refno);
    FETCH c_datatype INTO l_datatype;
    IF c_datatype%FOUND THEN
       OPEN c_char_value(p_que_refno,p_char_value);
       FETCH c_char_value INTO l_exists;
       IF c_char_value%FOUND AND l_datatype = 'C' THEN
       l_result := TRUE;
       END IF;
       IF l_datatype != 'C' THEN
       l_result := TRUE;
       END IF;
       CLOSE c_char_value;
    END IF;
    CLOSE c_datatype;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_char;
--
-- Column QPR_CURRENT_IND added to question_permitted_responses at v6.10
-- Not needed in check AJ 06Mar2015
--
FUNCTION f_exists_num(p_que_refno IN NUMBER,p_number_value IN NUMBER)
RETURN BOOLEAN
IS
 CURSOR c_numtype (p_que_refno NUMBER) IS
 SELECT que_datatype FROM questions
 WHERE que_refno = p_que_refno;

 CURSOR c_num_value (p_que_refno NUMBER) IS
 SELECT qpr_minval,qpr_maxval FROM question_permitted_responses
 WHERE qpr_que_refno = p_que_refno;

  l_qpr_minval question_permitted_responses.qpr_minval%TYPE;
  l_qpr_maxval question_permitted_responses.qpr_maxval%TYPE;
  l_exists VARCHAR2(1) := NULL;
  l_datatype questions.que_datatype%TYPE;
  l_result BOOLEAN := FALSE;
  l_count NUMBER := 0;
  -- l_count is used to find out if a record exists on
  -- question_permitted_responses
BEGIN
    OPEN  c_numtype(p_que_refno);
    FETCH c_numtype INTO l_datatype;
    IF c_numtype%FOUND
    THEN
       FOR p1 IN c_num_value(p_que_refno) LOOP
       BEGIN
           IF p_number_value BETWEEN p1.qpr_minval AND p1.qpr_maxval
           THEN
              l_result := TRUE;
           END IF;
           l_count := l_count + 1;
       END;
       END LOOP;

        IF l_count = 0 AND l_datatype = 'N'
        THEN
           l_result := TRUE;
        END IF;
    END IF;
    CLOSE c_numtype;
    RETURN( l_result);
EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_num;

FUNCTION f_exists_invpans(p_lpar_per_alt_ref IN VARCHAR2 ,p_lipn_que_refno IN NUMBER)
RETURN BOOLEAN
IS
 CURSOR c_ipn_refno (p_lpar_per_alt_ref VARCHAR2,p_lipn_que_refno NUMBER) IS
 SELECT 'X' FROM involved_party_answers, involved_parties, parties
 WHERE par_per_alt_ref = p_lpar_per_alt_ref
 AND  ipn_que_refno = p_lipn_que_refno
 AND  ipa_par_refno = par_refno
 AND  ipn_ipa_refno = ipa_refno;


  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_ipn_refno(p_lpar_per_alt_ref,p_lipn_que_refno);
    FETCH c_ipn_refno INTO l_exists;
    IF c_ipn_refno%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_ipn_refno;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_invpans;
--
FUNCTION f_exists_invpar(p_lpar_per_alt_ref IN VARCHAR2)
RETURN BOOLEAN
IS
 CURSOR c_ipa_par_refno (p_lpar_per_alt_ref VARCHAR2) IS
 SELECT 'X' FROM involved_parties, parties
 WHERE par_per_alt_ref = p_lpar_per_alt_ref
 AND   par_refno = ipa_par_refno;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_ipa_par_refno(p_lpar_per_alt_ref);
    FETCH c_ipa_par_refno INTO l_exists;
    IF c_ipa_par_refno%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_ipa_par_refno;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_invpar;

FUNCTION f_exists_menans(p_lman_que_refno IN NUMBER,
                         p_lman_mrf_assessment_refno IN VARCHAR2)
RETURN BOOLEAN
IS
CURSOR c_med_ans (p_lman_que_refno NUMBER,
                  p_lman_mrf_assessment_refno VARCHAR2 ) IS
 SELECT 'X' FROM medical_answers
 WHERE man_que_refno = p_lman_que_refno
 AND   man_mrf_assessment_refno = p_lman_mrf_assessment_refno;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_med_ans (p_lman_que_refno,
                     p_lman_mrf_assessment_refno);
    FETCH c_med_ans INTO l_exists;
    IF c_med_ans%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_med_ans;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_menans;

FUNCTION f_exists_letans(p_llaa_lar_code IN VARCHAR2,p_lapp_legacy_ref IN VARCHAR2,
  p_llaa_que_refno IN NUMBER)
RETURN BOOLEAN
IS
CURSOR c_let_ans (p_llaa_lar_code VARCHAR2,
                  p_lapp_legacy_ref VARCHAR2,p_llaa_que_refno NUMBER) IS
 SELECT 'X' FROM lettings_area_answers, applications
 WHERE laa_lar_code = p_llaa_lar_code
 AND   laa_que_refno = p_llaa_que_refno
 AND   app_refno = laa_app_refno
 AND   app_legacy_ref = p_lapp_legacy_ref;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_let_ans (p_llaa_lar_code,p_lapp_legacy_ref,p_llaa_que_refno);
    FETCH c_let_ans INTO l_exists;
    IF c_let_ans%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_let_ans;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_letans;

FUNCTION f_valid_larcode(p_llaa_lar_code IN VARCHAR2)
RETURN BOOLEAN
IS
 CURSOR c_lar_code (p_llaa_lar_code VARCHAR2) IS
 SELECT 'X' FROM lettings_areas
 WHERE lar_code = p_llaa_lar_code;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_lar_code(p_llaa_lar_code);
    FETCH c_lar_code INTO l_exists;
    IF c_lar_code%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_lar_code;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_valid_larcode;

FUNCTION f_valid_child_larcode(p_llaa_lar_code IN VARCHAR2)
RETURN BOOLEAN
IS
 CURSOR c_child_lar_code (p_llaa_lar_code VARCHAR2) IS
 SELECT 'X' FROM lettings_areas
 WHERE lar_code = p_llaa_lar_code
 AND  lar_type = 'C';

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_child_lar_code(p_llaa_lar_code);
    FETCH c_child_lar_code INTO l_exists;
    IF c_child_lar_code%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_child_lar_code;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_valid_child_larcode;

FUNCTION f_dates_required(p_lale_lst_code IN VARCHAR2)
RETURN VARCHAR2
IS
 CURSOR c_date_req (p_lale_lst_code VARCHAR2) IS
 SELECT lst_date_req_ind FROM list_statuses
 WHERE lst_code = p_lale_lst_code;

  l_lst_date_req_ind VARCHAR2(1) := NULL;
  --l_result BOOLEAN := FALSE;
  l_result VARCHAR2(1) := NULL;

BEGIN
    -- lst_date_req_ind = 'P' Then need both start and review date
    -- lst_date_req_ind = 'S' Then need start date only
    OPEN  c_date_req(p_lale_lst_code);
    FETCH c_date_req INTO l_lst_date_req_ind;
    IF c_date_req%FOUND THEN
       IF l_lst_date_req_ind = 'P' THEN
        l_result := 'P';
       END IF;
       IF l_lst_date_req_ind = 'S' THEN
        l_result := 'S';
       END IF;
    END IF;
    CLOSE c_date_req;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_dates_required;

FUNCTION f_exists_address_usages(p_laus_aut_fao_code IN VARCHAR2,
                                 p_laus_aut_far_code IN VARCHAR2,
                                 p_laus_start_date IN DATE,
                                 p_laus_app_refno IN NUMBER)
RETURN BOOLEAN
IS

CURSOR c_add_usa (p_laus_aut_fao_code VARCHAR2,
                  p_laus_aut_far_code VARCHAR2,
                  p_laus_start_date DATE,
                  p_laus_app_refno NUMBER) IS
 SELECT 'X' FROM address_usages
 WHERE aus_aut_fao_code = p_laus_aut_fao_code
 AND   aus_aut_far_code = p_laus_aut_far_code
 AND   aus_start_date =   p_laus_start_date
 AND   aus_app_refno =  p_laus_app_refno;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_add_usa(p_laus_aut_fao_code,
                    p_laus_aut_far_code,
                    p_laus_start_date,
                    p_laus_app_refno);
    FETCH c_add_usa INTO l_exists;
    IF c_add_usa%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_add_usa;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_address_usages;

FUNCTION f_exists_address_usage_types(p_laus_aut_fao_code IN VARCHAR2,
                                      p_laus_aut_far_code IN VARCHAR2)
RETURN BOOLEAN
IS
 CURSOR c_add_usa_typ (p_laus_aut_fao_code VARCHAR2,
                       p_laus_aut_far_code VARCHAR2) IS
 SELECT 'X' FROM address_usage_types
 WHERE  aut_fao_code = p_laus_aut_fao_code
 AND    aut_far_code = p_laus_aut_far_code;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_add_usa_typ(p_laus_aut_fao_code,
                       p_laus_aut_far_code);
    FETCH c_add_usa_typ INTO l_exists;
    IF c_add_usa_typ%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_add_usa_typ;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_address_usage_types;

FUNCTION app_refno_for_app_legacy_ref(p_app_legacy_ref IN VARCHAR2)
RETURN NUMBER
IS
 CURSOR c_app_swap (p_app_legacy_ref VARCHAR2) IS
 SELECT app_refno FROM applications
 WHERE app_legacy_ref = p_app_legacy_ref;

  l_app_refno applications.app_refno%TYPE;
BEGIN

    OPEN  c_app_swap(p_app_legacy_ref);
    FETCH c_app_swap INTO l_app_refno;
    CLOSE c_app_swap;
    RETURN (l_app_refno);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END app_refno_for_app_legacy_ref;

FUNCTION f_exists_menref(p_lmrf_assessment_legacy_ref IN VARCHAR2)
RETURN BOOLEAN
IS
CURSOR c_med_ref (p_lmrf_assessment_legacy_ref VARCHAR2) IS
 SELECT 'X' FROM medical_referrals
 WHERE mrf_assessment_refno = p_lmrf_assessment_legacy_ref;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_med_ref (p_lmrf_assessment_legacy_ref);
    FETCH c_med_ref INTO l_exists;
    IF c_med_ref%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_med_ref;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_menref;

FUNCTION f_exists_medassref(p_lman_mrf_assessment_refno IN VARCHAR2,p_app_refno IN NUMBER)
RETURN BOOLEAN
IS
CURSOR c_med_assref (p_lman_mrf_assessment_refno VARCHAR2,p_app_refno NUMBER) IS
 SELECT 'X' FROM medical_referrals
 WHERE mrf_assessment_refno = p_lman_mrf_assessment_refno
 AND   mrf_app_refno = p_app_refno;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_med_assref (p_lman_mrf_assessment_refno,p_app_refno);
    FETCH c_med_assref INTO l_exists;
    IF c_med_assref%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_med_assref;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_medassref;

FUNCTION f_exists_refdate(p_lmrf_referral_date IN DATE,p_app_refno IN NUMBER)
RETURN BOOLEAN
IS
CURSOR c_med_refdate (p_lmrf_referral_date DATE,p_app_refno NUMBER) IS
 SELECT 'X' FROM medical_referrals
 WHERE  mrf_referral_date = p_lmrf_referral_date
 AND   mrf_app_refno = p_app_refno;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;
BEGIN

    OPEN  c_med_refdate (p_lmrf_referral_date,p_app_refno);
    FETCH c_med_refdate INTO l_exists;
    IF c_med_refdate%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_med_refdate;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_refdate;

FUNCTION f_exists_app_refno
       (p_app_refno IN INTEGER) RETURN BOOLEAN
 IS

  CURSOR c_app_refno(p_app_refno NUMBER)    IS
  SELECT 'X'
  FROM  applications
  WHERE  app_refno = p_app_refno;

  l_exists VARCHAR2(1) := NULL;
  l_result BOOLEAN := FALSE;

BEGIN

    OPEN  c_app_refno(p_app_refno);
    FETCH c_app_refno INTO l_exists;
    IF c_app_refno%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_app_refno;
    RETURN( l_result);
  EXCEPTION
   WHEN   OTHERS THEN
     fsc_utils.handle_exception;
END f_exists_app_refno;


END s_dl_hat_utils;
/

show errors


