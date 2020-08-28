CREATE OR REPLACE PACKAGE BODY s_dl_hat_involved_parties
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Ver WHO  WHEN       WHY
--    1.0          RJ   03/04/2001  Dataload
--    1.1   5.1.6  PJD  22/05/2002  Corrected validation to use 
--                                  f_exists_application 
--    2.0   5.2.0  PJD  13/09/2002  Now creates Contact Details
--
--    3.0   5.3.0  PH   08/01/2003  Changed validate. Ethnic uses
--                                  domain ETHNIC2 and Geographic
--                                  uses ETHNIC.
--    3.1   5.3.0  SB   20/03/2003  Added commit on validate.
--    3.2   5.3.0  SB   25/03/2003  Amended gender, language
--                                  and Ethnicity validate.
--    3.3   5.3.0  PH   16/09/03    Amended Update to dl table on
--                                  create. (Changed p1.lpar_upd_ins_ind
--                                  to l_ins_upd_ind)
--    3.4   5.3.0  PH   16/09/03    Amended code as par_per_other_name is
--                                  fro Revs and Bens only. Changed to 
--                                  par_per_hou_surname_prefix.
--    3.5   5.6.0  PH   09/11/04    Amended validate on oap and disabled
--                                  indicators, did not allow null, does now.
--
--    3.6   5.9.0  VRS  16/01/2006  Removed reference to CDE_TCY_REFNO from contact_details
--    3.7   5.10.0 PH   20/07/2006  Moved delete from parties to after
--                                  delete from involved_parties
--    4.0   5.12.0 PH   09/08/2007  Added FSC to par_refno_seq as the same sequence
--                                  owned by HOU has appeared in 5.12
--    4.1   5.13.0 PH   06-FEB-2008 Now includes its own 
--                                  set_record_status_flag procedure.
--    4.2   6.1.1  MB   27-JUL-2010 Addition of new fields and associated validation
--    ****************  BESPOKE For HNB Allocations Migration  ***************
--    4.3   6.14   AJ   23-JAN-2018 Added ipa_legacy_ref and lipa_del_par_refno for delete
--                                  of party record if created by this process
--    4.4   6.14   AJ   26-JAN-2018 Further slight changes done around extra info collectedAdded
--                                  and removed also changes to validate and create during testing
--    4.5   6.14   AJ   12-FEB-2018 default set to N for Head of hhold indicator not supplied
--    4.6   6.14   AJ   13-FEB-2018 added missing coma at line 115
--    4.7   6.20   PL   11-AUG-2020 Allow PAR in Alt Ref
-- ***********************************************************************
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hat_involved_parties
  SET lipa_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_involved_parties');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
--  declare package variables AND constants
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2
,p_date              IN DATE    )
AS
--
CURSOR c1 IS
SELECT
     ROWID rec_rowid,
      LIPA_DLB_BATCH_ID,
      LIPA_DL_SEQNO,
      LIPA_DL_LOAD_STATUS,
      LIPA_JOINT_APPL_IND,
      LIPA_LIVING_APART_IND,
      LIPA_REHOUSE_IND,
      LIPA_MAIN_APPLICANT_IND,
      LIPA_CREATED_DATE,
      LIPA_CREATED_BY,
      LIPA_START_DATE,
      LIPA_END_DATE,
      LIPA_GROUPNO,
      LIPA_ACT_ROOMNO,
      LIPA_HRV_HPER_CODE,
      LIPA_HRV_REL_CODE,
      LPAR_TYPE,
      LPAR_CREATED_DATE,
      LPAR_CREATED_BY,
      LPAR_PER_SURNAME,
      LPAR_PER_FORENAME,
      LPAR_PER_INITIALS,
      LPAR_PER_TITLE,
      LPAR_PER_ALT_REF,
      LPAR_PER_DATE_OF_BIRTH,
      LPAR_PER_HOU_HRV_HMS_CODE,
      LPAR_PER_HOU_HRV_HGO_CODE,
      LPAR_UPD_INS_IND,
      LPAR_PER_HOU_OAP_IND,
      LPAR_PER_HOU_DISABLED_IND,
      LPAR_PER_FRV_FGE_CODE,
      LPAR_PHONE_NO,
      LPAR_PER_FRV_FEO_CODE,
      LPAR_PER_FRV_FNL_CODE,
      LPAR_PER_NI_NO,
      LPAR_PER_HOU_SURNAME_PREFIX,
	  nvl(LPAR_PER_HOU_AT_RISK_IND,'N') LPAR_PER_HOU_AT_RISK_IND,
	  LPAR_PER_HOU_HRV_NTLY_CODE,
	  LPAR_PER_HOU_HRV_SEXO_CODE,
	  LPAR_PER_HOU_HRV_RLGN_CODE,
	  LPAR_PER_HOU_HRV_ECST_CODE,
      LAPP_LEGACY_REF,
	  nvl(LIPA_HEAD_HHOLD_IND,'N') LIPA_HEAD_HHOLD_IND,
      LIPA_HHOLD_GROUP_NO,
      LIPA_MODIFIED_DATE,
      LIPA_MODIFIED_BY,
      LIPA_LEGACY_REF,
      LIPA_DEL_IPA_REFNO,
      LIPA_DEL_PAR_REFNO,
      LIPA_DEL_APP_REFNO,
      LIPA_DEL_CDE_REFNO
FROM  dl_hat_involved_parties
WHERE lipa_dlb_batch_id   = p_batch_id
AND   lipa_dl_load_status = 'V';
-- AND   rownum < 10;
--
-- *****************************
--
CURSOR c2 (p_lpar_per_alt_ref VARCHAR2) IS
SELECT par_refno
FROM parties
WHERE par_per_alt_ref = p_lpar_per_alt_ref
OR   'PAR'||par_refno = p_lpar_per_alt_ref;
-- *****************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_INVOLVED_PARTIES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_an_tab          VARCHAR2(1);
i                 INTEGER := 0;
l_app_refno       applications.app_refno%TYPE;
l_ipa_par_refno   involved_parties.ipa_par_refno%TYPE;
l_ins_upd_ind     VARCHAR2(3);
l_par_per_alt_ref parties.par_per_alt_ref%TYPE;
l_ipa_refno       involved_parties.ipa_refno%TYPE;
l_cde_refno       contact_details.cde_refno%TYPE;
--
-- *****************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_involved_parties.dataload_create');
fsc_utils.debug_message( 's_dl_hat_involved_parties.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lipa_dl_seqno;
  l_id := p1.rec_rowid;
--
-- Check if the party alt reference exists
-- if not create a new parties
-- record. Otherwise update the existing record.
-- LPAR_UPD_INS_IND  is an indicator used to find
-- out whether a new party was created or an
-- existing one was updated
-- I means Inserted U means Updated
-- Used for the Delete procedure

  l_ipa_par_refno := NULL;
  l_app_refno := NULL;
  l_par_per_alt_ref := NULL;
  l_ipa_refno := NULL;
  l_cde_refno := NULL;
--
  IF (p1.lpar_per_alt_ref IS NOT NULL)
   THEN
--
-- Get the par_refno
--
    OPEN  c2(p1.lpar_per_alt_ref);
    FETCH c2 INTO l_ipa_par_refno;
    CLOSE c2;
  END IF;
--
  IF (l_ipa_par_refno IS NULL)
   THEN
    SELECT par_refno_seq.nextval
    INTO l_ipa_par_refno
    FROM dual;
	
    l_par_per_alt_ref:= 'IPADL'||l_ipa_par_refno;

    INSERT INTO PARTIES(PAR_REFNO,
                        PAR_TYPE,
                        PAR_CREATED_DATE,
                        PAR_CREATED_BY,
                        PAR_REUSABLE_REFNO,
                        PAR_PER_SURNAME,
                        PAR_PER_FORENAME,
                        PAR_PER_INITIALS,
                        PAR_PER_HOU_SURNAME_PREFIX,
                        PAR_PER_ALT_REF,
                        PAR_PER_DATE_OF_BIRTH,
                        PAR_PER_TITLE,
                        PAR_PER_NI_NO,
                        PAR_PER_FRV_FNL_CODE,
                        PAR_PER_FRV_FGE_CODE,
                        PAR_PER_FRV_FEO_CODE,
                        PAR_PER_HOU_DISABLED_IND,
                        PAR_PER_HOU_OAP_IND,
                        PAR_PER_HOU_HRV_HMS_CODE,
                        PAR_PER_HOU_HRV_HGO_CODE,
						PAR_PER_HOU_AT_RISK_IND,
						PAR_PER_HOU_HRV_NTLY_CODE,
						PAR_PER_HOU_HRV_SEXO_CODE,
						PAR_PER_HOU_HRV_RLGN_CODE,
						PAR_PER_HOU_HRV_ECST_CODE)
    VALUES
                        (l_ipa_par_refno,
                         -- P1.LPAR_TYPE,
                         'HOUP',
                         P1.LPAR_CREATED_DATE,
                         P1.LPAR_CREATED_BY,
                         REUSABLE_REFNO_SEQ.nextval,
                         P1.LPAR_PER_SURNAME,
                         P1.LPAR_PER_FORENAME,
                         P1.LPAR_PER_INITIALS,
                         P1.LPAR_PER_HOU_SURNAME_PREFIX,
                         NVL(P1.LPAR_PER_ALT_REF,l_par_per_alt_ref),
                         P1.LPAR_PER_DATE_OF_BIRTH,
                         P1.LPAR_PER_TITLE,
                         P1.LPAR_PER_NI_NO,
                         P1.LPAR_PER_FRV_FNL_CODE,
                         P1.LPAR_PER_FRV_FGE_CODE,
                         P1.LPAR_PER_FRV_FEO_CODE,
                         P1.LPAR_PER_HOU_DISABLED_IND,
                         P1.LPAR_PER_HOU_OAP_IND,
                         P1.LPAR_PER_HOU_HRV_HMS_CODE,
                         P1.LPAR_PER_HOU_HRV_HGO_CODE,
						 p1.LPAR_PER_HOU_AT_RISK_IND,
						 p1.LPAR_PER_HOU_HRV_NTLY_CODE,
						 p1.LPAR_PER_HOU_HRV_SEXO_CODE,
						 p1.LPAR_PER_HOU_HRV_RLGN_CODE,
						 p1.LPAR_PER_HOU_HRV_ECST_CODE);
--
          l_ins_upd_ind := 'I';
--
  ELSE -- Update existing record
--
     UPDATE PARTIES
     SET par_modified_date  = sysdate
     ,   par_modified_by    = user
     ,   par_per_forename   = nvl(par_per_forename,p1.lpar_per_forename)
     ,   par_per_initials   = nvl(par_per_initials,p1.lpar_per_initials)
     ,   par_per_hou_surname_prefix = nvl(par_per_hou_surname_prefix,p1.lpar_per_hou_surname_prefix)
     ,   par_per_date_of_birth =
                         nvl(par_per_date_of_birth,p1.lpar_per_date_of_birth)
     ,   par_per_title      = nvl(par_per_title,p1.lpar_per_title)
     ,   par_per_ni_no      = nvl(par_per_ni_no,p1.lpar_per_ni_no)
     ,   par_per_frv_fnl_code  =
                   nvl(par_per_frv_fnl_code,p1.lpar_per_frv_fnl_code)
     ,   par_per_frv_fge_code  =
                   nvl(par_per_frv_fge_code,p1.lpar_per_frv_fge_code)
     ,   par_per_frv_feo_code  =
                     nvl(par_per_frv_feo_code,p1.lpar_per_frv_feo_code)
     ,   par_per_hou_disabled_ind =
                     nvl(par_per_hou_disabled_ind,p1.lpar_per_hou_disabled_ind)
     ,   par_per_hou_oap_ind   =
                     nvl(par_per_hou_oap_ind,p1.lpar_per_hou_oap_ind)
     ,   par_per_hou_hrv_hms_code =
                     nvl(par_per_hou_hrv_hms_code,p1.lpar_per_hou_hrv_hms_code)
     ,   par_per_hou_hrv_hgo_code =
                     nvl(par_per_hou_hrv_hgo_code,p1.lpar_per_hou_hrv_hgo_code)
     ,   par_per_hou_hrv_hpe_code =
                    nvl(par_per_hou_hrv_hpe_code,NULL)
     ,   par_per_hou_at_risk_ind   =
                    nvl(par_per_hou_at_risk_ind, p1.lpar_per_hou_at_risk_ind)
     ,   par_per_hou_hrv_ntly_code =
                    nvl(par_per_hou_hrv_ntly_code, p1.lpar_per_hou_hrv_ntly_code)
     ,   par_per_hou_hrv_sexo_code =
                    nvl(par_per_hou_hrv_sexo_code, p1.lpar_per_hou_hrv_sexo_code)
     ,   par_per_hou_hrv_rlgn_code =
                    nvl(par_per_hou_hrv_rlgn_code, p1.lpar_per_hou_hrv_rlgn_code)
     ,   par_per_hou_hrv_ecst_code =
                    nvl(par_per_hou_hrv_ecst_code, p1.lpar_per_hou_hrv_ecst_code)
     WHERE par_per_alt_ref = p1.lpar_per_alt_ref;
--
     l_ins_upd_ind := 'U';
--
  END IF;
--
-- Get the application reference number.
--
  l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lapp_legacy_ref);
--
    SELECT ipa_refno_seq.nextval
    INTO l_ipa_refno
    FROM dual;
--
--
  INSERT INTO involved_parties
          (IPA_REFNO,
           IPA_APP_REFNO,
           IPA_PAR_REFNO,
           IPA_JOINT_APPL_IND,
           IPA_LIVING_APART_IND,
           IPA_REHOUSE_IND,
           IPA_MAIN_APPLICANT_IND,
           IPA_CREATED_DATE,
           IPA_CREATED_BY,
           IPA_START_DATE,
           IPA_END_DATE,
           IPA_GROUPNO,
           IPA_ACT_ROOMNO,
           IPA_LEGACY_REF,
           IPA_MODIFIED_DATE,
           IPA_MODIFIED_BY,
           IPA_HRV_HPER_CODE,
           IPA_HRV_REL_CODE,
           IPA_HEAD_HHOLD_IND,
           IPA_HHOLD_GROUP_NO)
  VALUES
          (l_ipa_refno,
           l_app_refno,
           l_ipa_par_refno,
           p1.LIPA_JOINT_APPL_IND,
           p1.LIPA_LIVING_APART_IND,
           p1.LIPA_REHOUSE_IND,
           p1.LIPA_MAIN_APPLICANT_IND,
           p1.LIPA_CREATED_DATE,
           p1.LIPA_CREATED_BY,
           p1.LIPA_START_DATE,
           p1.LIPA_END_DATE,
           p1.LIPA_GROUPNO,
           p1.LIPA_ACT_ROOMNO,
           p1.LIPA_LEGACY_REF,
           p1.LIPA_MODIFIED_DATE,
           p1.LIPA_MODIFIED_BY,
           p1.LIPA_HRV_HPER_CODE,
           p1.LIPA_HRV_REL_CODE,
           p1.LIPA_HEAD_HHOLD_IND,
           p1.LIPA_HHOLD_GROUP_NO);
--
-- Do the insert into contact_details
--
  IF p1.lpar_phone_no IS NOT NULL
   THEN
--
    SELECT cde_refno.nextval
    INTO l_cde_refno
    FROM dual;
--
    INSERT INTO contact_details
     (CDE_REFNO 
     ,CDE_START_DATE
     ,CDE_CREATED_DATE
     ,CDE_CREATED_BY
     ,CDE_CONTACT_VALUE
     ,CDE_FRV_CME_CODE
     ,CDE_CONTACT_NAME
     ,CDE_END_DATE
     ,CDE_PRO_REFNO
     ,CDE_AUN_CODE
     ,CDE_PAR_REFNO
     ,CDE_BDE_REFNO
     ,CDE_COS_CODE
     ,CDE_CSE_CONTACT
     ,CDE_SRQ_NO)
  VALUES
     (l_cde_refno
     ,p1.lipa_start_date
     ,trunc(sysdate)
     ,'DATALOAD'
     ,p1.lpar_phone_no
     ,'TELEPHONE'
     ,null
     ,null
     ,null
     ,null
     ,l_ipa_par_refno
     ,null
     ,null
     ,null
     ,null);
--
  END IF;
--
-- *****************************
--
-- keep a count of the rows processed and commit after every 1000
--
  i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
  s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
  set_record_status_flag(l_id,'C');
--
-- update DL table with references for checking and help delete process
--
  UPDATE dl_hat_involved_parties
  SET   lpar_upd_ins_ind = l_ins_upd_ind
  ,     lipa_del_ipa_refno = l_ipa_refno
  ,     lipa_del_par_refno = l_ipa_par_refno
  ,     lipa_del_app_refno = l_app_refno
  ,     lipa_del_cde_refno = l_cde_refno
  WHERE lipa_dlb_batch_id = cb
    AND lipa_dl_seqno = cs
    AND rowid = l_id;
--
-- set to party alt ref created by data load in parties table
-- only done if par alt ref NOT supplied 'IPADL||par_refno'
--
  IF( p1.lpar_per_alt_ref IS NULL  AND l_ins_upd_ind = 'I')
   THEN
    UPDATE dl_hat_involved_parties
    SET   lpar_per_alt_ref = l_par_per_alt_ref
    WHERE lipa_dlb_batch_id = cb
      AND lipa_dl_seqno = cs
      AND rowid = l_id;
  END IF;
--
-- not done this way any more
-- s_dl_utils.set_record_status_flag(ct,cb,cs,'C');
--
  EXCEPTION
   WHEN OTHERS THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    set_record_status_flag(l_id,'O');
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
  END;
--
 END LOOP;
--
-- *****************************
--
-- Section to analyse the tables populated with this dataload

l_an_tab := s_dl_hem_utils.dl_comp_stats('INVOLVED_PARTIES');
l_an_tab := s_dl_hem_utils.dl_comp_stats('PARTIES');
l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HAT_INVOLVED_PARTIES');
l_an_tab := s_dl_hem_utils.dl_comp_stats('CONTACT_DETAILS');

fsc_utils.proc_end;
COMMIT;
--
  EXCEPTION
     WHEN OTHERS THEN
     s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
     RAISE;
--
END dataload_create;
--
-- ***********************************************************************
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2
     ,p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
     ROWID rec_rowid,
      LIPA_DLB_BATCH_ID,
      LIPA_DL_SEQNO,
      LIPA_DL_LOAD_STATUS,
      LIPA_JOINT_APPL_IND,
      LIPA_LIVING_APART_IND,
      LIPA_REHOUSE_IND,
      LIPA_MAIN_APPLICANT_IND,
      LIPA_CREATED_DATE,
      LIPA_CREATED_BY,
      LIPA_START_DATE,
      LIPA_END_DATE,
      LIPA_GROUPNO,
      LIPA_ACT_ROOMNO,
      LIPA_HRV_HPER_CODE,
      LIPA_HRV_REL_CODE,
      LPAR_TYPE,
      LPAR_CREATED_DATE,
      LPAR_CREATED_BY,
      LPAR_PER_SURNAME,
      LPAR_PER_FORENAME,
      LPAR_PER_INITIALS,
      LPAR_PER_TITLE,
      LPAR_PER_ALT_REF,
      LPAR_PER_DATE_OF_BIRTH,
      LPAR_PER_HOU_HRV_HMS_CODE,
      LPAR_PER_HOU_HRV_HGO_CODE,
      LPAR_UPD_INS_IND,
      LPAR_PER_HOU_OAP_IND,
      LPAR_PER_HOU_DISABLED_IND,
      LPAR_PER_FRV_FGE_CODE,
      LPAR_PHONE_NO,
      LPAR_PER_FRV_FEO_CODE,
      LPAR_PER_FRV_FNL_CODE,
      LPAR_PER_NI_NO,
      LPAR_PER_HOU_SURNAME_PREFIX,
	  nvl(LPAR_PER_HOU_AT_RISK_IND,'N') LPAR_PER_HOU_AT_RISK_IND,
	  LPAR_PER_HOU_HRV_NTLY_CODE,
	  LPAR_PER_HOU_HRV_SEXO_CODE,
	  LPAR_PER_HOU_HRV_RLGN_CODE,
	  LPAR_PER_HOU_HRV_ECST_CODE,
      LAPP_LEGACY_REF,
      nvl(LIPA_HEAD_HHOLD_IND,'N') LIPA_HEAD_HHOLD_IND,
      LIPA_HHOLD_GROUP_NO,
      LIPA_MODIFIED_DATE,
      LIPA_MODIFIED_BY,
      LIPA_LEGACY_REF,
      LIPA_DEL_IPA_REFNO,
      LIPA_DEL_PAR_REFNO,
      LIPA_DEL_APP_REFNO,
      LIPA_DEL_CDE_REFNO
FROM  dl_hat_involved_parties
WHERE lipa_dlb_batch_id    = p_batch_id
AND   lipa_dl_load_status IN ('L','F','O');
--
-- ************************************
--
CURSOR c_get_app_refno (p_lapp_legacy_ref VARCHAR2) IS
SELECT app_refno
FROM applications
WHERE app_legacy_ref = p_lapp_legacy_ref;
--WHERE app_refno = p_lapp_legacy_ref;
-- testing only as no app_legacy_ref matches(AJ)
-- *****************************
--
CURSOR c_app_legacy_ref (p_lapp_legacy_ref VARCHAR2) IS
SELECT count(*)
FROM applications
WHERE app_legacy_ref = p_lapp_legacy_ref;
--WHERE app_refno = p_lapp_legacy_ref;
-- testing only as no app_legacy_ref matches(AJ)
-- *****************************
--
CURSOR c_get_par_refno (p_lpar_per_alt_ref VARCHAR2) IS
SELECT par_refno
FROM parties
WHERE par_per_alt_ref = p_lpar_per_alt_ref
OR   'PAR'||par_refno = p_lpar_per_alt_ref;
-- *****************************
--
CURSOR c_hhold_dl( p_lapp_legacy_ref     VARCHAR2
                  ,p_lipa_dlb_batch_id   VARCHAR2
                  ,p_lipa_hhold_group_no NUMBER   )  IS
SELECT count (*)
FROM   dl_hat_involved_parties
WHERE  lipa_dlb_batch_id = p_lipa_dlb_batch_id
  AND  lapp_legacy_ref = p_lapp_legacy_ref
  AND  lipa_head_hhold_ind = 'Y'
  AND  lipa_hhold_group_no = p_lipa_hhold_group_no;
-- ************************************
--
CURSOR c_existing_hhold( p_app_refno           NUMBER
                        ,p_par_refno           NUMBER
                        ,p_lipa_hhold_group_no NUMBER
                        ,p_lipa_start_date     DATE   ) IS
SELECT count(*)
FROM   involved_parties
WHERE  ipa_app_refno = p_app_refno
  AND  ipa_hhold_group_no = p_lipa_hhold_group_no
  AND  ipa_par_refno != p_par_refno
  AND  p_lipa_start_date BETWEEN ipa_start_date AND nvl(ipa_end_date, p_lipa_start_date +1)
  AND  ipa_head_hhold_ind = 'Y';
-- ************************************
--
CURSOR c_pmco_check(p_column_name VARCHAR2) IS
SELECT DISTINCT 'X'
FROM   person_mandatory_columns
WHERE  pmco_mod_name IN ('PAR007','HSS-CLI-C','PAR008','HSS-CLI-U')
  AND  pmco_pdc_column_name = p_column_name
  AND  pmco_hrv_mle_code != 'OPTIONAL';
-- ************************************
--
CURSOR c_dup_ipa( p_par_refno           NUMBER
                 ,p_app_refno           NUMBER
                 ,p_lipa_start_date     DATE   ) IS
SELECT 'X'
FROM   involved_parties
WHERE  ipa_app_refno = p_app_refno
  AND  ipa_par_refno = p_par_refno
  AND  p_lipa_start_date BETWEEN ipa_start_date AND nvl(ipa_end_date, p_lipa_start_date +1);
-- ************************************
-- 
CURSOR c_dup_dl_ipa( p_lpar_per_alt_ref  VARCHAR2 
                    ,p_lapp_legacy_ref   VARCHAR2
                    ,p_lipa_start_date   DATE
                    ,p_lipa_dlb_batch_id VARCHAR2 )  IS
SELECT count(*)
FROM   dl_hat_involved_parties
WHERE  lipa_dlb_batch_id = p_lipa_dlb_batch_id
  AND  lapp_legacy_ref = p_lapp_legacy_ref
  AND  lpar_per_alt_ref = p_lpar_per_alt_ref
  AND  p_lipa_start_date BETWEEN lipa_start_date AND nvl(lipa_end_date, p_lipa_start_date +1);
-- ************************************
--
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_INVOLVED_PARTIES';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_errors            VARCHAR2(10);
l_error_ind         VARCHAR2(10);
i                   INTEGER := 0;
l_rec_date          applications.app_received_date%TYPE;
l_app_refno         involved_parties.ipa_app_refno%TYPE;
l_par_refno         involved_parties.ipa_par_refno%TYPE;
l_count_hhold_dl    INTEGER;
l_count_hhold_app   INTEGER;
l_count_total       INTEGER;
l_title_man         VARCHAR2(1);
l_dup_ipa           VARCHAR2(1);
l_dup_dl_ipa        INTEGER;
l_count_app         INTEGER;

--
-- ************************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_involved_parties.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_involved_parties.dataload_validate',3 );
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
  cs := p1.lipa_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors := 'V';
  l_error_ind := 'N';
  l_rec_date := NULL;
  l_app_refno := NULL;
  l_par_refno := NULL;
  l_count_hhold_dl  := NULL;
  l_count_hhold_app := NULL;
  l_count_total     := NULL;
  l_title_man := NULL;
  l_dup_ipa := NULL;
  l_dup_dl_ipa := NULL;
  l_count_app := NULL;
--
-- Mandatory field check not done below
--
  IF (p1.lipa_start_date IS NULL)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',509);
-- 'Involved Party Start Date (lipa_start_date)must be supplied'
  END IF;
--
  IF (p1.lipa_created_date IS NULL)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',510);
-- 'IPA Created Date(lipa_created_date)must be supplied'
  END IF;
--
  IF (p1.lipa_created_by IS NULL)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',511);
-- 'IPA Created By(lipa_created_by)must be supplied'
  END IF;
--
  IF (p1.lpar_created_date IS NULL)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',512);
-- 'PAR Created Date(lpar_created_date)must be supplied'
  END IF;
--
  IF (p1.lpar_created_by IS NULL)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',513);
-- 'PAR Created By(lpar_created_by) must be supplied'
  END IF;
--
  IF (p1.lpar_per_surname IS NULL)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',514);
-- 'Party Surname must be supplied'
  END IF;
--
  IF (p1.lpar_per_forename IS NULL)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',515);
-- 'Party Forename must be supplied'
  END IF;
--
-- The application legacy reference mandatory field
--
  IF (p1.lapp_legacy_ref IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',451);
-- 'Application Legacy Reference must be supplied'
  ELSE
   OPEN  c_get_app_refno(p1.lapp_legacy_ref);
   FETCH c_get_app_refno INTO l_app_refno;
   CLOSE c_get_app_refno;
   IF (l_app_refno IS NULL)
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',452);
-- 'Application cannot be found using Legacy Reference Supplied'
   END IF;
  END IF;  
--
-- Check is supplied par per alt ref and get the par_refno from parties
--
  IF (p1.lpar_per_alt_ref IS NOT NULL)
   THEN
    OPEN  c_get_par_refno(p1.lpar_per_alt_ref);
    FETCH c_get_par_refno INTO l_par_refno;
    CLOSE c_get_par_refno;
--
    IF (l_par_refno IS NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',450);
-- 'Party cannot be found using Party Alternate Reference Supplied'
    END IF;
  END IF;
--
-- Check that app_legacy ref is UNQIUE on applications table if not cannot find
-- correct application
--
  IF(p1.lapp_legacy_ref IS NOT NULL)
   THEN
--
    OPEN  c_app_legacy_ref(p1.lapp_legacy_ref);
    FETCH c_app_legacy_ref INTO l_count_app;
    CLOSE c_app_legacy_ref;
--
    IF (l_count_app >1)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',522);
-- 'The app_legacy_ref found against more than one application'
    END IF;
--
-- The start date should not be earlier the application
-- received date
--
    IF (l_count_app = 1)
     THEN  
      l_rec_date := s_dl_hat_utils.f_app_received(p1.lapp_legacy_ref);
      IF(l_rec_date IS NOT NULL)
       THEN
        IF (nvl(p1.lipa_start_date,trunc(sysdate)) < l_rec_date)
         THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',238);
        END IF;
      END IF;
    END IF;
  END IF;
--
-- The start date cannot be later then end date
-- received date
--
  IF (p1.lipa_start_date IS NOT NULL)
   THEN
    IF (p1.lipa_start_date > nvl(p1.lipa_end_date,p1.lipa_start_date+1))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',516);
-- 'IPA Start Date cannot be greater than IPA End Date supplied'
    END IF;
  END IF;
--
-- Check the Y/N columns are valid
--
-- Main applicant indicator
--
  IF (p1.lipa_main_applicant_ind NOT IN ('Y','N'))
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',455);
-- 'Main Applicant Indicator must be Y or N'
  END IF;
--
-- Joint applicant (tenant) indicator
--
  IF (p1.lipa_joint_appl_ind NOT IN ('Y','N'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',456);
-- 'Joint Applicant Indicator must be Y or N'
  END IF;
--
-- Living apart indicator
--
  IF (p1.lipa_living_apart_ind NOT IN ('Y','N'))
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',457);
-- 'Living Apart Indicator must be Y or N'
  END IF;
--
-- Rehouse indicator
--
  IF (p1.lipa_rehouse_ind NOT IN ('Y','N'))
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',458);
-- 'Rehousing Indicator must be Y or N'
  END IF;
--
-- OAP indicator
--
  IF (NOT s_dl_hem_utils.yornornull(p1.lpar_per_hou_oap_ind))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',82);
  END IF;
--
-- Disabled indicator
--
  IF (NOT s_dl_hem_utils.yornornull(p1.lpar_per_hou_disabled_ind))
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',81);
  END IF;
--
-- At Risk Indicator
--
  IF (NOT s_dl_hem_utils.yornornull(p1.lpar_per_hou_at_risk_ind))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',269);
  END IF;
--
-- Checks specific to main applicant indicator
--
  IF (p1.lipa_main_applicant_ind = 'Y')
  THEN
--
    IF (p1.lipa_joint_appl_ind != 'Y')
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',239);
    END IF;
--
    IF (p1.lipa_living_apart_ind != 'N')
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',240);
    END IF;
--
    IF (p1.lipa_rehouse_ind != 'Y')
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',241);
    END IF;
--
  END IF;
--
-- Check the reference values
--
-- Relationship to main applicant
--
  IF (p1.lipa_hrv_rel_code IS NOT NULL)
   THEN
    IF (NOT s_dl_hem_utils.exists_frv('RELATION',p1.lipa_hrv_rel_code))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',087);
    END IF;
  END IF;
--
-- Valid Geographical origin of person
--
-- Changed domian from ETHNIC2 to ETHNIC. PH 08/01/03
--
  IF (p1.lpar_per_hou_hrv_hgo_code IS NOT NULL)
   THEN
    IF (NOT s_dl_hem_utils.exists_frv('ETHNIC',p1.lpar_per_hou_hrv_hgo_code))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',377);
    END IF;
  END IF;
--
-- Marital status
--
  IF (p1.lpar_per_hou_hrv_hms_code IS NOT NULL)
   THEN
    IF (NOT s_dl_hem_utils.exists_frv('MAR_STAT',p1.lpar_per_hou_hrv_hms_code))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',088);
    END IF;
  END IF;
--
-- Gender
--
  IF (p1.lpar_per_frv_fge_code IS NOT NULL)
   THEN
    IF (NOT s_dl_hem_utils.exists_frv('SEX',p1.lpar_per_frv_fge_code))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',086);
    END IF;
  END IF;
--
-- Ethnic origin
--
-- Changed domian from ETHNIC to ETHNIC2. PH 08/01/03
--
  IF (p1.lpar_per_frv_feo_code IS NOT NULL)
   THEN
    IF (NOT s_dl_hem_utils.exists_frv('ETHNIC2',p1.lpar_per_frv_feo_code))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',092);
    END IF;
  END IF;
--
-- language code
--
  IF (p1.lpar_per_frv_fnl_code IS NOT NULL)
   THEN
    IF (NOT s_dl_hem_utils.exists_frv('SUPPORTED_NLD',p1.lpar_per_frv_fnl_code))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',342);
    END IF;
  END IF;
--
-- Nationality
-- 
  IF (NOT s_dl_hem_utils.exists_frv('NATIONALITY',p1.lpar_per_hou_hrv_ntly_code,'Y'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',270);
  END IF;
--
-- Sexual Orientation
--
  IF (NOT s_dl_hem_utils.exists_frv('SEXUAL_ORIENTATION',p1.lpar_per_hou_hrv_sexo_code,'Y'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',271);
  END IF;
--
-- Religion
--
  IF (NOT s_dl_hem_utils.exists_frv('RELIGION',p1.lpar_per_hou_hrv_rlgn_code,'Y'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',272);
  END IF;
--
-- Economic Status
--
  IF (NOT s_dl_hem_utils.exists_frv('ECONOMIC_STATUS',p1.lpar_per_hou_hrv_ecst_code,'Y'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',273);
  END IF;  
--
-- Check party type is a mandatory field
--
  IF p1.lpar_type NOT IN ('HOUP','BOTP')
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',517);
-- 'Party type must be either HOUP or BOTP only'
  END IF;
--
-- Check both ipa and par Created dates against ipa Modified dates if supplied (AJ Bespoke GNB)
--
  IF (p1.lpar_created_date > sysdate)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',360);
  END IF;
--
  IF (p1.lipa_created_date > sysdate)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',518);
-- 'IPA Created date must not be greater than today'
  END IF;
--
  IF (p1.lipa_modified_date is not null)
   THEN
    IF ((p1.lipa_modified_date < p1.lpar_created_date)
         OR
        (p1.lipa_modified_date < p1.lipa_created_date)) 
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',361);
    END IF;
--
    IF (p1.lipa_modified_date > sysdate)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',378);
    END IF;
--
    IF (
        (    p1.lipa_modified_date IS NOT NULL
         AND p1.lipa_modified_by   IS NULL     )
         OR
        (    p1.lipa_modified_date IS NULL
         AND p1.lipa_modified_by   IS NOT NULL )
       )
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',362);
    END IF;
  END IF;
--
-- Checks for Head of Household and House Groups added to
-- household_persons in v61.4 release
--
-- There must be one marked as head of household for each group 
-- the groups need to be different only if there is more that one
-- group against the same application
--
-- Check param to see if groups are required
--
-- If groups not required then head household and groups
-- fields must not be supplied
--
-- these groups do not apply to organisations or parties not linked
-- to tenancies
--
  IF ( p1.lipa_head_hhold_ind NOT IN('N','Y')
      AND p1.lipa_hhold_group_no IS NOT NULL ) 
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',519);
-- 'Head of Household Indicator must be Y or N if HHold Group No.is Supplied'
  END IF;
--
-- groupings are not required on any people or organisation if parameter not set
-- OK for Head HHold to be N and HHold Group NOT to be supplied
--
  IF fsc_utils.get_sys_param('HOUSEHOLD_GROUPINGS_REQD') = 'N'
   THEN
--
    IF( p1.lipa_head_hhold_ind IS NOT NULL )
     THEN
      IF(  p1.lipa_head_hhold_ind !='N'
        OR p1.lipa_hhold_group_no IS NOT NULL ) 
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',342);
      END IF;
    END IF;
  END IF; -- end of groupings not required
--
-- groupings are required exclude organisations or parties not linked to tenancies
--
--
  IF fsc_utils.get_sys_param('HOUSEHOLD_GROUPINGS_REQD') = 'Y'
   THEN
--
    IF ( p1.lipa_head_hhold_ind NOT IN ('Y','N')
      OR p1.lipa_hhold_group_no IS NULL ) 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',343);
    END IF;
--
-- check to see if head of hhold exists for group against the tenancy
--
-- first check batch being loaded count number of HHold against each group no
-- for each app_legacy_ref (application)
--
    IF ( p1.lipa_hhold_group_no IS NOT NULL
     AND p1.lapp_legacy_ref IS NOT NULL     ) 
     THEN     
      OPEN  c_hhold_dl( p1.lapp_legacy_ref
                       ,p1.lipa_dlb_batch_id
                       ,p1.lipa_hhold_group_no);
      FETCH c_hhold_dl INTO l_count_hhold_dl;
      CLOSE c_hhold_dl;
    END IF;
--
-- then check involved parties table for current involved parties
--
    IF (l_app_refno IS NOT NULL AND
        l_par_refno IS NOT NULL AND
        p1.lipa_hhold_group_no IS NOT NULL AND
        p1.lipa_start_date IS NOT NULL AND
        p1.lipa_head_hhold_ind = 'Y')
     THEN
      OPEN c_existing_hhold( l_app_refno
                            ,l_par_refno
                            ,p1.lipa_hhold_group_no
                            ,p1.lipa_start_date );
      FETCH c_existing_hhold INTO l_count_hhold_app;
      CLOSE c_existing_hhold;
    END IF;
--
    l_count_total := nvl(l_count_hhold_dl,0) + nvl(l_count_hhold_app,0);
--
-- if total of counts are zero then error as no head of household for 
-- the group against the tenancy
--
    IF (l_count_total = 0)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',345);
    END IF;
--
-- if total of counts are more than 1 then error as too many heads of household for 
-- the group against the tenancy
--
    IF (l_count_total > 1)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',346);
    END IF;
--
  END IF; -- end of groupings required check
--
-- title check is mandatory and if supplied exists in domain TITLE (AJ)
-- 
  OPEN c_pmco_check('PAR_PER_TITLE');
  FETCH c_pmco_check into l_title_man;
  CLOSE c_pmco_check;
--
  IF (p1.lpar_per_title IS NULL  AND l_title_man IS NOT NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',348);
  END IF;
--
  IF (NOT s_dl_hem_utils.exists_frv('TITLE',p1.lpar_per_title,'Y'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',347);
  END IF;
--
-- Check that involved party does not already exist for app and start date
-- provided only able to check if party already exists
--
  IF (l_par_refno IS NOT NULL    AND
      l_app_refno IS NOT NULL    AND
      p1.lipa_start_date IS NOT NULL  )
   THEN
-- 
    OPEN c_dup_ipa(l_par_refno
                  ,l_app_refno
                  ,p1.lipa_start_date);
    FETCH c_dup_ipa into l_dup_ipa;
    CLOSE c_dup_ipa;
--
    IF (l_dup_ipa IS NOT NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',520);
-- 'Duplicate IPA record found in involved parties table'
    END IF;
  END IF;  
--
-- Check that involved party is not duplicated in data load batch
-- only able to check if LPAR_PER_ALT_REF and LAPP_LEGACY_REF and
-- IPA Start Date supplied
--
  IF ( p1.lpar_per_alt_ref IS NOT NULL    AND
       p1.lapp_legacy_ref  IS NOT NULL    AND
       p1.lipa_start_date  IS NOT NULL        )
   THEN
-- 
    OPEN c_dup_dl_ipa(p1.lpar_per_alt_ref
                     ,p1.lapp_legacy_ref
                     ,p1.lipa_start_date
                     ,p1.lipa_dlb_batch_id);
    FETCH c_dup_dl_ipa into l_dup_dl_ipa;
    CLOSE c_dup_dl_ipa;
--
    IF (l_dup_dl_ipa > 1)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',521);
-- 'Duplicate IPA records found in data load batch'
    END IF;
  END IF;  
--
-- *****************************************
-- Now UPDATE the record count and error code
--
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
--
  s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
  set_record_status_flag(l_id,l_errors);
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
--
fsc_utils.proc_END;
commit;
--
 EXCEPTION
  WHEN OTHERS THEN
   s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
-- ***********************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 IS
SELECT
     ROWID rec_rowid,
     LIPA_DLB_BATCH_ID,
     LIPA_DL_SEQNO,
     LIPA_DL_LOAD_STATUS,
     LPAR_PER_ALT_REF,
     LPAR_UPD_INS_IND,
     LAPP_LEGACY_REF,
     LPAR_TYPE,
     LPAR_CREATED_DATE,
     LPAR_CREATED_BY,
     LPAR_PER_SURNAME,
     LPAR_PER_FORENAME,
     LIPA_CREATED_DATE,
     LIPA_CREATED_BY,
     LIPA_START_DATE,
     LPAR_PHONE_NO,
     LIPA_LEGACY_REF,
     LIPA_DEL_IPA_REFNO,
     LIPA_DEL_PAR_REFNO,
     LIPA_DEL_APP_REFNO,
     LIPA_DEL_CDE_REFNO
FROM  dl_hat_involved_parties
WHERE lipa_dlb_batch_id   = p_batch_id
AND   lipa_dl_load_status = 'C';
--
-- *****************************
--
CURSOR c2 (p_lpar_per_alt_ref VARCHAR2) IS
SELECT par_refno
FROM parties
WHERE par_per_alt_ref = p_lpar_per_alt_ref
OR   'PAR'||par_refno = p_lpar_per_alt_ref;
--
-- *****************************
--
i INTEGER := 0;
l_an_tab  VARCHAR2(1);
l_app_refno      applications.app_refno%TYPE;
l_ipa_par_refno  involved_parties.ipa_par_refno%TYPE;
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_INVOLVED_PARTIES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- *****************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_involved_parties.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_involved_parties.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lipa_dl_seqno;
  i  := i +1;
  l_id := p1.rec_rowid;
--
-- *****************************
--
  l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lapp_legacy_ref);
--
-- Get the par_refno
--
  IF p1.lpar_per_alt_ref IS NOT NULL
   THEN
    OPEN c2(p1.lpar_per_alt_ref);
    FETCH c2 INTO l_ipa_par_refno;
    CLOSE c2;
  END IF;
--
-- Check this delete
--
-- Firstly delete contact details
-- only created if lipa_cde_refno is not null
--
  IF (p1.lipa_del_cde_refno IS NOT NULL)
   THEN
    DELETE FROM contact_details
    WHERE cde_refno = p1.lipa_del_cde_refno
    AND   cde_start_date = p1.lipa_start_date
    AND   cde_created_by = 'DATALOAD'
    AND   cde_contact_value = p1.lpar_phone_no
    AND   cde_frv_cme_code = 'TELEPHONE'
    AND   cde_par_refno = p1.lipa_del_par_refno;
  END IF;
--
-- Secondly delete involved parties record
-- always created by data loader now use new del fields
-- IPA(UK) app par and start date
-- for par, ipa and app refno's (AJ 23/01/2018)
--
  DELETE FROM involved_parties
  WHERE ipa_refno = NVL(p1.lipa_del_ipa_refno,ipa_refno)
  AND   ipa_app_refno = NVL(p1.lipa_del_app_refno,l_app_refno)
  AND   ipa_par_refno = NVL(p1.lipa_del_par_refno,l_ipa_par_refno)
  AND   ipa_start_date = p1.lipa_start_date;
--
-- Now delete party record if created by this process
-- I indicates that an insert was made into the parties table
--
  IF (p1.lpar_upd_ins_ind = 'I')
   THEN
    DELETE FROM parties
    WHERE par_refno = NVL(p1.lipa_del_par_refno,l_ipa_par_refno)
    AND   par_type = p1.lpar_type
    AND   par_created_date = p1.lpar_created_date
    AND   par_created_by   = p1.lpar_created_by
    AND   par_per_surname  = p1.lpar_per_surname
    AND   par_per_forename = p1.lpar_per_forename;
  END IF;
--
-- update DL table by removing del references after delete process
-- has run
--
  UPDATE dl_hat_involved_parties
  SET   lipa_del_ipa_refno = NULL
  ,     lipa_del_par_refno = NULL
  ,     lipa_del_app_refno = NULL
  ,     lipa_del_cde_refno = NULL
  WHERE lipa_dlb_batch_id = cb
    AND lipa_dl_seqno = cs
    AND rowid = l_id;
--
-- update DL table by removing par alt ref after delete process
-- has run if created by this process
-- set to party alt ref created by data load in parties table
-- only done if par alt ref NOT supplied 'IPADL||par_refno'
-- and parties record created
--
  IF (p1.lpar_upd_ins_ind = 'I')
   THEN  
    UPDATE dl_hat_involved_parties
    SET   lpar_per_alt_ref = NULL
    WHERE lpar_per_alt_ref LIKE 'IPADL%'
      AND lipa_dlb_batch_id = cb
      AND lipa_dl_seqno = cs
      AND rowid = l_id;
  END IF;
--
-- finally remove update indicator
--
  UPDATE dl_hat_involved_parties
  SET   lpar_upd_ins_ind = NULL
  WHERE lipa_dlb_batch_id = cb
    AND lipa_dl_seqno = cs
    AND rowid = l_id;
--
--
-- *****************************
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
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'C');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
  END;
--
 END LOOP;
--
-- *****************************
--
-- Section to analyse the tables populated with this dataload
--
l_an_tab := s_dl_hem_utils.dl_comp_stats('INVOLVED_PARTIES');
l_an_tab := s_dl_hem_utils.dl_comp_stats('PARTIES');
l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HAT_INVOLVED_PARTIES');
l_an_tab := s_dl_hem_utils.dl_comp_stats('CONTACT_DETAILS');
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
--
END s_dl_hat_involved_parties;
/

SHOW ERRORS


