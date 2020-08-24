CREATE OR REPLACE PACKAGE BODY s_dl_hem_people
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER   WHO  WHEN        WHY
--    1.0             PJD  05/09/2000  Iworld Product Dataload
--    1.1             MH   09/09/2001  Address format changes
--    1.2             SPAU 09/11/2001  Creation of Contact_details
--    1.3    5.1.4    PJD  03/02/2002  Added Savepoint to Delete plus
--                                     added NVL clauses in insert to Contact
--                                     Details
--                                     Replaced references to s_hdl_utils to be
--                                     s_dl_hem_utils.
--    1.4    5.1.6    PJD  23/02/2002  Added periodic analyze table clause
--                                     within create process
--    1.5    5.1.6    PJD  05/06/2002  Added extra tables into the periodic
--                                     analyze
--    1.6    5.1.6    PH   07/06/2002  Amended validation on lpar_hop_hpsr_code
--                                     from HDL to HLD
--    2.0    5.2.0    PH   09/07/2002  Amendments for 520 release. Allow for
--                                     main_tenant_ind and
--                                     also insert into addresses.
--    2.1    5.2.0    PJD  11/07/2002  Added question 'Use tenancy address as
--                                     default'
--                                     Added lpar_add_start_date and
--                                     lpar_add_end_date in create proc
--    2.2    5.2.0    PH   16/08/2002  Amended create process on insert to
--                                     household_persons where
--                                     person already exists to exclude those
--                                     with ltcy_alt_ref of ~NONE~
--    2.3    5.2.0    PJD  25/08/2000  Only create a PAR/CONTACT address if not
--                                     already one for the same date.
--    2.4    5.2.0    PJD  05/09/2002  Use the the above logic when picking up
--                                     tenancy addresses
--    2.5    5.2.0    PH   06/09/2002  Added nvl in insert addresses as may
--                                     have a forward address
--                                     where still a current tenancy.
--    2.6    5.2.0    MH   10/09/02    Code added to rationalise multiple
--                                     PAR/CONTACT rows created if PAR/CONTACT
--                                     addresses supplied with different
--                                     start dates
--    2.7    5.2.0    MH  12/09/02     Code added to cleanse address details
--                                     after delete
--    2.8    5.2.0    PJD 31/10/02     Further refinement of code where person
--                                     already exists
--    3.0    5.3.0    PH  08/01/2003   Changed validate. Ethnic uses domain
--                                     ETHNIC2 and Geographic uses ETHNIC.
--    3.1    5.3.0    PJD 09/01/2003   Changed validation on hop_hper_code to
--                                     use HLD_END domain.
--    3.2    5.3.0    PJD 05-FEB-2003  Created by field now defaults to DATALOA
--    3.3    5.3.0    PJD 06-JUN-2003  Added final commit to validate.
--    3.4    5.3.0    PJD 24-JUL-2003  If per_alt_ref is nul we need to allow
--                                     for large batch names by subtr it to
--                                     first 11 chars.
--    3.5    5.4.0    PJD 19-NOV-2003  Update Status before Commit statement
--    3.6    5.5.0    PH  21-APR-2004  Added insert into Interested Parties if
--                                     values supplied.
--    3.7    5.5.0    PJD 12-MAY-2004  Nvl Clause on Placement and Current Ind
--                                     and lpar_ipp_ipt_code only validated if not null
--    3.8    5.5.0    PJD 07-JUL-2004  Added validation on Address Field
--                                     combinations.
--    3.9    5.5.0    PH  26-JUL-2004  New fields added to allow for loading
--                                     organisations.
--    4.0    5.6.0    PJD 12-JAN-2005  Extra check in Delete Proc to only delete from
--                                     addresses and address elements if stand alone
--                                     addresses didn't already exist.
--    4.1    5.8.0    PH  28-JUL-2005  Commented out address cleansing as doesn't
--                                     work correctly in all situations.
--    4.2    5.8.0    MB  05-AUG-2005  Added in extra NULLs in call to dl_hem_utils.
--				                       insert_address
--    4.3    5.9.0   VST  09-01-2006   Added in extra NULLs in call to dl_hem_utils.
--				                       insert_address
--
--    4.4    5.9.0   PJD  09/01/2006   Removed reference to CDE_TCY_REFNO
--    4.5    5.9.0   PH   17/01/2006   Corrected compilation errors, too many
--                                     fields into insert_address.
--    5.0    5.10.0  PH   08/05/2006   Removed all references to Addresses as these
--                                     should be loaded in the Addresses Dataload.
--    5.1    5.10.0  PH   09/03/2007   Added missing interested party fields
--    5.2    5.10.0  PH   10/05/2007   Added 'Order By' in create process to make
--                                     sure that households and household_persons
--                                     get populated correctly. If non tenant is
--                                     created first they end up in wrong household.
--    5.3    5.10.0  PH   15/06/2007   Amended insert to interested parties to
--                                     only use one of par_refno or username.
--    6.0    5.12.0  PH   16/07/2007   Added new fields LPAR_PER_HOU_AT_RISK_IND
--                                     LPAR_PER_HOU_HRV_NTLY_CODE, LPAR_PER_HOU_HRV_SEXO_CODE
--                                     LPAR_PER_HOU_HRV_RLGN_CODE, LPAR_PER_HOU_HRV_ECST_CODE
--    6.1    5.12.0  PH   09/08/2007   Added FSC to par_refno_seq as the same sequence
--                                     owned by HOU has appeared in 5.12
--    6.2    5.13.0  PH   05/02/2008   Amended insert to tenancy instances (not
--                                     exists) clause to check on start date
--                                     Now includes its own
--                                     set_record_status_flag procedure.
--    6.3    5.13.0  MB   22/05/2009   Added default N to par_per_hou_at_risk_ind if null
--    6.4    5.15.1  MB   09/12/2009   Removed update to par_hou_end_date being set to
--                                     lpar_hop_end_date incorrectly - identified at BFH migration
--    6.5    6.12.0  VS   03/11/2015   Added a check in the CREATE process to see if interested party
--                                     already exists before it is created.
--    6.6    6.13.0  AJ   08/02/2016   1) Added lpar_org_current_ind to create and validate and have made
--                                     mandatory in line with v6.13 install script by TC
--                                     2) Removed validate check c_ipp_exists which fails if found as check
--                                     now in dataload_create which prevents duplicates being created
--    6.7    6.13.0  AJ   02/02/2016   1) Amended lpar_org_current_ind to create and validate as only
--                                     mandatory for an organisation as per Sue Allen
--                                     2) Added validate check on telephone number (lpar_phone) to
--                                     allow for options of spaces allowed, min max length and
--                                     digits only against contact method
--    6.8    6.13.0  AJ   21/03/2016   Amended check on lpar_org_current_ind to error if not Y or N as not
--                                     correct (nvl(p1.lpar_org_current_ind,'X') NOT IN ('Y','N'))
--    6.9    6.13/4  AJ   13/12/2016   Amended validate added check on lpar_hop_start_date to error if not
--                                     supplied and lpar_tcy_alt_ref is not set to '~NONE~' to match create
--                                     as a household_persons record will need to be either created or updated
--    6.10   6.13/4  AJ   14/12/2016   Amended validate HD3 021 check on lpar_hop_start_date amended to 078
--    7.0    6.13/4  AJ   18/07/2017   Amended validate added checkd for duplicate Organisation record in batch
--                                     and check so party fields not supplied with Organisation except alt ref
--    7.1    6.14    AJ   07/11/2017   1) Added household groupings (hop_head_hhold_ind hop_hhold_group_no)
--                                     new at this release (v6.14) and added further order by to create so
--                                     head of households get created first
--    7.2    6.14    AJ   08/11/2017   head of household and household groups added organisations check
--    7.3    6.14    AJ   09/11/2017   Check for Title added against domain TITLE and person matrix
--    7.4    6.14    AJ   10/11/2017   Checked and slight amendments done to validation during testing
--                                     to hhold groups required checks
--    7.5    6.14    AJ   14/11/2017   a null value Check added to l_answer if not answered in the positive
--                                     HOUSEHOLD_GROUPINGS_REQD parameter value check altered from cursor to
--                                     system function so c_hhold_param removed
--    7.6    6.16.1  PL   01/06/2018   Amended to copy par_alt_refno into par_refno on create as requested by SAHT
--    7.7    6.16.1  PN   05/07/2018   Amended household_persons check to also consider hop_start_date
--    7.8    6.16.1  PL   20/08/2018   Amended to allow orgs to have multiple tenancies
--    7.6    6.16    DB   28/08/2018   Queensland Tenancy Release enhancement to allow update of parties with values
--                                     supplied in file.
--    7.7    6.18    PL   26/02/2018   SA changes to duplicate org check
--    7.8    6.18    PL   27/02/2018   SA Changes to Household Ind Checks.
--    7.9    6.18    PL   16/04/2019   SA Changes to validation default to HOUP
--    7.10   6.18    PL   14/05/2019   SA Changes to create multiple admin unit ipp assignments
--    7.11   6.18    PL   07/04/2019   Changes to Household Ind Checks.
-- ***********************************************************************
--
--  declare package variables AND constants
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hem_people
  SET lpar_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hem_people');
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
CURSOR c1 is
SELECT
rowid rec_rowid
,lpar_dlb_batch_id
,lpar_dl_seqno
,lpar_hop_start_date
,lpar_per_surname
,lpar_tcy_alt_ref
,lpar_per_forename
,lpar_hop_hpsr_code
,lpar_per_title
,lpar_per_initials
,lpar_per_date_of_birth
,nvl(LPAR_PER_HOU_DISABLED_IND,'N') LPAR_PER_HOU_DISABLED_IND
,nvl(LPAR_PER_HOU_OAP_IND,'N')      LPAR_PER_HOU_OAP_IND
,lpar_per_frv_fge_code
,lpar_hop_hrv_rel_code
,lpar_per_hou_employer
,lpar_per_hou_hrv_hms_code
,lpar_phone
,lpar_hop_end_date
,lpar_hop_hper_code
,lpar_tcy_ind
,lpar_tin_main_tenant_ind
,lpar_tin_start_date
,lpar_tin_end_date
,lpar_tin_hrv_tir_code
,lpar_tin_stat_successor_ind
,lpar_per_alt_ref
,lpar_per_frv_feo_code
,lpar_per_ni_no
,lpar_per_frv_hgo_code
,lpar_per_frv_fnl_code
,lpar_per_other_name
,lpar_per_hou_surname_prefix
,lpar_hou_legacy_ref
,lpar_ipp_shortname
,nvl(LPAR_IPP_PLACEMENT_IND,'N') LPAR_IPP_PLACEMENT_IND
,nvl(LPAR_IPP_CURRENT_IND,'N')   LPAR_IPP_CURRENT_IND
,lpar_ipp_ipt_code
,lpar_ipp_usr_username
,lpar_ipp_spr_printer_name
,lpar_ipp_comments
,lpar_ipp_vca_code
,lpar_ipu_aun_code
,lpar_ipp_staff_id
,lpar_ipp_cos_code
,lpar_ipp_hrv_fit_code
,nvl(LPAR_TYPE, 'HOUP')  LPAR_TYPE
,lpar_org_sort_code
,lpar_org_name
,lpar_org_short_name
,lpar_org_frv_oty_code
,nvl(LPAR_PER_HOU_AT_RISK_IND,'N') LPAR_PER_HOU_AT_RISK_IND
,lpar_per_hou_hrv_ntly_code
,lpar_per_hou_hrv_sexo_code
,lpar_per_hou_hrv_rlgn_code
,lpar_per_hou_hrv_ecst_code
,lpar_org_current_ind
,lpar_hop_head_hhold_ind
,lpar_hop_hhold_group_no
,lpar_c_par_refno
FROM  dl_hem_people
WHERE lpar_dlb_batch_id   = p_batch_id
AND   lpar_dl_load_status = 'V'
ORDER by nvl(LPAR_TIN_MAIN_TENANT_IND, 'N') desc
       , nvl(LPAR_TCY_IND, 'N') desc
       , lpar_hop_start_date;
--
-- ************************************
CURSOR c_per_exists(p_per_alt_ref varchar2) is
SELECT par_refno
FROM parties
WHERE par_per_alt_ref = p_per_alt_ref;
-- ************************************
CURSOR c_par_refno is
SELECT fsc.par_refno_seq.nextval
FROM dual;
-- ************************************
CURSOR c_hou_refno(p_tcy_alt_ref varchar2) is
SELECT max(hop_hou_refno)
FROM household_persons,tenancy_instances,tenancies
WHERE tcy_alt_ref       = p_tcy_alt_ref
  AND tin_tcy_refno     = tcy_refno
  AND tin_hop_refno     = hop_refno;
-- ************************************
CURSOR c_new_hou_refno is
SELECT hou_refno_seq.nextval from dual;
-- ************************************
CURSOR c_new_hop_refno is
SELECT hop_refno_seq.nextval from dual;
-- ************************************
CURSOR c_tcy(p_alt_ref VARCHAR2) IS
SELECT tcy_refno,tcy_act_start_date,tcy_act_end_date, tcy_hrv_ttr_code
FROM tenancies
WHERE tcy_alt_ref = p_alt_ref;
-- ************************************
CURSOR c_ipp_refno IS
SELECT ipp_refno_seq.nextval from dual;
-- ************************************
CURSOR c_existing_hop_refno(p_hou_refno NUMBER,p_par_refno NUMBER,p_hop_start_date DATE) IS
SELECT hop_refno
      ,hop_head_hhold_ind
      ,hop_hhold_group_no
FROM   household_persons
WHERE  hop_hou_refno = p_hou_refno
  AND  hop_par_refno = p_par_refno
  AND  hop_start_date = p_hop_start_date;
-- ************************************
CURSOR c_ipp_exists(p_ipp_short varchar2,
                    p_ipt_code  varchar2)
IS
SELECT 'Y', ipp_refno
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_short
   AND ipp_ipt_code  = p_ipt_code;
--
-- ************************************
--
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HEM_PEOPLE';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_exists          VARCHAR2(1);
l_reusable_refno  INTEGER;
l_hou_refno       INTEGER;
l_hop_refno       INTEGER;
l_existing_hop_refno INTEGER;
l_hop_start_date  DATE; -- v6.16.2 Added
l_par_refno       INTEGER;
l_per_alt_ref     VARCHAR2(20);
l_tcy_refno       INTEGER;
l_con_tcy_start   DATE;
l_tcy_start_date  DATE;
l_tcy_end_date    DATE;
l_tcy_end_reason  VARCHAR2(10);
l_exist_par_refno INTEGER;
ai                INTEGER:=100;
l_answer          VARCHAR2(1);
i                 INTEGER := 0;
l_an_tab          VARCHAR2(1);
l_street_index_code   VARCHAR2(12);
l_ipp_refno       INTEGER;
l_ipp_exists      VARCHAR2(1);
l_head_hhold      VARCHAR2(1);
l_hhold_group     INTEGER;
l_c_par_refno     NUMBER(8);
--
-- ************************************
BEGIN
--
fsc_utils.proc_start('s_dl_hem_people.dataload_create');
fsc_utils.debug_message('s_dl_hem_people.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lpar_dl_seqno;
  l_id := p1.rec_rowid;
--
  SAVEPOINT SP1;
--
  IF p1.lpar_tcy_alt_ref != '~NONE~' THEN
--
--
-- IF a household refno exists FOR the given
-- tenancy use it, otherwise get a new refno FROM the sequence AND
-- create an entry in HOUSEHOLDS
--
    l_hou_refno := NULL;
    l_hop_refno := NULL;
--
    OPEN c_hou_refno(p1.lpar_tcy_alt_ref);
    FETCH c_hou_refno INTO l_hou_refno;
    CLOSE c_hou_refno;
--
    IF l_hou_refno IS NULL THEN
     OPEN c_new_hou_refno;
     FETCH c_new_hou_refno INTO l_hou_refno;
     CLOSE c_new_hou_refno;
--
     INSERT INTO households
     (hou_refno)
     VALUES
     (l_hou_refno);
--
    END IF;
  END IF; -- lpar_tcy_alt_ref != '~NONE~'
--
-- INSERT a record INTO PEOPLE having checked that the person does
-- NOT already exist on the table
--
  l_exist_par_refno := NULL;
  l_par_refno       := NULL;
  l_per_alt_ref     := NULL;
  l_hop_refno       := NULL;
  l_head_hhold      := NULL;
  l_hhold_group     := NULL;
  l_c_par_refno     := NULL;
--
  IF p1.lpar_per_alt_ref IS NULL
   THEN
    l_per_alt_ref := substr(p1.lpar_dlb_batch_id,1,11)||
                        '~'||to_char(p1.lpar_dl_seqno);
  ELSE
    l_per_alt_ref := p1.lpar_per_alt_ref;
    OPEN c_per_exists(p1.lpar_per_alt_ref);
    FETCH c_per_exists INTO l_par_refno;
    CLOSE c_per_exists;
  END IF;
--
  l_c_par_refno:= l_par_refno;
--
  IF l_par_refno IS NULL THEN
    IF p1.lpar_type = 'HOUP' AND l_per_alt_ref IS NOT NULL
	THEN
	   BEGIN
	      l_par_refno := l_per_alt_ref;
	   EXCEPTION WHEN OTHERS THEN NULL;
	   END;
	END IF;

	IF l_par_refno IS NULL
	THEN
		l_par_refno := fsc.par_refno_seq.nextval;
	END IF;
--
    l_c_par_refno:= l_par_refno;
--

   INSERT INTO PARTIES
   (par_refno                ,par_type
   ,par_created_date         ,par_created_bY
   ,par_reusable_refno       ,par_modified_date
   ,par_modified_by          ,par_per_surname
   ,par_per_forename         ,par_per_initials
   ,par_per_other_name       ,par_per_alt_ref
   ,par_per_date_of_birth    ,par_per_title
   ,par_per_ni_no            ,par_per_frv_fnl_code
   ,par_per_frv_fge_code     ,par_per_frv_feo_code
   ,par_per_hou_end_date     ,par_per_hou_employer
   ,par_per_hou_disabled_ind ,par_per_hou_oap_ind
   ,par_per_hou_hrv_hms_code ,par_per_hou_hrv_hgo_code
   ,par_per_hou_hrv_hpe_code ,par_per_hou_surname_prefix
   ,par_org_sort_code        ,par_org_name
   ,par_org_short_name       ,par_org_frv_oty_code
   ,par_per_hou_at_risk_ind  ,par_per_hou_hrv_ntly_code
   ,par_per_hou_hrv_sexo_code,par_per_hou_hrv_rlgn_code
   ,par_per_hou_hrv_ecst_code,par_org_current_ind
   )
   values
   (l_par_refno                      ,p1.lpar_type
   ,sysdate                          ,'DATALOAD'
   ,reusable_refno_seq.nextval       ,null
   ,null                             ,p1.lpar_per_surname
   ,p1.lpar_per_forename             ,p1.lpar_per_initials
   ,p1.lpar_per_other_name           ,l_per_alt_ref
   ,p1.lpar_per_date_of_birth        ,p1.lpar_per_title
   ,p1.lpar_per_ni_no                ,p1.lpar_per_frv_fnl_code
   ,p1.lpar_per_frv_fge_code         ,p1.lpar_per_frv_feo_code
   ,null                             ,p1.lpar_per_hou_employer
   ,p1.lpar_per_hou_disabled_ind     ,p1.lpar_per_hou_oap_ind
   ,p1.lpar_per_hou_hrv_hms_code     ,p1.lpar_per_frv_hgo_code
   ,null                             ,p1.lpar_per_hou_surname_prefix
   ,p1.lpar_org_sort_code            ,p1.lpar_org_name
   ,p1.lpar_org_short_name           ,p1.lpar_org_frv_oty_code
   ,p1.lpar_per_hou_at_risk_ind      ,p1.lpar_per_hou_hrv_ntly_code
   ,p1.lpar_per_hou_hrv_sexo_code    ,p1.lpar_per_hou_hrv_rlgn_code
   ,p1.lpar_per_hou_hrv_ecst_code    ,p1.lpar_org_current_ind);
--
   IF p1.lpar_tcy_alt_ref != '~NONE~' THEN
     OPEN  c_new_hop_refno;
     FETCH c_new_hop_refno INTO l_hop_refno;
     CLOSE c_new_hop_refno;
--
     IF fsc_utils.get_sys_param('HOUSEHOLD_GROUPINGS_REQD') = 'N'
      THEN
--
      INSERT INTO household_persons
        (HOP_REFNO
        ,HOP_HOU_REFNO
        ,HOP_PAR_REFNO
        ,HOP_START_DATE
        ,HOP_END_DATE
        ,HOP_HRV_REL_CODE
        ,HOP_HRV_HPSR_CODE
        ,HOP_HRV_HPER_CODE
        )
         values
        (l_hop_refno
        ,l_hou_refno
        ,l_par_refno
        ,p1.lpar_hop_start_date
        ,p1.lpar_hop_end_date
        ,p1.lpar_hop_hrv_rel_code
        ,p1.lpar_hop_hpsr_code
        ,p1.lpar_hop_hper_code
        );
--
     ELSE
--
      INSERT INTO household_persons
        (HOP_REFNO
        ,HOP_HOU_REFNO
        ,HOP_PAR_REFNO
        ,HOP_START_DATE
        ,HOP_END_DATE
        ,HOP_HRV_REL_CODE
        ,HOP_HRV_HPSR_CODE
        ,HOP_HRV_HPER_CODE
        ,HOP_HEAD_HHOLD_IND
        ,HOP_HHOLD_GROUP_NO
	    )
        values
        (l_hop_refno
        ,l_hou_refno
        ,l_par_refno
        ,p1.lpar_hop_start_date
        ,p1.lpar_hop_end_date
        ,p1.lpar_hop_hrv_rel_code
        ,p1.lpar_hop_hpsr_code
        ,p1.lpar_hop_hper_code
        ,NVL(p1.lpar_hop_head_hhold_ind,'N') -- 7.8
        ,p1.lpar_hop_hhold_group_no
        );
--
     END IF;
--
   END IF; -- 'tcy_alt_ref of ~NONE~
--
-- Update the Parties record set the Person Hou End Date and Person Hou End Reason if the
-- Person End Reason Supplied (lpar_hop_hper_code) is valid and exist in the Domain PEO_END
-- only update if fields are not already populated
--
   IF (p1.lpar_hop_hper_code IS NOT NULL AND p1.lpar_hop_end_date IS NOT NULL)
	THEN
--
    IF (s_dl_hem_utils.exists_frv('PEO_END',p1.lpar_hop_hper_code,'N'))
     THEN
--
     UPDATE PARTIES
     SET par_per_hou_hrv_hpe_code  =
                    nvl(par_per_hou_hrv_hpe_code, p1.lpar_hop_hper_code)
     ,   par_per_hou_end_date      =
                    nvl(par_per_hou_end_date, p1.lpar_hop_end_date)
     WHERE par_refno = l_par_refno;
--
    END IF; -- Setting Person End Date and Reason
--
   END IF; -- check if update parties needed
--
  ELSE
--
-- start person already exists
-- update the existing details
--
    UPDATE PARTIES
    SET par_modified_date  = sysdate
    ,   par_modified_by    = user
    ,   par_per_forename   = nvl(par_per_forename,p1.lpar_per_forename)
    ,   par_per_initials   = nvl(par_per_initials,p1.lpar_per_initials)
    ,   par_per_other_name = nvl(par_per_other_name,p1.lpar_per_other_name)
    ,   par_per_hou_surname_prefix =
                 nvl(par_per_hou_surname_prefix,
                                         p1.lpar_per_hou_surname_prefix)
    ,   par_per_date_of_birth     =
                    nvl(par_per_date_of_birth, p1.lpar_per_date_of_birth)
    ,   par_per_title             =
                    nvl(par_per_title, p1.lpar_per_title)
    ,   par_per_ni_no             =
                    nvl(par_per_ni_no, p1.lpar_per_ni_no)
    ,   par_per_frv_fnl_code      =
                    nvl(par_per_frv_fnl_code, p1.lpar_per_frv_fnl_code)
    ,   par_per_frv_fge_code      =
                    nvl(par_per_frv_fge_code, p1.lpar_per_frv_fge_code)
    ,   par_per_frv_feo_code      =
                    nvl(par_per_frv_feo_code, p1.lpar_per_frv_feo_code)
    ,   par_per_hou_employer      =
                    nvl(par_per_hou_employer, p1.lpar_per_hou_employer)
    ,   par_per_hou_disabled_ind  =
                    nvl(par_per_hou_disabled_ind, p1.lpar_per_hou_disabled_ind)
    ,   par_per_hou_oap_ind       =
                    nvl(par_per_hou_oap_ind, p1.lpar_per_hou_oap_ind)
    ,   par_per_hou_hrv_hms_code  =
                    nvl(par_per_hou_hrv_hms_code, p1.lpar_per_hou_hrv_hms_code)
    ,   par_per_hou_hrv_hgo_code  =
                    nvl(par_per_hou_hrv_hgo_code, p1.lpar_per_frv_hgo_code)
    ,   par_per_hou_hrv_hpe_code  =
                   nvl(par_per_hou_hrv_hpe_code, NULL)
    ,   par_org_sort_code         =
                    nvl(par_org_sort_code, p1.lpar_org_sort_code)
    ,   par_org_name              =
                    nvl(par_org_name, p1.lpar_org_name)
    ,   par_org_short_name        =
                    nvl(par_org_short_name, p1.lpar_org_short_name)
    ,   par_org_frv_oty_code      =
                    nvl(par_org_frv_oty_code, p1.lpar_org_frv_oty_code)
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
    ,   par_org_current_ind =
                    nvl(par_org_current_ind, p1.lpar_org_current_ind)
    WHERE par_per_alt_ref = p1.lpar_per_alt_ref;
--
    OPEN  c_new_hop_refno;
    FETCH c_new_hop_refno INTO l_hop_refno;
    CLOSE c_new_hop_refno;
--
    IF p1.lpar_tcy_alt_ref != '~NONE~' THEN
--
-- dbms_output.put_line('Inserting into Household Persons');
-- dbms_output.put_line('Hop_refno = '||l_hop_refno);
-- dbms_output.put_line('Hop_hou_refno = '||l_hou_refno);
-- dbms_output.put_line('Hop_par_refno = '||l_par_refno);
--
-- check if the party for that tenancy household exists in household persons
-- and bring it back if it does and what the group parameter is set to
-- to check if head hhold and group number need considering
--
      l_existing_hop_refno := NULL;
      OPEN c_existing_hop_refno(l_hou_refno,l_par_refno, p1.lpar_hop_start_date); -- v6.16.2 Modified
      FETCH c_existing_hop_refno INTO l_existing_hop_refno
                                     ,l_head_hhold
                                     ,l_hhold_group;

      CLOSE c_existing_hop_refno;
--
      IF l_hhold_group IS NULL
       THEN
        l_hhold_group := 0;
      END IF;
--
      IF l_existing_hop_refno IS NOT NULL
       THEN
        l_hop_refno := l_existing_hop_refno;
--
-- If exits check and update head of hhold and group number
-- if different
--
        IF fsc_utils.get_sys_param('HOUSEHOLD_GROUPINGS_REQD') = 'Y'
        AND p1.lpar_hop_head_hhold_ind IS NOT NULL -- PL
         THEN
--
          IF (l_head_hhold != p1.lpar_hop_head_hhold_ind
              OR
              l_hhold_group != p1.lpar_hop_hhold_group_no)
            THEN
              UPDATE HOUSEHOLD_PERSONS
              SET hop_head_hhold_ind = p1.lpar_hop_head_hhold_ind
                 ,hop_hhold_group_no = p1.lpar_hop_hhold_group_no
              WHERE hop_refno = l_existing_hop_refno
                AND hop_hou_refno = l_hou_refno
                AND hop_par_refno = l_par_refno;
          END IF;
        END IF;
--
-- create record in household persons for tenancy if does not
-- exist for the party and tenancy household
--
      ELSE
--
        IF fsc_utils.get_sys_param('HOUSEHOLD_GROUPINGS_REQD') = 'N'
         THEN
--
         INSERT INTO household_persons
         (hop_refno
         ,hop_hou_refno
         ,hop_par_refno
         ,hop_start_date
         ,hop_end_date
         ,hop_hrv_rel_code
         ,hop_hrv_hpsr_code
         ,hop_hrv_hper_code)
         SELECT
          l_hop_refno
         ,l_hou_refno
         ,l_par_refno
         ,p1.lpar_hop_start_date
         ,p1.lpar_hop_end_date
         ,p1.lpar_hop_hrv_rel_code
         ,p1.lpar_hop_hpsr_code
         ,p1.lpar_hop_hper_code
         FROM DUAL;
--
       ELSE
--
         INSERT INTO household_persons
         (hop_refno
         ,hop_hou_refno
         ,hop_par_refno
         ,hop_start_date
         ,hop_end_date
         ,hop_hrv_rel_code
         ,hop_hrv_hpsr_code
         ,hop_hrv_hper_code
         ,hop_head_hhold_ind
         ,hop_hhold_group_no)
         SELECT
          l_hop_refno
         ,l_hou_refno
         ,l_par_refno
         ,p1.lpar_hop_start_date
         ,p1.lpar_hop_end_date
         ,p1.lpar_hop_hrv_rel_code
         ,p1.lpar_hop_hpsr_code
         ,p1.lpar_hop_hper_code
         ,NVL(p1.lpar_hop_head_hhold_ind,'N') -- PL
         ,p1.lpar_hop_hhold_group_no
         FROM DUAL;
--
       END IF; -- parm check hop group
--
      END IF; -- existing hop_refno
--
    END IF; -- 'tcy_alt_ref of ~NONE~
--
-- Update the Parties record set the Person Hou End Date and Person Hou End Reason if the
-- Person End Reason Supplied (lpar_hop_hper_code) is valid and exist in the Domain PEO_END
-- only update if fields are not already populated
--
    IF (p1.lpar_hop_hper_code IS NOT NULL AND p1.lpar_hop_end_date IS NOT NULL)
	 THEN
--
     IF (s_dl_hem_utils.exists_frv('PEO_END',p1.lpar_hop_hper_code,'N'))
      THEN
--
      UPDATE parties
      SET par_per_hou_hrv_hpe_code  =
                       nvl(par_per_hou_hrv_hpe_code, p1.lpar_hop_hper_code)
      ,   par_per_hou_end_date      =
                       nvl(par_per_hou_end_date, p1.lpar_hop_end_date)
      WHERE par_per_alt_ref = p1.lpar_per_alt_ref;
--
     END IF; -- update parties
--
    END IF; -- check if update parties needed
--
  END IF; -- person already exists
--
--
-- IF the person is a tenant THEN create an entry in TCY_INSTANCES.
-- IF no tenancy instance start AND end dates have been supplied,
-- get them FROM TENANCIES. Check that a duplicate record does NOT
-- already exist.
--
--
  IF (    (p1.lpar_tcy_ind = 'Y')
     and ( p1.lpar_tcy_alt_ref != '~NONE~')
   )
   THEN
    l_tcy_refno      := NULL;
    l_tcy_start_date := NULL;
    l_tcy_end_date   := NULL;
    l_tcy_end_reason := NULL;
--
    OPEN  c_tcy(p1.lpar_tcy_alt_ref);
    FETCH c_tcy INTO l_tcy_refno, l_tcy_start_date
                   , l_tcy_end_date, l_tcy_end_reason;
    CLOSE c_tcy;
--
    IF p1.lpar_tin_start_date IS NOT NULL
     THEN
      l_tcy_start_date := p1.lpar_tin_start_date;
    END IF;
--
--
-- dbms_output.put_line('Inserting into Tenancy Instances');
-- dbms_output.put_line('Tin_tcy_refno = '||l_tcy_refno);
-- dbms_output.put_line('Tin_hop_refno = '||l_hop_refno);
--
    INSERT INTO tenancy_instances(tin_tcy_refno
                                 ,tin_hop_refno
                                 ,tin_start_date
                                 ,tin_stat_successor_ind
                                 ,tin_created_by
                                 ,tin_created_date
                                 ,tin_end_date
                                 ,tin_hrv_tir_code
                                 ,tin_main_tenant_ind)
    SELECT                        l_tcy_refno
                                 ,l_hop_refno
                                 ,l_tcy_start_date
                                 ,nvl(p1.lpar_tin_stat_successor_ind,'N')
                                 ,'DATALOAD'
                                 ,sysdate
                                 ,nvl(p1.lpar_tin_end_date,l_tcy_end_date)
                                 ,l_tcy_end_reason
                                 ,p1.lpar_tin_main_tenant_ind
                                 FROM dual
                                 WHERE NOT EXISTS
                                    (SELECT null
                                     FROM  tenancy_instances h2
                                     WHERE h2.tin_tcy_refno = l_tcy_refno
                                       AND h2.tin_hop_refno = l_hop_refno
                                       AND h2.tin_start_date = l_tcy_start_date);
--
  END IF; -- tcy_ind = 'Y'
--
--
  l_con_tcy_start := NULL;
  l_con_tcy_start := nvl(p1.lpar_tin_start_date,l_tcy_start_date);
  l_con_tcy_start := nvl(l_con_tcy_start,p1.lpar_hop_start_date);
  l_con_tcy_start := nvl(l_con_tcy_start,sysdate);
--
-- Do the insert into contact_details
--
  IF p1.lpar_phone IS NOT NULL
   THEN
    INSERT INTO contact_details
        (cde_refno
        ,cde_start_date
        ,cde_created_date
        ,cde_created_by
        ,cde_contact_value
        ,cde_frv_cme_code
        ,cde_contact_name
        ,cde_end_date
        ,cde_pro_refno
        ,cde_aun_code
        ,cde_par_refno
        ,cde_bde_refno
        ,cde_cos_code
        ,cde_cse_contact
        ,cde_srq_no )
    VALUES
        (cde_refno.nextval
        ,l_con_tcy_start
        ,trunc(sysdate)
        ,'DATALOAD'
        ,p1.lpar_phone
        ,'TELEPHONE'
        ,null
        ,null
        ,null
        ,null
        ,l_par_refno
        ,null
        ,null
        ,null
        ,null
        );
  END IF;
--
-- Do the Insert into Interested Parties
--
  IF p1.lpar_ipp_shortname IS NOT NULL
   THEN
--
    l_ipp_refno  := NULL;
    l_ipp_exists := NULL;
--
-- Check that this Interested Party doesn't
-- already exist
--
    OPEN c_ipp_exists(p1.lpar_ipp_shortname, p1.lpar_ipp_ipt_code);
    FETCH c_ipp_exists into l_ipp_exists, l_ipp_refno;
    CLOSE c_ipp_exists;
--
    IF (l_ipp_exists IS NULL) THEN
--
     OPEN c_ipp_refno;
     FETCH c_ipp_refno INTO l_ipp_refno;
     CLOSE c_ipp_refno;
--
-- A trigger only allows one of par_refno or
-- username to be populated on interested_parties
-- therefore if username is supplied don't use
-- par_refno
--
     IF p1.lpar_ipp_usr_username IS NOT NULL THEN
      l_par_refno := NULL;
     END IF;
--
     INSERT INTO interested_parties
          (ipp_refno
          ,ipp_shortname
          ,ipp_placement_ind
          ,ipp_current_ind
          ,ipp_ipt_code
          ,ipp_par_refno
          ,ipp_usr_username
          ,ipp_spr_printer_name
          ,ipp_comments
          ,ipp_vca_code
          ,ipp_staff_id
          ,ipp_hrv_fit_code
          ,ipp_cos_code
          )
       VALUES
          (l_ipp_refno
          ,p1.lpar_ipp_shortname
          ,p1.lpar_ipp_placement_ind
          ,p1.lpar_ipp_current_ind
          ,p1.lpar_ipp_ipt_code
          ,l_par_refno
          ,p1.lpar_ipp_usr_username
          ,p1.lpar_ipp_spr_printer_name
          ,p1.lpar_ipp_comments
          ,p1.lpar_ipp_vca_code
          ,p1.lpar_ipp_staff_id
          ,p1.lpar_ipp_hrv_fit_code
          ,p1.lpar_ipp_cos_code
          );
--
     END IF; -- l_ipp_exists IS NULL
     
     IF p1.lpar_ipu_aun_code IS NOT NULL 
     AND l_ipp_refno IS NOT NULL THEN
--
       INSERT INTO interested_party_admin_unit
          (ipu_ipp_refno
          ,ipu_aun_code
          ,ipu_created_by
          ,ipu_created_date
          )
       VALUES
          (l_ipp_refno
          ,p1.lpar_ipu_aun_code
          ,'DATALOAD'
          ,trunc(sysdate)
          );    
--  
    END IF; -- p1.lpar_ipu_aun_code IS NOT NULL AND l_ipp_refno IS NOT NULL THEN
--
  END IF; -- p1.lpar_ipp_shortname IS NOT NULL
--
-- now hold the par_refno for each record for ease of checking as
-- interested party update could set the l_par_refno
--
  UPDATE DL_HEM_PEOPLE
  SET lpar_c_par_refno = l_c_par_refno
  WHERE rowid = p1.rec_rowid
  AND lpar_dlb_batch_id = p1.lpar_dlb_batch_id
  AND lpar_dl_seqno = p1.lpar_dl_seqno;
--
-- ************************************
-- Set the dataload statuses
--
  s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
  set_record_status_flag(l_id,'C');
--
-- keep a count of the rows processed and commit after every 1000
--
  i := i+1;
  IF MOD(i,1000)=0
   THEN
    COMMIT;
-- Do a regular analyse table based on 10 times as many records as last time
    IF i>= (ai*10)
     THEN
      ai := i;
      l_an_tab:=s_dl_hem_utils.dl_comp_stats('PARTIES');
      l_an_tab:=s_dl_hem_utils.dl_comp_stats('HOUSEHOLD_PERSONS');
      l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANCY_INSTANCES');
      l_an_tab:=s_dl_hem_utils.dl_comp_stats('CONTACT_DETAILS');
    END IF;
  END IF;
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
--
COMMIT;
--
-- Section to analyse the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PARTIES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('HOUSEHOLD_PERSONS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANCY_INSTANCES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CONTACT_DETAILS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('INTERESTED_PARTIES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HEM_PEOPLE');
--
--
fsc_utils.proc_end;
COMMIT;
--
 EXCEPTION
  WHEN OTHERS THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
  RAISE;
--
END dataload_create;
--**************************************************************
--
PROCEDURE dataload_validate
(p_batch_id          IN VARCHAR2
,p_date              IN DATE    )
AS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,lpar_dlb_batch_id
,lpar_dl_seqno
,lpar_hop_start_date
,lpar_per_surname
,lpar_tcy_alt_ref
,lpar_per_forename
,lpar_hop_hpsr_code
,lpar_per_title
,lpar_per_initials
,lpar_per_date_of_birth
,lpar_per_hou_disabled_ind
,lpar_per_hou_oap_ind
,lpar_per_frv_fge_code
,lpar_hop_hrv_rel_code
,lpar_per_hou_employer
,lpar_per_hou_hrv_hms_code
,lpar_hop_end_date
,lpar_hop_hper_code
,lpar_tcy_ind
,lpar_tin_main_tenant_ind
,lpar_tin_start_date
,lpar_tin_end_date
,lpar_tin_hrv_tir_code
,lpar_tin_stat_successor_ind
,lpar_per_alt_ref
,lpar_per_frv_feo_code
,lpar_per_ni_no
,lpar_per_frv_hgo_code
,lpar_per_frv_fnl_code
,lpar_per_other_name
,lpar_phone
,lpar_hou_legacy_reF
,lpar_ipp_shortname
,lpar_ipp_placement_ind
,lpar_ipp_current_ind
,lpar_ipp_ipt_code
,lpar_ipp_usr_username
,lpar_ipp_spr_printer_name
,lpar_ipp_comments
,lpar_ipp_vca_code
,lpar_ipu_aun_code
,lpar_ipp_staff_id
,lpar_ipp_cos_code
,lpar_ipp_hrv_fit_code
,NVL(lpar_type,'HOUP')  lpar_type
,lpar_org_sort_code
,lpar_org_name
,lpar_org_short_name
,lpar_org_frv_oty_code
,lpar_per_hou_at_risk_ind
,lpar_per_hou_hrv_ntly_code
,lpar_per_hou_hrv_sexo_code
,lpar_per_hou_hrv_rlgn_code
,lpar_per_hou_hrv_ecst_code
,lpar_org_current_ind
,lpar_hop_head_hhold_ind
,lpar_hop_hhold_group_no
,lpar_c_par_refno
FROM  dl_hem_people
WHERE lpar_dlb_batch_id    = p_batch_id
AND   lpar_dl_load_status IN ('L','F','O');
--
-- ************************************
CURSOR c_chk_alt_ref(p_per_alt_ref VARCHAR2
                    ,p_per_surname VARCHAR2
                    ,p_per_forename VARCHAR2
                    ,p_per_initials VARCHAR2) is
  SELECT 'X'
  FROM   parties
  WHERE  par_per_alt_ref        = p_per_alt_ref
    AND  (par_per_surname       != p_per_surname
          or  (    p_per_forename   is NOT NULL
               AND par_per_forename is NOT NULL
               AND p_per_forename   != par_per_forename
               )
          or  (    p_per_initials   is NOT NULL
               AND par_per_initials is NOT NULL
               AND p_per_initials   != par_per_initials
              )
          );
--
-- ************************************
CURSOR c_tcy_dates(p_tcy_refno number) is
SELECT tcy_act_start_date, tcy_act_end_date
FROM tenancies
WHERE tcy_refno = p_tcy_refno;
--
-- ************************************
CURSOR c_ipp_exists(p_ipp_short varchar2,
                    p_ipt_code  varchar2) IS
SELECT 'X'
FROM   interested_parties
WHERE  ipp_shortname = p_ipp_short
AND    ipp_ipt_code  = p_ipt_code;
--
-- ************************************
CURSOR c_ipt_code(p_ipt_code varchar2) is
SELECT 'X'
FROM   interested_party_types
WHERE  ipt_code = p_ipt_code;
--
-- ************************************
CURSOR c_usr(p_user varchar2) is
SELECT 'X'
FROM   users
WHERE  usr_username = p_user;
--
-- ************************************
CURSOR c_printer(p_printer varchar2) is
SELECT 'X'
FROM   system_printers
WHERE  spr_printer_name = p_printer;
--
-- ************************************
CURSOR c_vca_code (p_vca_code varchar2) is
SELECT 'X'
FROM   vat_categories
WHERE  vca_code = p_vca_code;
--
-- ************************************
CURSOR c_aun_check(p_aun_code varchar2) is
SELECT 'X'
FROM   admin_units
WHERE  aun_code = p_aun_code;
--
-- ************************************
CURSOR c_ipt_data(p_ipt_code  varchar2) IS
SELECT ipt_capture_staff_ind,
       ipt_contractor_site_ind
FROM   interested_party_types
WHERE  ipt_code   =  p_ipt_code;
--
-- ************************************
CURSOR c_cos_exists (p_cos_code varchar2) IS
SELECT 'X'
FROM   contractor_sites
WHERE  cos_code = p_cos_code;
--
-- ************************************
CURSOR c_staff_exists(p_staff_id varchar2) IS
SELECT 'X'
FROM   interested_parties
WHERE  ipp_staff_id = p_staff_id;
--
-- ************************************
CURSOR c_conm(p_conm_code VARCHAR2)  IS
SELECT conm_current_ind
      ,conm_code
      ,conm_digits_only_ind
      ,conm_value_min_length
      ,conm_value_max_length
      ,conm_spaces_allow_ind
FROM   contact_methods
WHERE  conm_code = p_conm_code;
--
-- ************************************
CURSOR c_conm_spaces(p_dl_conm_value     VARCHAR2
                    ,p_lpar_dlb_batch_id VARCHAR2
                    ,p_lpar_dl_seqno     NUMBER
                    ,p_lpar_phone        VARCHAR2)  IS
SELECT 'X'
FROM   dl_hem_people
WHERE  lpar_dlb_batch_id = p_lpar_dlb_batch_id
AND  lpar_dl_seqno = p_lpar_dl_seqno
AND  lpar_phone = p_lpar_phone
AND  NVL(p_dl_conm_value,'') NOT LIKE '% %';
--
-- ************************************
CURSOR c_chk_duporg_batch(p_org_name         VARCHAR2
                         ,p_org_short_name   VARCHAR2
                         ,p_org_frv_oty_code VARCHAR2
                         ,p_batch            VARCHAR2) IS
SELECT COUNT(*)
FROM   dl_hem_people
WHERE  lpar_org_name != p_org_name
AND    lpar_org_short_name = p_org_short_name
AND    lpar_org_frv_oty_code != p_org_frv_oty_code
AND    lpar_dlb_batch_id = p_batch;
--
-- ************************************
CURSOR c_hhold_dl( p_lpar_tcy_alt_ref    VARCHAR2
                  ,p_lpar_dlb_batch_id   VARCHAR2
                  ,p_lpar_hhold_group_no NUMBER   )  IS
SELECT count (*)
FROM   dl_hem_people
WHERE  lpar_dlb_batch_id = p_lpar_dlb_batch_id
  AND  NVL(lpar_type,'HOUP') != 'ORG'
  AND  lpar_tcy_alt_ref = p_lpar_tcy_alt_ref
  AND  lpar_hop_head_hhold_ind = 'Y'
  AND  lpar_hop_hhold_group_no = p_lpar_hhold_group_no;
--
-- ************************************
CURSOR c_hou_refno(p_tcy_alt_ref VARCHAR2) is
SELECT max(hop_hou_refno)
FROM household_persons
    ,tenancy_instances
    ,tenancies
WHERE tcy_alt_ref       = p_tcy_alt_ref
  AND tin_tcy_refno     = tcy_refno
  AND tin_hop_refno     = hop_refno;
-- ************************************
--CURSOR c_existing_hhold(p_hou_refno NUMBER,p_group_no NUMBER) IS
--SELECT count(*)
--FROM   household_persons
--WHERE  hop_hou_refno = p_hou_refno
--  AND  hop_hhold_group_no = p_group_no;
--
CURSOR c_existing_hhold(p_hou_refno         NUMBER
                       ,p_group_no          NUMBER
                       ,p_lpar_tcy_alt_ref  VARCHAR2
                       ,p_lpar_dlb_batch_id VARCHAR2 ) IS
SELECT count(*)
FROM   household_persons,
       parties
WHERE  hop_hou_refno = p_hou_refno
  AND  hop_hhold_group_no = p_group_no
  AND  hop_par_refno = par_refno
  AND  hop_head_hhold_ind = 'Y'
  AND  par_per_alt_ref NOT IN (SELECT lpar_per_alt_ref
                               FROM   dl_hem_people
                               WHERE  lpar_dlb_batch_id = p_lpar_dlb_batch_id
                                 AND  NVL(lpar_type,'HOUP') != 'ORG'
                                 AND  lpar_tcy_alt_ref = p_lpar_tcy_alt_ref
                                 AND  lpar_hop_head_hhold_ind = 'Y'
                                 AND  lpar_hop_hhold_group_no = p_group_no);
--
-- ************************************
CURSOR c_pmco_check(p_column_name VARCHAR2) IS
SELECT DISTINCT 'X'
FROM   person_mandatory_columns
WHERE  pmco_mod_name IN ('PAR007','HSS-CLI-C','PAR008','HSS-CLI-U')
  AND  pmco_pdc_column_name = p_column_name
  AND  pmco_hrv_mle_code != 'OPTIONAL';
-- ************************************
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HEM_PEOPLE';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- Other Variables
l_exists             VARCHAR2(1);
l_tcy_refno          INTEGER;
l_tcy_act_start_date DATE;
l_tcy_act_end_date   DATE;
l_car_exists         VARCHAR2(1);
i                    INTEGER:=0;
l_errors             VARCHAR2(10);
l_error_ind          VARCHAR2(10);
l_capture_staff      VARCHAR2(1);
l_cos_reqd           VARCHAR2(1);
l_answer             VARCHAR2(1);
l_duporg             INTEGER;
l_count_hhold_dl     INTEGER;
l_hou_refno          INTEGER;
l_count_hhold_hop    INTEGER;
l_count_total        INTEGER;
l_title_man          VARCHAR2(1);
--
-- Variables for contact details check
--
li                  INTEGER := 0;
l_conm_code_in      VARCHAR2(10);
l_conm_code_out     VARCHAR2(10);
l_conm_cur          VARCHAR2(1);
l_conm_dig          VARCHAR2(1);
l_conm_min_len      NUMBER(3,0);
l_conm_max_len      NUMBER(3,0);
l_conm_spaces       VARCHAR2(1);
l_chk_conm_spaces   VARCHAR2(1);
l_char contact_details.cde_contact_value%TYPE;
--
-- ************************************
BEGIN
--
 fsc_utils.proc_start('s_dl_hem_people.dataload_validate');
 fsc_utils.debug_message('s_dl_hem_people.dataload_validate',3);
--
 cb := p_batch_id;
 cd := p_date;
--
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- check if Surname is mandatory for Person type records
--
 l_answer := s_dl_batches.get_answer(p_batch_id, 1);
 IF l_answer = NULL THEN l_answer := 'N'; END IF;
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lpar_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors := 'V';
  l_error_ind := 'N';
--
-- Check tenancy reference exists on TENANCIES
--
  IF p1.lpar_tcy_alt_ref != '~NONE~' THEN
--
   l_tcy_refno := NULL;
   l_tcy_refno := s_dl_hem_utils.tcy_refno_FOR_alt_ref(p1.lpar_tcy_alt_ref);
--
   IF l_tcy_refno IS NULL THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',080);
   ELSE
--
-- Check tenancy instances dates fall within tenancy dates
--
     l_tcy_act_start_date := NULL;
     l_tcy_act_end_date   := NULL;
--
     OPEN c_tcy_dates(l_tcy_refno);
     FETCH c_tcy_dates INTO l_tcy_act_start_date, l_tcy_act_end_date;
     CLOSE c_tcy_dates;
--
     IF (p1.lpar_tin_start_date
        NOT BETWEEN l_tcy_act_start_date
            AND nvl(l_tcy_act_end_date,p1.lpar_tin_start_date))
      THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',090);
     END IF;
--
     IF (p1.lpar_tin_end_date is NOT NULL)
     THEN
      IF (p1.lpar_tin_end_date NOT BETWEEN
       l_tcy_act_start_date AND nvl(l_tcy_act_end_date,p1.lpar_tin_end_date))
       THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',091);
      END IF;
     END IF;
   END IF; -- tcy_refno found
--
-- If linked to a tenancy the lpar_hop_start_date is mandatory for
-- household_persons record
--
   IF (p1.lpar_hop_start_date IS NULL)
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',078);
   END IF;
  END IF; -- not a standalone party i.e not ~NONE~
--
-- Check Y/N values
--
-- Disabled indicator
--
  IF (NOT s_dl_hem_utils.yornornull(p1.lpar_per_hou_disabled_ind))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',081);
  END IF;
--
-- OAP indicator
--
  IF (NOT s_dl_hem_utils.yornornull(p1.lpar_per_hou_oap_ind))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',082);
  END IF;
--
-- Tenancy indicator
--
  IF (NOT s_dl_hem_utils.yornornull(p1.lpar_tcy_ind))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',083);
  END IF;
--
-- Main Tenant Indicator
--
  IF (NOT s_dl_hem_utils.yornornull(p1.lpar_tin_main_tenant_ind))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',412);
  END IF;
--
-- Succession indicator
--
  IF (NOT s_dl_hem_utils.yornornull(p1.lpar_tin_stat_successor_ind))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',084);
  END IF;
--
-- At Risk Indicator
--
  IF (NOT s_dl_hem_utils.yornornull(p1.lpar_per_hou_at_risk_ind))
   THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',269);
  END IF;
--
-- Check reference values
--
-- Start reason
--
  IF (NOT s_dl_hem_utils.exists_frv('HLD_START',p1.lpar_hop_hpsr_code,'Y'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',085);
  END IF;
--
-- Sex
--
  IF (NOT s_dl_hem_utils.exists_frv('SEX',p1.lpar_per_frv_fge_code,'Y'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',086);
  END IF;
-- Relationship
--
  IF (NOT s_dl_hem_utils.exists_frv('RELATION',p1.lpar_hop_hrv_rel_code,'Y'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',087);
  END IF;
--
-- Marital status
--
  IF (NOT s_dl_hem_utils.exists_frv('MAR_STAT',p1.lpar_per_hou_hrv_hms_code,'Y'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',088);
  END IF;
--
-- End reason
--
  IF (NOT s_dl_hem_utils.exists_frv('HLD_END',p1.lpar_hop_hper_code,'Y'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',089);
  END IF;
--
  IF ((p1.lpar_hop_hper_code IS NOT NULL AND p1.lpar_hop_end_date IS NULL)
    or
     (p1.lpar_hop_hper_code IS NULL AND p1.lpar_hop_end_date IS NOT NULL)
   )
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',093);
  END IF;
--
-- Tenancy end reason code
--
  IF (NOT s_dl_hem_utils.exists_frv('TCY_TERM',p1.lpar_tin_hrv_tir_code,'Y'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',413);
  END IF;
--
-- Ethnic origin
--
-- Changed domian from ETHNIC to ETHNIC2. PH 08/01/03
--
  IF (NOT s_dl_hem_utils.exists_frv('ETHNIC2',p1.lpar_per_frv_feo_code,'Y'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',092);
  END IF;
--
-- Geographic Origin
--
-- Changed domian from ETHNIC2 to ETHNIC. PH 08/01/03
--
  IF (NOT s_dl_hem_utils.exists_frv('ETHNIC',p1.lpar_per_frv_hgo_code,'Y'))
   THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',326);
  END IF;
--
-- Language Code
--
  IF (NOT s_dl_hem_utils.exists_frv('SUPPORTED_NLD',p1.lpar_per_frv_fnl_code,'Y'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',327);
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
-- title check is mandatory and if supplied exists in domain TITLE (AJ)
--
  l_title_man := NULL;
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
-- Check that the person end date is NOT earlier than the person
-- start date
--
  IF (p1.lpar_hop_end_date IS NOT NULL)
   THEN
   IF (p1.lpar_hop_end_date < p1.lpar_hop_start_date)
    THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',094);
   END IF;
  END IF;
--
-- Ensure the Alternative Reference Number does NOT clash with any others
-- (of course this person may have been loaded before on another tenancy
--  so IF it does exist THEN just only a problem IF name doesn't match)
--
-- OPEN c_chk_alt_ref(p1.lpar_per_alt_ref,p1.lpar_per_surname,
--                    p1.lpar_per_forename,p1.lpar_per_initials);
-- FETCH c_chk_alt_ref INTO l_car_exists;
-- IF c_chk_alt_ref%FOUND
--   THEN
--   CLOSE c_chk_alt_ref;
--   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',765);
-- ELSE
--   CLOSE c_chk_alt_ref;
-- END IF;
--
-- Checks on Interested Parties Data
--
-- IF Shortname not supplied none of the other
-- Interested Party fields should be supplied
--
  IF (    p1.lpar_ipp_placement_ind     IS NOT NULL
       OR p1.lpar_ipp_current_ind       IS NOT NULL
       OR p1.lpar_ipp_ipt_code          IS NOT NULL
       OR p1.lpar_ipp_usr_username      IS NOT NULL
       OR p1.lpar_ipp_spr_printer_name  IS NOT NULL
       OR p1.lpar_ipp_comments          IS NOT NULL
       OR p1.lpar_ipp_vca_code          IS NOT NULL
       OR p1.lpar_ipu_aun_code          IS NOT NULL
     )
   THEN
     IF (p1.lpar_ipp_shortname IS NULL)
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',424);
     END IF;
  END IF;
--
-- ************
-- Check that this Interested Party doesn't
-- already exist
-- removed IPP check as now in create section AJ 08Feb2016
--
--   OPEN c_ipp_exists(p1.lpar_ipp_shortname, p1.lpar_ipp_ipt_code);
--    FETCH c_ipp_exists into l_exists;
--     IF c_ipp_exists%found
--      THEN
--       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',425);
--     END IF;
--   CLOSE c_ipp_exists;
-- ************
--
-- Check if the Shortname IS supplied
-- Then the other Mandatory fields should
-- be supplied.
--
  IF   (p1.lpar_ipp_shortname     IS NOT NULL)
--
   THEN
    IF (p1.lpar_ipp_placement_ind IS NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',426);
    END IF;
--
    IF (p1.lpar_ipp_current_ind   IS NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',427);
    END IF;
--
    IF (p1.lpar_ipp_ipt_code      IS NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',428);
    END IF;
--
  END IF;
--
-- Check the Y/N Fields
--
-- Placement Indicator
--
  IF (NOT s_dl_hem_utils.yornornull(p1.lpar_ipp_placement_ind))
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',429);
  END IF;
--
-- Current Ind
--
  IF (NOT s_dl_hem_utils.yornornull(p1.lpar_ipp_current_ind))
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',430);
  END IF;
--
-- Check the Interested Party Type is Valid
--
  IF p1.lpar_ipp_ipt_code IS NOT NULL
  THEN
    OPEN c_ipt_code(p1.lpar_ipp_ipt_code);
    FETCH c_ipt_code into l_exists;
      IF c_ipt_code%notfound
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',431);
      END IF;
    CLOSE c_ipt_code;
  END IF;
--
-- Check user exists if supplied
--
  IF (p1.lpar_ipp_usr_username IS NOT NULL)
   THEN
    OPEN c_usr(p1.lpar_ipp_usr_username);
     FETCH c_usr into l_exists;
      IF c_usr%notfound
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',432);
      END IF;
    CLOSE c_usr;
  END IF;
--
-- Check Printer Exists if supplied
--
  IF (p1.lpar_ipp_spr_printer_name IS NOT NULL)
   THEN
    OPEN c_printer(p1.lpar_ipp_spr_printer_name);
     FETCH c_printer into l_exists;
      IF c_printer%notfound
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',433);
      END IF;
    CLOSE c_printer;
  END IF;
--
-- Vat Category
--
  IF (p1.lpar_ipp_vca_code IS NOT NULL)
   THEN
    OPEN c_vca_code(p1.lpar_ipp_vca_code);
     FETCH c_vca_code into l_exists;
      IF c_vca_code%notfound
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',656);
      END IF;
    CLOSE c_vca_code;
  END IF;
--
-- Admin Unit
--
  IF (p1.lpar_ipu_aun_code IS NOT NULL)
   THEN
    OPEN c_aun_check(p1.lpar_ipu_aun_code);
    FETCH c_aun_check into l_exists;
     IF c_aun_check%notfound
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',434);
     END IF;
    CLOSE c_aun_check;
  END IF;
--
-- Check the Party type
--
  IF p1.lpar_type NOT IN ('ORG', 'HOUP', 'FREE', 'BOTP')
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',436);
  END IF;
  IF (p1.lpar_type  IN ('HOUP', 'BOTP')
      AND l_answer = 'Y'
      AND p1.lpar_per_surname IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',856);
  END IF;
--
-- Organisation Checks brought together and added too (AJ)
--
--
-- Check for Duplicate Organisation in batch
--
  IF (p1.lpar_org_sort_code IS NOT NULL    OR
      p1.lpar_org_name IS NOT NULL         OR
      p1.lpar_org_short_name IS NOT NULL   OR
      p1.lpar_org_frv_oty_code IS NOT NULL  )
   THEN
   IF (nvl(p1.lpar_org_current_ind,'X') NOT IN ('Y','N'))
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',430);
   END IF;
  END IF;
--
-- Organisation Current Indicator
-- Check mandatory only if organisation
--
  l_duporg:=0;
--
  IF (p1.lpar_org_name IS NOT NULL         AND
      p1.lpar_org_short_name IS NOT NULL   AND
      p1.lpar_org_frv_oty_code IS NOT NULL    )
   THEN
    OPEN c_chk_duporg_batch( p1.lpar_org_name
                            ,p1.lpar_org_short_name
                            ,p1.lpar_org_frv_oty_code
                            ,p1.lpar_dlb_batch_id);
    FETCH c_chk_duporg_batch into l_duporg;
    CLOSE c_chk_duporg_batch;
    IF l_duporg > 0 -- 7.8
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',252);
    END IF;
  END IF;
--
-- Check that if the Organisation Type is supplied
-- the Party type is of type ORG
--
  IF (p1.lpar_org_frv_oty_code IS NOT NULL)
    THEN
      IF p1.lpar_type != 'ORG'
       THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',437);
      END IF;
  END IF;
--
-- Check the Organisation Party type is valid
-- if supplied
--
  IF (NOT s_dl_hem_utils.exists_frv('ORG_TYPE',p1.lpar_org_frv_oty_code,'Y'))
     THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',438);
  END IF;
--
-- IF Organisation supplied none of the other
-- party fields should be supplied except alternative reference
-- head of household and household groups added to check (AJ)
--
  IF (   p1.lpar_org_sort_code    IS NOT NULL
      OR p1.lpar_org_name         IS NOT NULL
      OR p1.lpar_org_short_name   IS NOT NULL
      OR p1.lpar_org_frv_oty_code IS NOT NULL
     )
   THEN
     IF (   p1.lpar_per_surname           IS NOT NULL
         OR p1.lpar_per_forename          IS NOT NULL
         OR p1.lpar_per_title             IS NOT NULL
         OR p1.lpar_per_initials          IS NOT NULL
         OR p1.lpar_per_date_of_birth     IS NOT NULL
         OR p1.lpar_per_hou_disabled_ind  IS NOT NULL
         OR p1.lpar_per_hou_oap_ind       IS NOT NULL
         OR p1.lpar_per_frv_fge_code      IS NOT NULL
         OR p1.lpar_hop_hrv_rel_code      IS NOT NULL
         OR p1.lpar_per_hou_employer      IS NOT NULL
         OR p1.lpar_per_hou_hrv_hms_code  IS NOT NULL
         OR p1.lpar_per_frv_feo_code      IS NOT NULL
         OR p1.lpar_per_ni_no             IS NOT NULL
         OR p1.lpar_per_frv_hgo_code      IS NOT NULL
         OR p1.lpar_per_frv_fnl_code      IS NOT NULL
         OR p1.lpar_per_other_name        IS NOT NULL
         OR p1.lpar_per_hou_at_risk_ind   IS NOT NULL
         OR p1.lpar_per_hou_hrv_ntly_code IS NOT NULL
         OR p1.lpar_per_hou_hrv_sexo_code IS NOT NULL
         OR p1.lpar_per_hou_hrv_rlgn_code IS NOT NULL
         OR p1.lpar_per_hou_hrv_ecst_code IS NOT NULL
         OR p1.lpar_hop_head_hhold_ind    IS NOT NULL 
         OR p1.lpar_hop_hhold_group_no    IS NOT NULL 
        )
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',253);
     END IF;
  END IF;
--
-- Check Staff Id
--
  l_capture_staff      := NULL;
  l_cos_reqd           := NULL;
--
  OPEN c_ipt_data(p1.lpar_ipp_ipt_code);
  FETCH c_ipt_data into l_capture_staff, l_cos_reqd;
  CLOSE c_ipt_data;
--
  IF nvl(l_capture_staff, 'X') = 'M'
    AND p1.lpar_ipp_staff_id IS NULL
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',240);
--
  ELSIF nvl(l_capture_staff, 'X') = 'N'
    AND p1.lpar_ipp_staff_id IS NOT NULL
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',241);
  END IF;
--
  IF p1.lpar_ipp_staff_id IS NOT NULL
   THEN
    OPEN c_staff_exists(p1.lpar_ipp_staff_id);
    FETCH c_staff_exists into l_exists;
      IF c_staff_exists%found
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',245);
      END IF;
    CLOSE c_staff_exists;
  END IF;
--
-- Check the Contractor Site
--
  IF nvl(l_cos_reqd, 'X') = 'M'
   AND p1.lpar_ipp_cos_code IS NULL
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',242);
--
  ELSIF nvl(l_cos_reqd, 'X') = 'N'
   AND p1.lpar_ipp_cos_code IS NOT NULL
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',243);
--
  END IF;
--
  IF nvl(l_cos_reqd, 'X') in ('O', 'M' )
    AND p1.lpar_ipp_cos_code IS NOT NULL
    THEN
     OPEN c_cos_exists(p1.lpar_ipp_cos_code);
     FETCH c_cos_exists into l_exists;
      IF c_cos_exists%notfound
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',244);
      END IF;
     CLOSE c_cos_exists;
  END IF;
--
-- add check on telephone number check if supplied AJ 09Feb2016
--
  l_conm_cur        := NULL;
  l_conm_code_in    := 'TELEPHONE';
  l_conm_code_out   := NULL;
  l_conm_dig        := NULL;
  l_conm_min_len    := NULL;
  l_conm_max_len    := NULL;
  l_conm_spaces     := NULL;
  l_chk_conm_spaces := NULL;
  l_char            := NULL;
--
  IF p1.lpar_phone IS NOT NULL
   THEN
--
   OPEN c_conm(l_conm_code_in);
   FETCH c_conm INTO l_conm_cur
                    ,l_conm_code_out
                    ,l_conm_dig
                    ,l_conm_min_len
                    ,l_conm_max_len
                    ,l_conm_spaces;
   CLOSE c_conm;
--
-- check that contact method exists (l_conm_code_out)
--
   IF l_conm_code_out IS NULL
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',829);
   END IF;
--
-- further checks only if contact method found
--
   IF l_conm_code_out IS NOT NULL
     THEN
      li := LENGTH(p1.lpar_phone);
--
-- check that only contains digits if set to Y (l_conm_dig)
--
    IF l_conm_dig = 'Y'
      THEN
       l_char := SUBSTR(p1.lpar_phone,li,1);
        IF l_char NOT IN ('0','1','2','3','4','5','6','7','8','9')
         THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',830);
        END IF;
    END IF;
--
-- check the contact value length conforms to min and max lengths specified
-- l_conm_min_len and l_conm_max_len
--
    IF NVL(l_conm_min_len,li) > li
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',831);
    END IF;
--
    IF NVL(l_conm_max_len,li) < li
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',832);
    END IF;
--
-- check that contact values does not contain spaces if set (l_conm_spaces)
--
    IF l_conm_spaces = 'N'
      THEN
--
       OPEN c_conm_spaces(p1.lpar_phone
                         ,p1.lpar_dlb_batch_id
                         ,p1.lpar_dl_seqno
                         ,p1.lpar_phone);
       FETCH c_conm_spaces INTO l_chk_conm_spaces;
        IF c_conm_spaces%NOTFOUND
         THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',833);
        END IF;
       CLOSE c_conm_spaces;
--
    END IF;
--
   END IF;
--
  END IF;		-- end of telephone number check
--
-- Checks for Head of Household and House Groups added to
-- household_persons in v61.4 release
--
-- There must be one marked as head of household for each group
-- the groups need to be different only if there is more that one
-- group against the same tenancy
--
-- Check param to see if groups are required
--
  l_count_hhold_dl  := NULL;
  l_hou_refno       := NULL;
  l_count_hhold_hop := NULL;
  l_count_total     := NULL;
--
-- If groups not required then head household and groups
-- fields must not be supplied
--
-- these groups do not apply to organisations or parties not linked
-- to tenancies
--
  IF (p1.lpar_type = 'ORG'  OR  p1.lpar_tcy_alt_ref = '~NONE~')
   THEN
    IF ( p1.lpar_hop_head_hhold_ind   IS NOT NULL
      OR p1.lpar_hop_hhold_group_no   IS NOT NULL
       )
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',344);
    END IF;
  END IF;
--
-- groupings are not required on any people or organisation if no required
--
--  IF fsc_utils.get_sys_param('HOUSEHOLD_GROUPINGS_REQD') = 'N'
--   THEN
--
--    IF ( p1.lpar_hop_head_hhold_ind IS NOT NULL
--     OR p1.lpar_hop_hhold_group_no IS NOT NULL
--       )
--       THEN
--        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',342);
--    END IF;
--  END IF; -- end of groupings not required
--
-- groupings are required exclude organisations or parties not linked to tenancies
--
--
  IF fsc_utils.get_sys_param('HOUSEHOLD_GROUPINGS_REQD') = 'Y'
   THEN
--
    IF (
        ( p1.lpar_type != 'ORG' AND p1.lpar_tcy_alt_ref != '~NONE~' )
      --   OR
      --  ( p1.lpar_type = 'ORG')
       )
     THEN
--
      IF ( p1.lpar_hop_head_hhold_ind IS NULL
        OR p1.lpar_hop_hhold_group_no IS NULL
         )
         THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',343);
      END IF;
--
-- check to see if head of hhold exists for group against the tenancy
--
-- first check batch being loaded
--
      OPEN  c_hhold_dl( p1.lpar_tcy_alt_ref
                       ,p1.lpar_dlb_batch_id
                       ,p1.lpar_hop_hhold_group_no);
      FETCH c_hhold_dl INTO l_count_hhold_dl;
      CLOSE c_hhold_dl;
--
-- then get latest household ref for tenancy
--
      OPEN c_hou_refno(p1.lpar_tcy_alt_ref);
      FETCH c_hou_refno INTO l_hou_refno;
      CLOSE c_hou_refno;
--
-- then check household persons for household ref for tenancy
--
      IF (l_hou_refno IS NOT NULL)
       THEN
        OPEN c_existing_hhold( l_hou_refno
                              ,p1.lpar_hop_hhold_group_no
                              ,p1.lpar_tcy_alt_ref
                              ,p1.lpar_dlb_batch_id );
        FETCH c_existing_hhold INTO l_count_hhold_hop;
        CLOSE c_existing_hhold;
      END IF;
--
    l_count_total := nvl(l_count_hhold_dl,0) + nvl(l_count_hhold_hop,0);
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
    END IF;
  END IF; -- end of groupings required check
--
-- ************************************
-- Now UPDATE the record count and error code
--
  IF l_errors = 'F' THEN
    l_error_ind := 'Y';
  ELSE
    l_error_ind := 'N';
  END IF;
--
-- Now UPDATE the record count and error code
  IF l_errors = 'F' THEN
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
  i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
--
  EXCEPTION
    WHEN OTHERS THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
    set_record_status_flag(l_id,'O');
  END;
--
 END LOOP;
--
COMMIT;
--
fsc_utils.proc_END;
--
 EXCEPTION
   WHEN OTHERS THEN
   s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
   RAISE;
--
END dataload_validate;
--
--**************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) AS
--
CURSOR c1 is
SELECT
dl_hem_people.rowid rec_rowid
,lpar_dlb_batch_id
,lpar_dl_seqno
,par_refno
,par_per_alt_ref
,nvl(lpar_per_alt_ref,substr(lpar_dlb_batch_id,1,11)
                      ||'~'||to_char(lpar_dl_seqno))
     lpar_per_alt_ref
,lpar_ipu_aun_code
,lpar_c_par_refno
FROM  parties,dl_hem_people
WHERE par_per_alt_ref =
          nvl(lpar_per_alt_ref,substr(lpar_dlb_batch_id,1,11)
                              ||'~'||to_char(lpar_dl_seqno))
AND   lpar_dlb_batch_id   = p_batch_id
AND   lpar_dl_load_status = 'C';
--
-- ************************************
--
i INTEGER := 0;
l_pro_refno INTEGER;
l_an_tab VARCHAR2(1);
l_sa VARCHAR2(1) := 'N'; -- do stand alone addresses exist
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HEM_PEOPLE';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- ************************************
BEGIN
--
fsc_utils.proc_start('s_dl_hem_people.dataload_DELETE');
fsc_utils.debug_message( 's_dl_hem_people.dataload_DELETE',3 );
--
cb := p_batch_id;
cd := p_DATE;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lpar_dl_seqno;
  l_id := p1.rec_rowid;
--
  SAVEPOINT SP1;
--
  DELETE
  FROM tenancy_instances
  WHERE  tin_hop_refno IN
              (SELECT hop_refno
               FROM   household_persons
               WHERE  hop_par_refno = p1.par_refno);
--
  DELETE
  FROM household_persons
  WHERE hop_par_refno = p1.par_refno;
--
  DELETE from address_usages
  WHERE aus_par_refno = p1.par_refno;
--
--
  DELETE
  FROM contact_details
  WHERE cde_par_refno = p1.par_refno;
--
  DELETE
  FROM  interested_party_admin_unit
  WHERE exists (select null
                from   interested_parties
                where  ipp_refno     = ipu_ipp_refno
                and    ipp_par_refno = p1.par_refno)
  AND   ipu_aun_code = p1.lpar_ipu_aun_code;
--
  DELETE
  FROM  interested_parties
  WHERE ipp_par_refno = p1.par_refno;
--
  DELETE
  FROM parties
  WHERE par_refno = p1.par_refno;
--
-- now remove the par_refno that was saved on create
--
  UPDATE DL_HEM_PEOPLE
  SET lpar_c_par_refno = NULL
  WHERE rowid = p1.rec_rowid
  AND lpar_dlb_batch_id = p1.lpar_dlb_batch_id
  AND lpar_dl_seqno = p1.lpar_dl_seqno;
--
-- *******************************************
--
  s_dl_process_summary.UPDATE_processed_count(cb,cp,cd,'N');
  set_record_status_flag(l_id,'V');
--
  i := i +1; IF mod(i,1000) = 0 THEN commit; END IF;
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
DELETE
FROM households
WHERE NOT EXISTS (SELECT NULL FROM household_persons
                  WHERE hop_hou_refno =hou_refno);

--
COMMIT;
--
-- Section to analyse the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PARTIES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('HOUSEHOLD_PERSONS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANCY_INSTANCES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('INTERESTED_PARTIES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HEM_PEOPLE');
--
fsc_utils.proc_end;
--
   EXCEPTION
      WHEN OTHERS THEN
      set_record_status_flag(l_id,'C');
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_delete;
--
--
END s_dl_hem_people;
/

show errors

