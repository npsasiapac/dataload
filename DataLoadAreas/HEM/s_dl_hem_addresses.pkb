CREATE OR REPLACE PACKAGE BODY s_dl_hem_addresses
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Ver WHO  WHEN       WHY
--    1.0          MH   12/01/02   Initial Creation    
--    1.1   5.1.4  PJD  03/02/02   Changed references to s_hdl_utils 
--                                 to s_dl_hem_utils
--    1.2   5.1.4  PJD  02/04/02   Simplified various inserts       
--                                 Set the cs variable in each loop
--    1.3   5.1.6  SB   17/05/02   Check to see IF flat no supplied that
--                                 building or street no also supplied
--                                 Check that either flat, building or
--                                 street no has been supplied.
--    1.4   5.1.6  PJD  22/05/02   Changes to validation to look up and use
--                                 the aut_format_ind from address_usage_types
--
--    1.5   5.1.6  PJD  12/06/02   Correct error codes being used
--
--    1.6   5.1.6  PJD  13/06/02   Add question about reformatting to FFA
--                                 Add missing Fetch statements
--    1.7   5.1.7  PJD  19/06/02   changes to validation of error 944
--    1.8   5.1.6  SB   16/07/02   Insert.address statement amended to use l_street, 
--                                 l_area, l_town
--    1.9   5.2.0  SB   11/09/02   Tidied references to Contractor Addresses.
--    2.0   5.2.0  MH	22/11/02   Added reference for bank account addresses.
--    2.1   5.3.0  PH   10/12/02   Added deletes for AUN, CON, NOM, PAR, PRO and TCY.
--    2.2   5.3.0  PJD  14/12/02   Set cs variable within the delete procedure
--    2.3   5.3.0  PH   29/01/03   Commented out validation on FFA where 
--                                 ladd_addl1 is not null and ladd_addl2 and 3
--                                 are null as it's accepted in database
--    2.4   5.3.0  MH   13/02/03   Change to bank address create and val
--                                 to only pick up current details. 
--    2.5   5.3.0  PJD  06/06/03   Added final commit to validate
--    2.6   5.3.0  PH   16/09/03   Added additional column for address usages
--                                 contact_name field. Amemded Create and Validate.
--    2.7   5.3.0  DH   16/01/04   Removed reference to aus_tcy_refno.
--    2.8   5.4.0  PJD  08/02/04   Added validation on Duplicate address usages.
--    2.9   5.6.0  PJD  20/10/04   Street no longer mandatory on structured addresses
--    3.0   5.8.0  PH   19/07/05   Added new fields Eastings and Northings
--    3.1   5.8.0  PJD  05/08/05   Added new validation on Sub-Building (error 963)
--    3.2   5.10.0 PH   22/09/06   Changed length of l_street from 60 to 100.
--    4.0   5.12.0 PH   17/07/07   Amended code to allow for Housing Advice
--                                 Cases, these are REG PHYSICAL Addresses.
--    5.0   5.13.0 PH   06-FEB-2008 Now includes its own 
--                                  set_record_status_flag procedure.
--    5.1   5.13.0 PH   04-MAR-2008 Amended validate on 943, only perform if
--                                  not FFA address.
--    5.2   5.13.1 PH   08-MAY-2008 Added new field adr_uprn  
--    5.3   5.15.1 PH   06-APR-2009 Added additional fields for Landlord
--                                  information 
--    5.4   5.15.1 PH   09-OCT-2009 Amended create and use of variables as
--                                  l_street, l_area, l_town are only 60 
--                                  but ffa lines can be up to 240
--    5.5   5.16.1 PH   15-OCT-2010 Amended type for Advice cases, should
--                                  be ACAS not REG
--    5.6   6.6    PJD  21-MAY-2013 Alter Address Delete to allow for 
--                                  Self Service and Prop Landlord 
--                                  address being linked directly
--    6.11  6.7    AJ               Bilingual Changes dealt with in s_dl_hem_utils
--                                  in insert_address procedure.
--    6.11  6.7.1  PAH  10-NOV-2015 Altered Error code when checking Landlord type. 
--									Was showing an error relating to SC Bases
--
--  declare package variables AND constants
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hem_addresses
  SET ladd_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hem_addresses');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2
,p_date              IN DATE    )
AS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,ladd_dlb_batch_id
,ladd_dl_seqno
,ladd_dl_load_status
,LAUS_LEGACY_REF
,LAUS_AUT_FAO_CODE
,LAUS_AUT_FAR_CODE
,LAUS_START_DATE
,LAUS_END_DATE
,LADR_FLAT
,LADR_BUILDING
,LADR_STREET_NUMBER
,LAEL_STREET
,lael_sub_street1
,LAEL_SUB_STREET2
,LAEL_SUB_STREET3
,LAEL_AREA
,LAEL_TOWN
,LAEL_COUNTY
,LAEL_COUNTRY
,LAEL_POSTCODE
,LAEL_LOCAL_IND
,LAEL_ABROAD_IND
,LADD_ADDL1
,LADD_ADDL2
,LADD_ADDL3
,LAEL_STREET_INDEX_CODE
,LAUS_CONTACT_NAME
,LADR_EASTINGS
,LADR_NORTHINGS
,LADR_UPRN
,LAUS_LANDLORD_PAR_ALT_REF
,LAUS_HRV_LLT_CODE
,LAUS_HRV_AAT_CODE
,LAUS_PTY_CODE
,LAUS_PROPERTY_SIZE
,LAUS_FLOOR_LEVEL
,LAUS_HRV_ALR_CODE
,LAUS_TENANCY_LEAVE_DATE
,LAUS_ARREARS_AMOUNT
,LAUS_STORAGE_IND
,LAUS_STORAGE_UNIT_COST
,LAUS_STORAGE_COST
FROM  dl_hem_addresses
WHERE ladd_dlb_batch_id   = p_batch_id
AND   ladd_dl_load_status = 'V';
--
--
--CURSORS
--
CURSOR c_adr_refno IS
SELECT adr_refno_seq.nextval FROM dual;
--
CURSOR c_existing_adr_refno(p_add_line1 VARCHAR2
                           ,p_add_line2 VARCHAR2
                           ,p_add_line3 VARCHAR2) is
SELECT adr_refno from addresses
where adr_free_text1 = p_add_line1
and   adr_free_text2 = p_add_line2
and   adr_free_text3 = p_add_line3;
--
CURSOR c_get_app_refno(p_legacy_ref varchar2) is
SELECT app_refno
FROM applications
WHERE p_legacy_ref = app_legacy_ref;
--
CURSOR c_get_nom_refno(p_legacy_ref varchar2) is
SELECT app_refno
FROM applications
WHERE p_legacy_ref = app_legacy_ref
AND app_nomination_flag='Y';
--
CURSOR c_get_par_refno(p_per_alt_ref varchar2) is
SELECT par_refno
FROM parties
WHERE par_per_alt_ref = p_per_alt_ref;
--
CURSOR c_get_pro_refno(p_pro_propref varchar2) is
SELECT pro_refno
FROM properties
WHERE p_pro_propref = pro_propref;
--
CURSOR c_get_tcy_refno(p_tcy_alt_ref varchar2) is
SELECT tcy_refno
FROM tenancies
WHERE tcy_alt_ref = p_tcy_alt_ref;
-- 
CURSOR c_get_bde_refno(p_bad_sort_code varchar2) is
SELECT bad_bde_refno
FROM   bank_account_details
WHERE  bad_sort_code = p_bad_sort_code
AND    sysdate between bad_start_date
               and     nvl(bad_end_date,sysdate+1);
--
CURSOR c_get_acas_ref(p_acas_alternate_ref varchar2) IS
SELECT acas_reference
FROM   advice_cases
WHERE  acas_alternate_reference = p_acas_alternate_ref;
--
--
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HEM_ADDRESSES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_reusable_refno        INTEGER;
l_aun_code      VARCHAR2(20);
l_cos_code      VARCHAR2(30);
l_app_refno     NUMBER(10);
l_pro_refno     NUMBER(10);
l_nom_refno     NUMBER(10);
l_par_refno     NUMBER(10);
l_tcy_refno     NUMBER(10);
l_bde_refno     NUMBER(10);
l_acas_ref      NUMBER(10);
i                       INTEGER := 0;
l_an_tab                VARCHAR2(1);
l_street_index          VARCHAR2(12);
l_adr_refno             NUMBER(10);
l_adr_free_text1        VARCHAR2(60);
l_adr_free_text2        VARCHAR2(60);
l_adr_free_text3        VARCHAR2(60);
l_street                VARCHAR2(100);
l_area                  VARCHAR2(60);
l_town                  VARCHAR2(60);
l_street_ffa            VARCHAR2(240);
l_area_ffa              VARCHAR2(240);
l_town_ffa              VARCHAR2(240);
l_llord_par_refno       NUMBER(10);
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_addresses.dataload_create');
fsc_utils.debug_message('s_dl_hem_addresses.dataload_create',3);
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
cs := p1.ladd_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
--Reset all the variables            
--
l_adr_refno := null;
l_app_refno := null;
l_aun_code  := NULL;
l_cos_code  := NULL;
l_nom_refno := null;
l_par_refno := null;
l_pro_refno := null;
l_tcy_refno := null;
l_bde_refno := null;
l_acas_ref  := null;
--
IF p1.LAUS_AUT_FAO_CODE = 'APP'
THEN
OPEN  c_get_app_refno(p1.laus_legacy_ref);
FETCH c_get_app_refno into l_app_refno;
CLOSE c_get_app_refno;
dbms_output.put_line('app_refno = '||l_app_refno);
--
ELSIF p1.LAUS_AUT_FAO_CODE = 'AUN'
THEN
l_aun_code := p1.laus_legacy_ref;
--
ELSIF p1.LAUS_AUT_FAO_CODE = 'CON'
THEN
l_cos_code := p1.laus_legacy_ref;
--
ELSIF p1.LAUS_AUT_FAO_CODE = 'NOM'
THEN
  OPEN c_get_nom_refno(p1.laus_legacy_ref);
  FETCH c_get_nom_refno into l_nom_refno;
  CLOSE c_get_nom_refno;
--
ELSIF p1.LAUS_AUT_FAO_CODE = 'PAR'
THEN
  OPEN c_get_par_refno(p1.laus_legacy_ref);
  FETCH c_get_par_refno into l_par_refno;
  CLOSE c_get_par_refno;
--
ELSIF p1.LAUS_AUT_FAO_CODE = 'PRO'
THEN
  OPEN c_get_pro_refno (p1.laus_legacy_ref);
  FETCH c_get_pro_refno into l_pro_refno;
  CLOSE c_get_pro_refno;
--
ELSIF p1.LAUS_AUT_FAO_CODE = 'TCY'
THEN
  OPEN c_get_tcy_refno(p1.laus_legacy_ref);
  FETCH c_get_tcy_refno into l_tcy_refno;
  CLOSE c_get_tcy_refno;
--
ELSIF p1.LAUS_AUT_FAO_CODE = 'BDE'
THEN 
  OPEN c_get_bde_refno(p1.laus_legacy_ref);
  FETCH c_get_bde_refno into l_bde_refno;
  CLOSE c_get_bde_refno;
--
ELSIF p1.LAUS_AUT_FAO_CODE = 'ACAS'
THEN
  OPEN c_get_acas_ref(p1.laus_legacy_ref);
  FETCH c_get_acas_ref into l_acas_ref;
  CLOSE c_get_acas_ref;
--
END IF;
--
-- Get Par Refno for Landlord if supplied
--
l_llord_par_refno := NULL;
--
  IF p1.laus_landlord_par_alt_ref IS NOT NULL
   THEN
     OPEN c_get_par_refno(p1.laus_landlord_par_alt_ref);
     FETCH c_get_par_refno into l_llord_par_refno;
    CLOSE c_get_par_refno;
  END IF;
--
-- Now decide whether this is a free format address
--
l_street     := NULL;
l_area       := NULL;
l_town       := NULL;
l_street_ffa := NULL;
l_area_ffa   := NULL;
l_town_ffa   := NULL;
--
IF (p1.LAEL_STREET IS NULL
    AND
    P1.LAEL_AREA IS NULL
    AND 
    P1.LAEL_TOWN IS NULL)
THEN
  l_street_ffa := p1.ladd_addl1;
  l_area_ffa   := p1.ladd_addl2;
  l_town_ffa   := p1.ladd_addl3;
ELSE
--
  l_street    := p1.lael_street;
  l_area      := p1.lael_area;
  l_town      := p1.lael_town;
END IF;
   s_dl_hem_utils.insert_address
           (upper(p1.laus_aut_fao_code)
           ,upper(p1.laus_aut_far_code)
           ,p1.lael_street_index_code
           ,l_adr_refno
           ,p1.ladr_flat
           ,p1.ladr_building
           ,p1.ladr_street_number
           ,p1.lael_sub_street1
           ,p1.lael_sub_street2
           ,p1.lael_sub_street3
           ,nvl(l_street_ffa, l_street)
           ,nvl(l_area_ffa, l_area)
           ,nvl(l_town_ffa, l_town)
           ,p1.lael_county
           ,p1.lael_postcode
           ,p1.lael_country
           ,l_aun_code    
           ,l_pro_refno     
           ,l_par_refno
           ,l_tcy_refno
           ,l_bde_refno
           ,NULL
           ,p1.laus_start_date
           ,p1.laus_end_date
           ,p1.lael_local_ind
           ,p1.lael_abroad_ind
           ,l_app_refno
           ,l_cos_code  
           ,NULL
           ,NULL
           ,l_nom_refno
           ,NULL
           ,p1.laus_contact_name
           ,p1.ladr_eastings
           ,p1.ladr_northings
           ,l_acas_ref
           ,p1.ladr_uprn
           ,l_llord_par_refno 
           ,p1.laus_hrv_llt_code
           ,p1.laus_hrv_aat_code
           ,p1.laus_pty_code
           ,p1.laus_property_size
           ,p1.laus_floor_level
           ,p1.laus_hrv_alr_code
           ,p1.laus_tenancy_leave_date
           ,p1.laus_arrears_amount
           ,p1.laus_storage_ind
           ,p1.laus_storage_unit_cost
           ,p1.laus_storage_cost
           );
--
-- Set the dataload statuses
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
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
--
  -- Section to analyze the table(s) populated by this dataload
  --
  l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADDRESSES');
  l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADDRESS_ELEMENTS');
  l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADDRESSES_USAGES');
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
--
--
--
--
PROCEDURE dataload_validate
(p_batch_id          IN VARCHAR2
,p_date              IN DATE    )
AS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,ladd_dlb_batch_id
,ladd_dl_seqno
,ladd_dl_load_status
,LAUS_LEGACY_REF
,LAUS_AUT_FAO_CODE
,LAUS_AUT_FAR_CODE
,LAUS_START_DATE
,LAUS_END_DATE
,LADR_FLAT
,LADR_BUILDING
,LADR_STREET_NUMBER
,LAEL_STREET
,lael_sub_street1
,LAEL_SUB_STREET2
,LAEL_SUB_STREET3
,LAEL_AREA
,LAEL_TOWN
,LAEL_COUNTY
,LAEL_COUNTRY
,LAEL_POSTCODE
,LAEL_LOCAL_IND
,LAEL_ABROAD_IND
,LADD_ADDL1
,LADD_ADDL2
,LADD_ADDL3
,LAEL_STREET_INDEX_CODE
,LAUS_CONTACT_NAME
,LAUS_LANDLORD_PAR_ALT_REF
,LAUS_HRV_LLT_CODE
,LAUS_HRV_AAT_CODE
,LAUS_PTY_CODE
,LAUS_PROPERTY_SIZE
,LAUS_FLOOR_LEVEL
,LAUS_HRV_ALR_CODE
,LAUS_TENANCY_LEAVE_DATE
,LAUS_ARREARS_AMOUNT
,LAUS_STORAGE_IND
,LAUS_STORAGE_UNIT_COST
,LAUS_STORAGE_COST
FROM  dl_hem_addresses
WHERE ladd_dlb_batch_id    = p_batch_id
AND   ladd_dl_load_status IN ('L','F','O')
-- AND rownum < 20
;
--
-- VALIDATION CURSORS
--
 CURSOR c_role_usage (  p_role VARCHAR2
                       ,p_usage VARCHAR2)
 IS
 SELECT aut_format_ind  
 FROM  address_usage_types
 WHERE aut_fao_code = p_usage
 AND   aut_far_code = p_role;
--
 CURSOR c_legacy_ref (p_legacy_ref VARCHAR2)
 IS
 SELECT to_char(app_refno)
 FROM applications
 WHERE  app_legacy_ref = nvl(p_legacy_ref,app_legacy_ref||'X');
--
 CURSOR c_aun_code (p_aun_code VARCHAR2)
 IS
 SELECT aun_code
 FROM admin_units
 WHERE aun_code = p_aun_code;
--
 CURSOR c_cos_code (p_cos_code VARCHAR2)
 IS
 SELECT cos_code
 FROM contractor_sites
 WHERE cos_code = p_cos_code;
--
 CURSOR c_nom_code (p_nom_code VARCHAR2)
 IS
 SELECT to_char(app_refno)
 FROM applications
 WHERE  app_legacy_ref = nvl(p_nom_code,app_legacy_ref||'X')
 AND app_nomination_flag='Y';
--
 CURSOR c_par_per_alt_ref (p_par_alt_ref VARCHAR2)
 IS
 SELECT to_char(par_refno)
 FROM parties
 WHERE par_per_alt_ref = nvl(p_par_alt_ref,par_per_alt_ref||'X');
--
 CURSOR c_pro_refno (p_pro_propref VARCHAR2)
 IS
 SELECT to_char(pro_refno)
 FROM properties
 WHERE pro_propref  = p_pro_propref;
--
 CURSOR c_tcy_alt_ref (p_tcy_alt_ref VARCHAR2)
 IS
 SELECT to_char(tcy_refno)
 FROM tenancies
 WHERE tcy_alt_ref = nvl(p_tcy_alt_ref,tcy_alt_ref||'X');
--
 CURSOR c_get_bad_sort_code (p_bad_sort_code VARCHAR2)
 IS 
 SELECT to_char(bad_refno)
 FROM bank_account_details
 WHERE bad_sort_code = nvl(p_bad_sort_code,'z')
 AND    sysdate between bad_start_date
               and     nvl(bad_end_date,sysdate+1);
--
 CURSOR c_get_acas_ref(p_acas_alternate_ref varchar2) IS
SELECT to_char(acas_reference)
FROM   advice_cases
WHERE  acas_alternate_reference = nvl(p_acas_alternate_ref, p_acas_alternate_ref||'X');
--
 CURSOR c_contact (p_role VARCHAR2
                  ,p_usage VARCHAR2)
 IS
 SELECT 'X'  
 FROM  address_usage_types
 WHERE aut_fao_code               = p_usage
 AND   aut_far_code               = p_role
 AND   aut_allow_contact_name_ind = 'Y';
--
 CURSOR c_par_landlord (p_par_alt_ref VARCHAR2)
 IS
 SELECT par_refno
 FROM   parties
 WHERE  par_per_alt_ref = nvl(p_par_alt_ref,par_per_alt_ref||'X');
--
 CURSOR c_pty_exists (p_pty_code VARCHAR2)
 IS
 SELECT 'X'
 FROM   prop_types
 WHERE  pty_code = p_pty_code;
--
 CURSOR c_dup(p_obj_ref VARCHAR2
             ,p_fao_code VARCHAR2
             ,p_far_code VARCHAR2
             ,p_start_date DATE)
IS
SELECT 'x'
FROM   address_usages
WHERE  aus_aut_fao_code = p_fao_code
  AND  aus_aut_far_code = p_far_code
  AND  aus_start_date = p_start_date
  AND  aus_object_reference = p_obj_ref;
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HEM_ADDRESSES';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- Other Variables
l_exists             VARCHAR2(1);
l_obj_ref            VARCHAR2(30);
l_car_exists         VARCHAR2(1);
l_aut_format_ind     VARCHAR2(3);
i                    INTEGER:=0;
l_errors             VARCHAR2(10);
l_error_ind          VARCHAR2(10);
l_answer             VARCHAR2(1);
l_llord_par_refno    NUMBER(10);
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_addresses.dataload_validate');
fsc_utils.debug_message('s_dl_hem_addresses.dataload_validate',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the 'Update address Format' Question'
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
IF l_answer = NULL THEN l_answer := 'N'; END IF;
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.ladd_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
l_errors := 'V';
l_error_ind := 'N';
--
--
-- Check Address usage/role type combination exists
--
        OPEN c_role_usage(p1.laus_aut_far_code,
                          p1.laus_aut_fao_code);
        FETCH c_role_usage INTO l_aut_format_ind;
        CLOSE c_role_usage;
        IF l_aut_format_ind IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',262);
        END IF;
--
-- Check that a valid combination of address columns have been supplied
--
        IF (    l_answer = 'N' 
            AND l_aut_format_ind = 'FFA' 
            AND p1.ladd_addl1 IS NULL) 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',962);
--
        ELSIF (    l_answer = 'N' 
               AND l_aut_format_ind = 'FFA' 
               AND p1.lael_street IS NOT NULL)
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',962);
--
        ELSIF (    l_answer = 'Y' 
               AND l_aut_format_ind = 'FFA' 
               AND p1.lael_street IS NULL
               AND p1.ladd_addl1  IS NULL)
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',962);
--
-- **Commented out by PH as this is allowed in database
--
--        ELSIF (    l_aut_format_ind = 'FFA' 
--               AND p1.ladd_addl1  IS NOT NULL
--               AND p1.ladd_addl2  IS NULL
--               AND p1.ladd_addl3  IS NULL)
--          THEN
--            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',962);
--
        ELSIF (    l_answer = 'Y'
               AND l_aut_format_ind = 'FFA'
               AND p1.ladd_addl1     IS NULL
               AND p1.lael_street    IS NOT NULL
               AND p1.ladr_flat      IS NOT NULL
               AND p1.ladr_building  IS NULL
               AND p1.ladr_street_number IS NULL) 
         THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',962);
--
        ELSIF (    l_aut_format_ind != 'FFA' 
               AND p1.lael_street IS NULL
               AND p1.lael_area   IS NULL
               AND p1.lael_town   IS NULL)
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',962);
--
        ELSIF (l_aut_format_ind != 'FFA' AND p1.ladd_addl1 IS NOT NULL)
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',962);
--
        ELSIF (    p1.lael_street    IS NOT NULL
               AND p1.ladr_flat      IS NOT NULL
               AND p1.ladr_building  IS NULL
               AND p1.ladr_street_number IS NULL) 
         THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',962);
        END IF;
--
--
-- Check the Street name has been supplied
--
IF l_aut_format_ind != 'FFA'
 THEN
  IF (   p1.ladr_flat            IS NOT NULL
      OR p1.ladr_building        IS NOT NULL
      OR p1.ladr_street_number   IS NOT NULL
      OR p1.lael_street          IS NOT NULL
      OR p1.lael_sub_street1     IS NOT NULL
      OR p1.lael_sub_street2     IS NOT NULL
      OR p1.lael_sub_street3     IS NOT NULL
      OR p1.lael_area            IS NOT NULL
      OR p1.lael_town            IS NOT NULL
      OR p1.lael_county          IS NOT NULL
      OR p1.lael_postcode        IS NOT NULL
     )
  THEN 
    IF (    p1.lael_street IS NULL
        AND p1.lael_area   IS NULL
        AND p1.lael_town   IS NULL) 
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',943);
    END IF;
  END IF;
END IF;
--
-- Check that IF street name supplied that either the flat No, 
-- building or street no has also been supplied.
    IF (    p1.lael_street        IS NOT NULL
        OR  p1.lael_area          IS NOT NULL
        OR  p1.lael_town          IS NOT NULL)
    THEN
      IF (    p1.ladr_flat          IS NULL
          AND p1.ladr_building      IS NULL
          AND p1.ladr_street_number IS NULL) 
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',944);
      END IF;
    END IF;
--
-- Check that IF street name supplied that either the flat No, 
-- building or street no has also been supplied.
--
    IF (    p1.lael_street        IS NOT NULL
        OR  p1.lael_area          IS NOT NULL
        OR  p1.lael_town          IS NOT NULL)
    THEN
      IF (    p1.ladr_flat          IS NOT NULL
          AND p1.ladr_building      IS NULL
          AND p1.ladr_street_number IS NULL) 
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',963);
      END IF;
    END IF;
--
-- Check that the address END date is NOT earlier than the address
-- start date
--
        IF (p1.laus_end_date IS NOT NULL)
         THEN
          IF (p1.laus_end_date < p1.laus_start_date)
          THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',882);
          END IF;
        END IF;
--
-- Abroad Ind
--
        IF (NOT s_dl_hem_utils.yornornull(p1.lael_abroad_ind))
        THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',972);
        END IF;
--
--
-- Local Ind
--
        IF (NOT s_dl_hem_utils.yornornull(p1.lael_local_ind))
        THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',005);
        END IF;
--
-- Clear out variable ready to hold obj_ref - this is used later to
-- check for duplicates
--
   l_obj_ref := NULL;
--
-- If app legacy ref supplied does it exist on applications
--
   IF p1.LAUS_AUT_FAO_CODE = 'APP'
     THEN
--        dbms_output.put_line(checking app_refno);
        OPEN c_legacy_ref(p1.laus_legacy_ref);
        FETCH c_legacy_ref INTO l_obj_ref;
        IF c_legacy_ref%NOTFOUND
        THEN
          CLOSE c_legacy_ref;
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',233);
        ELSE
          CLOSE c_legacy_ref;
        END if;
--
-- If aun code supplied does it exist on admin units
--
   ELSIF p1.LAUS_AUT_FAO_CODE = 'AUN'
     THEN
        OPEN c_aun_code(p1.laus_legacy_ref);
        FETCH c_aun_code INTO l_obj_ref;
        IF c_aun_code%NOTFOUND
        THEN
        CLOSE c_aun_code;
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',725);
        ELSE
        CLOSE c_aun_code;
        END if;
--
--
-- If contractor site code supplied does it exist on contractor sites
--
   ELSIF p1.LAUS_AUT_FAO_CODE = 'CON'
     THEN
        OPEN c_cos_code(p1.laus_legacy_ref);
        FETCH c_cos_code INTO l_obj_ref;
        IF c_cos_code%NOTFOUND
        THEN
        CLOSE c_cos_code;
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',736);
        ELSE
        CLOSE c_cos_code;
        END if;
--
--
-- If nomination code supplied does it exist on applications
--
   ELSIF p1.LAUS_AUT_FAO_CODE = 'NOM'
     THEN
        OPEN c_nom_code(p1.laus_legacy_ref);
        FETCH c_nom_code INTO l_obj_ref;
        IF c_nom_code%NOTFOUND
        THEN
        CLOSE c_nom_code;
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',233);
        ELSE
        CLOSE c_nom_code;
        END if;
--
-- If a party alt reference supplied does it exist on parties
--
   ELSIF p1.LAUS_AUT_FAO_CODE = 'PAR'
     THEN
        OPEN c_par_per_alt_ref(p1.laus_legacy_ref);
        FETCH c_par_per_alt_ref INTO l_obj_ref;
        IF c_par_per_alt_ref%NOTFOUND
        THEN
        CLOSE c_par_per_alt_ref;
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',868);
        ELSE
        CLOSE c_par_per_alt_ref;
        END if;
--
--
-- If pro_propref supplied does it exist on properties
--
   ELSIF p1.LAUS_AUT_FAO_CODE = 'PRO'
     THEN
        OPEN c_pro_refno(p1.laus_legacy_ref);
        FETCH c_pro_refno INTO l_obj_ref;
        IF c_pro_refno%NOTFOUND
        THEN
        CLOSE c_pro_refno;
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',30);
        ELSE
        CLOSE c_pro_refno;
        END if;
--
--
-- If tcy alt ref supplied does it exist on tenancies
--
   ELSIF p1.LAUS_AUT_FAO_CODE = 'TCY'
     THEN
        OPEN c_tcy_alt_ref(p1.laus_legacy_ref);
        FETCH c_tcy_alt_ref INTO l_obj_ref;
        IF c_tcy_alt_ref%NOTFOUND
        THEN
        CLOSE c_tcy_alt_ref;
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',80);
        ELSE
        CLOSE c_tcy_alt_ref;
        END if;
--
--
-- If bad_sort_code supplied does bank account exist
--
--
  ELSIF p1.LAUS_AUT_FAO_CODE = 'BDE'
     THEN
        OPEN c_get_bad_sort_code(p1.laus_legacy_ref);
        FETCH c_get_bad_sort_code INTO l_obj_ref;
          IF c_get_bad_sort_code%NOTFOUND
          THEN
          CLOSE c_get_bad_sort_code;
	  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',867);
          ELSE
          CLOSE c_get_bad_sort_code;
          END if;
--
-- If Advice Case has been supplied, does it exist
--
   ELSIF p1.LAUS_AUT_FAO_CODE = 'ACAS'
     THEN
        OPEN c_get_acas_ref(p1.laus_legacy_ref);
        FETCH c_get_acas_ref INTO l_obj_ref;
        IF c_get_acas_ref%NOTFOUND
        THEN
        CLOSE c_get_acas_ref;
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',275);
        ELSE
        CLOSE c_get_acas_ref;
        END if;
--
    ELSIF p1.laus_aut_fao_code IS NULL 
   THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',80);
   ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',80);
   END IF;
--
-- Check that if a contact name has been supplied it's valid
-- combination in system build.
--
  IF (p1.laus_contact_name IS NOT NULL)
   THEN
    OPEN c_contact(p1.laus_aut_far_code,
                   p1.laus_aut_fao_code);
     FETCH c_contact INTO l_exists;
          IF c_contact%NOTFOUND
           THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',997);
          END IF;
    CLOSE c_contact;
  END IF;
--
-- If the Landlord Details have been supplied check it exists on parties
--
   l_llord_par_refno := NULL;
--
   IF p1.laus_landlord_par_alt_ref IS NOT NULL
    THEN
     OPEN c_par_landlord(p1.laus_landlord_par_alt_ref);
      FETCH c_par_landlord INTO l_llord_par_refno;
       IF c_par_landlord%NOTFOUND
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',551);
       END IF;
     CLOSE c_par_landlord;
   END if;
--
-- Check Landlord Type
--
   IF (NOT s_dl_hem_utils.exists_frv('LANDTYPE',p1.laus_hrv_llt_code,'Y'))
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',552);
   END IF;
--
-- Agreement Type
--
   IF (NOT s_dl_hem_utils.exists_frv('AGRETYPE',p1.laus_hrv_aat_code,'Y'))
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',553);
   END IF;
--
-- Property Type
--
   IF p1.laus_pty_code IS NOT NULL
    THEN
     OPEN c_pty_exists(p1.laus_pty_code);
      FETCH c_pty_exists INTO l_exists;
       IF c_pty_exists%NOTFOUND
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',554);
       END IF;
     CLOSE c_pty_exists;
   END IF;
--
-- Leave Reason
--
   IF (NOT s_dl_hem_utils.exists_frv('ADDLEAVE',p1.laus_hrv_alr_code,'Y'))
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',555);
   END IF;
--
-- Storage Ind
--
   IF nvl(p1.laus_storage_ind, 'Y') NOT IN ( 'Y', 'N' )
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',556);
   END IF;
--
-- Finally check to see if it is a duplicate
--
l_exists := NULL;
OPEN c_dup(l_obj_ref,            p1.laus_aut_fao_code,
           p1.laus_aut_far_code, p1.laus_start_date);
FETCH c_dup into l_exists;
CLOSE c_dup;
IF l_exists IS NOT NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',263);
END IF;
--
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
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) 
IS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,ladd_dlb_batch_id
,ladd_dl_seqno
,laus_legacy_ref
,laus_aut_fao_code
,laus_aut_far_code
,laus_start_date
,nvl(laus_end_date,'31-DEC-2099') laus_end_date 
FROM  dl_hem_addresses
WHERE  ladd_dlb_batch_id   = p_batch_id
AND   ladd_dl_load_status = 'C';
--
--
CURSOR c_get_app_refno(p_legacy_ref varchar2) is
SELECT app_refno
FROM applications
WHERE p_legacy_ref = app_legacy_ref;
--
CURSOR c_get_nom_refno(p_legacy_ref varchar2) is
SELECT app_refno
FROM applications
WHERE p_legacy_ref = app_legacy_ref
AND app_nomination_flag='Y';
--
CURSOR c_get_par_refno(p_per_alt_ref varchar2) is
SELECT par_refno
FROM parties
WHERE par_per_alt_ref = p_per_alt_ref;
--
CURSOR c_get_pro_refno(p_pro_propref varchar2) is
SELECT pro_refno
FROM properties
WHERE p_pro_propref = pro_propref;
--
CURSOR c_get_tcy_refno(p_tcy_alt_ref varchar2) is
SELECT tcy_refno
FROM tenancies
WHERE tcy_alt_ref = p_tcy_alt_ref;
--
CURSOR c_get_bde_refno(p_bad_sort_code varchar2) is
SELECT bad_bde_refno
FROM   bank_account_details
WHERE  bad_sort_code = p_bad_sort_code;
--
CURSOR c_get_acas_ref(p_acas_alternate_ref varchar2) IS
SELECT acas_reference
FROM   advice_cases
WHERE  acas_alternate_reference = p_acas_alternate_ref;
--

i INTEGER := 0;
--
l_an_tab VARCHAR2(1);
l_aun_code      VARCHAR2(20);
l_cos_code      VARCHAR2(30);
l_app_refno     NUMBER(10);
l_pro_refno     NUMBER(10);
l_nom_refno     NUMBER(10);
l_par_refno     NUMBER(10);
l_tcy_refno     NUMBER(10);
l_bde_refno     NUMBER(10);
l_acas_ref      NUMBER(10);
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HEM_ADDRESSES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_addresses.dataload_DELETE');
fsc_utils.debug_message( 's_dl_hem_addresses.dataload_DELETE',3 );
--
cb := p_batch_id;
cd := p_DATE;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.ladd_dl_seqno;
l_id := p1.rec_rowid;
--
   i := i +1;
--
-- Delete for usage type APP
--
IF p1.laus_aut_fao_code = 'APP'
THEN
  OPEN  c_get_app_refno(p1.laus_legacy_ref);
  FETCH c_get_app_refno into l_app_refno;
  CLOSE c_get_app_refno;
  DELETE from address_usages
  WHERE l_app_refno = aus_app_refno
    AND aus_start_date = p1.laus_start_date
    AND aus_aut_fao_code = p1.laus_aut_fao_code
    AND aus_aut_far_code = p1.laus_aut_far_code;
--
ELSIF p1.LAUS_AUT_FAO_CODE = 'AUN'
THEN
  l_aun_code := p1.laus_legacy_ref;
  DELETE from address_usages
  WHERE l_aun_code        = aus_aun_code
    AND aus_start_date   = p1.laus_start_date
    AND aus_aut_fao_code = p1.laus_aut_fao_code
    AND aus_aut_far_code = p1.laus_aut_far_code;
--
ELSIF p1.LAUS_AUT_FAO_CODE = 'CON'
THEN
  l_cos_code := p1.laus_legacy_ref;
  DELETE  from address_usages
  WHERE l_cos_code        = aus_cos_code
    AND aus_start_date   = p1.laus_start_date
    AND aus_aut_fao_code = p1.laus_aut_fao_code
    AND aus_aut_far_code = p1.laus_aut_far_code;
--
 ELSIF  p1.laus_aut_fao_code = 'NOM'
   THEN
    l_nom_refno := NULL;
    OPEN c_get_nom_refno(p1.laus_legacy_ref);
     FETCH c_get_nom_refno into l_nom_refno;
    CLOSE c_get_nom_refno;
    DELETE from address_usages
    WHERE  l_nom_refno    = aus_nom_refno
       AND aus_start_date = p1.laus_start_date
       AND aus_aut_fao_code = p1.laus_aut_fao_code
       AND aus_aut_far_code = p1.laus_aut_far_code;
--
ELSIF p1.LAUS_AUT_FAO_CODE = 'PAR'
THEN
  OPEN c_get_par_refno(p1.laus_legacy_ref);
  FETCH c_get_par_refno into l_par_refno;
  CLOSE c_get_par_refno;
  DELETE  from address_usages
  WHERE l_par_refno      = aus_par_refno
    AND aus_start_date   = p1.laus_start_date
    AND aus_aut_fao_code = p1.laus_aut_fao_code
    AND aus_aut_far_code = p1.laus_aut_far_code;
--
ELSIF p1.LAUS_AUT_FAO_CODE = 'PRO'
THEN
  OPEN c_get_pro_refno (p1.laus_legacy_ref);
  FETCH c_get_pro_refno into l_pro_refno;
  CLOSE c_get_pro_refno;
  DELETE  from address_usages
  WHERE l_pro_refno      = aus_pro_refno
    AND aus_start_date   = p1.laus_start_date
    AND aus_aut_fao_code = p1.laus_aut_fao_code
    AND aus_aut_far_code = p1.laus_aut_far_code;

--
ELSIF p1.LAUS_AUT_FAO_CODE = 'BDE'
THEN 
  OPEN c_get_bde_refno(p1.laus_legacy_ref);
  FETCH c_get_bde_refno into l_bde_refno;
  CLOSE c_get_bde_refno;
  DELETE  from address_usages
  WHERE l_bde_refno      = aus_bde_refno
    AND aus_start_date   = p1.laus_start_date
    AND aus_aut_fao_code = p1.laus_aut_fao_code
    AND aus_aut_far_code = p1.laus_aut_far_code;
--
-- Delete for ACAS - Housing Advice
--
ELSIF p1.LAUS_AUT_FAO_CODE = 'ACAS'
THEN 
  OPEN c_get_acas_ref(p1.laus_legacy_ref);
  FETCH c_get_acas_ref into l_acas_ref;
  CLOSE c_get_acas_ref;
  DELETE  from address_usages
  WHERE l_acas_ref       = aus_acas_reference
    AND aus_start_date   = p1.laus_start_date
    AND aus_aut_fao_code = p1.laus_aut_fao_code
    AND aus_aut_far_code = p1.laus_aut_far_code;
--
END IF;
--
  IF mod(i,5000) = 0 THEN commit; END IF;
--
  s_dl_process_summary.UPDATE_processed_count(cb,cp,cd,'N');
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
--
COMMIT;
--
-- Section to cleanse address details
-- Now that address usages have been removed
--
DELETE FROM addresses
WHERE adr_refno NOT IN (
                        SELECT aus_adr_refno
                        FROM   address_usages
                         )
AND   adr_refno NOT IN  (
                        SELECT ssad_adr_refno
                        FROM   self_service_addresses
                         )
AND   adr_refno NOT IN  (
                        SELECT pld_adr_refno
                        FROM   property_landlords
                         )
;
--
COMMIT;
--
DELETE FROM address_elements
WHERE ael_street_index_code NOT IN (
                        SELECT adr_ael_street_index_code
                        FROM   addresses);
--
COMMIT;

-- Section to analyze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADDRESS_USAGES');
--
fsc_utils.proc_end;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.set_record_status_flag(ct,cb,cs,'C');
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_delete;
--
--
END s_dl_hem_addresses;
/

