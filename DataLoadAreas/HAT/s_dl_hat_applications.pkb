CREATE OR REPLACE PACKAGE BODY s_dl_hat_applications
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Ver   WHO  WHEN       WHY
--      1.0            RJ  03/04/2001  Dataload
--      1.1  5.1.6    PJD  14/07/2002  Reset l_tcy_refno to null for each row.
--      2.0  5.2.0     SB  14/08/2002  to_char ltcy_alt_ref on f_exists_tenancy 
--                                     function so picks correct function from 
--                                     s_dl_hat_utils
--      2.1  5.2.0    PJD  12/09/2002  Now creates Addresses for transfer 
--                                     applications
--      2.2  5.2.0    SB   14/10/2002  Minor Amendments to correct compilation errors
--      2.3  5.3.0    SB   20/03/2003  Added Commit after validate.
--      3.0  5.3.4    PH   09/06/2003  Changed Validate on lapp_sco_code. removed 
--                                     CLO as it's not a valid code for allocations.
--      3.1  5.3.5    PJD  01/07/2003  Removed to_char clause around ltcy_alt_ref now that it is
--                                     a VARCHAR2 column. 
--      3.2  5.4.0    PJD  07/11/2003  New column for lapp_refno
--      3.3  5.10.0   PH   16/03/2007  Added Rent Account Details field to
--                                     insert in applications
--      4.0  5.12.0   PH   17/07/2007  New fields for Source and Case Advice Ref
--      4.1  5.13.0   PH   06-FEB-2008 Now includes its own 
--                                     set_record_status_flag procedure.
--      4.2  6.13     AJ   29-FEB-2016 added new validation for mandatory items
--                                     and removed NOT NULL from data load table--
--      ***   BESPOKE VERSION CHANGES  ******** FROM 4.3 ONWARDS *****--
--      4.3  6.14     MOK 23-Aug-2017  Added field for GNB for APP_AUN_CODE and
--                                     APP_BECAME_ACTIVE_DATE
--      4.4  6.14     AJ  05-SEP-2017  Added further aun_type check for APP_AUN_CODE
--                                     as must be of type HOU
--      4.5  6.14     AJ  18-SEP-2017  Slight amendments made to format only
--
--      4.6  6.16     PJD 19-OCT-2018  Add delete from address usages into the
--                                     Delete proc
--      4.7  6.18     PJD 19-DEC-2018  Tidying up of variable names  
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
  UPDATE dl_hat_applications
  SET lapp_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_applications');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
--
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
     rowid rec_rowid,
     lapp_dlb_batch_id,
     lapp_dl_seqno,
     lapp_dl_load_status,
     nvl(lapp_offer_flag,'N')                 lapp_offer_flag,
     nvl(lapp_nomination_flag,'N')            lapp_nomination_flaG,
     lapp_received_date,
     lapp_corr_name,
     nvl(lapp_sco_code,'new')                 lapp_sco_code,
     nvl(lapp_status_date,lapp_received_date) lapp_status_date,
     lapp_rent_account_details,
     lapp_legacy_ref,
     ltcy_alt_ref,
     lapp_refno,
     lapp_hrv_fssa_code,
     lapp_acas_alt_ref,
     lapp_aun_code,
     lapp_became_active_date
FROM  dl_hat_applications
WHERE lapp_dlb_batch_id   = p_batch_id
AND   lapp_dl_load_status = 'V';
--
--
CURSOR c2(p_ltcy_alt_ref IN VARCHAR2) IS
SELECT tcy_refno
FROM tenancies
WHERE tcy_alt_ref = p_ltcy_alt_ref;
--
--
CURSOR c3(p_tcy_refno in NUMBER) IS
SELECT aus_adr_refno
FROM   address_usages,properties,tenancy_holdings
WHERE  aus_pro_refno = pro_refno 
AND    tho_tcy_refno = p_tcy_refno
AND    pro_refno     = tho_pro_refno
AND    pro_hou_residential_ind   = 'Y';
--
CURSOR c4 IS
SELECT app_refno_seq.nextval FROM dual;
--
CURSOR c_acas_ref(p_acas_alt_ref IN VARCHAR2) IS
SELECT acas_reference
FROM   advice_cases
WHERE  acas_alternate_reference = p_acas_alt_ref;
--
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_APPLICATIONS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
i1               PLS_INTEGER := 0;
l_an_tab         VARCHAR2(1);
l_tcy_refno      PLS_INTEGER;
l_app_refno      PLS_INTEGER;
l_adr_refno      PLS_INTEGER;
l_acas_ref       PLS_INTEGER;
l_corr_name      VARCHAR2(250);
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_applications.dataload_create');
fsc_utils.debug_message( 's_dl_hat_aplications.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lapp_dl_seqno;
l_id := p1.rec_rowid;
--
-- Get the correspondence name from DL_HAT_INVOLVED_PARTIES
--...NO!  Validation ensures this has been supplied so no need to do this
-- IF p1.lapp_corr_name is NULL OR p1.lapp_corr_name = ' '
-- THEN
--   l_corr_name := s_dl_hat_utils.f_correspondence_name(p1.lapp_legacy_ref);
-- ELSE
--   l_corr_name := p1.lapp_corr_name;
-- END IF;
--
-- Get the tenancy reference number
l_tcy_refno := NULL;
--
IF p1.ltcy_alt_ref IS NOT NULL
THEN
  OPEN c2 (p1.ltcy_alt_ref);
  FETCH c2 INTO l_tcy_refno;
  CLOSE c2;
END IF;
--
l_app_refno := p1.lapp_refno;
IF l_app_refno IS NULL 
THEN
  OPEN c4;
  FETCH c4 INTO l_app_refno;
  CLOSE c4;
END IF;
--
-- Get the advice Case ref
--
l_acas_ref := NULL;
--
IF p1.LAPP_ACAS_ALT_REF IS NOT NULL
 THEN
  OPEN c_acas_ref(p1.LAPP_ACAS_ALT_REF);
   FETCH c_acas_ref into l_acas_ref;
  CLOSE c_acas_ref;
END IF;
--
INSERT INTO applications
          (app_refno,
           app_reusable_refno,
           app_offer_flag,
           app_nomination_flag,
           app_received_date,
           app_corr_name,
           app_sco_code,
           app_status_date,
           app_rent_account_details,
           app_tcy_refno,
           app_legacy_ref,
           app_hrv_fssa_code,
           app_acas_reference,
           app_aun_code,
           app_became_active_date
           )
VALUES
    (l_app_refno,
     reusable_refno_seq.nextval,
     p1.lapp_offer_flag,
     p1.lapp_nomination_flag,
     p1.lapp_received_date,
     p1.lapp_corr_name,
     p1.lapp_sco_code,
     p1.lapp_status_date,
     p1.lapp_rent_account_details,
     l_tcy_refno,
     p1.lapp_legacy_ref,
     p1.lapp_hrv_fssa_code,
     l_acas_ref,
     p1.lapp_aun_code,
     p1.lapp_became_active_date);
--
l_adr_refno := NULL;
--
IF l_tcy_refno IS NOT NULL
THEN
  OPEN c3 (l_tcy_refno);
  FETCH c3 INTO l_adr_refno;
  CLOSE c3;
END IF;
--
IF l_adr_refno IS NOT NULL
THEN 
  INSERT INTO address_usages
     (aus_aut_fao_code
     ,aus_aut_far_code
     ,aus_start_date
     ,aus_adr_refno
     ,aus_app_refno
     ,aus_end_date)
     VALUES
     ('APP'
     ,'APPLICATN'   
     ,p1.lapp_received_date
     ,l_adr_refno               
     ,l_app_refno     
     ,NULL);
--
END IF;
--
--
-- keep a count of the rows processed and commit after every 1000
--
i1 := i1+1; IF MOD(i1,1000)=0 THEN COMMIT; END IF;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'C');
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
-- Section to analyse the table populated by this dataload

 l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLICATIONS');


fsc_utils.proc_end;
COMMIT;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_create;
-- ************************************************************************
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2
     ,p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT
     rowid rec_rowid,
     lapp_dlb_batch_id,
     lapp_dl_seqno,
     lapp_dl_load_status,
     lapp_offer_flag,
     lapp_nomination_flag,
     lapp_received_date,
     lapp_corr_name,
     lapp_sco_code,
     lapp_status_date,
     lapp_rent_account_details,
     lapp_legacy_ref,
     ltcy_alt_ref,
     lapp_refno,
     lapp_hrv_fssa_code,
     lapp_acas_alt_ref,
     lapp_aun_code,           ---This is the default Admin Unit code associated with the user who created the application
     lapp_became_active_date  ---This is the date the status of the application was changed. Used to determine when history should commence
FROM  dl_hat_applications
WHERE lapp_dlb_batch_id    = p_batch_id
AND   lapp_dl_load_status IN ('L','F','O');
--
--
CURSOR c_acas_ref(p_acas_alt_ref IN VARCHAR2) IS
SELECT 'X'
FROM   advice_cases
WHERE  acas_alternate_reference = p_acas_alt_ref;
--
CURSOR c_aun_exists(p_aun_code VARCHAR2) IS
SELECT 'x'
FROM   admin_units
WHERE  aun_code = p_aun_code;
--
CURSOR c_aun_type(p_aun_code VARCHAR2) IS
SELECT auy.auy_type,auy.auy_code
FROM admin_units aun, admin_unit_types auy
WHERE aun.aun_auy_code = auy.auy_code
AND aun.aun_code = p_aun_code;
--
-- constants FOR error process
--
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_APPLICATIONS';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
--
l_exists            VARCHAR2(1);
l_errors            VARCHAR2(10);
l_error_ind         VARCHAR2(10);
i                   INTEGER := 0;
l_auy_type admin_unit_types.auy_type%TYPE;
l_auy_code admin_unit_types.auy_code%TYPE;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_applications.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_applications.dataload_validate',3 );
--
cb := p_batch_id;
cd := p_date;
--
-- s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lapp_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
l_auy_type := null;
l_auy_code := null;
l_exists   := null;
--
-- Now that we allow the site to supply the app_refno
-- we need to check that it is not a duplicate
--
 IF p1.lapp_refno IS NOT NULL
 AND (s_dl_hat_utils.f_exists_app_refno(p1.lapp_refno))
 THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',803);
 END IF;
--
-- Check that the tenancy ref if supplied must exist on the tenancies table
--
 IF p1.ltcy_alt_ref IS NOT NULL
 AND (NOT s_dl_hat_utils.f_exists_tenacy_ref(p1.ltcy_alt_ref))
 THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',080);
 END IF;
--
-- The user application reference must not already exist on the
-- APPLIC_LIST_ENTRIES table
--
    IF (s_dl_hat_utils.f_exists_applic_list_entries (p1.lapp_legacy_ref))
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',200);
    END IF;
--
--
-- The application received date must not be after the current date
--
  IF p1.LAPP_RECEIVED_DATE > TRUNC(SYSDATE)
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',202);
  END IF;
--
-- The application sco_code must be set to 'NEW','CUR','HSD' or 'CLD'
--
  IF p1.LAPP_SCO_CODE NOT IN('NEW','CUR','HSD','CLD')
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',203);
  END IF;
--
-- Check Source
--
  IF (NOT s_dl_hem_utils.exists_frv('SSAPPSRC',p1.lapp_hrv_fssa_code,'Y'))
   THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',274);
  END IF;
--
-- Check the Advice Case exists
--
  IF p1.lapp_acas_alt_ref IS NOT NULL
   THEN
--
    l_exists   := null;
--
    OPEN c_acas_ref(p1.lapp_acas_alt_ref);
     FETCH c_acas_ref into l_exists;
      IF c_acas_ref%notfound
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',275);
      END IF;
    CLOSE c_acas_ref;
  END IF;
--  
-- Check aun code is valid
-- 
  IF p1.lapp_aun_code IS NOT NULL
   THEN
--
    l_exists   := null;
--
    OPEN c_aun_exists(p1.lapp_aun_code);
    FETCH c_aun_exists INTO l_exists;
    IF c_aun_exists%notfound 
    THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',893) ;
    END IF; 
   CLOSE  c_aun_exists;
  END IF;
--
-- Check aun code type can only be for type HOU
-- 
  IF p1.lapp_aun_code IS NOT NULL
   THEN
    OPEN c_aun_type(p1.lapp_aun_code);
    FETCH c_aun_type INTO l_auy_type, l_auy_code;
    CLOSE c_aun_type;
--
    IF  (l_auy_type <> 'HOU' AND l_auy_code <> 'TOP')
     THEN
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',316) ;
    END IF; 
  END IF;
--
--
-- Added validate for mandatory fields so NOT NULL is removed from Data Load table
-- we want the validate to fail if not supplied not the actual load itself
--
  IF p1.lapp_offer_flag IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',860);
  END IF;
--
  IF p1.lapp_nomination_flag IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',861);
  END IF;
--
  IF p1.lapp_received_date IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',862);
  END IF;
--
  IF LTRIM(RTRIM(p1.lapp_corr_name)) IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',863);
  END IF;
--
  IF p1.lapp_sco_code IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',864);
  END IF;
--
  IF p1.lapp_status_date IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',865);
  END IF;
--
  IF p1.lapp_legacy_ref IS NULL
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',866);
  END IF;
--
-- Now UPDATE the record count and error code
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
fsc_utils.proc_END;
commit;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
-- ************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 is
SELECT
     rowid rec_rowid,
     lapp_dlb_batch_id,
     lapp_dl_seqno,
     lapp_dl_load_status,
     lapp_legacy_ref
FROM  dl_hat_applications
WHERE lapp_dlb_batch_id   = p_batch_id
AND   lapp_dl_load_status = 'C';
--
i INTEGER := 0;
l_an_tab VARCHAR2(1);
l_app_refno applications.app_refno%TYPE;
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_APPLICATIONS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_applications.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_applications.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lapp_dl_seqno;
i  := i +1;
l_id := p1.rec_rowid;
--
--
-- Get the app_refno
--
l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lapp_legacy_ref);
--
--
DELETE FROM APPLICATIONS
WHERE app_refno = l_app_refno;
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
DELETE FROM address_usages
WHERE  aus_aut_fao_code = 'APP'
  AND  NOT EXISTS (SELECT NULL FROM applications
                   WHERE app_refno = aus_app_refno
                  )
;
--Section to analyse the table populated by this dataload

l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLICATIONS');
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
--
END s_dl_hat_applications;
/
show errors
