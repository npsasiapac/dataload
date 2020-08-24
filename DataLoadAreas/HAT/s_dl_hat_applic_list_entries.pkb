CREATE OR REPLACE PACKAGE BODY s_dl_hat_applic_list_entries
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB VER    WHO  WHEN         WHY
--      1.0           RJ   03/04/2001   Dataload
--      1.1 5.1.6     PJD  21/05/2002   Moved analyze table command to the correct place
--                                      within the create process. i.e outside the loop
--      2.0 5.2.0     PJD  09/07/2002   Changes for 5.2.0
--      3.0 5.3.0     PJD  05-FEB-2003  Created by field now defaults to DATALOAD
--      3.1 5.3.0     SB   20-mar-2003  Added Commit on Validate
--      3.2 5.4.0     MH   13/02/2004   Missing RAISE statement from end of create process
--      3.3 5.5.0     PH   02/03/2004   Moved the process summary update for all processes
--                                      to before the commit and added final commit.
--                                      Also added new validate on rli_code/ala_code
--                                      combination.
--      3.4 5.5.0     MH   09/03/2004   Too many CLOSE C_APP_TYPE statements causing 
--                                      batch to FAIL everytime - now removed.
--      3.5 5.6.0     PH   13/08/2004   Added new field lale_category_start_date and
--                                      validation against it.
--      3.6 5.6.0     PJD  17/11/2004   Changed check on Homeless List entries
--                                      Removed check on application having entries
--                                      on Homeless and General lists.
--                                      (entry for err 224 also updated in hdl_errs_in)
--      3.7 5.10.0    PH   26/07/2006   Added delete from applic_list_entry_hist as 
--                                      this gets populated by a trigger on create.
--                                      This same changed applied to v5.9.0 code too
--      3.8 5.12.0    PH   29/10/2007   Amended validate on category start date
--                                      from <= to < registered date.
--      3.9 5.13.0    PH   06-FEB-2008  Now includes its own 
--                                      set_record_status_flag procedure.
--      ******  BESPOKE VERSION FOR GNB ALLOCATIONS MIGRATION  *******
--      4.0 6.14      MOK  30-AUG-2017  Added extra validation and fields for NB
--      4.1 6.14      AJ   05-SEP-2017  Added validation for app_refno and hty_code for NB
--      4.2 6.14      AJ   06-SEP-2017  LALE_DEFINED_HTY_CODE removed as calculated by HAT004
--      4.3 6.14      AJ   19-SEP-2017  LALE_CHANGED_IND and LALE_AMENDED_ONLINE_IND not used
--                                      in CREATE as ALL applications MUST be recalculated
--                                      after data load anyway (as per Sharron G)
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
  UPDATE dl_hat_applic_list_entries
  SET lale_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_applic_list_entries');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
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
CURSOR c1 IS
SELECT
     ROWID rec_rowid,
     LALE_DLB_BATCH_ID,
     LALE_DL_SEQNO,
     LALE_DL_LOAD_STATUS,
     LALE_RLI_CODE,
     LALE_LST_CODE,
     LALE_ALT_REF,
     LALE_AUN_CODE,
     LALE_CREATED_BY,
     LALE_CREATED_DATE,
     LALE_ALS_ACTIVE_IND,
     LALE_REGISTERED_DATE,
     LALE_STATUS_START_DATE,
     LALE_STATUS_REVIEW_DATE,
     LALE_REREG_BY_DATE,
     LALE_BECAME_ACTIVE_DATE,
     LALE_REFUSALS_COUNT,
     LALE_ALA_HRV_APC_CODE,
     LALE_HRV_LRQ_CODE,
     LALE_HRV_APS_CODE,
     NVL(LALE_CURRENT_NOMINATION_COUNT,0)  LALE_CURRENT_NOMINATION_COUNT,
     NVL(LALE_CURRENT_OFFER_COUNT,0)       LALE_CURRENT_OFFER_COUNT,
     LALE_CATEGORY_START_DATE,
     LALE_APP_REFNO,
     LALE_CHANGED_IND,               --Indicates whether any changes have been made against the application that require a re-assessment
     LALE_MODIFIED_DATE,           
     LALE_MODIFIED_BY,
     LALE_AMENDED_ONLINE_IND         --Indicates that a change has occurred that requires on line assessment
FROM  dl_hat_applic_list_entries
WHERE lale_dlb_batch_id   = p_batch_id
AND   lale_dl_load_status = 'V';
--
--

--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_APPLIC_LIST_ENTRIES';
cs       INTEGER;
ce	     VARCHAR2(200);
l_id     ROWID;
-- Other variables
--
i                INTEGER := 0;
l_an_tab         VARCHAR2(1);
l_app_refno      applications.app_refno%TYPE;
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_applic_list_entries.dataload_create');
fsc_utils.debug_message( 's_dl_hat_applic_list_entries.dataload_create',3);
--
cb := p_batch_id;
cd := p_DATE;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lale_dl_seqno;
l_id := p1.rec_rowid;
--
-- Get the application reference number
IF p1.LALE_APP_REFNO IS NULL 
THEN
l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lale_alt_ref);
ELSE 
l_app_refno := p1.LALE_APP_REFNO;
END IF;
--
IF  p1.LALE_CREATED_BY IS NULL
THEN
   p1.LALE_CREATED_BY := 'DATALOAD';
END IF;

IF p1.LALE_CREATED_DATE IS NULL
THEN
     p1.LALE_CREATED_DATE := SYSDATE;
END IF;
--
INSERT INTO applic_list_entries
          (ALE_APP_REFNO,
           ALE_RLI_CODE,
           ALE_LST_CODE,
           ALE_ALT_REF,
           ALE_AUN_CODE,
           ALE_CREATED_BY,
           ALE_CREATED_DATE,
           ALE_ALS_ACTIVE_IND,
           ALE_REGISTERED_DATE,
           ALE_STATUS_START_DATE,
           ALE_STATUS_REVIEW_DATE,
           ALE_REREG_BY_DATE,
           ALE_BECAME_ACTIVE_DATE,
           ALE_REFUSALS_COUNT,
           ALE_ALA_HRV_APC_CODE,
           ALE_HRV_LRQ_CODE,
           ALE_HRV_APS_CODE,
           ALE_CURRENT_NOMINATION_COUNT,
           ALE_CURRENT_OFFER_COUNT,
           ALE_CATEGORY_START_DATE,
           ALE_MODIFIED_DATE,
           ALE_MODIFIED_BY
)
VALUES
    (l_app_refno,
     p1.LALE_RLI_CODE,
     p1.LALE_LST_CODE,
     p1.LALE_ALT_REF,
     p1.LALE_AUN_CODE,
     p1.LALE_CREATED_BY,
     p1.LALE_CREATED_DATE,
     p1.LALE_ALS_ACTIVE_IND,
     p1.LALE_REGISTERED_DATE,
     p1.LALE_STATUS_START_DATE,
     p1.LALE_STATUS_REVIEW_DATE,
     p1.LALE_REREG_BY_DATE,
     p1.LALE_BECAME_ACTIVE_DATE,
     p1.LALE_REFUSALS_COUNT,
     p1.LALE_ALA_HRV_APC_CODE,
     p1.LALE_HRV_LRQ_CODE,
     p1.LALE_HRV_APS_CODE,
     p1.LALE_CURRENT_NOMINATION_COUNT,
     p1.LALE_CURRENT_OFFER_COUNT,
     p1.LALE_CATEGORY_START_DATE,
     p1.LALE_MODIFIED_DATE,
     p1.LALE_MODIFIED_BY
     
);
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'C');
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
--
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
-- section to analyse the table populated by this dataload
--
l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLIC_LIST_ENTRIES');
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
COMMIT;
--
END dataload_create;
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2
     ,p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
     ROWID rec_rowid,
     LALE_DLB_BATCH_ID,
     LALE_DL_SEQNO,
     LALE_DL_LOAD_STATUS,
     LALE_RLI_CODE,
     LALE_LST_CODE,
     LALE_ALT_REF,
     LALE_AUN_CODE,
     LALE_CREATED_BY,
     LALE_CREATED_DATE,
     LALE_ALS_ACTIVE_IND,
     LALE_REGISTERED_DATE,
     LALE_STATUS_START_DATE,
     LALE_STATUS_REVIEW_DATE,
     LALE_REREG_BY_DATE,
     LALE_BECAME_ACTIVE_DATE,
     LALE_REFUSALS_COUNT,
     LALE_ALA_HRV_APC_CODE,
     LALE_HRV_LRQ_CODE,
     LALE_HRV_APS_CODE,
     LALE_CURRENT_NOMINATION_COUNT,
     LALE_CURRENT_OFFER_COUNT,
     LALE_CATEGORY_START_DATE,
     LALE_APP_REFNO,
     LALE_CHANGED_IND,               --Indicates whether any changes have been made against the application that require a re-assessment
     LALE_MODIFIED_DATE,           
     LALE_MODIFIED_BY,
     LALE_AMENDED_ONLINE_IND         --Indicates that a change has occurred that requires on line assessment
FROM  dl_hat_applic_list_entries
WHERE lale_dlb_batch_id    = p_batch_id
AND   lale_dl_load_status IN ('L','F','O');
--
-- Find out the application type
--
CURSOR c_app_type (p_rli_code VARCHAR2) IS
  SELECT rli_type FROM rehousing_lists
  WHERE rli_code = p_rli_code;
--
CURSOR c_ala_check (p_rli_code   VARCHAR2,
                    p_ala_code   VARCHAR2)  IS
SELECT 'X'
FROM   applic_list_categories
WHERE  ala_rli_code      =  p_rli_code
AND    ala_hrv_apc_code  =  p_ala_code;
--
CURSOR c_int_par (p_alt_ref VARCHAR2) IS
SELECT 'Y'
FROM   interested_party_types ,interested_parties,
       interested_party_usages, applications
WHERE  app_legacy_ref = p_alt_ref
AND    app_refno = ipus_app_refno
AND    ipp_refno = ipus_ipp_refno
AND    ipp_ipt_code = ipt_code
AND    ipt_hrv_fiy_code = 'HLCW';
--
CURSOR c_app_check (p_app_ref NUMBER) IS
SELECT 'X'
FROM   applications
WHERE  app_refno = p_app_ref;
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_APPLIC_LIST_ENTRIES';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_errors            VARCHAR2(10);
l_error_ind         VARCHAR2(10);
i                   INTEGER := 0;
l_exists            VARCHAR2(1);
l_app_exists        VARCHAR2(1);
l_rli_type          rehousing_lists.rli_type%TYPE;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_applic_list_entries.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_applic_list_entries.dataload_validate',3 );
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
cs := p1.lale_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors     := 'V';
l_error_ind  := 'N';
l_exists     := null;
l_app_exists := null;
--
-- A valid rehousing list code should have been supplied
  IF (NOT s_dl_hat_utils.f_exists_rlicode(p1.lale_rli_code))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',218);
  END IF;
--
-- Check that the application list entry must relate to an existing
-- application
--
 IF (NOT s_dl_hat_utils.f_exists_application(p1.lale_alt_ref))
 THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',216);
 END IF;
--
-- Get the type
--
OPEN  c_app_type(p1.lale_rli_code);
FETCH c_app_type INTO l_rli_type;
CLOSE c_app_type;
--
-- If the application list entry is for a homeless list
-- then check that a Homeless Case worker has been assigned
-- to that application
--
IF l_rli_type = 'H'
THEN
  l_exists := NULL;
  OPEN c_int_par(p1.lale_alt_ref);
  FETCH c_int_par into l_exists;
  CLOSE c_int_par;
  --
  IF nvl(l_exists,'Z') != 'Y'
   THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',224);
   END IF;
END IF;
--
-- If the application list entry relates to the general application
-- the application should not be on the homeless list
-- REMOVED AS THIS DOESN'T MAKE SENSE
-- IF l_rli_type = 'S'
-- THEN
--   IF (NOT s_dl_hat_utils.f_application_type(p1.lale_alt_ref,'S'))
--   THEN
--      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',225);
--   END IF;
-- END IF;
--
--
-- If the application is a transfer application there must be a valid
-- tenancy in the tenancies table
IF l_rli_type = 'T'
THEN
   IF (NOT s_dl_hat_utils.f_application_type(p1.lale_alt_ref,'T'))
   THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',226);
   END IF;
END IF;
--
-- The admin unit code supplied must be a valid admin unit
--
   IF (NOT s_dl_hat_utils.f_exists_auncode(p1.lale_aun_code))
   THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
   END IF;
--
-- The admin unit code supplied must be of type 'off'
   IF (NOT s_dl_hat_utils.f_exists_auntype(p1.lale_aun_code))
   THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',228);
   END IF;
-- Check that a valid list statues code is supplied
--
   IF (NOT s_dl_hat_utils.f_exists_lstcode(p1.lale_lst_code))
   THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',330);
   END IF;
--
-- Check the registration date is not later than today's date
--
  IF (p1.lale_registered_date > TRUNC(SYSDATE))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',229);
  END IF;
--
-- Check the re-registration date, if supplied, is not earlier or equal to the
-- the registration date
--
  IF (p1.lale_rereg_by_date IS NOT NULL)
  THEN
     IF (p1.lale_rereg_by_date <= p1.lale_registered_date)
     THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',230);
     END IF;
  END IF;
--
-- Check reference values
--
-- List qualification code
--
  IF (p1.lale_hrv_lrq_code IS NOT NULL)
  THEN
    IF (NOT s_dl_hem_utils.exists_frv('LIST_QUAL',p1.lale_hrv_lrq_code))
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',231);
    END IF;
  END IF;
--
-- Application category
--
  IF (p1.lale_ala_hrv_apc_code IS NOT NULL)
  THEN
     IF (NOT s_dl_hem_utils.exists_frv('APPLCAT',p1.lale_ala_hrv_apc_code))
     THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',232);
     END IF;
  END IF;
--
-- Application Category/ List Combination
--
  IF (p1.lale_ala_hrv_apc_code IS NOT NULL)
  THEN
   l_exists := NULL;
   OPEN c_ala_check(p1.lale_rli_code, p1.lale_ala_hrv_apc_code);
    FETCH c_ala_check INTO l_exists;
     IF c_ala_check%notfound
      THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',249);
     END IF;
   CLOSE c_ala_check;
  END IF;
--
-- Check that the Category Start date is not before registered date
--
  IF (p1.lale_category_start_date IS NOT NULL)
   THEN
    IF (p1.lale_category_start_date < p1.lale_registered_date)
     THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',251);
     END IF;
  END IF;
--
-- Check app_refno exists
--
  IF (p1.lale_app_refno IS NOT NULL)
  THEN
   OPEN c_app_check(p1.lale_app_refno);
    FETCH c_app_check INTO l_app_exists;
     IF c_app_check%notfound
      THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',317);
     END IF;
   CLOSE c_app_check;
  END IF;
--
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
--
commit;
--
fsc_utils.proc_END;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
COMMIT;
--
END dataload_validate;
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 IS
SELECT
     ROWID rec_rowid,
     LALE_DLB_BATCH_ID,
     LALE_DL_SEQNO,
     LALE_DL_LOAD_STATUS,
     LALE_RLI_CODE,
     LALE_ALT_REF
FROM  dl_hat_applic_list_entries
WHERE lale_dlb_batch_id   = p_batch_id
AND   lale_dl_load_status = 'C';
--
i INTEGER := 0;
l_app_refno applications.app_refno%TYPE;
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_APPLIC_LIST_ENTRIES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_an_tab VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_applic_list_entries.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_applic_list_entries.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lale_dl_seqno;
i  := i +1;
l_id := p1.rec_rowid;
--
--
-- Get the application reference number

l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lale_alt_ref);
--
DELETE FROM applic_list_entry_history
WHERE  leh_app_refno = l_app_refno
AND    leh_rli_code  = leh_rli_code;
--
DELETE FROM applic_list_entries
WHERE ale_app_refno = l_app_refno
AND   ale_rli_code  = p1.lale_rli_code;
--
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'V');
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
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
-- section to analyse the table populated by this dataload

 l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLIC_LIST_ENTRIES');

fsc_utils.proc_end;
COMMIT;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
COMMIT;
--
END dataload_delete;
--
--
END s_dl_hat_applic_list_entries;
/

