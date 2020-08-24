CREATE OR REPLACE PACKAGE BODY s_dl_hat_applic_list_ent_hist
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Ver    WHO  WHEN        WHY
--      1.0             RJ  03/04/2001  Dataload
--      2.0  5.3.0     PJD  05-FEB-2003 Created by field now defaults to
--                                      DATALOAD
--
--      2.1  5.8.0     VRS  17-JAN-2006 c_app_type cursor left open in VALIDATE
--      3.0  5.13.0    PH   06-FEB-2008 Now includes its own 
--                                      set_record_status_flag procedure
--      3.1  6.14.0    MOK  30-AUG-2017 Additional fields added for GNB Migrate
--      3.2  6.14.0    AJ   20-SEP-2017 Additional validation added for lleh_app_refno
--      3.3  6.14.0    AJ   26-SEP-2017 data load table name changed to match package
--      3.4  6.14.0    AJ   11-APR-2018	Validation check for lleh_app_refno amended as
--                                      wrong function used amended to use cursor instead
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
  UPDATE dl_hat_applic_list_ent_hist
  SET lleh_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_applic_list_ent_hist');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
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
     LLEH_DLB_BATCH_ID,
     LLEH_DL_SEQNO,
     LLEH_DL_LOAD_STATUS,
     LLEH_RLI_CODE,
     LLEH_TYPE_IND,
     LLEH_LST_CODE,
     LLEH_CREATED_DATE,
     LLEH_CREATED_BY,
     LLEH_MODIFED_DATE,
     LLEH_MODIFIED_BY,
     LLEH_ACTION_IND,
     LLEH_ALT_REF,
     LLEH_ALS_ACTIVE_IND,
     LLEH_REGISTERED_DATE,
     LLEH_HTY_CODE,
     LLEH_MODEL_HTY_CODE,
     LLEH_STATUS_START_DATE,
     LLEH_STATUS_REVIEW_DATE,
     LLEH_REREG_BY_DATE,
     LLEH_CPR_PRI,
     LLEH_BECAME_ACTIVE_DATE,
     LLEH_APPLICATION_CATEGORY,
     LLEH_LIST_REASON_QUALIFICATION,
     LLEH_APPLICATION_STATUS_REASON,
     LLEH_APP_REFNO,
     LLEH_CATEGORY_START_DATE,
     LLEH_CATEGORY_SYS_GEN_IND   
FROM  dl_hat_applic_list_ent_hist
WHERE lleh_dlb_batch_id   = p_batch_id
AND   lleh_dl_load_status = 'V';
--
--
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_APPLIC_LIST_ENT_HIST';
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
fsc_utils.proc_start('s_dl_hat_applic_list_ent_hist.dataload_create');
fsc_utils.debug_message( 's_dl_hat_applic_list_ent_hist.dataload_create',3);
--
cb := p_batch_id;
cd := p_DATE;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lleh_dl_seqno;
l_id := p1.rec_rowid;
--
-- Get the application reference number
IF p1.LLEH_APP_REFNO IS NULL 
THEN
l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lleh_alt_ref);
ELSE 
l_app_refno := p1.LLEH_APP_REFNO;
END IF;

INSERT INTO applic_list_entry_history
          (LEH_APP_REFNO,
           LEH_RLI_CODE,
           LEH_TYPE_IND,
           LEH_LST_CODE,
           LEH_CREATED_DATE,
           LEH_CREATED_BY,
           LEH_MODIFED_DATE,
           LEH_MODIFIED_BY,
           LEH_ACTION_IND,
           LEH_ALT_REF,
           LEH_ALS_ACTIVE_IND,
           LEH_REGISTERED_DATE,
           LEH_HTY_CODE,
           LEH_MODEL_HTY_CODE,
           LEH_STATUS_START_DATE,
           LEH_STATUS_REVIEW_DATE,
           LEH_REREG_BY_DATE,
           LEH_CPR_PRI,
           LEH_BECAME_ACTIVE_DATE,
           LEH_APPLICATION_CATEGORY,
           LEH_LIST_REASON_QUALIFICATION,
           LEH_APPLICATION_STATUS_REASON,
           LEH_CATEGORY_START_DATE,
           LEH_CATEGORY_SYS_GENERATED_IND  
     )
VALUES
    (l_app_refno,         
     p1.LLEH_RLI_CODE,
     p1.LLEH_TYPE_IND,
     p1.LLEH_LST_CODE,
     p1.LLEH_CREATED_DATE,
     NVL(p1.LLEH_CREATED_BY,'DATALOAD'),
     p1.LLEH_MODIFED_DATE,
     p1.LLEH_MODIFIED_BY,
     p1.LLEH_ACTION_IND,
     p1.LLEH_ALT_REF,
     p1.LLEH_ALS_ACTIVE_IND,
     p1.LLEH_REGISTERED_DATE,
     p1.LLEH_HTY_CODE,
     p1.LLEH_MODEL_HTY_CODE,
     p1.LLEH_STATUS_START_DATE,
     p1.LLEH_STATUS_REVIEW_DATE,
     p1.LLEH_REREG_BY_DATE,
     p1.LLEH_CPR_PRI,
     p1.LLEH_BECAME_ACTIVE_DATE,
     p1.LLEH_APPLICATION_CATEGORY,
     p1.LLEH_LIST_REASON_QUALIFICATION,
     p1.LLEH_APPLICATION_STATUS_REASON,
     p1.LLEH_CATEGORY_START_DATE,
     p1.LLEH_CATEGORY_SYS_GEN_IND 
     );

--
-- Update status of records processed
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'C');
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
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
-- Section to analyse the table populated by the dataload

l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLIC_LIST_ENTRY_HISTORY');

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
-- ****************************************************************************
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2
     ,p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
     ROWID rec_rowid,
     LLEH_DLB_BATCH_ID,
     LLEH_DL_SEQNO,
     LLEH_DL_LOAD_STATUS,
     LLEH_RLI_CODE,
     LLEH_TYPE_IND,
     LLEH_LST_CODE,
     LLEH_CREATED_DATE,
     LLEH_CREATED_BY,
     LLEH_MODIFED_DATE,
     LLEH_MODIFIED_BY,
     LLEH_ACTION_IND,
     LLEH_ALT_REF,
     LLEH_ALS_ACTIVE_IND,
     LLEH_REGISTERED_DATE,
     LLEH_HTY_CODE,
     LLEH_MODEL_HTY_CODE,
     LLEH_STATUS_START_DATE,
     LLEH_STATUS_REVIEW_DATE,
     LLEH_REREG_BY_DATE,
     LLEH_CPR_PRI,
     LLEH_BECAME_ACTIVE_DATE,
     LLEH_APPLICATION_CATEGORY,
     LLEH_LIST_REASON_QUALIFICATION,
     LLEH_APPLICATION_STATUS_REASON,
     LLEH_APP_REFNO,
     LLEH_CATEGORY_START_DATE,
     LLEH_CATEGORY_SYS_GEN_IND     
FROM  dl_hat_applic_list_ent_hist
WHERE lleh_dlb_batch_id    = p_batch_id
AND   lleh_dl_load_status IN ('L','F','O');
--
--*****************************
--
-- Find out the application type
--
CURSOR c_app_type (p_rli_code VARCHAR2) IS
SELECT rli_type FROM rehousing_lists
WHERE rli_code = p_rli_code;
--
--********************
--
CURSOR c_app_refno(p_app_refno NUMBER)    IS
SELECT app_refno
FROM  applications
WHERE  app_refno = p_app_refno;
--
--*****************************
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_APPLIC_LIST_ENT_HIST';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_errors            VARCHAR2(10);
l_error_ind         VARCHAR2(10);
i                   INTEGER := 0;
l_rli_type          rehousing_lists.rli_type%TYPE;
l_app_refno         applications.app_refno%TYPE;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_applic_list_ent_hist.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_applic_list_ent_hist.dataload_validate',3 );
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
  cs := p1.lleh_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors := 'V';
  l_error_ind := 'N';
  l_rli_type  := NULL;
  l_app_refno := NULL;
--
-- A valid rehousing list code should have been supplied
  IF (NOT s_dl_hat_utils.f_exists_rlicode(p1.lleh_rli_code))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',218);
  END IF;
--
  OPEN  c_app_type(p1.lleh_rli_code);
  FETCH c_app_type INTO l_rli_type;
  CLOSE c_app_type;
--
--  An application list entry history record must relate to an existing
--  application list entry
--
  IF (NOT s_dl_hat_utils.f_exists_applistentry(p1.lleh_alt_ref,
                                               p1.lleh_rli_code))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',207);
  END IF;
--
-- Check that the application list entry history record must relate to an
-- existing application
--
  IF (NOT s_dl_hat_utils.f_exists_application(p1.lleh_alt_ref))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',216);
  END IF;
-- -- ***************
-- These validation checks are not required 16/07/01
--
-- If the application list entry relates to an homeless application
-- the application should not be on the general list
--
--
--IF l_rli_type = 'H'
--THEN
--   IF (NOT s_dl_hat_utils.f_application_type(p1.lleh_alt_ref,'H'))
--   THEN
--    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',224);
--   END IF;
--END IF;
--
--
-- If the application list entry relates to the general application
-- the application should not be on the homeless list
--IF l_rli_type = 'S'
--THEN
--   IF (NOT s_dl_hat_utils.f_application_type(p1.lleh_alt_ref,'S'))
--   THEN
--      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',225);
--   END IF;
--END IF;
-- ***************
--
-- If the application is a transfer application there must be a valid
-- tenancy in the tenancies table
  IF l_rli_type = 'T'
   THEN
   IF (NOT s_dl_hat_utils.f_application_type(p1.lleh_alt_ref,'T'))
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',226);
   END IF;
  END IF;
-- Check the registration date is not later than today's date
--
  IF (p1.lleh_registered_date > TRUNC(SYSDATE))
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',229);
  END IF;
--
-- Check the re-registration date, if supplied, is not earlier than
-- the registration date
--
  IF (p1.lleh_rereg_by_date IS NOT NULL)
   THEN
   IF (p1.lleh_rereg_by_date <= p1.lleh_registered_date)
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',230);
   END IF;
  END IF;
--
-- Check that a valid list statues code is supplied
--
  IF (NOT s_dl_hat_utils.f_exists_lstcode(p1.lleh_lst_code))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',330);
  END IF;
--
-- Check reference values
--
-- List qualification code
--
  IF (p1.lleh_list_reason_qualification IS NOT NULL)
  THEN
   IF (NOT s_dl_hem_utils.exists_frv('LIST_QUAL',p1.lleh_list_reason_qualification))
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',231);
   END IF;
  END IF;
--
-- Application category
--
  IF (p1.lleh_application_category IS NOT NULL)
   THEN
    IF (NOT s_dl_hem_utils.exists_frv('APPLCAT',p1.lleh_application_category))
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',232);
    END IF;
  END IF;
--
-- Check the type_ind to make sure that it is 'S','C' or 'G'
-- indicates the type of change 'S'tatus 'C'ategory or 'G'eneral
--
   IF p1.lleh_type_ind NOT IN ('S','C','G')
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',208);
   END IF;
--
-- Check that the action indicator is 'U' for updated or 'I' for inserted
--
  IF p1.lleh_action_ind NOT IN ('U','I')
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',209);
  END IF;
--
-- Check that the lleh_app_refno supplied exist in the applications table
--
  IF (p1.lleh_app_refno IS NOT NULL)
   THEN
    OPEN  c_app_refno(p1.lleh_app_refno);
    FETCH c_app_refno INTO l_app_refno;
    CLOSE c_app_refno;
--
    IF (l_app_refno IS NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',317);
    END IF;
  END IF;
--
--******************************************
-- Now UPDATE the record count and error code
--
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
--
END dataload_validate;
--
-- *********************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 IS
SELECT
     ROWID rec_rowid,
     LLEH_DLB_BATCH_ID,
     LLEH_DL_SEQNO,
     LLEH_DL_LOAD_STATUS,
     LLEH_ALT_REF,
     LLEH_RLI_CODE,
     LLEH_TYPE_IND,
     LLEH_LST_CODE
FROM  dl_hat_applic_list_ent_hist
WHERE lleh_dlb_batch_id   = p_batch_id
AND   lleh_dl_load_status = 'C';
--

i INTEGER := 0;
l_an_tab  VARCHAR2(1);
l_app_refno applications.app_refno%TYPE;
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_APPLIC_LIST_ENT_HIST';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_applic_list_ent_hist.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_applic_list_ent_hist.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lleh_dl_seqno;
  i  := i +1;
  l_id := p1.rec_rowid;
--
--
-- Get the application reference number

  l_app_refno := s_dl_hat_utils.app_refno_for_app_legacy_ref(p1.lleh_alt_ref);
--
--
  DELETE FROM applic_list_entry_history
  WHERE leh_app_refno =  l_app_refno
  AND   leh_rli_code = p1.lleh_rli_code
  AND   leh_type_ind = p1.lleh_type_ind
  AND   leh_lst_code = p1.lleh_lst_code;
--
-- *******************************************************
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
-- Section to analyse the table populated by the dataload

l_an_tab := s_dl_hem_utils.dl_comp_stats('APPLIC_LIST_ENTRY_HISTORY');
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
END s_dl_hat_applic_list_ent_hist;
/

