CREATE OR REPLACE PACKAGE BODY s_dl_hat_involved_party_hist
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Ver   WHO  WHEN       WHY
--      1.0  6.14     AJ   13/10/2017 Initial Creation for GNB Migration Project
--                                    based on involved_parties ver 4.2
--      1.1  6.14     AJ   16/01/2018 Updated and completed
--      1.2  6.14     AJ   17/01/2018 Added coding for ipa_legacy_ref if supplied
--      1.3  6.14     AJ   22/01/2018 l_par_refno put in wrong field (liph_app_refno) amended package
--
-- ***********************************************************************
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hat_involved_party_hist
  SET liph_dl_load_status = p_status
  WHERE rowid = p_rowid;
--
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hat_involved_party_hist');
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
     rowid rec_rowid,
     liph_dlb_batch_id,
     liph_dl_seqno,
     liph_dl_load_status,
     liph_app_legacy_ref,  -- nvl(app_legacy_ref,iph_app_refno)
     liph_par_per_alt_ref, -- nvl(par_per_alt_ref,iph_par_refno)
     liph_ipa_start_date,
     liph_ipa_legacy_ref,  -- involved parties table legacy ref
     liph_modified_date,
     liph_modified_by,
     liph_action_ind,
     liph_joint_appl_ind,
     liph_living_apart_ind,
     liph_rehouse_ind,
     liph_main_applicant_ind,
     liph_created_by,
     liph_created_date,
     liph_start_date,
     liph_end_date,
     liph_groupno,
     liph_act_roomno,
     liph_frv_end_reason,
     liph_frv_relation,
     liph_ipa_refno,
     liph_app_refno,
     liph_par_refno
FROM  dl_hat_involved_party_hist
WHERE liph_dlb_batch_id   = p_batch_id
AND   liph_dl_load_status = 'V';
--
-- *****************************
--
CURSOR c_get_ipa_refno ( p_ipa_app_refno  NUMBER
                        ,p_ipa_par_refno  NUMBER
                        ,p_ipa_start_date DATE   ) IS
SELECT ipa_refno
FROM involved_parties
WHERE ipa_app_refno = p_ipa_app_refno
AND ipa_par_refno = p_ipa_par_refno
AND trunc(ipa_start_date) = p_ipa_start_date;
-- *******************
--
CURSOR c_get_par_refno (p_lpar_per_alt_ref VARCHAR2) IS
SELECT par_refno
FROM parties
WHERE par_per_alt_ref = p_lpar_per_alt_ref;
-- *******************
--
CURSOR c_get_app_refno (p_lapp_legacy_ref VARCHAR2) IS
SELECT app_refno
FROM applications
WHERE app_legacy_ref = p_lapp_legacy_ref;
--WHERE app_refno = p_lapp_legacy_ref;
-- testing only as no app_legacy_ref matches(AJ)
-- *****************************
--
CURSOR c_get_ipa_refno2 ( p_ipa_app_refno  NUMBER
                         ,p_ipa_par_refno  NUMBER
                         ,p_ipa_legacy_ref NUMBER ) IS
SELECT ipa_refno
FROM involved_parties
WHERE ipa_legacy_ref = p_ipa_legacy_ref
AND ipa_par_refno = p_ipa_par_refno
AND ipa_app_refno = p_ipa_app_refno;
-- *****************************
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HAT_INVOLVED_PARTY_HIST';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_an_tab         VARCHAR2(1);
i                INTEGER := 0;
l_app_refno      involved_party_history.iph_app_refno%TYPE;
l_par_refno      involved_party_history.iph_par_refno%TYPE;
l_ipa_refno      involved_party_history.iph_ipa_refno%TYPE;
l_updated_ind    VARCHAR2(1);
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_involved_party_hist.dataload_create');
fsc_utils.debug_message( 's_dl_hat_involved_party_hist.dataload_create',3);
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
  cs := p1.liph_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_app_refno := NULL;
  l_par_refno := NULL;
  l_ipa_refno := NULL;
  l_updated_ind := NULL;
--
  SAVEPOINT SP1;
--
-- Get the par_refno from parties
--
  OPEN  c_get_par_refno(p1.liph_par_per_alt_ref);
  FETCH c_get_par_refno INTO l_par_refno;
  CLOSE c_get_par_refno;
--
-- Get the app_refno from applications
--
  OPEN  c_get_app_refno(p1.liph_app_legacy_ref);
  FETCH c_get_app_refno INTO l_app_refno;
  CLOSE c_get_app_refno;
--
-- Get the ipa_refno from involved parties use ipa_legacy_ref if supplied
-- first if not found then use start date instead but less accurate as this
-- will not match if start dates not sync'd correctly or changed
--
  IF(    l_app_refno            IS NOT NULL
     AND l_par_refno            IS NOT NULL
     AND p1.liph_ipa_legacy_ref IS NOT NULL )
   THEN
    OPEN  c_get_ipa_refno2(l_app_refno,l_par_refno,p1.liph_ipa_legacy_ref);
    FETCH c_get_ipa_refno2 INTO l_ipa_refno;
    CLOSE c_get_ipa_refno2;
--
    IF (l_ipa_refno IS NULL AND p1.liph_ipa_start_date IS NOT NULL )
     THEN
      OPEN  c_get_ipa_refno(l_app_refno, l_par_refno, p1.liph_ipa_start_date);
      FETCH c_get_ipa_refno INTO l_ipa_refno;
      CLOSE c_get_ipa_refno;
    END IF;
--
  ELSE
--
   IF(   l_app_refno            IS NOT NULL
     AND l_par_refno            IS NOT NULL
     AND p1.liph_ipa_start_date IS NOT NULL )
    THEN
     OPEN  c_get_ipa_refno(l_app_refno, l_par_refno, p1.liph_ipa_start_date);
     FETCH c_get_ipa_refno INTO l_ipa_refno;
     CLOSE c_get_ipa_refno;
   END IF;
--
  END IF;
--
  IF(    l_app_refno  IS NOT NULL
     AND l_par_refno  IS NOT NULL
     AND l_ipa_refno  IS NOT NULL )
   THEN
--
    INSERT INTO involved_party_history
          (IPH_IPA_REFNO,
           IPH_PAR_REFNO,
           IPH_APP_REFNO,
           IPH_MODIFIED_DATE,
           IPH_MODIFIED_BY,
           IPH_ACTION_IND,
           IPH_JOINT_APPL_IND,
           IPH_LIVING_APART_IND,
           IPH_REHOUSE_IND,
           IPH_MAIN_APPLICANT_IND,
           IPH_CREATED_BY,
           IPH_CREATED_DATE,
           IPH_START_DATE,
           IPH_END_DATE,
           IPH_GROUPNO,
           IPH_ACT_ROOMNO,
           IPH_FRV_END_REASON,
           IPH_FRV_RELATION)
    VALUES
          (l_ipa_refno,
           l_par_refno,
           l_app_refno,
           p1.liph_modified_date,
           p1.liph_modified_by,
           p1.liph_action_ind,
           p1.liph_joint_appl_ind,
           p1.liph_living_apart_ind,
           p1.liph_rehouse_ind,
           p1.liph_main_applicant_ind,
           p1.liph_created_by,
           p1.liph_created_date,
           p1.liph_start_date,
           p1.liph_end_date,
           p1.liph_groupno,
           p1.liph_act_roomno,
           p1.liph_frv_end_reason,
           p1.liph_frv_relation);
--
   l_updated_ind := 'Y';
--
  END IF;
--
-- *****************************
-- keep a count of the rows processed and commit after every 1000
--
  i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
  s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
  set_record_status_flag(l_id,'C');
--
-- allow for insert to not meet conditions set status back to VALIDATE
-- if it does not
--
  IF(    l_app_refno  IS NULL
     OR  l_par_refno  IS NULL
     OR  l_ipa_refno  IS NULL )
   THEN
    set_record_status_flag(l_id,'V');
  END IF;
--
  UPDATE dl_hat_involved_party_hist
  SET   liph_ipa_refno = l_ipa_refno
  ,     liph_app_refno = l_app_refno
  ,     liph_par_refno = l_par_refno
  WHERE liph_dlb_batch_id = cb
    AND liph_dl_seqno = cs
    AND rowid = l_id;
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
-- *****************************
-- Section to analyse the tables populated with this data load
--
  l_an_tab := s_dl_hem_utils.dl_comp_stats('INVOLVED_PARTY_HISTORY');
  l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HAT_INVOLVED_PARTY_HIST');
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
-- ***********************************************************************
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2
     ,p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
     rowid rec_rowid,
     liph_dlb_batch_id,
     liph_dl_seqno,
     liph_dl_load_status,
     liph_app_legacy_ref,  -- nvl(app_legacy_ref,iph_app_refno)
     liph_par_per_alt_ref, -- nvl(par_per_alt_ref,iph_par_refno)
     liph_ipa_start_date,
     liph_ipa_legacy_ref,  -- involved parties table legacy ref
     liph_modified_date,
     liph_modified_by,
     liph_action_ind,
     liph_joint_appl_ind,   -- Y N
     liph_living_apart_ind, -- Y N
     liph_rehouse_ind,      -- Y N
     liph_main_applicant_ind, -- Y N
     liph_created_by,
     liph_created_date,
     liph_start_date,
     liph_end_date,
     liph_groupno,
     liph_act_roomno,
     liph_frv_end_reason,
     liph_frv_relation,
     liph_ipa_refno,
     liph_app_refno,
     liph_par_refno
FROM  dl_hat_involved_party_hist
WHERE liph_dlb_batch_id   = p_batch_id
AND   liph_dl_load_status IN ('L','F','O');
--
-- *****************************
--
CURSOR c_get_ipa_refno ( p_ipa_app_refno  NUMBER
                        ,p_ipa_par_refno  NUMBER
                        ,p_ipa_start_date DATE   ) IS
SELECT ipa_refno
FROM involved_parties
WHERE ipa_app_refno = p_ipa_app_refno
AND ipa_par_refno = p_ipa_par_refno
AND trunc(ipa_start_date) = p_ipa_start_date;
-- *******************
--
CURSOR c_get_par_refno (p_lpar_per_alt_ref VARCHAR2) IS
SELECT par_refno
FROM parties
WHERE par_per_alt_ref = p_lpar_per_alt_ref;
-- *******************
--
CURSOR c_get_app_refno (p_lapp_legacy_ref VARCHAR2) IS
SELECT app_refno
FROM applications
WHERE app_legacy_ref = p_lapp_legacy_ref;
-- WHERE app_refno = p_lapp_legacy_ref;
-- testing only as no app_legacy_ref matches(AJ)
-- *****************************
--
CURSOR c_get_ipa_refno2 ( p_ipa_app_refno  NUMBER
                         ,p_ipa_par_refno  NUMBER
                         ,p_ipa_legacy_ref NUMBER ) IS
SELECT ipa_refno
FROM involved_parties
WHERE ipa_legacy_ref = p_ipa_legacy_ref
AND ipa_par_refno = p_ipa_par_refno
AND ipa_app_refno = p_ipa_app_refno;
-- *****************************
--
CURSOR c_chk_iph_dup ( p_liph_ipa_refno      NUMBER
                      ,p_liph_par_refno      NUMBER
                      ,p_liph_app_refno      NUMBER
                      ,p_liph_modified_date  DATE
                      ,p_liph_modified_by    VARCHAR2
                      ,p_liph_action_ind     VARCHAR2
                      ,p_liph_joint_appl_ind VARCHAR2
                      ,p_liph_living_apart_ind VARCHAR2
                      ,p_liph_rehouse_ind    VARCHAR2
                      ,p_liph_main_applicant_ind VARCHAR2
                      ,p_liph_created_by     VARCHAR2
                      ,p_liph_created_date   DATE
                      ,p_liph_start_date     DATE
                      ,p_liph_end_date       DATE
                      ,p_liph_groupno        NUMBER
                      ,p_liph_act_roomno     VARCHAR2
                      ,p_liph_frv_end_reason VARCHAR2
                      ,p_liph_frv_relation   VARCHAR2   ) IS
SELECT 'X'
FROM involved_party_history
WHERE IPH_IPA_REFNO = nvl(p_liph_ipa_refno,IPH_IPA_REFNO)
AND IPH_PAR_REFNO = nvl(p_liph_par_refno,IPH_PAR_REFNO)
AND IPH_APP_REFNO = nvl(p_liph_app_refno,IPH_APP_REFNO)
AND IPH_MODIFIED_DATE = p_liph_modified_date
AND IPH_MODIFIED_BY = p_liph_modified_by
AND IPH_ACTION_IND = p_liph_action_ind
AND IPH_JOINT_APPL_IND = p_liph_joint_appl_ind
AND IPH_LIVING_APART_IND = p_liph_living_apart_ind
AND IPH_REHOUSE_IND = p_liph_rehouse_ind
AND IPH_MAIN_APPLICANT_IND = p_liph_main_applicant_ind
AND IPH_CREATED_BY = p_liph_created_by
AND IPH_CREATED_DATE = p_liph_created_date
AND IPH_START_DATE = p_liph_start_date
AND NVL(IPH_END_DATE,TRUNC(SYSDATE)) = nvl(p_liph_end_date,TRUNC(SYSDATE))
AND NVL(IPH_GROUPNO,'99999999') = NVL(p_liph_groupno,'99999999')
AND NVL(IPH_ACT_ROOMNO,'ZZZ') = NVL(p_liph_act_roomno,'ZZZ')
AND NVL(IPH_FRV_END_REASON,'ZZZZZZZZZZ') = NVL(p_liph_frv_end_reason,'ZZZZZZZZZZ')
AND NVL(IPH_FRV_RELATION,'ZZZZZZZZZZ') = NVL(p_liph_frv_relation,'ZZZZZZZZZZ');
-- *****************************
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAT_INVOLVED_PARTY_HIST';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_errors            VARCHAR2(10);
l_error_ind         VARCHAR2(10);
i                   INTEGER := 0;
l_rec_date       applications.app_received_date%TYPE;
l_app_refno      involved_party_history.iph_app_refno%TYPE;
l_par_refno      involved_party_history.iph_par_refno%TYPE;
l_ipa_refno      involved_party_history.iph_ipa_refno%TYPE;
l_chk_dup        VARCHAR2(1);

--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_involved_party_hist.dataload_validate');
fsc_utils.debug_message( 's_dl_hat_involved_party_hist.dataload_validate',3 );
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
  cs := p1.liph_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors := 'V';
  l_error_ind := 'N';
  l_app_refno := NULL;
  l_par_refno := NULL;
  l_ipa_refno := NULL;
  l_rec_date := NULL;
  l_chk_dup := NULL;
--
-- Mandatory field check not done below
--
  IF (p1.liph_ipa_start_date IS NULL)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',441);
-- 'Involved Party Start Date(liph_ipa_start_date)must be supplied'
  END IF;
--
  IF (p1.liph_modified_date IS NULL)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',442);
-- 'Modified Date(liph_modified_date)must be supplied'
  END IF;
--
  IF (p1.liph_modified_by IS NULL)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',443);
-- 'Modified By(liph_modified_by)must be supplied'
  END IF;
--
  IF (p1.liph_action_ind IS NULL)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',444);
-- 'Action Indicator(liph_action_ind)must be supplied'
  ELSE 
   IF(p1.liph_action_ind != 'U')
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',445);
-- 'Action Indicator(liph_action_ind)must be set to U for Update'
   END IF;
  END IF;
--
  IF (p1.liph_created_date IS NULL)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',446);
-- 'Created Date(liph_created_date)must be supplied'
  END IF;
--
  IF (p1.liph_created_by IS NULL)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',447);
-- 'Created By(liph_created_by)must be supplied'
  END IF;
--
  IF (p1.liph_start_date IS NULL)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',448);
-- 'Start Date(iph_start_date)must be supplied'
  END IF;
--
-- Check and Get the par_refno from parties
--
  IF (p1.liph_par_per_alt_ref IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',449);
-- 'Party Alternative(liph_par_per_alt_ref)must be supplied'
  ELSE
   OPEN  c_get_par_refno(p1.liph_par_per_alt_ref);
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
-- Get the app_refno from applications
--
  IF (p1.liph_app_legacy_ref IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',451);
-- 'Application Legacy Reference(liph_app_legacy_ref)must be supplied'
  ELSE
   OPEN  c_get_app_refno(p1.liph_app_legacy_ref);
   FETCH c_get_app_refno INTO l_app_refno;
   CLOSE c_get_app_refno;
   IF (l_app_refno IS NULL)
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',452);
-- 'Application cannot be found using Legacy Reference Supplied'
   END IF;
  END IF;  
--
-- Get the ipa_refno from involved parties use ipa_legacy_ref if supplied
-- first if not found then use start date instead but less accurate as this
-- will not match if start dates not sync'd correctly or changed
--
  IF(p1.liph_ipa_legacy_ref IS NOT NULL OR p1.liph_ipa_start_date IS NOT NULL)
   THEN
--
    IF(    l_app_refno            IS NOT NULL
       AND l_par_refno            IS NOT NULL
       AND p1.liph_ipa_legacy_ref IS NOT NULL )
     THEN
      OPEN  c_get_ipa_refno2(l_app_refno,l_par_refno,p1.liph_ipa_legacy_ref);
      FETCH c_get_ipa_refno2 INTO l_ipa_refno;
      CLOSE c_get_ipa_refno2;
--
      IF (l_ipa_refno IS NULL AND p1.liph_ipa_start_date IS NOT NULL )
       THEN
        OPEN  c_get_ipa_refno(l_app_refno, l_par_refno, p1.liph_ipa_start_date);
        FETCH c_get_ipa_refno INTO l_ipa_refno;
        CLOSE c_get_ipa_refno;
      END IF;
--
    ELSE
--
     IF(   l_app_refno            IS NOT NULL
       AND l_par_refno            IS NOT NULL
       AND p1.liph_ipa_start_date IS NOT NULL )
      THEN
       OPEN  c_get_ipa_refno(l_app_refno, l_par_refno, p1.liph_ipa_start_date);
       FETCH c_get_ipa_refno INTO l_ipa_refno;
       CLOSE c_get_ipa_refno;
     END IF;
--
    END IF;
--
    IF (l_ipa_refno IS NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',453);
-- 'Involved Party cannot be found using Party Legacy References IPA start date or Supplied'
    END IF;
--
  END IF;
--
-- The start date should not be earlier the application
-- received date
  l_rec_date := s_dl_hat_utils.f_app_received(p1.liph_app_legacy_ref);
  IF  l_rec_date IS NOT NULL
   THEN
     IF (p1.liph_start_date < l_rec_date)
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',454);
-- 'Application Start Date cannot be greater than involved party start date'
     END IF;
  END IF;
--
-- Check the Y/N columns are valid
--
-- Main applicant indicator
--
  IF (p1.liph_main_applicant_ind NOT IN ('Y','N'))
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',455);
-- 'Main Applicant Indicator must be Y or N'
  END IF;
--
-- Joint applicant (tenant) indicator
--
  IF (p1.liph_joint_appl_ind NOT IN ('Y','N'))
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',456);
-- 'Joint Applicant Indicator must be Y or N'
  END IF;
--
-- Living apart indicator
--
  IF (p1.liph_living_apart_ind NOT IN ('Y','N'))
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',457);
-- 'Living Apart Indicator must be Y or N'
  END IF;
--
-- Rehouse indicator
--
  IF (p1.liph_rehouse_ind NOT IN ('Y','N'))
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',458);
-- 'Rehousing Indicator must be Y or N'
  END IF;
--
-- Checks specific to main applicant indicator
--
  IF (p1.liph_main_applicant_ind = 'Y')
   THEN
--
    IF (p1.liph_joint_appl_ind != 'Y')
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',459);
-- 'Joint applicant indicator must be Y for main applicant'
    END IF;
--
    IF (p1.liph_living_apart_ind != 'N')
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',460);
-- 'Living apart indicator must be N for main applicant'
    END IF;
--
    IF (p1.liph_rehouse_ind != 'Y')
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',461);
-- 'Rehouse indicator must be Y for main applicant'
    END IF;
--
  END IF;
--
-- Check the reference values
--
-- Relationship to main applicant
--
  IF (p1.liph_frv_relation IS NOT NULL)
   THEN
    IF (NOT s_dl_hem_utils.exists_frv('RELATION',p1.liph_frv_relation))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',462);
-- 'Invalid relationship code'
    END IF;
  END IF;
--
-- Check that history record is not a duplicate
--
  OPEN  c_chk_iph_dup( l_ipa_refno
                      ,l_par_refno
                      ,l_app_refno
                      ,p1.liph_modified_date
                      ,p1.liph_modified_by
                      ,p1.liph_action_ind
                      ,p1.liph_joint_appl_ind
                      ,p1.liph_living_apart_ind
                      ,p1.liph_rehouse_ind
                      ,p1.liph_main_applicant_ind
                      ,p1.liph_created_by
                      ,p1.liph_created_date
                      ,p1.liph_start_date
                      ,p1.liph_end_date
                      ,p1.liph_groupno
                      ,p1.liph_act_roomno
                      ,p1.liph_frv_end_reason
                      ,p1.liph_frv_relation);
       FETCH c_chk_iph_dup INTO l_chk_dup;
       CLOSE c_chk_iph_dup;
  IF (l_chk_dup IS NOT NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',463);
-- 'Duplicate History record supplied'
  END IF;
--
-- Check iph end and start dates and end reason
--
  IF (nvl(p1.liph_end_date,p1.liph_start_date) < p1.liph_start_date)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',464);
-- 'End Date is earlier than Start Date supplied'
  END IF;
--
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
     rowid rec_rowid,
     liph_dlb_batch_id,
     liph_dl_seqno,
     liph_dl_load_status,
     liph_modified_date,
     liph_modified_by,
     liph_action_ind,
     liph_joint_appl_ind,
     liph_living_apart_ind,
     liph_rehouse_ind,
     liph_main_applicant_ind,
     liph_created_by,
     liph_created_date,
     liph_start_date,
     liph_end_date,
     liph_groupno,
     liph_act_roomno,
     liph_frv_end_reason,
     liph_frv_relation,
     liph_ipa_refno,
     liph_app_refno,
     liph_par_refno
FROM  dl_hat_involved_party_hist
WHERE liph_dlb_batch_id   = p_batch_id
AND   liph_dl_load_status = 'C';
--
-- ********************************
i INTEGER := 0;
l_an_tab  VARCHAR2(1);
--
-- *******************************
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAT_INVOLVED_PARTY_HIST';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hat_involved_party_hist.dataload_delete');
fsc_utils.debug_message( 's_dl_hat_involved_party_hist.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.liph_dl_seqno;
  i  := i +1;
  l_id := p1.rec_rowid;
--
-- delete record created so needs to match all fields
--
   DELETE FROM involved_party_history
   WHERE IPH_IPA_REFNO = p1.liph_ipa_refno
     AND IPH_PAR_REFNO = p1.liph_par_refno
     AND IPH_APP_REFNO = p1.liph_app_refno
     AND IPH_MODIFIED_DATE = p1.liph_modified_date
     AND IPH_MODIFIED_BY = p1.liph_modified_by
     AND IPH_ACTION_IND = p1.liph_action_ind
     AND IPH_JOINT_APPL_IND = p1.liph_joint_appl_ind
     AND IPH_LIVING_APART_IND = p1.liph_living_apart_ind
     AND IPH_REHOUSE_IND = p1.liph_rehouse_ind
     AND IPH_MAIN_APPLICANT_IND = p1.liph_main_applicant_ind
     AND IPH_CREATED_BY = p1.liph_created_by
     AND IPH_CREATED_DATE = p1.liph_created_date
     AND IPH_START_DATE = p1.liph_start_date
     AND NVL(IPH_END_DATE,TRUNC(SYSDATE)) = nvl(p1.liph_end_date,TRUNC(SYSDATE))
     AND NVL(IPH_GROUPNO,'99999999') = NVL(p1.liph_groupno,'99999999')
     AND NVL(IPH_ACT_ROOMNO,'ZZZ') = NVL(p1.liph_act_roomno,'ZZZ')
     AND NVL(IPH_FRV_END_REASON,'ZZZZZZZZZZ') = NVL(p1.liph_frv_end_reason,'ZZZZZZZZZZ')
     AND NVL(IPH_FRV_RELATION,'ZZZZZZZZZZ') = NVL(p1.liph_frv_relation,'ZZZZZZZZZZ');
--
-- remove liph_ipa_refno liph_par_refno and liph_app_refno
-- from data load tables
--
-- ***********************************
-- keep a count of the rows processed and commit after every 1000
--
  i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
  s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
  set_record_status_flag(l_id,'V');
--
   UPDATE dl_hat_involved_party_hist
   SET   liph_ipa_refno = null
   ,     liph_app_refno = null
   ,     liph_par_refno = null
   WHERE liph_dlb_batch_id = cb
     AND liph_dl_seqno = cs
     AND rowid = l_id;
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
-- ***********************************
-- Section to analyse the tables populated with this dataload
--
l_an_tab := s_dl_hem_utils.dl_comp_stats('INVOLVED_PARTY_HISTORY');
l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HAT_INVOLVED_PARTY_HIST');
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
END s_dl_hat_involved_party_hist;
/

show errors

