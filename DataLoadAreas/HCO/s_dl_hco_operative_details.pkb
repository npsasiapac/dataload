CREATE OR REPLACE PACKAGE BODY s_dl_hco_operative_details
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Vers  WHO  WHEN        WHY
--      1.0          MJK  31/01/05    Dataload
--      1.1 5.10.0   PH   27/07/06    Corrected Delete and added savepoints
--      1.2 5.10.0   PH   08/08/06    Corrected compilation errors
--      1.3 5.10.0   PH   15/08/06    Added odet_start_date to insert. Added
--                                    validate on operative_type_grades
--      2.0 5.13.0   PH   06-FEB-2008 Now includes its own 
--                                    set_record_status_flag procedure.
--      2.1 5.15.1   PH   15-FEb-2010 Added validate on start/end
--                                    location ind.
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
  UPDATE dl_hco_operative_details
  SET lodet_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hco_operative_details');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
--
--  declare package variables AND constants
--
--
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) is
SELECT rowid rec_rowid
,      lodet_dlb_batch_id
,      lodet_dl_seqno
,      lodet_dl_load_status
,      lodet_ipp_shortname
,      lodet_start_date
,      lodet_otgr_ipt_code
,      lodet_otgr_hrv_gra_code
,      lodet_default_start_locn_ind
,      lodet_default_end_locn_ind
,      lodet_max_wkly_std_working_tim
,      lodet_max_wkly_overtime
,      lodet_start_unpaid_trav_time
,      lodet_end_unpaid_trav_time
,      lodet_hourly_rate
,      lodet_overtime_rate
,      lodet_max_travel_time
,      lodet_cost_per_km
,      lodet_refno
FROM   dl_hco_operative_details
WHERE  lodet_dlb_batch_id    = p_batch_id
AND    lodet_dl_load_status = 'V';
--
CURSOR c2 (cp_ipp_shortname VARCHAR2
          ,cp_ipt_code      VARCHAR2
          ) IS
SELECT ipp_refno
FROM   interested_parties
WHERE  ipp_shortname = cp_ipp_shortname
AND    ipp_ipt_code = cp_ipt_code;
r2 c2%ROWTYPE;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HCO_OPERATIVE_DETAILS';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
l_id     ROWID;
--
-- Other variables
--
i            INTEGER := 0;
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_operative_details.dataload_create');
  fsc_utils.debug_message( 's_dl_hco_operative_details.dataload_create',3);
--
  cb := p_batch_id;
  cd := p_date;
--
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 in c1(p_batch_id)
  LOOP
--
    BEGIN
--
cs := p1.lodet_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
      OPEN c2 (p1.lodet_ipp_shortname,p1.lodet_otgr_ipt_code);
      FETCH c2 INTO r2;
      CLOSE c2;
--
      -- Create operative_details record
      --
      INSERT INTO operative_details
      (odet_refno
      ,odet_ipp_refno
      ,odet_start_date
      ,odet_otgr_ipt_code
      ,odet_otgr_hrv_gra_code
      ,odet_default_start_locn_ind
      ,odet_default_end_locn_ind
      ,odet_max_wkly_std_working_time
      ,odet_max_wkly_overtime
      ,odet_start_unpaid_trav_time
      ,odet_end_unpaid_trav_time
      ,odet_created_by
      ,odet_created_date
      ,odet_hourly_rate
      ,odet_overtime_rate
      ,odet_max_travel_time
      ,odet_cost_per_km
      )
      VALUES
      (p1.lodet_refno
      ,r2.ipp_refno
      ,p1.lodet_start_date
      ,p1.lodet_otgr_ipt_code
      ,p1.lodet_otgr_hrv_gra_code
      ,p1.lodet_default_start_locn_ind
      ,p1.lodet_default_end_locn_ind
      ,p1.lodet_max_wkly_std_working_tim
      ,p1.lodet_max_wkly_overtime
      ,p1.lodet_start_unpaid_trav_time
      ,p1.lodet_end_unpaid_trav_time
      ,'DATALOAD'
      ,sysdate
      ,p1.lodet_hourly_rate
      ,p1.lodet_overtime_rate
      ,p1.lodet_max_travel_time
      ,p1.lodet_cost_per_km
      );
      --
      -- keep a count of the rows processed and commit after every 1000
      --
      i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
      --
      s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
      set_record_status_flag(l_id,'C');
      --
    EXCEPTION
    WHEN OTHERS 
    THEN
      ROLLBACK TO SP1;
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      set_record_status_flag(l_id,'O');
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
    END;
--
  END LOOP;
--
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('OPERATIVE_DETAILS');
--
  fsc_utils.proc_end;
--
  commit;
--
EXCEPTION
WHEN OTHERS 
THEN
  s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
  s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
--
END dataload_create;
--
--
-- As defined in FUNCTION H400.60.10.40.10.20
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT rowid rec_rowid
,      lodet_dlb_batch_id
,      lodet_dl_seqno
,      lodet_dl_load_status
,      lodet_ipp_shortname
,      lodet_otgr_ipt_code
,      lodet_otgr_hrv_gra_code
,      lodet_default_start_locn_ind
,      lodet_default_end_locn_ind
,      lodet_max_wkly_std_working_tim
,      lodet_max_wkly_overtime
,      lodet_start_unpaid_trav_time
,      lodet_end_unpaid_trav_time
,      lodet_hourly_rate
,      lodet_overtime_rate
,      lodet_max_travel_time
,      lodet_cost_per_km
FROM   dl_hco_operative_details
WHERE  lodet_dlb_batch_id    = p_batch_id
AND    lodet_dl_load_status in ('L','F','O');
--
--
CURSOR c2(p_ipp_shortname VARCHAR2
         ,p_ipt_code      VARCHAR2
         ) IS
SELECT ipp_refno
FROM   interested_parties
WHERE  ipp_shortname = p_ipp_shortname
AND    ipp_ipt_code  = p_ipt_code;
r2 c2%ROWTYPE;
--
CURSOR c_get_otgr(p_ipt_code      VARCHAR2
                 ,p_hrv_gra_code  VARCHAR2)  IS
SELECT 'X'
FROM   operative_type_grades
WHERE  otgr_ipt_code      =  p_ipt_code
AND    otgr_hrv_gra_code  =  p_hrv_gra_code;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HCO_OPERATIVE_DETAILS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_link1          VARCHAR2(1);
l_link2          VARCHAR2(1);
l_parent_type    VARCHAR2(1);
l_grandchild     VARCHAR2(1);
--
-- Other variables
--
l_dummy             VARCHAR2(2000);
l_date              DATE;
l_is_inactive       BOOLEAN DEFAULT FALSE;
l_exists            VARCHAR2(1);
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_operative_details.dataload_validate');
  fsc_utils.debug_message( 's_dl_hco_operative_details.dataload_validate',3);
--
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 IN c1
  LOOP
--
    BEGIN
--
      cs := p1.lodet_dl_seqno;
      l_errors := 'V';
      l_error_ind := 'N';
      l_exists    := null;
      l_id := p1.rec_rowid;
      --
      -- Validate interested party type exists
      --
      IF p1.lodet_otgr_ipt_code IS NOT NULL
      THEN
        l_dummy := s_interested_party_types.get_ipt_description(p1.lodet_otgr_ipt_code);
        IF l_dummy IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',153);
        END IF;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',153);
      END IF;
      --
      -- Validate interested party exists
      --
      IF p1.lodet_ipp_shortname IS NOT NULL
      AND p1.lodet_otgr_ipt_code IS NOT NULL
      THEN
        OPEN c2(p1.lodet_ipp_shortname,p1.lodet_otgr_ipt_code);
        FETCH c2 INTO r2;
        IF c2%NOTFOUND
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',190);
        END IF;
        CLOSE c2;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',190);
      END IF;
      --
      -- Validate grade code exists
      --
      IF p1.lodet_otgr_hrv_gra_code IS NOT NULL
      THEN
        IF NOT s_hrv_grades.check_grade(p1.lodet_otgr_hrv_gra_code)
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',154);
        END IF;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',154);
      END IF;
      --
      -- Check entry exists on operative_type_grades (OTGR_FK Constraint)
      --
      OPEN c_get_otgr(p1.lodet_otgr_ipt_code, p1.lodet_otgr_hrv_gra_code);
       FETCH c_get_otgr INTO l_exists;
        IF c_get_otgr%NOTFOUND
         THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',160);
        END IF;
      CLOSE c_get_otgr;
      --
      -- Validate max_wkly_std_working_time exists and is in correct format
      --
      IF p1.lodet_max_wkly_std_working_tim IS NOT NULL
      THEN
        IF LENGTH(p1.lodet_max_wkly_std_working_tim) != 5
        OR SUBSTR(p1.lodet_max_wkly_std_working_tim,3,1) != ':'
        OR SUBSTR(p1.lodet_max_wkly_std_working_tim,1,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')
        OR SUBSTR(p1.lodet_max_wkly_std_working_tim,2,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')
        OR SUBSTR(p1.lodet_max_wkly_std_working_tim,3,1) != ':'
        OR SUBSTR(p1.lodet_max_wkly_std_working_tim,4,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')
        OR SUBSTR(p1.lodet_max_wkly_std_working_tim,5,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')
        OR TO_NUMBER(SUBSTR(p1.lodet_max_wkly_std_working_tim,4,2)) > 59
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',155);
        END IF;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',155);
      END IF;
      --
      -- Validate max_wkly_overtime exists and is in correct format
      --
      IF p1.lodet_max_wkly_overtime IS NOT NULL
      THEN
        IF LENGTH(p1.lodet_max_wkly_overtime) != 5
        OR SUBSTR(p1.lodet_max_wkly_overtime,1,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')
        OR SUBSTR(p1.lodet_max_wkly_overtime,2,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')
        OR SUBSTR(p1.lodet_max_wkly_overtime,3,1) != ':'
        OR SUBSTR(p1.lodet_max_wkly_overtime,4,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')
        OR SUBSTR(p1.lodet_max_wkly_overtime,5,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')
        OR TO_NUMBER(SUBSTR(p1.lodet_max_wkly_overtime,4,2)) > 59
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',156);
        END IF;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',156);
      END IF;
      --
      -- Validate the Start and End Location Ind
      --
         IF nvl(p1.lodet_default_start_locn_ind, '~') NOT IN ( 'H', 'D' )
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',706);
          END IF;
      --
         IF nvl(p1.lodet_default_end_locn_ind, '~') NOT IN ( 'H', 'D' )
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',707);
          END IF;
      --
      -- Validate default_hourly_rate
      --
      --
      -- Validate default_overtime_rate
      --
      --
      -- Validate default_overtime_rate
      --
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
      s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
      set_record_status_flag(l_id,l_errors);
      --
    EXCEPTION
      WHEN OTHERS THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
    --
    END;
  --
  END LOOP;
  --
  COMMIT;
  --
  fsc_utils.proc_END;
--
EXCEPTION
WHEN OTHERS
THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
CURSOR c1 is
SELECT
   rowid rec_rowid
  ,lodet_dlb_batch_id
  ,lodet_dl_seqno
  ,lodet_dl_load_status
  ,lodet_refno
FROM  dl_hco_operative_details
WHERE lodet_dlb_batch_id   = p_batch_id
AND   lodet_dl_load_status = 'C';
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HCO_OPERATIVE_DETAILS';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
l_id     ROWID;
--
i        INTEGER := 0;
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_operative_details.dataload_delete');
  fsc_utils.debug_message( 's_dl_hco_operative_details.dataload_delete',3 );
--
  cp := p_batch_id;
  cd := p_date;
--
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 IN c1 LOOP
--
BEGIN
--
    cs := p1.lodet_dl_seqno;
    l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
    DELETE FROM operative_details
    WHERE  odet_refno = p1.lodet_refno;
    --
    s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
    set_record_status_flag(l_id,'V');
    --
    -- keep a count of the rows processed and commit after every 1000
    --
    i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
    --
--
EXCEPTION
WHEN OTHERS 
THEN
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'C');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
--
  END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('OPERATIVE_DETAILS');
--
  fsc_utils.proc_end;
--
  commit;
--
  EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_delete;
--
--
--
END s_dl_hco_operative_details;
/
