CREATE OR REPLACE PACKAGE BODY s_dl_hco_operative_type_grades
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Vers WHO  WHEN       WHY
--      1.0          MJK  31/01/05   Dataload
--      1.1 5.10.0   PH   27/07/06   Corrected Delete and added savepoints
--      1.2 5.10.0   PH   16/08/06   Removed validate on created by/date
--      2.0 5.13.0   PH   06-FEB-2008 Now includes its own 
--                                    set_record_status_flag procedure.
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
  UPDATE dl_hco_operative_type_grades
  SET lotgr_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hco_operative_type_grades');
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
,      lotgr_dlb_batch_id
,      lotgr_dl_seqno
,      lotgr_dl_load_status
,      lotgr_ipt_code
,      lotgr_hrv_gra_code
,      lotgr_current_ind
,      lotgr_max_wkly_std_working_tim
,      lotgr_max_wkly_overtime
,      lotgr_default_hourly_rate
,      lotgr_default_overtime_rate
FROM   dl_hco_operative_type_grades
WHERE  lotgr_dlb_batch_id    = p_batch_id
AND    lotgr_dl_load_status = 'V';
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HCO_OPERATIVE_TYPE_GRADES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
i            INTEGER := 0;
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_operative_type_grades.dataload_create');
  fsc_utils.debug_message( 's_dl_hco_operative_type_grades.dataload_create',3);
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
      cs := p1.lotgr_dl_seqno;
      l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
      -- Create operative_type_grades record
      --
      INSERT INTO operative_type_grades 
      (otgr_ipt_code
      ,otgr_hrv_gra_code
      ,otgr_current_ind
      ,otgr_max_wkly_std_working_time
      ,otgr_max_wkly_overtime
      ,otgr_created_by
      ,otgr_created_date
      ,otgr_default_hourly_rate
      ,otgr_default_overtime_rate
      )
      VALUES
      (p1.lotgr_ipt_code
      ,p1.lotgr_hrv_gra_code
      ,p1.lotgr_current_ind
      ,p1.lotgr_max_wkly_std_working_tim
      ,p1.lotgr_max_wkly_overtime
      ,'DATALOAD'
      ,sysdate
      ,p1.lotgr_default_hourly_rate
      ,p1.lotgr_default_overtime_rate
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('OPERATIVE_TYPE_GRADES');
--
  fsc_utils.proc_end;
  commit;
--
EXCEPTION
WHEN OTHERS 
THEN
  set_record_status_flag(l_id,'O');
  s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
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
,      lotgr_dlb_batch_id
,      lotgr_dl_seqno
,      lotgr_dl_load_status
,      lotgr_ipt_code
,      lotgr_hrv_gra_code
,      lotgr_current_ind
,      lotgr_max_wkly_std_working_tim
,      lotgr_max_wkly_overtime
,      lotgr_default_hourly_rate
,      lotgr_default_overtime_rate
FROM   dl_hco_operative_type_grades
WHERE  lotgr_dlb_batch_id    = p_batch_id
AND    lotgr_dl_load_status in ('L','F','O');
--
--
CURSOR c2 (p_ipt_code       VARCHAR2
          ,p_hrv_grade_code VARCHAR2
          ) IS
SELECT otgr_ipt_code     ipt_code
FROM   operative_type_grades
WHERE  otgr_ipt_code = p_ipt_code
AND    otgr_hrv_gra_code = p_hrv_grade_code
UNION ALL
SELECT lotgr_ipt_code
FROM   dl_hco_operative_type_grades
WHERE  lotgr_dlb_batch_id    = p_batch_id
AND    lotgr_dl_load_status  = 'V'
AND    lotgr_ipt_code        = p_ipt_code
AND    lotgr_hrv_gra_code    = p_hrv_grade_code;
r2 c2%ROWTYPE;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HCO_OPERATIVE_TYPE_GRADES';
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
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_operative_type_grades.dataload_validate');
  fsc_utils.debug_message( 's_dl_hco_operative_type_grades.dataload_validate',3);
--
  cb := p_batch_id;
  cd := p_date;
--
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 IN c1 
  LOOP
--
    BEGIN
--
      cs := p1.lotgr_dl_seqno;
      l_errors := 'V';
      l_error_ind := 'N';
      l_id := p1.rec_rowid;
      --  
      -- Validate interested party type exists
      --
      IF p1.lotgr_ipt_code IS NOT NULL
      THEN
        l_dummy := s_interested_party_types.get_ipt_description(p1.lotgr_ipt_code);                      
        IF l_dummy IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',153);
        END IF;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',153);
      END IF;
      --  
      -- Validate grade code exists
      --
      IF p1.lotgr_hrv_gra_code IS NOT NULL
      THEN
        -- check for duplicates
        OPEN c2(p1.lotgr_ipt_code,p1.lotgr_hrv_gra_code);
        FETCH c2 into r2;
        IF c2%FOUND
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',220);
        END IF;
        CLOSE c2;
        IF NOT s_hrv_grades.check_grade(p1.lotgr_hrv_gra_code)                      
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',154);
        END IF;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',154);
      END IF;
      --  
      -- Validate current ind exists and is Y or N
      --
      IF p1.lotgr_current_ind IS NOT NULL
      THEN
        IF p1.lotgr_current_ind NOT IN ('Y','N')                      
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',159);
        END IF;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',159);
      END IF;
      --  
      -- Validate max_wkly_std_working_time exists and is in correct format
      --
      IF p1.lotgr_max_wkly_std_working_tim IS NOT NULL
      THEN
        IF LENGTH(p1.lotgr_max_wkly_std_working_tim) != 5
        OR SUBSTR(p1.lotgr_max_wkly_std_working_tim,3,1) != ':'
        OR SUBSTR(p1.lotgr_max_wkly_std_working_tim,1,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')           
        OR SUBSTR(p1.lotgr_max_wkly_std_working_tim,2,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')      
        OR SUBSTR(p1.lotgr_max_wkly_std_working_tim,3,1) != ':'
        OR SUBSTR(p1.lotgr_max_wkly_std_working_tim,4,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')    
        OR SUBSTR(p1.lotgr_max_wkly_std_working_tim,5,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')     
        OR TO_NUMBER(SUBSTR(p1.lotgr_max_wkly_std_working_tim,4,2)) > 59
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',155);
        END IF;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',155);
      END IF;
      --  
      -- Validate max_wkly_overtime exists and is in correct format
      --
      IF p1.lotgr_max_wkly_overtime IS NOT NULL
      THEN
        IF LENGTH(p1.lotgr_max_wkly_overtime) != 5
        OR SUBSTR(p1.lotgr_max_wkly_overtime,1,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')    
        OR SUBSTR(p1.lotgr_max_wkly_overtime,2,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')   
        OR SUBSTR(p1.lotgr_max_wkly_overtime,3,1) != ':'
        OR SUBSTR(p1.lotgr_max_wkly_overtime,4,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')    
        OR SUBSTR(p1.lotgr_max_wkly_overtime,5,1) NOT IN ('1','2','3','4','5','6','7','8','9','0')      
        OR TO_NUMBER(SUBSTR(p1.lotgr_max_wkly_overtime,4,2)) > 59
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',156);
        END IF;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',156);
      END IF;
      --  
      --  
      -- Validate default_hourly_rate
      --
      --
      -- Validate default_overtime_rate
      --
      --
      -- Validate default_overtime_rate
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
      --
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
END dataload_validate;
--
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
CURSOR c1 is
SELECT rowid rec_rowid
,      lotgr_dlb_batch_id
,      lotgr_dl_seqno
,      lotgr_dl_load_status
,      lotgr_ipt_code
,      lotgr_hrv_gra_code
FROM  dl_hco_operative_type_grades
WHERE lotgr_dlb_batch_id   = p_batch_id
AND   lotgr_dl_load_status = 'C';
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HCO_OPERATIVE_TYPE_GRADES';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
l_id     ROWID;
--
i        INTEGER := 0;
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_operative_type_grades.dataload_delete');
  fsc_utils.debug_message( 's_dl_hco_operative_type_grades.dataload_delete',3 );
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
    cs := p1.lotgr_dl_seqno;
    l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
    DELETE FROM operative_type_grades
    WHERE  otgr_ipt_code        = p1.lotgr_ipt_code
    AND    otgr_hrv_gra_code    = p1.lotgr_hrv_gra_code;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('OPERATIVE_TYPE_GRADES');
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
END s_dl_hco_operative_type_grades;
/
