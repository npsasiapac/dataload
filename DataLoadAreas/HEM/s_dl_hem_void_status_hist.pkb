CREATE OR REPLACE PACKAGE BODY s_dl_hem_void_status_hist
AS
-- *****************************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VER  DB VER   WHO  WHEN         WHY
--  1.0  6.14/5   AJ   06-FEB-2017  Initial Creation Bespoke Dataload for Void
--                                  Status History for Queensland Migration CR461
--  1.1  6.14/5   AJ   07-FEB-2017  Further additions and script ready for testing
--
-- 
-- *****************************************************************************
--
--
--  declare package variables and constants
--
--
-- *****************************************************************************
--
PROCEDURE set_record_status_flag
  (p_rowid  IN ROWID
  ,p_status IN VARCHAR2
  )
IS
BEGIN
    UPDATE dl_hem_void_status_hist
    SET    lvsh_dl_load_status = p_status
    WHERE  rowid = p_rowid;
EXCEPTION
WHEN OTHERS 
THEN
  dbms_output.put_line('Error updating status of dl_hem_void_status_hist');
  RAISE;
END set_record_status_flag;
--
-- *****************************************************************************
--
PROCEDURE dataload_create 
  (p_batch_id IN VARCHAR2
  ,p_date     IN DATE
  )
IS
CURSOR c1 
IS
SELECT rowid rec_rowid
,   lvsh_dlb_batch_id
,   lvsh_dl_seqno
,   lvsh_dl_load_status
,   lvsh_pro_propref
,   lvsh_vin_hps_start_date
,   lvsh_vin_hps_end_date
,   lvsh_vin_hps_hpc_code
,   lvsh_vin_vst_code
,   lvsh_vin_status_started
,   lvsh_modified_date
,   lvsh_username
,   lvsh_upd_vin_refno
FROM   dl_hem_void_status_hist
WHERE  lvsh_dlb_batch_id    = p_batch_id
AND    lvsh_dl_load_status = 'V';
    
CURSOR c_pro_refno(cp_propref VARCHAR2)IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = cp_propref;
--
CURSOR check_vin(cp_pro_refno NUMBER
                ,cp_vin_refno NUMBER)IS
SELECT 'X'
FROM   void_instances
WHERE  vin_refno = cp_vin_refno
AND    vin_pro_refno = cp_pro_refno;
--
-- Constants for process_summary
--
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'CREATE';
ct          VARCHAR2(30) := 'DL_HEM_VOID_STATUS_HIST';
cs          INTEGER;
ce          VARCHAR2(200);
l_id        ROWID;
i           INTEGER := 0;
l_an_tab    VARCHAR2(1);
--
-- Other variables
--
l_pro_refno  NUMBER(10);
l_vin_refno  NUMBER(8);
l_vin_exists VARCHAR2(1);
  
BEGIN
  fsc_utils.proc_start('s_dl_hem_void_status_hist.dataload_create');
  fsc_utils.debug_message( 's_dl_hem_void_status_hist.dataload_create',3);
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 in c1 LOOP
    BEGIN
      cs := p1.lvsh_dl_seqno;
      l_id := p1.rec_rowid;
      
    SAVEPOINT SP1;
        
      l_pro_refno  := NULL;
      l_vin_refno  := NULL;
      l_vin_exists := NULL;
--
-- get the pro_refno
--
      OPEN c_pro_refno(p1.lvsh_pro_propref);
      FETCH c_pro_refno INTO l_pro_refno;
      CLOSE c_pro_refno;
--
-- Just check again before insert just in case void instance
-- found by validate no longer exists for property as this point
--
      OPEN check_vin (l_pro_refno, p1.lvsh_upd_vin_refno);
      FETCH check_vin INTO l_vin_exists;
      CLOSE check_vin;
--	  
      IF l_vin_exists IS NOT NULL
      THEN
       INSERT INTO VOID_STATUS_HIST
        (
		 vsh_vin_refno
        ,vsh_vin_vst_code
        ,vsh_vin_status_started
        ,vsh_modified_by
        ,vsh_modified_date
        )
       VALUES
        (
		 p1.lvsh_upd_vin_refno
        ,p1.lvsh_vin_vst_code
        ,p1.lvsh_vin_status_started
        ,DECODE(p1.lvsh_username,NULL,'DATALOAD',p1.lvsh_username)
        ,p1.lvsh_modified_date
		);        
      END IF;
--
-- keep a count of the rows processed and commit after every 5000
--
     i := i+1;
     IF MOD(i,5000)=0 
     THEN
       COMMIT;
     END IF;
     s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
     set_record_status_flag(l_id,'C');
     EXCEPTION
     WHEN OTHERS 
     THEN
       ROLLBACK TO SP1;
       ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
       s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
       s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
     END; 
  END LOOP;
  --
  -- Section to analyse the table(s) populated by this dataload
  --
  l_an_tab:=s_dl_hem_utils.dl_comp_stats('VOID_STATUS_HIST');
  fsc_utils.proc_end;
  COMMIT;  
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
    RAISE;
 END dataload_create;
--
-- *****************************************************************************
--
PROCEDURE dataload_validate 
   (p_batch_id  IN VARCHAR2
   ,p_date      IN DATE
   )
 IS
--
CURSOR c1(cp_batch_id  VARCHAR2) 
IS
SELECT rowid rec_rowid
,   lvsh_dlb_batch_id
,   lvsh_dl_seqno
,   lvsh_dl_load_status
,   lvsh_pro_propref
,   lvsh_vin_hps_start_date
,   lvsh_vin_hps_end_date
,   lvsh_vin_hps_hpc_code
,   lvsh_vin_vst_code
,   lvsh_vin_status_started
,   lvsh_modified_date
,   lvsh_username
,   lvsh_upd_vin_refno
FROM   dl_hem_void_status_hist
WHERE  lvsh_dlb_batch_id    = cp_batch_id
AND    lvsh_dl_load_status IN ('L','F','O');
-- PRO REFNO    
CURSOR c_get_prop (cp_propref VARCHAR2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = cp_propref;
--VST
CURSOR c_vst (p_vst VARCHAR2)IS
SELECT vst_code
FROM   void_statuses
WHERE  vst_code = p_vst;
--VIN REFNO
CURSOR c_get_hps_vin (p_pro_refno  NUMBER
                     ,p_start_date DATE
                     ,p_hpc_code   VARCHAR2) IS
SELECT TO_NUMBER(SUBSTR(hps_comments,14,INSTR(hps_comments,')',1,1) -14)) hps_vin_refno
FROM   hou_prop_statuses
WHERE  hps_pro_refno = p_pro_refno
AND    trunc(hps_start_date) = p_start_date
AND    hps_hpc_code = p_hpc_code
AND    hps_comments LIKE '(VIN REFNO = %';
--
CURSOR check_vinprop(cp_pro_refno  NUMBER
                    ,cp_vin_refno  NUMBER)IS
SELECT 'X'
FROM   void_instances
WHERE  vin_refno = cp_vin_refno
AND    vin_pro_refno = cp_pro_refno;
--
CURSOR check_vin(cp_vin_refno  NUMBER)IS
SELECT 'X'
FROM   void_instances
WHERE  vin_refno = cp_vin_refno;
--
CURSOR chk_notcurstatus(cp_vin_refno  NUMBER
                       ,cp_vst_code   VARCHAR2
                       ,cp_start_date DATE
                       ,cp_pro_refno  NUMBER)IS
SELECT 'X'
FROM   void_instances
WHERE  vin_refno = cp_vin_refno
AND    vin_pro_refno = cp_pro_refno
AND    trunc(vin_status_start) = cp_start_date
AND    vin_vst_code = cp_vst_code;
--
CURSOR chk_vsh(cp_vin_refno  NUMBER
              ,cp_vst_code   VARCHAR2
              ,cp_start_date DATE
              ,cp_mod_date   DATE    )IS
SELECT 'X'
FROM   void_status_hist
WHERE  vsh_vin_refno = cp_vin_refno
AND    trunc(vsh_modified_date) = cp_mod_date
AND    trunc(vsh_vin_status_started) = cp_start_date
AND    vsh_vin_vst_code = cp_vst_code;
--   
-- Constants for process_summary
--
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'VALIDATE';
ct          VARCHAR2(30) := 'DL_HEM_VOID_STATUS_HIST';
cs          INTEGER;
ce          VARCHAR2(200);
l_id        ROWID;
i           INTEGER := 0;
l_an_tab    VARCHAR2(1);
--
-- Other variables
--
l_pro_refno        NUMBER(10);
l_errors           VARCHAR2(1);
l_error_ind        VARCHAR2(1);
l_vst              VARCHAR2(4);
l_vin_refno        NUMBER(8);
l_vin_exists       VARCHAR2(1);
l_vinprop_exists   VARCHAR2(1);
l_vinvststatus     VARCHAR2(1);
l_vsh_exists       VARCHAR2(1);
--
BEGIN
  
  fsc_utils.proc_start('s_dl_hem_void_status_hist.dataload_validate');
  fsc_utils.debug_message( 's_dl_hem_void_status_hist.dataload_validate',3);
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--   
  FOR p1 in c1(p_batch_id) LOOP   
    BEGIN
        cs   := p1.lvsh_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors    := 'V';
        l_error_ind := 'N';
        l_pro_refno := NULL;
        l_vst       := NULL;
        l_vin_refno := NULL;
        l_vin_exists := NULL;
        l_vinprop_exists := NULL;
        l_vinvststatus := NULL;
        l_vsh_exists := NULL;
    --
    -- Check that the property supplied exists and get the pro_refno if it does
    --
    IF p1.lvsh_pro_propref IS NULL 
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',364);
    ELSE
      OPEN c_get_prop(p1.lvsh_pro_propref);
      FETCH c_get_prop INTO l_pro_refno;
      IF c_get_prop%NOTFOUND 
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',30);
      END IF;
      CLOSE c_get_prop;      
    END IF;
    --VST CODE
    IF p1.lvsh_vin_vst_code IS NULL 
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',905);
    ELSE
     OPEN  c_vst (p1.lvsh_vin_vst_code);
      FETCH c_vst into l_vst;
      CLOSE c_vst;

      IF l_vst IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',359);
      END IF;
    END IF;
    --
    -- Check that the linked property status record exits and find the referenced void instance
    -- from hps_comments field
    --
    IF p1.lvsh_vin_hps_start_date is NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',125);
    END IF;
    --
    IF p1.lvsh_vin_hps_hpc_code is NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',126);
    END IF;
    --
    IF p1.lvsh_vin_hps_end_date is NOT NULL
     THEN
      IF p1.lvsh_vin_hps_end_date < p1.lvsh_vin_hps_start_date
        THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',127);
      END IF;
    END IF;
    --
    IF (p1.lvsh_vin_hps_start_date is NOT NULL  AND
        p1.lvsh_vin_hps_hpc_code is NOT NULL    AND
        l_pro_refno is NOT NULL                    )
     THEN
    --
      OPEN c_get_hps_vin(l_pro_refno,p1.lvsh_vin_hps_start_date,p1.lvsh_vin_hps_hpc_code);
      FETCH c_get_hps_vin INTO l_vin_refno;
      CLOSE c_get_hps_vin;	
    --	
      IF l_vin_refno IS NULL
       THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',945);
      END IF;
    --
    -- If l_vin_refno returned then check it actually is valid
    --
      IF l_vin_refno IS NOT NULL
       THEN
        OPEN check_vin (l_vin_refno);
        FETCH check_vin INTO l_vin_exists;
        CLOSE check_vin;
    --		
        OPEN check_vinprop (l_pro_refno, l_vin_refno);
        FETCH check_vinprop INTO l_vinprop_exists;
        CLOSE check_vinprop;
    --	
        IF l_vin_exists is NULL
         THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',128);
        END IF;
    --
        IF l_vinprop_exists is NULL
         THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',129);
        END IF;
    --
    --
    -- If vin refno returned passes checks then update the data load table with it
    --
        UPDATE dl_hem_void_status_hist
        SET    lvsh_upd_vin_refno = l_vin_refno
        WHERE  ROWID = p1.rec_rowid;
    --
      END IF;
    END IF;
    --
    IF (l_vin_refno is NOT NULL                AND
        p1.lvsh_vin_vst_code  IS NOT NULL      AND
        p1.lvsh_vin_status_started is NOT NULL AND
        l_pro_refno is NOT NULL                    )
     THEN
    --
      OPEN chk_notcurstatus(l_vin_refno
                           ,p1.lvsh_vin_vst_code
                           ,p1.lvsh_vin_status_started
                           ,l_pro_refno);
      FETCH chk_notcurstatus INTO l_vinvststatus;
      CLOSE chk_notcurstatus;	
    --	
      IF l_vinvststatus IS NOT NULL
       THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',130);
      END IF;
    END IF;	  
    --
    IF (l_vin_refno IS NOT NULL                AND
        p1.lvsh_vin_vst_code  IS NOT NULL      AND
        p1.lvsh_vin_status_started is NOT NULL AND
        l_pro_refno IS NOT NULL                AND
        p1.lvsh_modified_date IS NOT NULL          )
     THEN
    --
      OPEN chk_vsh(l_vin_refno
                  ,p1.lvsh_vin_vst_code
                  ,p1.lvsh_vin_status_started
                  ,p1.lvsh_modified_date);
      FETCH chk_vsh INTO l_vsh_exists;
      CLOSE chk_vsh;	
    --	
      IF l_vsh_exists IS NOT NULL
       THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',131);
      END IF;
    END IF;	  
    --
    -- Mandatory Fields not already checked
    --
    IF p1.lvsh_vin_status_started is NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',123);
    END IF;
    --
    IF p1.lvsh_modified_date is NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',124);
    END IF;
    --
    -- Now UPDATE the record count AND error code
    --
    IF l_errors = 'F' 
    THEN
      l_error_ind := 'Y';
    ELSE
      l_error_ind := 'N';
    END IF;
    --
    -- keep a count of the rows processed and commit after every 1000
    --
    i := i+1; 
    IF MOD(i,1000) = 0
    THEN 
      COMMIT; 
    END IF;
    s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
    set_record_status_flag(l_id,l_errors);
    EXCEPTION
    WHEN OTHERS 
    THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
    END; 
  END LOOP;
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HEM_VOID_STATUS_HIST');
fsc_utils.proc_end;
COMMIT;  
EXCEPTION
WHEN OTHERS 
THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
END dataload_validate;
--
-- *****************************************************************************
--
PROCEDURE dataload_delete  
  (p_batch_id IN VARCHAR2
  ,p_date     IN DATE
  )
AS
CURSOR c1(cp_batch_id  VARCHAR2) IS
SELECT rowid rec_rowid
,       lvsh_dlb_batch_id
,       lvsh_dl_seqno
,       lvsh_dl_load_status
,       lvsh_vin_vst_code
,       lvsh_vin_status_started
,       DECODE(lvsh_username,NULL,'DATALOAD',lvsh_username) lvsh_username
,       lvsh_modified_date
,       lvsh_upd_vin_refno
FROM   dl_hem_void_status_hist
WHERE  lvsh_dlb_batch_id    = cp_batch_id
AND    lvsh_dl_load_status = 'C';
--
-- Constants for process_summary
--
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'DELETE';
ct          VARCHAR2(30) := 'DL_HEM_VOID_STATUS_HIST';
cs          INTEGER;
ce          VARCHAR2(200);
--
l_id        ROWID;
i           INTEGER := 0;
l_an_tab    VARCHAR2(1);
--
-- Other variables
--
BEGIN
  fsc_utils.proc_start('s_dl_hem_void_status_hist.dataload_delete');
  fsc_utils.debug_message( 's_dl_hem_void_status_hist.dataload_delete',3);
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  FOR p1 IN c1 (p_batch_id) 
  LOOP
    BEGIN
      cs := p1.lvsh_dl_seqno;
      l_id := p1.rec_rowid;
      SAVEPOINT SP1;
      DELETE FROM void_status_hist
      WHERE  vsh_vin_refno = p1.lvsh_upd_vin_refno
      AND    vsh_vin_vst_code = p1.lvsh_vin_vst_code
      AND    vsh_vin_status_started = p1.lvsh_vin_status_started
      AND    vsh_modified_by = p1.lvsh_username
      AND    vsh_modified_date = p1.lvsh_modified_date;
      --
      -- keep a count of the rows processed and commit after every 1000
      --
      i := i+1; 
      IF MOD(i,1000)=0 
      THEN 
        COMMIT; 
      END IF;
      s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
      set_record_status_flag(l_id,'V');
      EXCEPTION
      WHEN OTHERS 
      THEN
        ROLLBACK TO SP1;
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
        set_record_status_flag(l_id,'C');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
    END;
  END LOOP;
  COMMIT;
  --
  -- Section to analyse the table(s) populated by this data load
  --
  l_an_tab:=s_dl_hem_utils.dl_comp_stats('VOID_STATUS_HIST');
  l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HEM_VOID_STATUS_HIST');
  fsc_utils.proc_end;
  COMMIT;  
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
    RAISE;
END dataload_delete;
END s_dl_hem_void_status_hist;
/
show errors


