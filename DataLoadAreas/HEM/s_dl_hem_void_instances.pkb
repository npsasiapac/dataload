CREATE OR REPLACE PACKAGE BODY s_dl_hem_void_instances
AS
-- *****************************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VER  DB VER  WHO  WHEN         WHY
--  1.0  6.9.0   AJ   15-NOV-2013  Bespoke Dataload for Void Instances GMT 
--                                 Migration
--  1.1  6.13    MOK  22-MAY-2016  Updated to 6.13 removed the delete from VIN 
--                                 and VEN amended to update where existing VIN
--                                 to check for an existing Void instance record
--                                 that matches on propref and start date and 
--                                 create when VIN doesnt exist. Also update 
--                                 hps_comments in hou_prop_statuses.
--                                 Added validation checks also.
--  1.2  6.13    AJ   06-JUN-2016  Validation copy and past corrected and also 
--                                 validate added for Void Reason Code 
--                                 (lvin_hrv_rfv_code) and mandatory fields as
--                                 dl table amended so all columns except top 
--                                 three nullable 
--                                 Length check added for text line as max 240 
--                                 CHAR 
--                                 forward slash missing from from last line and
--                                 show errors added at the end
--  1.4  6.13    AJ   08-AUG-2016  Validation for text length amended from 
--                                 cursor to If statement
--                                 (causing ORA-6511 error fixed)
--  1.5  6.13    AJ   09-AUG-2016  Error code for no property ref supplied 
--                                 amended from HDL 30 to HDL 364. 
--                                 Cursor c_rfv_code and l_frv_code data type 
--                                 mismatch causing ORA-6502 error fixed
--  1.6  6.13    PJD  19-AUG-2016  Changed c_vcl_code and c_vst (previously 
--                                 c_vst_comp) cursors as the previous versions 
--                                 had been designed for use on completed VINs 
--                                 only and for default class only.
--  1.7  6.13    AJ   02-FEB-2017  Join incorrect in create when updating for vin_apt_code
--                                 when matches one that already exists now using l_pro_refno
--  1.8  6.14/15 AJ   20-OCT-2017  Changed check for associated hps record to effective date
--                                 as status start date of associated vin may no longer match the
--                                 hps_start_date which is set by the standard property_statuses
--                                 data loader for QL Aus
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
    UPDATE dl_hem_void_instances
    SET    lvin_dl_load_status = p_status
    WHERE  rowid = p_rowid;
EXCEPTION
WHEN OTHERS 
THEN
  dbms_output.put_line('Error updating status of dl_hem_void_instances');
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
,   lvin_dlb_batch_id
,   lvin_dl_seqno
,   lvin_dl_load_status
,   lvin_status_start
,   lvin_created_date
,   lvin_dec_allowance
,   lvin_text
,   lvin_man_created
,   lvin_vst_code
,   lvin_tgt_date
,   lvin_apt_code
,   lvin_vgr_code
,   lvin_vpa_curr_code
,   lvin_sco_code
,   lvin_hrv_rfv_code
,   lvin_hrv_vcl_code
,   lvin_pro_propref
,   lvin_effective_date
FROM   dl_hem_void_instances
WHERE  lvin_dlb_batch_id    = p_batch_id
AND    lvin_dl_load_status = 'V';
    
CURSOR c_pro_refno(cp_propref VARCHAR2)IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = cp_propref;
  
CURSOR chk_vst_code(cp_vin_refno NUMBER,cp_vin_pro_refno VARCHAR2)IS
SELECT vin_vst_code
FROM   void_instances
WHERE  vin_refno = cp_vin_refno
AND    vin_pro_refno = cp_vin_pro_refno;
  
CURSOR check_vin (cp_pro_ref VARCHAR
                , cp_start_date DATE
                , cp_eff_start_date DATE) IS
SELECT 'X' 
FROM void_instances
WHERE vin_pro_refno = cp_pro_ref
AND(vin_status_start = cp_start_date
    OR
    vin_effective_date = cp_eff_start_date);
--
-- Constants for process_summary
--
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'CREATE';
ct          VARCHAR2(30) := 'DL_HEM_VOID_INSTANCES';
cs          INTEGER;
ce          VARCHAR2(200);
l_id        ROWID;
i           INTEGER := 0;
l_an_tab    VARCHAR2(1);
--
-- Other variables
--
l_pro_refno NUMBER(10);
l_vin_refno NUMBER(8);
l_vst_code  VARCHAR2(4);
l_exists    VARCHAR2(1);
  
BEGIN
  fsc_utils.proc_start('s_dl_hem_void_instances.dataload_create');
  fsc_utils.debug_message( 's_dl_hem_void_instances.dataload_create',3);
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Need to delete any void_instances and void_events created by the 
-- prop status dataload commented out as now updated if found and created
-- if not found (MOK MAY2016)
--
    /*DELETE FROM void_events
    WHERE  vev_text      = 'DATALOAD';
    
  DELETE FROM void_instances
  WHERE  vin_text      = 'DATALOADED';
  */
  FOR p1 in c1 LOOP
    BEGIN
      cs := p1.lvin_dl_seqno;
      l_id := p1.rec_rowid;
      
    SAVEPOINT SP1;
        
      l_pro_refno := NULL;
      l_vin_refno := NULL;
      l_vst_code  := NULL;
      l_exists    := NULL;
--
-- get the pro_refno
--
      OPEN c_pro_refno(p1.lvin_pro_propref);
      FETCH c_pro_refno INTO l_pro_refno;
      CLOSE c_pro_refno;

-- get the vin_vst_code and check whether its the same as the lvin_vst_code
-- commented out not sure when code left just in case needed again (AJ 02FEB17)
--
--        OPEN chk_vst_code(l_vin_refno,l_pro_refno);
--        FETCH chk_vst_code INTO l_vst_code;
--        CLOSE chk_vst_code;
--
--
-- the viod instances being updated conditions altered to match check_vin
-- cursor that is used to see if it exists (AJ 20Oct2017)
--
      OPEN check_vin (l_pro_refno, p1.lvin_status_start, p1.lvin_effective_date);
      FETCH check_vin INTO l_exists;
      CLOSE check_vin;
      IF (l_exists IS NOT NULL)
      THEN 
        UPDATE void_instances
           SET vin_created_date = p1.lvin_created_date,
               vin_dec_allowance  = p1.lvin_dec_allowance,
               vin_text           = p1.lvin_text,
               vin_man_created    = p1.lvin_man_created,
               vin_vst_code       = p1.lvin_vst_code,
               vin_tgt_date       = p1.lvin_tgt_date,
               vin_apt_code       = p1.lvin_apt_code,
               vin_vgr_code       = p1.lvin_vgr_code,
               vin_vpa_curr_code  = p1.lvin_vpa_curr_code,
               vin_sco_code       = p1.lvin_sco_code,
               vin_hrv_rfv_code   = p1.lvin_hrv_rfv_code,
               vin_hrv_vcl_code   = p1.lvin_hrv_vcl_code,
               vin_effective_date = p1.lvin_effective_date
         WHERE  vin_pro_refno     = l_pro_refno
         AND   (vin_status_start  = p1.lvin_status_start
                OR
                vin_effective_date = p1.lvin_effective_date);
      ELSE                 
                 
        l_vin_refno := vin_refno_seq.NEXTVAL;    
        
      INSERT INTO void_instances
        (vin_status_start
        ,vin_created_date
        ,vin_dec_allowance
        ,vin_text
        ,vin_man_created
        ,vin_vst_code
        ,vin_tgt_date
        ,vin_apt_code
        ,vin_vgr_code
        ,vin_vpa_curr_code
        ,vin_refno
        ,vin_sco_code
        ,vin_hrv_rfv_code
        ,vin_hrv_vcl_code
        ,vin_pro_refno
        ,vin_effective_date
        ,vin_reusable_refno
        )
      VALUES
        (p1.lvin_status_start
        ,SYSDATE
        ,p1.lvin_dec_allowance
        ,p1.lvin_text
        ,p1.lvin_man_created
        ,p1.lvin_vst_code
        ,p1.lvin_tgt_date
        ,p1.lvin_apt_code 
        ,p1.lvin_vgr_code
        ,p1.lvin_vpa_curr_code
        ,l_vin_refno
        ,p1.lvin_sco_code
        ,p1.lvin_hrv_rfv_code
        ,p1.lvin_hrv_vcl_code
        ,l_pro_refno
        ,p1.lvin_effective_date
        ,reusable_refno_seq.NEXTVAL);
        
       UPDATE dl_hem_void_instances
       SET    lvin_upd_vin_refno = l_vin_refno
       WHERE  rowid = p1.rec_rowid;
        
       UPDATE hou_prop_statuses
       SET    hps_comments   = '(VIN REFNO = '||l_vin_refno||')'
       WHERE  hps_pro_refno  = l_pro_refno
       AND  hps_hpc_code   = 'VOID'
       AND  hps_start_date = TRUNC(p1.lvin_effective_date);
--
-- amended as status start date may no longer match hps_start_date
-- as this can be amended by this data loader (AJ 20Oct17)	   
--       AND  hps_start_date = p1.lvin_status_start;
--       
     END IF; --check_vin
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
  l_an_tab:=s_dl_hem_utils.dl_comp_stats('VOID_INSTANCES');
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
,      lvin_dlb_batch_id
,      lvin_dl_seqno
,      lvin_dl_load_status
,      lvin_status_start
,      lvin_created_date
,      lvin_dec_allowance
,      lvin_text
,      lvin_man_created
,      lvin_vst_code
,      lvin_tgt_date
,      lvin_apt_code
,      lvin_vgr_code
,      lvin_vpa_curr_code
,      lvin_sco_code
,      lvin_hrv_rfv_code
,      lvin_hrv_vcl_code
,      lvin_pro_propref
,      lvin_effective_date
FROM   dl_hem_void_instances
WHERE  lvin_dlb_batch_id    = cp_batch_id
AND    lvin_dl_load_status IN ('L','F','O');
    
CURSOR c_get_prop (cp_propref VARCHAR2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = cp_propref;
-- VCL
CURSOR c_vcl_code (p_frv VARCHAR2) IS
SELECT frv_code
FROM   first_ref_values
WHERE  frv_frd_domain = 'VOID_CLASS'
AND    frv_code = p_frv;
--VST
CURSOR c_vst (p_vst VARCHAR2)IS
SELECT vst_code
FROM   void_statuses
WHERE  vst_code = p_vst;
--APT
CURSOR c_apt_code (p_apt VARCHAR2)IS
SELECT 'X'
FROM   alloc_prop_types
WHERE  apt_code = p_apt
   AND apt_current = 'Y';
--VOID_GROUPS
CURSOR c_vgr_code (p_vgr VARCHAR2)IS
SELECT 'X' 
FROM   void_groups
WHERE  vgr_code = p_vgr 
AND    vgr_current = 'Y';
--VPA
CURSOR c_vpa_code (p_vpa VARCHAR2)IS
SELECT 'X' 
FROM   void_paths
WHERE  vpa_code = p_vpa 
AND    vpa_current = 'Y';
-- RFV
CURSOR c_rfv_code (p_rfv VARCHAR2) IS
SELECT 'X' 
FROM   first_ref_values
WHERE  frv_frd_domain = 'VREASON'
AND    frv_current_ind = 'Y'
AND    frv_code = p_rfv;
--  
-- Constants for process_summary
--
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'VALIDATE';
ct          VARCHAR2(30) := 'DL_HEM_VOID_INSTANCES';
cs          INTEGER;
ce          VARCHAR2(200);
l_id        ROWID;
i           INTEGER := 0;
l_an_tab    VARCHAR2(1);
--
-- Other variables
--
l_pro_refno        NUMBER(10);
l_prop_exists      VARCHAR2(1);
l_errors           VARCHAR2(1);
l_error_ind        VARCHAR2(1);
l_vcl_code         VARCHAR2(10);
l_vst              VARCHAR2(4);
l_apt_code         VARCHAR2(1);
l_vgr_code         VARCHAR2(1);
l_vpa_code         VARCHAR2(1);
l_frv_code         VARCHAR2(1);
--  l_len              NUMBER(10);
--
BEGIN
  
  fsc_utils.proc_start('s_dl_hem_void_instances.dataload_validate');
  fsc_utils.debug_message( 's_dl_hem_void_instances.dataload_validate',3);
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    
  FOR p1 in c1(p_batch_id) LOOP
    
    BEGIN
        cs   := p1.lvin_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors    := 'V';
        l_error_ind := 'N';
        l_pro_refno := NULL;
        l_prop_exists := NULL;
        l_vcl_code  := NULL;
        l_vst       := NULL;
        l_apt_code  := NULL;
        l_vgr_code  := NULL;
        l_vpa_code  := NULL;
        l_frv_code  := NULL;
    --
    -- Check that the property supplied exists and get the pro_refno if it does
    --
    IF p1.lvin_pro_propref IS NULL 
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',364);
    ELSE
      OPEN c_get_prop(p1.lvin_pro_propref);
      FETCH c_get_prop INTO l_pro_refno;
      IF c_get_prop%NOTFOUND 
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',30);
      END IF;
      CLOSE c_get_prop;      
    END IF;
    --VCL
    IF p1.lvin_hrv_vcl_code IS NULL 
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',904);
    ELSE
      OPEN  c_vcl_code (p1.lvin_hrv_vcl_code);
      FETCH c_vcl_code into l_vcl_code;
      CLOSE c_vcl_code;

      IF l_vcl_code IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',360);
      END IF;
    END IF;
    --VST CODE
    IF p1.lvin_vst_code IS NULL 
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',905);
    ELSE
     OPEN  c_vst (p1.lvin_vst_code);
      FETCH c_vst into l_vst;
      CLOSE c_vst;

      IF l_vst IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',359);
      END IF;
    END IF;
    --APT
    IF p1.lvin_apt_code IS NULL 
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',906);
    ELSE
      OPEN  c_apt_code (p1.lvin_apt_code);
      FETCH c_apt_code into l_apt_code;
      CLOSE c_apt_code;

      IF l_apt_code IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',897);
      END IF;
    END IF;
    --SCO
    IF p1.lvin_sco_code NOT IN ('FIN','COM','CUR','CAN','PRO','NEW') 
    THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',898);
    END IF;
    --VOID GROUP
    IF p1.lvin_vgr_code IS NULL 
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',907);
    ELSE
      OPEN  c_vgr_code (p1.lvin_vgr_code);
      FETCH c_vgr_code into l_vgr_code;
      CLOSE c_vgr_code;

      IF l_vgr_code IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',358);
      END IF;
    END IF;
    --VOID PATH
    IF p1.lvin_vpa_curr_code IS NOT NULL
     THEN
      OPEN  c_vpa_code (p1.lvin_vpa_curr_code);
      FETCH c_vpa_code into l_vpa_code;
      CLOSE c_vpa_code;
    
      IF l_vpa_code IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',899);
      END IF;
    END IF;
    --VOID REASON
    IF p1.lvin_hrv_rfv_code IS NULL 
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',908);
    ELSE
      OPEN  c_rfv_code (p1.lvin_hrv_rfv_code);
      FETCH c_rfv_code into l_frv_code;
      CLOSE c_rfv_code;
    
      IF l_frv_code IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',903);
      END IF;
    END IF;
    --
    --TEXT LEGTH CHECK MAXIMUM 240
    --
    IF p1.lvin_text IS NOT NULL
     THEN
     IF LENGTH(RTRIM(p1.lvin_text)) > 240
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',909);
      END IF;
    END IF;
    --
    -- Mandatory Fields not already checked
    --
   IF p1.lvin_effective_date is NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',900);
   END IF;
    --
   IF p1.lvin_created_date is NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',901);
   END IF;
    --
   IF p1.lvin_status_start is NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',902);
   END IF;
    --
    /*  IF p1.LVIN_LEGACY_VIN_REFNO is NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',97);--
        END IF;*/
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
  COMMIT;
  fsc_utils.proc_END;
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
,       lvin_dlb_batch_id
,       lvin_dl_seqno
,       lvin_dl_load_status
,      lvin_upd_vin_refno
FROM   dl_hem_void_instances
WHERE  lvin_dlb_batch_id    = cp_batch_id
AND    lvin_dl_load_status = 'C';
--
-- Constants for process_summary
--
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'DELETE NOT ALLOWED';
ct          VARCHAR2(30) := 'DL_HEM_VOID_INSTANCES';
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
  fsc_utils.proc_start('s_dl_hem_void_instances.dataload_delete');
  fsc_utils.debug_message( 's_dl_hem_void_instances.dataload_delete',3);
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  FOR p1 IN c1 (p_batch_id) 
  LOOP
    BEGIN
      cs := p1.lvin_dl_seqno;
      l_id := p1.rec_rowid;
      SAVEPOINT SP1;
      DELETE FROM void_instances
      WHERE  vin_refno   = p1.lvin_upd_vin_refno;
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
  -- Section to analyze the table(s) populated by this dataload
  --
  l_an_tab:=s_dl_hem_utils.dl_comp_stats('VOID_INSTANCES');
  fsc_utils.proc_end;
  COMMIT;  
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
    RAISE;
END dataload_delete;
END s_dl_hem_void_instances;
/
show errors


