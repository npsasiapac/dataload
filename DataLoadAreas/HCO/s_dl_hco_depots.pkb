CREATE OR REPLACE PACKAGE BODY s_dl_hco_depots
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Vers   WHO  WHEN        WHY
--      1.0            MJK  31/01/05    Dataload
--      1.1  5.10.0    PH   27/07/06    Corrected Delete process and
--                                      added savepoints
--      1.2  5.10.0    PH   16/08/06    Removed validate on created by/date
--      2.0  5.13.0    PH   06-FEB-2008 Now includes its own 
--                                      set_record_status_flag procedure.
--      3.0  6.11      AJ   18-AUG-2015 Added ldep_code_mlang and ldep_description_mlang
--                                      into create and validate ldep_code_mlang must be
--                                      unique but description doesn't 
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
  UPDATE dl_hco_depots
  SET ldep_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hco_depots');
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
,      ldep_dlb_batch_id
,      ldep_dl_seqno
,      ldep_dl_load_status
,      ldep_code
,      ldep_description
,      ldep_current_ind
,      ldep_dep_code
,      ldep_code_mlang
,      ldep_description_mlang
FROM   dl_hco_depots
WHERE  ldep_dlb_batch_id    = p_batch_id
AND    ldep_dl_load_status = 'V';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HCO_DEPOTS';
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
  fsc_utils.proc_start('s_dl_hco_depots.dataload_create');
  fsc_utils.debug_message( 's_dl_hco_depots.dataload_create',3);
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
      cs := p1.ldep_dl_seqno;
      l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
      --
      -- Create depots record
      --              
      INSERT INTO depots 
      (dep_code
      ,dep_description
      ,dep_current_ind
      ,dep_created_by
      ,dep_created_date
      ,dep_dep_code
      ,dep_code_mlang
      ,dep_description_mlang
      )
      VALUES
      (p1.ldep_code
      ,p1.ldep_description
      ,p1.ldep_current_ind
      ,'DATALOAD'
      ,sysdate
      ,p1.ldep_dep_code
      ,p1.ldep_code_mlang
      ,p1.ldep_description_mlang
      );
      --
      -- keep a count of the rows processed and commit after every 1000
      --
      i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
      --
      s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
      set_record_status_flag(l_id,'C');
    EXCEPTION
    WHEN OTHERS 
    THEN
      ROLLBACK TO SP1;
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE, SQLERRM);
      set_record_status_flag(l_id,'O');
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      --
    END;
  --
  END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DEPOTS');
--
  --
  fsc_utils.proc_end;
  --
  commit;
  --
EXCEPTION
WHEN OTHERS 
THEN
  set_record_status_flag(l_id,'O');
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
,      ldep_dlb_batch_id
,      ldep_dl_seqno
,      ldep_dl_load_status
,      ldep_code
,      ldep_description
,      ldep_current_ind
,      ldep_dep_code
,      ldep_code_mlang
,      ldep_description_mlang
FROM   dl_hco_depots
WHERE  ldep_dlb_batch_id    = p_batch_id
AND    ldep_dl_load_status in ('L','F','O');
--
CURSOR c2 (cp_rowid    ROWID
          ,cp_dep_code dl_hco_depots.ldep_code%TYPE
          )
IS
SELECT 'duplicate'
FROM   dl_hco_depots
WHERE  rowid    != cp_rowid
AND    ldep_code = cp_dep_code
UNION
SELECT 'duplicate'
FROM   depots
WHERE  dep_code = cp_dep_code;
--
r2 c2%ROWTYPE;
--
CURSOR c_chk_mlang (cp_dep_code_mlang VARCHAR2)
IS
SELECT 'x'
FROM   depots
WHERE  dep_code_mlang = cp_dep_code_mlang;
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HCO_DEPOTS';
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
l_exists_mlang   VARCHAR2(1);
--
-- Other variables
--
l_dummy             VARCHAR2(2000);
l_date              DATE;
l_is_inactive       BOOLEAN DEFAULT FALSE; 
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_depots.dataload_validate');
  fsc_utils.debug_message( 's_dl_hco_depots.dataload_validate',3);
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
      cs := p1.ldep_dl_seqno;
      l_errors := 'V';
      l_error_ind := 'N';
      l_id := p1.rec_rowid;
      l_exists_mlang := NULL;
      --
      -- Check for duplicate primary key
      --
      OPEN c2(p1.rec_rowid,p1.ldep_code);
      FETCH c2 INTO r2;
      IF c2%FOUND
      THEN
        CLOSE c2;
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',221);
      ELSE 
        CLOSE c2;
      END IF;
      --  
      -- Validate depot code is not null
      --
      IF p1.ldep_code IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',153);
      END IF;
      --  
      -- Validate description is not null
      --
      IF p1.ldep_description IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',154);
      END IF;
      --  
      -- Validate current ind exists and is Y or N
      --
      IF p1.ldep_current_ind IS NOT NULL
      THEN
        IF p1.ldep_current_ind NOT IN ('Y','N')                      
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',159);
        END IF;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',159);
      END IF;
      -- 
      -- Validate dep_code_mlang not exists
      --
      OPEN c_chk_mlang(p1.ldep_code_mlang);
      FETCH c_chk_mlang INTO l_exists_mlang;
      CLOSE c_chk_mlang;
      --
      IF l_exists_mlang IS NOT NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',282);
      END IF;
      --
      -- If ldep_code_mlang provided then ldep_description_mlang
      -- is a mandatory field
      --
      IF ( p1.ldep_code_mlang IS NOT NULL    AND
           p1.ldep_description_mlang IS NULL     )
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',283);
      END IF;
      --
      --
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
END dataload_validate;
--
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
CURSOR c1 is
SELECT rowid rec_rowid
,      ldep_dlb_batch_id
,      ldep_dl_seqno
,      ldep_dl_load_status
,      ldep_code
,      ldep_description
,      ldep_current_ind
,      ldep_dep_code
,      ldep_code_mlang
,      ldep_description_mlang
FROM  dl_hco_depots
WHERE ldep_dlb_batch_id   = p_batch_id
AND   ldep_dl_load_status = 'C';
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HCO_DEPOTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
l_id     ROWID;
--
i        INTEGER := 0;
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_depots.dataload_delete');
  fsc_utils.debug_message( 's_dl_hco_depots.dataload_delete',3 );
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
    cs := p1.ldep_dl_seqno;
    l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
    DELETE FROM depots
    WHERE  dep_code = p1.ldep_code;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DEPOTS');
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
END s_dl_hco_depots;
/
