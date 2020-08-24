CREATE OR REPLACE PACKAGE BODY s_dl_hco_cos_depots
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Vers   WHO  WHEN        WHY
--      1.0            MJK  31/01/05    Dataload
--      1.1  5.10.0    PH   27/07/06    Corrected Delete process
--                                      added savepoints.
--      1.2  5.10.0    PH   16/08/06    Removed created by/date
--      2.0  5.13.0    PH   06-FEB-2008 Now includes its own 
--                                      set_record_status_flag procedure.
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
  UPDATE dl_hco_cos_depots
  SET lcdep_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hco_cos_depots');
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
,      lcdep_dlb_batch_id
,      lcdep_dl_seqno
,      lcdep_dl_load_status
,      lcdep_cos_code
,      lcdep_dep_code
,      lcdep_current_ind
FROM   dl_hco_cos_depots
WHERE  lcdep_dlb_batch_id    = p_batch_id
AND    lcdep_dl_load_status = 'V';
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HCO_COS_DEPOTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
l_id     ROWID;
--
-- Other variables
--
i            INTEGER := 0;
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_cos_depots.dataload_create');
  fsc_utils.debug_message( 's_dl_hco_cos_depots.dataload_create',3);
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
      cs := p1.lcdep_dl_seqno;
      l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
      -- Create cos_depots record
      --
      INSERT INTO cos_depots 
      (cdep_cos_code
      ,cdep_dep_code
      ,cdep_current_ind
      ,cdep_created_by
      ,cdep_created_date

      )
      VALUES
      (p1.lcdep_cos_code
      ,p1.lcdep_dep_code
      ,p1.lcdep_current_ind
      ,'DATALOAD'
      ,sysdate
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
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('COS_DEPOTS');
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
,      lcdep_dlb_batch_id
,      lcdep_dl_seqno
,      lcdep_dl_load_status
,      lcdep_cos_code
,      lcdep_dep_code
,      lcdep_current_ind
FROM   dl_hco_cos_depots
WHERE  lcdep_dlb_batch_id    = p_batch_id
AND    lcdep_dl_load_status in ('L','F','O');
--
CURSOR c2 (cp_rowid    ROWID
          ,cp_cos_code dl_hco_cos_depots.lcdep_cos_code%TYPE
          ,cp_dep_code dl_hco_cos_depots.lcdep_dep_code%TYPE
          )
IS
SELECT 'duplicate'
FROM   dl_hco_cos_depots
WHERE  rowid         != cp_rowid
AND    lcdep_cos_code = cp_cos_code
AND    lcdep_dep_code = cp_dep_code
UNION
SELECT 'duplicate'
FROM   cos_depots
WHERE  cdep_cos_code = cp_cos_code
AND    cdep_dep_code = cp_dep_code;
--
r2 c2%ROWTYPE;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HCO_COS_DEPOTS';
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
  fsc_utils.proc_start('s_dl_hco_cos_depots.dataload_validate');
  fsc_utils.debug_message( 's_dl_hco_cos_depots.dataload_validate',3);
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
      cs := p1.lcdep_dl_seqno;
      l_errors := 'V';
      l_error_ind := 'N';
      l_id := p1.rec_rowid;
      --
      -- Check for duplicate primary key
      --
      OPEN c2(p1.rec_rowid,p1.lcdep_cos_code,p1.lcdep_dep_code);
      FETCH c2 INTO r2;
      IF c2%FOUND
      THEN 
        CLOSE c2;
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',221);
      ELSE
        CLOSE c2;
      END IF;
      --  
      -- Validate cos code is not null
      --
      IF p1.lcdep_cos_code IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',188);
      END IF;
      --  
      -- Validate depot code is not null
      --
      IF p1.lcdep_dep_code IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',202);
      END IF;
      --  
      -- Validate current ind exists and is Y or N
      --
      IF p1.lcdep_current_ind IS NOT NULL
      THEN
        IF p1.lcdep_current_ind NOT IN ('Y','N')                      
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',159);
        END IF;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',159);
      END IF;
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
  ,lcdep_dlb_batch_id
  ,lcdep_dl_seqno
  ,lcdep_dl_load_status
  ,lcdep_cos_code
  ,lcdep_dep_code
FROM  dl_hco_cos_depots
WHERE lcdep_dlb_batch_id   = p_batch_id
AND   lcdep_dl_load_status = 'C';
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HCO_COS_DEPOTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
l_id     ROWID;
--
i        INTEGER := 0;
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_cos_depots.dataload_delete');
  fsc_utils.debug_message( 's_dl_hco_cos_depots.dataload_delete',3 );
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
    cs := p1.lcdep_dl_seqno;
    l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
    DELETE FROM cos_depots
    WHERE  cdep_cos_code = p1.lcdep_cos_code
    AND    cdep_dep_code = p1.lcdep_dep_code;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('COS_DEPOTS');
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
END s_dl_hco_cos_depots;
/
