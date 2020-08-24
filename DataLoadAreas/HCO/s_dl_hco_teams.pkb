CREATE OR REPLACE PACKAGE BODY s_dl_hco_teams
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Vers  WHO  WHEN       WHY
--      1.0           MJK  31/01/05   Dataload
--      1.1  5.10.0   PH   27/07/06   Corrected Delete and added savepoints
--      1.2  5.10.0   PH   16/08/06   Removed validate on created by/date
--      2.0  5.12.0   PH   17/07/07   Corrected Validate as failing 
--                                    to report errors.
--      3.0  5.13.0   PH   06-FEB-2008 Now includes its own 
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
  UPDATE dl_hco_teams
  SET ltea_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hco_teams');
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
,      ltea_dlb_batch_id
,      ltea_dl_seqno
,      ltea_dl_load_status
,      ltea_code
,      ltea_name
,      ltea_type_ind
,      ltea_level_ind
,      ltea_current_ind
,      ltea_cdep_cos_code
,      ltea_cdep_dep_code
,      ltea_tea_code
,      ltea_default_utilisation_pct
,      ltea_comments
FROM   dl_hco_teams
WHERE  ltea_dlb_batch_id    = p_batch_id
AND    ltea_dl_load_status = 'V';
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HCO_TEAMS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
i            INTEGER := 0;
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_teams.dataload_create');
  fsc_utils.debug_message( 's_dl_hco_teams.dataload_create',3);
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
      cs := p1.ltea_dl_seqno;
      l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
      -- Create teams record
      --
      INSERT INTO teams 
      (tea_code
      ,tea_name
      ,tea_type_ind
      ,tea_level_ind
      ,tea_current_ind
      ,tea_cdep_cos_code
      ,tea_cdep_dep_code
      ,tea_created_by
      ,tea_created_date
      ,tea_tea_code
      ,tea_default_utilisation_pct
      ,tea_comments
      )
      VALUES
      (p1.ltea_code
      ,p1.ltea_name
      ,p1.ltea_type_ind
      ,p1.ltea_level_ind
      ,p1.ltea_current_ind
      ,p1.ltea_cdep_cos_code
      ,p1.ltea_cdep_dep_code
      ,'DATALOAD'
      ,sysdate
      ,p1.ltea_tea_code
      ,p1.ltea_default_utilisation_pct
      ,p1.ltea_comments
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('TEAMS');
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
,      ltea_dlb_batch_id
,      ltea_dl_seqno
,      ltea_dl_load_status
,      ltea_code
,      ltea_name
,      ltea_type_ind
,      ltea_level_ind
,      ltea_current_ind
,      ltea_cdep_cos_code
,      ltea_cdep_dep_code
,      ltea_tea_code
,      ltea_default_utilisation_pct
,      ltea_comments
FROM   dl_hco_teams
WHERE  ltea_dlb_batch_id    = p_batch_id
AND    ltea_dl_load_status in ('L','F','O');
--
--
CURSOR c2 ( p_tea_code IN teams.tea_code%TYPE ) IS
SELECT tea_type_ind
,      tea_level_ind
FROM   teams
WHERE  tea_code = p_tea_code
UNION ALL 
SELECT ltea_type_ind  tea_type_ind
,      ltea_level_ind tea_level_ind
FROM   dl_hco_teams
WHERE  ltea_code            = p_tea_code
AND    ltea_dlb_batch_id    = p_batch_id
AND    ltea_dl_load_status  = 'V';
r2 c2%ROWTYPE;
--
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HCO_TEAMS';
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
  fsc_utils.proc_start('s_dl_hco_teams.dataload_validate');
  fsc_utils.debug_message( 's_dl_hco_teams.dataload_validate',3);
--
  cb := p_batch_id;
  cd := p_date;
--
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 IN c1 
  LOOP
    BEGIN
--
      cs := p1.ltea_dl_seqno;
      l_id := p1.rec_rowid;
--
      l_errors := 'V';
      l_error_ind := 'N';
      --  
      -- Validate team code is not null
      --
      IF p1.ltea_code IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',196);
      END IF;
      --  
      -- Validate team name is not null
      --
      IF p1.ltea_name IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',197);
      END IF;
      --  
      -- Validate team type ind exists and is R or S
      --
      IF p1.ltea_type_ind IS NOT NULL
      THEN
        IF p1.ltea_type_ind NOT IN  ('R','S')
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',198);
        END IF;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',198);
      END IF;
      --  
      -- Validate team level ind exists and is S or O
      --
      IF p1.ltea_level_ind IS NOT NULL
      THEN
        IF p1.ltea_level_ind NOT IN  ('S','O')
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',199);
        END IF;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',199);
      END IF;
      --  
      -- Validate current ind exists and is Y or N
      --
      IF p1.ltea_current_ind IS NOT NULL
      THEN
        IF p1.ltea_current_ind NOT IN ('Y','N')                      
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',159);
        END IF;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',159);
      END IF;
      --  
      -- Validate cos_code/dep_code key exists on cos_depots
      --
      IF p1.ltea_cdep_cos_code IS NOT NULL
      AND p1.ltea_cdep_dep_code IS NOT NULL
      THEN
        IF NOT s_cos_depots.cos_depot_exists(p1.ltea_cdep_dep_code,p1.ltea_cdep_cos_code)
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',200);
        END IF;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',200);
      END IF;
      --  
      --  
      -- Validate tea_tea_code exists
      --
      IF p1.ltea_tea_code IS NOT NULL 
      THEN
        OPEN c2(p1.ltea_tea_code);
        FETCH c2 INTO r2;
        IF c2%NOTFOUND
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',222);
        END IF;
        CLOSE c2;
        -- Test parent team type and error if not the same
        IF p1.ltea_type_ind != r2.tea_type_ind 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',219);
        END IF;
        -- If parent team code set then that parent team must be of type 'O'ganisational
        IF r2.tea_level_ind != 'O' 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',218);
        END IF;
      END IF;
      --  
      -- Validate default_utilisation_pct
      --
      IF p1.ltea_default_utilisation_pct IS NOT NULL
      AND p1.ltea_level_ind !=  'S'
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',216);
      END IF;
      IF p1.ltea_default_utilisation_pct IS NULL
      AND p1.ltea_type_ind = 'R'
      AND p1.ltea_level_ind = 'S'
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',217);
      END IF;
      IF p1.ltea_default_utilisation_pct IS NOT NULL
      THEN
        IF p1.ltea_default_utilisation_pct > 100
        OR p1.ltea_default_utilisation_pct < 0
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',201);
        END IF;
      END IF;
      --  
      -- Validate tea_comments
      --
      --
      -- Now UPDATE the record status and process count
      IF l_errors = 'F' THEN
        l_error_ind := 'Y';
      ELSE
        l_error_ind := 'N';
      END IF;
      --
      s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
      set_record_status_flag(l_id,l_errors);
      --
      -- keep a count of the rows processed AND COMMIT after every 1000
      --
      i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
      --
      EXCEPTION
      WHEN OTHERS THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
      --
END;
--
END LOOP;
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
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
CURSOR c1 is
SELECT rowid rec_rowid
,      ltea_dlb_batch_id
,      ltea_dl_seqno
,      ltea_dl_load_status
,      ltea_code
FROM  dl_hco_teams
WHERE ltea_dlb_batch_id   = p_batch_id
AND   ltea_dl_load_status = 'C';
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HCO_TEAMS';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
l_id     ROWID;
--
i        INTEGER := 0;
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_teams.dataload_delete');
  fsc_utils.debug_message( 's_dl_hco_teams.dataload_delete',3 );
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
    cs := p1.ltea_dl_seqno;
    l_id := p1.rec_rowid;
--
    DELETE FROM teams
    WHERE  tea_code = p1.ltea_code;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('TEAMS');
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
END s_dl_hco_teams;
/
