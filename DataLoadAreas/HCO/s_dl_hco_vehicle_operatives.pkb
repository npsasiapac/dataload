CREATE OR REPLACE PACKAGE BODY s_dl_hco_vehicle_operatives
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Vers  WHO  WHEN       WHY
--      1.0           MJK  31/01/05   Dataload
--      1.1  5.10.0   PH   27/07/06   Corrected Delete added savepoints
--      1.2  5.10.0   PH   08/08/06   Corrected Compilation errors
--                                    and commented out c2 cursor as invalid
--      1.3  5.10.0   PH   16/08/06   Removed validate on created by/date
--      1.4  5.10.0   PH   10/05/07   Added lvop_sto_location to insert
--                                    on create process.
--      2.0  5.13.0   PH   06-FEB-2008 Now includes its own 
--                                     set_record_status_flag procedure.
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
  UPDATE dl_hco_vehicle_operatives
  SET lvop_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hco_vehicle_operatives');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
--
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) IS
SELECT rowid rec_rowid
,      lvop_dlb_batch_id
,      lvop_dl_seqno
,      lvop_dl_load_status
,      lvop_sto_location
,      lvop_ipp_shortname
,      lvop_ipt_code
,      lvop_start_date
,      lvop_end_date
,      lvop_refno
FROM   dl_hco_vehicle_operatives
WHERE  lvop_dlb_batch_id    = p_batch_id
AND    lvop_dl_load_status = 'V';
--
--CURSOR c2(p_sto_location VARCHAR2) IS
--SELECT sto_refno
--FROM   stores
--WHERE  sto_location = p_sto_location;
--r2 c2%ROWTYPE;
--
CURSOR c3 (p_ipp_shortname VARCHAR2
          ,p_ipt_code      VARCHAR2
          ) IS
SELECT ipp_refno
FROM   interested_parties
WHERE  ipp_shortname = p_ipp_shortname
AND    ipp_ipt_code  = p_ipt_code;
r3 c3%ROWTYPE;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HCO_VEHICLE_OPERATIVES';
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
  fsc_utils.proc_start('s_dl_hco_vehicle_operatives.dataload_create');
  fsc_utils.debug_message( 's_dl_hco_vehicle_operatives.dataload_create',3);
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
      cs := p1.lvop_dl_seqno;
      l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
      -- Create vehicle_operatives record
      --
      --OPEN c2(p1.lvop_sto_location);
      --FETCH c2 INTO r2;
      --CLOSE c2;
      --
      OPEN c3(p1.lvop_ipp_shortname,p1.lvop_ipt_code);
      FETCH c3 INTO r3;
      CLOSE c3;
      --
      INSERT INTO vehicle_operatives
      (vop_refno
      ,vop_ipp_refno
      ,vop_start_date
      ,vop_created_by
      ,vop_created_date
      ,vop_end_date
      ,vop_sto_location
      )
      VALUES
      (p1.lvop_refno
      ,r3.ipp_refno
      ,p1.lvop_start_date
      ,'DATALOAD'
      ,sysdate
      ,p1.lvop_end_date
      ,p1.lvop_sto_location
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('VEHICLE_OPERATIVES');
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
CURSOR c1 IS
SELECT rowid rec_rowid
,      lvop_dlb_batch_id
,      lvop_dl_seqno
,      lvop_dl_load_status
,      lvop_sto_location
,      lvop_ipp_shortname
,      lvop_ipt_code
,      lvop_start_date
,      lvop_end_date
FROM   dl_hco_vehicle_operatives
WHERE  lvop_dlb_batch_id    = p_batch_id
AND    lvop_dl_load_status in ('L','F','O');
--
--
CURSOR c2 (p_sto_location VARCHAR2) IS
SELECT sto_type
FROM   stores
WHERE  sto_location = p_sto_location;
r2 c2%ROWTYPE;
--
--
CURSOR c3 (p_ipp_shortname VARCHAR2
          ,p_ipt_code      VARCHAR2
          ) IS
SELECT ipp_refno
FROM   interested_parties
WHERE  ipp_shortname = p_ipp_shortname
AND    ipp_ipt_code  = p_ipt_code;
r3 c3%ROWTYPE;
--
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HCO_VEHICLE_OPERATIVES';
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
l_start_date        DATE;
l_end_date          DATE;
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_vehicle_operatives.dataload_validate');
  fsc_utils.debug_message( 's_dl_hco_vehicle_operatives.dataload_validate',3);
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
      cs := p1.lvop_dl_seqno;
      l_id := p1.rec_rowid;
--
      l_errors := 'V';
      l_error_ind := 'N';
      --
      -- Validate sto_location is not null and is on stores table
      --
      IF p1.lvop_sto_location IS NOT NULL
      THEN
        OPEN c2(p1.lvop_sto_location);
        FETCH c2 into r2;
        IF c2%NOTFOUND
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',205);
        ELSIF r2.sto_type != 'V'
        THEN 
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',215);
        END IF;
        CLOSE c2;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',194);
      END IF;
      --
      -- Validate interested party exists
      --
      IF p1.lvop_ipp_shortname IS NOT NULL
      AND p1.lvop_ipt_code IS NOT NULL
      THEN
        OPEN c3(p1.lvop_ipp_shortname,p1.lvop_ipt_code);
        FETCH c3 into r3;
        IF c3%NOTFOUND
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',190);
        END IF;
        CLOSE c3;
      ELSE
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',190);
      END IF;
      --
      -- Validate start_date and end_date
      --
      IF p1.lvop_start_date IS NOT NULL
      THEN
        BEGIN
          l_start_date := TO_DATE(p1.lvop_start_date,'DD-MON-RRRR HH24:MI');
          IF p1.lvop_end_date IS NOT NULL
          THEN
            l_end_date := TO_DATE(p1.lvop_start_date,'DD-MON-RRRR HH24:MI');
            IF l_start_date > l_end_date
            THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',204);
            END IF;
          END IF;
        EXCEPTION
        WHEN OTHERS
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',204);
        END;
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
SELECT
   rowid rec_rowid
  ,lvop_dlb_batch_id
  ,lvop_dl_seqno
  ,lvop_dl_load_status
  ,lvop_refno
FROM  dl_hco_vehicle_operatives
WHERE lvop_dlb_batch_id   = p_batch_id
AND   lvop_dl_load_status = 'C';
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HCO_VEHICLE_OPERATIVES';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
l_id     ROWID;
--
i        INTEGER := 0;
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_vehicle_operatives.dataload_delete');
  fsc_utils.debug_message( 's_dl_hco_vehicle_operatives.dataload_delete',3 );
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
    cs := p1.lvop_dl_seqno;
    l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
    DELETE FROM vehicle_operatives
    WHERE  vop_refno    = p1.lvop_refno;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('VEHICLE_OPERATIVES');
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
END s_dl_hco_vehicle_operatives;
/
