CREATE OR REPLACE PACKAGE BODY s_dl_hsc_service_assignments
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER   WHO  WHEN         WHY
--      1.0  5.2.0    PH   04-AUG-2002  Bespoke Dataload for NCCW
--      2.0  5.13.0   PH   06-FEB-2008  Now includes its own 
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
  UPDATE dl_hsc_service_assignments
  SET lsea_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_service_assignments');
     RAISE;
  --
END set_record_status_flag;
--      
-- ***********************************************************************     
--
--  declare package variables and constants
--
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lsea_dlb_batch_id
,lsea_dl_seqno
,lsea_DL_LOAD_STATUS
,lsea_aun_code
,lsea_svc_att_ele_code
,lsea_svc_att_code
,lsea_start_date
,lsea_end_date
,lsea_sea_aun_code
,lsea_sea_svc_att_ele_code
,lsea_sea_svc_att_code
,lsea_sea_start_date
FROM dl_hsc_service_assignments
WHERE lsea_dlb_batch_id    = p_batch_id
AND   lsea_dl_load_status = 'V';
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HSC_SERVICE_ASSIGNMENTS';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
i           integer := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_service_assignments.dataload_create');
fsc_utils.debug_message( 's_dl_hsc_service_assignments_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lsea_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
--
      INSERT INTO service_assignments
         (sea_aun_code
         ,sea_svc_att_ele_code
         ,sea_svc_att_code
         ,sea_start_date
         ,sea_created_by
         ,sea_created_date
         ,sea_end_date
         ,sea_sea_aun_code
         ,sea_sea_svc_att_ele_code
         ,sea_sea_svc_att_code
         ,sea_sea_start_date
         )
      VALUES
         (p1.lsea_aun_code
         ,p1.lsea_svc_att_ele_code
         ,p1.lsea_svc_att_code
         ,p1.lsea_start_date
         ,'DATALOAD'
         ,trunc(sysdate)
         ,p1.lsea_end_date
         ,p1.lsea_sea_aun_code
         ,p1.lsea_sea_svc_att_ele_code
         ,p1.lsea_sea_svc_att_code
         ,p1.lsea_sea_start_date
         );
--
-- keep a count of the rows processed and commit after every 5000
--
i := i+1; IF MOD(i,5000)=0 THEN COMMIT; END If;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'C');
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
COMMIT;
--
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SERVICE_ASSIGNMENTS');
--
fsc_utils.proc_end;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
     RAISE;
--
END dataload_create;
--
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lsea_dlb_batch_id
,lsea_dl_seqno
,lsea_DL_LOAD_STATUS
,lsea_aun_code
,lsea_svc_att_ele_code
,lsea_svc_att_code
,lsea_start_date
,lsea_end_date
,lsea_sea_aun_code
,lsea_sea_svc_att_ele_code
,lsea_sea_svc_att_code
,lsea_sea_start_date
FROM  dl_hsc_service_assignments
WHERE lsea_dlb_batch_id      = p_batch_id
AND   lsea_dl_load_status   in ('L','F','O');
--
CURSOR c_aun_code(p_aun_code varchar2) IS
SELECT 'X'
FROM   admin_units
WHERE  aun_code      = p_aun_code;
--
CURSOR c_ele_code(p_ele_code VARCHAR2) IS
SELECT 'X'
FROM   elements
WHERE  ele_code  = p_ele_code;
--
CURSOR c_att_code(p_ele_code VARCHAR2, p_att_code VARCHAR2) IS
SELECT 'X'
FROM   attributes
WHERE  att_ele_code = p_ele_code
AND    att_code     = p_att_code;
--
CURSOR c_aun_parent (p_parent VARCHAR2, p_child VARCHAR2) IS
SELECT 'X'
FROM   admin_groupings
WHERE  agr_aun_code_parent = p_parent
AND    agr_aun_code_child  = p_child;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HSC_SERVICE_ASSIGNMENTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_service_assignments.dataload_validate');
fsc_utils.debug_message( 's_dl_hsc_service_assignments.dataload_validate',3);
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
cs := p1.lsea_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
--
-- Check the admin_unit code exists on ADMIN UNITS
--
-- Child  Code
--
  OPEN  c_aun_code(p1.lsea_aun_code);
   FETCH c_aun_code into l_exists;
    IF c_aun_code%notfound 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
    END IF;
  CLOSE c_aun_code;
--
-- Parent Code
--
  IF p1.lsea_sea_aun_code IS NOT NULL
  THEN
   OPEN  c_aun_code(p1.lsea_sea_aun_code);
   FETCH c_aun_code into l_exists;
    IF c_aun_code%notfound 
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',539);
    END IF;
  CLOSE c_aun_code;
  END IF;
--
-- Check Element exists on ELEMENTS
--
  OPEN c_ele_code(p1.lsea_svc_att_ele_code);
   FETCH c_ele_code INTO l_exists;
    IF c_ele_code%notfound
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',126);
    END IF;
  CLOSE c_ele_code;
--
-- Check the attribute code is valid
--
   OPEN  c_att_code(p1.lsea_svc_att_ele_code, p1.lsea_svc_att_code);
    FETCH c_att_code INTO l_exists;
     IF c_att_code%NOTFOUND
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',540);
     END IF;
   CLOSE c_att_code;
--
-- Check start date supplied
--
   IF p1.lsea_start_date IS NULL
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',541);
   END IF;
--
-- Check End date is not before start date
--
   IF p1.lsea_end_date IS NOT NULL
    THEN
     IF p1.lsea_end_date <= p1.lsea_start_date
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',542);
     END IF;
   END IF;
--
-- Check Parent Element exists on ELEMENTS
--
   IF p1.lsea_sea_svc_att_ele_code IS NOT NULL
    THEN
     OPEN c_ele_code(p1.lsea_sea_svc_att_ele_code);
      FETCH c_ele_code INTO l_exists;
       IF c_ele_code%notfound
        THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',543);
       END IF;
     CLOSE c_ele_code;
   END IF;
--
-- Check the attribute code is valid
--
   IF p1.lsea_sea_svc_att_code IS NOT NULL
    THEN
     OPEN  c_att_code(p1.lsea_sea_svc_att_ele_code, p1.lsea_sea_svc_att_code);
      FETCH c_att_code INTO l_exists;
       IF c_att_code%NOTFOUND
        THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',544);
       END IF;
     CLOSE c_att_code;
   END IF;
--
-- Check the Parent Start date is not before start date
--
   IF p1.lsea_sea_start_date IS NOT NULL
    THEN
     IF p1.lsea_sea_start_date < p1.lsea_start_date
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',545);
     END IF;
   END IF;
--
-- Check the Parent/Child admin unit links
--
   IF p1.lsea_sea_aun_code IS NOT NULL
    THEN
     OPEN  c_aun_parent(p1.lsea_sea_aun_code, p1.lsea_aun_code);
      FETCH c_aun_parent INTO l_exists;
       IF c_aun_parent%NOTFOUND
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',546);
       END IF;
     CLOSE c_aun_parent;
   END IF;
--
--
-- Now UPDATE the record count AND error code
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
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
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
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lsea_dlb_batch_id
,lsea_dl_seqno
,lsea_DL_LOAD_STATUS
,lsea_aun_code
,lsea_svc_att_ele_code
,lsea_svc_att_code
,lsea_start_date
,lsea_end_date
,lsea_sea_aun_code
,lsea_sea_svc_att_ele_code
,lsea_sea_svc_att_code
,lsea_sea_start_date
FROM  dl_hsc_service_assignments
WHERE lsea_dlb_batch_id      = p_batch_id
  AND lsea_dl_load_status   = 'C';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HSC_SERVICE_ASSIGNMENTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i integer := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_service_assignments.dataload_delete');
fsc_utils.debug_message( 's_dl_hsc_service_assignments.dataload_delete',3 );
--
cb := p_batch_id;
cd := p_date;
--
-- s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lsea_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
     DELETE FROM service_assignments
     WHERE  sea_aun_code             = p1.lsea_aun_code
     AND    sea_svc_att_ele_code     = p1.lsea_svc_att_ele_code
     AND    sea_svc_att_code         = p1.lsea_svc_att_code
     AND    sea_start_date           = p1.lsea_start_date
     AND    sea_end_date             = p1.lsea_end_date
     AND    sea_sea_aun_code         = p1.lsea_sea_aun_code
     AND    sea_sea_svc_att_ele_code = p1.lsea_sea_svc_att_ele_code
     AND    sea_sea_svc_att_code     = p1.lsea_sea_svc_att_code
     AND    sea_sea_start_date       = p1.lsea_sea_start_date;  
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'V');
--
EXCEPTION
   WHEN OTHERS THEN
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'C');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
--
END LOOP;
--
COMMIT;
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HSC_SERVICE_ASSIGNMENTS');
--
fsc_utils.proc_end;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_delete;
--
--
END s_dl_hsc_service_assignments;
/


