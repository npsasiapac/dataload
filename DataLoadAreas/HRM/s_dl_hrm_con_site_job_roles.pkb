CREATE OR REPLACE PACKAGE BODY s_dl_hrm_con_site_job_roles
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      1.0     AJ   08-JUL-2016  Created new data load to add to Contractors
--                                to load job Role object rows also in main
--                                Contractors data load but can only use that
--                                when initially creating a Contractor and
--                                Contractor Site - initially done because
--                                of problems found by Nigel M when loading
--                                contractors data at Manitoba
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
 lcsj_dlb_batch_id
,lcsj_dl_seqno
,lcsj_dl_load_status
,lcsj_jrb_jro_code
,lcsj_jrb_read_write_ind
,lcsj_jrb_pk_code1
FROM  dl_hrm_con_site_job_roles
WHERE lcsj_dlb_batch_id    = p_batch_id
AND   lcsj_dl_load_status = 'V';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HRM_CON_SITE_JOB_ROLES';
cs       INTEGER;
ce	   VARCHAR2(200);
l_an_tab VARCHAR2(1);
--
-- Other variables
--
i           INTEGER := 0;
l_jro_code  VARCHAR2(15);
l_obj_name  VARCHAR2(255);
l_rw_ind    VARCHAR2(1);
l_cos_code  VARCHAR2(15);
--
BEGIN
--
 fsc_utils.proc_start('s_dl_hrm_con_site_job_roles.dataload_create');
 fsc_utils.debug_message( 's_dl_hrm_con_site_job_roles.dataload_create',3);
--
 cb := p_batch_id;
 cd := p_date;
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 in c1 LOOP
--
  BEGIN
--
   cs := p1.lcsj_dl_seqno;
--
   SAVEPOINT SP1;
--
-- set variable for insert and make sure they are
-- upper case just in case
--
   l_jro_code  := UPPER(p1.lcsj_jrb_jro_code);
   l_obj_name  := 'CONTRACTOR_SITES';
   l_rw_ind    := UPPER(p1.lcsj_jrb_read_write_ind);
   l_cos_code  := UPPER(p1.lcsj_jrb_pk_code1);
--
   INSERT INTO job_role_object_rows
    (jrb_jro_code
    ,jrb_obj_name
    ,jrb_read_write_ind
    ,jrb_pk_code1
    )
    VALUES
    (l_jro_code
    ,l_obj_name
    ,l_rw_ind
    ,l_cos_code
    );	
--
-- keep a count of the rows processed and commit after every 100
--
   i := i+1; IF MOD(i,100)=0 THEN COMMIT; END If;
--
   s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
   s_dl_utils.set_record_status_flag(ct,cb,cs,'C');
--
   EXCEPTION
    WHEN OTHERS THEN
     ROLLBACK TO SP1;
     ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
     s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
     s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
  END;
--
 END LOOP;
COMMIT;
--
-- Section to analyse the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('JOB_ROLE_OBJECT_ROWS');
--
fsc_utils.proc_end;
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
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
 lcsj_dlb_batch_id
,lcsj_dl_seqno
,lcsj_dl_load_status
,lcsj_jrb_jro_code
,lcsj_jrb_read_write_ind
,lcsj_jrb_pk_code1
FROM  dl_hrm_con_site_job_roles
WHERE lcsj_dlb_batch_id      = p_batch_id
AND   lcsj_dl_load_status   in ('L','F','O');
--
CURSOR c_chk_cos_site(p_cos_code VARCHAR2) IS
SELECT 'X'
  FROM contractor_sites
 WHERE cos_code = p_cos_code;
--
CURSOR c_chk_jro (p_jro_code VARCHAR2) IS
SELECT 'X'
  FROM job_roles
 WHERE jro_code = p_jro_code;
--
CURSOR c_chk_cos_jro(p_jro_code     VARCHAR2, 
                     p_jrb_cos_code VARCHAR2) IS
SELECT 'X'
  FROM job_role_object_rows
 WHERE jrb_jro_code = p_jro_code
   AND jrb_obj_name = 'CONTRACTOR_SITES'
   AND jrb_pk_code1 = p_jrb_cos_code;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRM_CON_SITE_JOB_ROLES';
cs       INTEGER;
ce       VARCHAR2(200);
--
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_jro_code       VARCHAR2(15);
l_obj_name       VARCHAR2(255);
l_rw_ind         VARCHAR2(1);
l_cos_code       VARCHAR2(15);
l_jro_chk        VARCHAR2(1);
l_cos_jro_chk    VARCHAR2(1);
l_cos_chk        VARCHAR2(1);
--
BEGIN
--
 fsc_utils.proc_start('s_dl_hrm_con_site_job_roles.dataload_validate');
 fsc_utils.debug_message( 's_dl_hrm_con_site_job_roles.dataload_validate',3);
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
   cs := p1.lcsj_dl_seqno;
--
   l_errors := 'V';
   l_error_ind := 'N';
   l_jro_code  := UPPER(p1.lcsj_jrb_jro_code);
   l_obj_name  := 'CONTRACTOR_SITES';
   l_rw_ind    := UPPER(p1.lcsj_jrb_read_write_ind);
   l_cos_code  := UPPER(p1.lcsj_jrb_pk_code1);
   l_jro_chk   := NULL;
   l_cos_jro_chk  := NULL;
   l_cos_chk   := NULL;
--
-- Check the other mandatory fields
--
   IF l_jro_code IS NULL   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',927);
   END IF;
--
   IF l_rw_ind IS NULL   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',928);
   END IF;
--
   IF l_cos_code IS NULL   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',929);
   END IF;
--
-- Check the Y/N fields
--
   IF l_rw_ind NOT IN ('Y','N')   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',930);
   END IF;
--
-- Check Contractor Site Exists
--
   IF (l_cos_code IS NOT NULL)  THEN
    OPEN  c_chk_cos_site(l_cos_code);
    FETCH c_chk_cos_site into l_cos_chk;  
    CLOSE c_chk_cos_site;
--
    IF l_cos_chk IS NULL   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',931);
    END IF;
   END IF;
--   
-- Check Job Role Exists
--
   IF (l_jro_code IS NOT NULL)  THEN
    OPEN  c_chk_jro(l_jro_code);
    FETCH c_chk_jro into l_jro_chk;  
    CLOSE c_chk_jro;
--
    IF l_jro_chk IS NULL   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',932);
    END IF;
   END IF;
--
-- Check the Job Role for Contractor Site does not already exist on the database
--
   IF (l_cos_chk IS NOT NULL AND l_jro_chk IS NOT NULL)  THEN
--
    OPEN  c_chk_cos_jro(l_jro_code, l_cos_code);
    FETCH c_chk_cos_jro INTO l_cos_jro_chk;
    CLOSE c_chk_cos_jro;
--
    IF l_cos_jro_chk IS NOT NULL   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',933);
    END IF;
   END IF;
--
--
-- Now UPDATE the record count AND error code
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
   s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
   s_dl_utils.set_record_status_flag(ct,cb,cs,l_errors);
--
   EXCEPTION
      WHEN OTHERS THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
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
-- ***********************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT
 lcsj_dlb_batch_id
,lcsj_dl_seqno
,lcsj_dl_load_status
,lcsj_jrb_jro_code
,lcsj_jrb_read_write_ind
,lcsj_jrb_pk_code1
FROM  dl_hrm_con_site_job_roles
WHERE lcsj_dlb_batch_id     = p_batch_id
  AND lcsj_dl_load_status   = 'C';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRM_CON_SITE_JOB_ROLES';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
--
i integer := 0;
--
BEGIN
--
 fsc_utils.proc_start('s_dl_hrm_con_site_job_roles.dataload_delete');
 fsc_utils.debug_message( 's_dl_hrm_con_site_job_roles.dataload_delete',3 );
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
   cs := p1.lcsj_dl_seqno;
--
   SAVEPOINT SP1;
--
-- Delete from Job Role Object Rows
--
   DELETE FROM job_role_object_rows
   WHERE jrb_obj_name = 'CONTRACTOR_SITES'
   AND   jrb_pk_code1 = UPPER(p1.lcsj_jrb_pk_code1)
   and   jrb_read_write_ind = UPPER(p1.lcsj_jrb_read_write_ind);
--
-- keep a count of the rows processed and commit after every 1000
--
   i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
   s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
   s_dl_utils.set_record_status_flag(ct,cb,cs,'V');
--
   EXCEPTION
    WHEN OTHERS THEN
     ROLLBACK TO SP1;
     ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
     s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
     s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
  END;
--
 END LOOP;
--
COMMIT;
--
-- Section to analyse the table populated by this Dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('JOB_ROLE_OBJECT_ROWS');
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
END s_dl_hrm_con_site_job_roles;
/

show errors
