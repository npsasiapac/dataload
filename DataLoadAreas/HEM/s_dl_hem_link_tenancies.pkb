CREATE OR REPLACE PACKAGE BODY s_dl_hem_link_tenancies
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      1.0  5.9.0     PH   19-DEC-2005  Initial Creation
--
--      2.0  5.13.0    PH   06-FEB-2008  Now includes its own 
--                                       set_record_status_flag procedure.
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
  UPDATE dl_hem_link_tenancies
  SET llte_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hem_link_tenancies');
     RAISE;
  --
END set_record_status_flag;
--
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
,llte_dlb_batch_id
,llte_dl_seqno
,llte_dl_load_status
,llte_start_date
,llte_tcy_alt_ref
,llte_tcy_alt_ref_is_for
,llte_hrv_ftlr_code
,llte_comments
FROM dl_hem_link_tenancies
WHERE llte_dlb_batch_id    = p_batch_id
AND   llte_dl_load_status = 'V';
--
CURSOR c_tcy_refno(p_alt_ref varchar2) IS
SELECT tcy_refno
FROM   tenancies
WHERE  tcy_alt_ref = p_alt_ref;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HEM_LINK_TENANCIES';
cs       INTEGER;
ce	 VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
l_tcy_refno        number;
l_tcy_refno_is_for number;
i                  integer := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_link_tenancies.dataload_create');
fsc_utils.debug_message( 's_dl_hem_link_tenancies.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.llte_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- get the tcy_refno's
--
l_tcy_refno        := null;
l_tcy_refno_is_for := null;
 --
OPEN  c_tcy_refno(p1.llte_tcy_alt_ref);
FETCH c_tcy_refno INTO l_tcy_refno;
CLOSE c_tcy_refno;
--
OPEN  c_tcy_refno(p1.llte_tcy_alt_ref_is_for);
FETCH c_tcy_refno INTO l_tcy_refno_is_for;
CLOSE c_tcy_refno;
--
      INSERT INTO link_tenancies
         (lte_start_date
         ,lte_tcy_refno
         ,lte_tcy_refno_is_for
         ,lte_hrv_ftlr_code
         ,lte_created_date
         ,lte_created_by
         ,lte_modified_date
         ,lte_modified_by
         ,lte_end_date
         ,lte_comments
         )
      VALUES
         (p1.llte_start_date
         ,l_tcy_refno
         ,l_tcy_refno_is_for
         ,p1.llte_hrv_ftlr_code
         ,sysdate
         ,'DATALOAD'
         ,null
         ,null
         ,null
         ,p1.llte_comments
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('LINK_TENANCIES');
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
,llte_dlb_batch_id
,llte_dl_seqno
,llte_dl_load_status
,llte_start_date
,llte_tcy_alt_ref
,llte_tcy_alt_ref_is_for
,llte_hrv_ftlr_code
,llte_comments
FROM dl_hem_link_tenancies
WHERE llte_dlb_batch_id    = p_batch_id
AND   llte_dl_load_status   in ('L','F','O');
--
CURSOR c_tcy_refno(p_alt_ref varchar2) IS
SELECT tcy_refno
FROM   tenancies
WHERE  tcy_alt_ref = p_alt_ref;
--
CURSOR c_cur_tcy(p_tcy_ref number) IS
SELECT 'X'
FROM   tenancies
WHERE  tcy_refno = p_tcy_ref
and    tcy_act_end_date is null;
--
CURSOR c_chk_tenant( p_tcy_ref        number, 
                     p_tcy_ref_is_for number) IS
SELECT 'X'
FROM   tenancy_instances tin1,
       tenancy_instances tin2,
       household_persons hop1,
       household_persons hop2
WHERE  tin1.tin_tcy_refno = p_tcy_ref
AND    tin2.tin_tcy_refno = p_tcy_ref_is_for
AND    tin1.tin_hop_refno = hop1.hop_refno
AND    tin2.tin_hop_refno = hop2.hop_refno
AND    hop1.hop_par_refno = hop2.hop_par_refno
AND    tin1.tin_end_date is null
AND    tin2.tin_end_date is null;
--
CURSOR c_non_res_prop(p_tcy_ref        number) IS
SELECT 'X'
FROM   properties,
       tenancy_holdings
WHERE  tho_tcy_refno = p_tcy_ref
AND    pro_refno     = tho_pro_refno
AND    pro_hou_residential_ind = 'N';
--
CURSOR c_check_child(p_tcy_ref       varchar2) IS
SELECT 'X'
FROM  link_tenancies
WHERE lte_tcy_refno_is_for = p_tcy_ref;
--
CURSOR c_check_parent(p_tcy_ref       varchar2) IS
SELECT 'X'
FROM  link_tenancies
WHERE lte_tcy_refno = p_tcy_ref;
--
CURSOR c_get_start(p_tcy_ref        number) IS
SELECT tcy_act_start_date
FROM   tenancies
WHERE  tcy_refno = p_tcy_ref;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HEM_LINK_TENANCIES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists           VARCHAR2(1);
l_tcy_refno        NUMBER(10);
l_tcy_refno_is_for NUMBER(10);
l_tcy_start        DATE;
l_tcy_start_is_for DATE;  
l_errors           VARCHAR2(10);
l_error_ind        VARCHAR2(10);
i                  INTEGER :=0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_link_tenancies.dataload_validate');
fsc_utils.debug_message( 's_dl_hem_link_tenancies.dataload_validate',3);
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
cs := p1.llte_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors           := 'V';
l_error_ind        := 'N';
l_tcy_refno        := null;   -- This is the Child Link
l_tcy_refno_is_for := null;   -- This is the Parent Link
l_tcy_start        := null;
l_tcy_start_is_for := null;
--
-- Get the Tenancy Refno's
--
   OPEN c_tcy_refno(p1.llte_tcy_alt_ref);
    FETCH c_tcy_refno INTO l_tcy_refno;
   CLOSE c_tcy_refno;
--
   OPEN c_tcy_refno(p1.llte_tcy_alt_ref_is_for);
    FETCH c_tcy_refno INTO l_tcy_refno_is_for;
   CLOSE c_tcy_refno;
-- 
-- Check the Links to Other Tables
--
-- Tenancies
--
     IF l_tcy_refno IS NULL
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',31);
     END IF;
--
     IF l_tcy_refno_is_for IS NULL
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',32);
     END IF;
--
-- Make sure that the Tenancies are Current
--
   OPEN c_cur_tcy(l_tcy_refno);
    FETCH c_cur_tcy INTO l_exists;
     IF c_cur_tcy%NOTFOUND
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',33);
     END IF;
   CLOSE c_cur_tcy;
--
   OPEN c_cur_tcy(l_tcy_refno_is_for);
    FETCH c_cur_tcy INTO l_exists;
     IF c_cur_tcy%NOTFOUND
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',34);
     END IF;
   CLOSE c_cur_tcy;
--
--
-- Validate the reference value fields
--
   IF (NOT s_hdl_utils.exists_frv('TCY_LINK_REASON',p1.llte_hrv_ftlr_code,'Y'))
     THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',35);
   END IF;
--
-- One tenant must be common to both tenancies If the 
-- system Parameter LINKTCYPER is set to 'Y'
-- 
   IF fsc_utils.get_sys_param('LINKTCYPER') = 'Y'
    THEN
     OPEN c_chk_tenant(l_tcy_refno, l_tcy_refno_is_for);
      FETCH c_chk_tenant INTO l_exists;
       IF c_chk_tenant%NOTFOUND
        THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',36);
       END IF;
     CLOSE c_chk_tenant;
   END IF;
--
-- The Parent cannot be a Non Residential Property
--
   OPEN c_non_res_prop(l_tcy_refno_is_for);
    FETCH c_non_res_prop INTO l_exists;
     IF c_non_res_prop%FOUND
      THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',37);
     END IF;
   CLOSE c_non_res_prop;
--
-- A child Tenancy cannot already exist as a Parent
--
  OPEN c_check_child(l_tcy_refno);
   FETCH c_check_child INTO l_exists;
    IF c_check_child%FOUND
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',38);
     END IF;
   CLOSE c_check_child;
--
-- A Parent cannot also be a child
--
  OPEN c_check_parent(l_tcy_refno_is_for);
   FETCH c_check_parent INTO l_exists;
    IF c_check_parent%FOUND
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',39);
     END IF;
   CLOSE c_check_parent;
--
-- Check the other mandatory fields
--
   IF p1.llte_start_date IS NULL
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',40);
   END IF;

--
-- The link Period cannot be earlier than the tenancies
--
   OPEN c_get_start(l_tcy_refno);
    FETCH c_get_start INTO l_tcy_start;
     IF p1.llte_start_date < l_tcy_start
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',41);
     END IF;
   CLOSE c_get_start;
--
   OPEN c_get_start(l_tcy_refno_is_for);
    FETCH c_get_start INTO l_tcy_start_is_for;
     IF p1.llte_start_date < l_tcy_start_is_for
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',42);
     END IF;
   CLOSE c_get_start;
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
,llte_dlb_batch_id
,llte_dl_seqno
,llte_dl_load_status
,llte_start_date
,llte_tcy_alt_ref
,llte_tcy_alt_ref_is_for
,llte_hrv_ftlr_code
,llte_comments
FROM dl_hem_link_tenancies
WHERE llte_dlb_batch_id     = p_batch_id
  AND llte_dl_load_status   = 'C';
--
CURSOR c_tcy_refno(p_alt_ref varchar2) IS
SELECT tcy_refno
FROM   tenancies
WHERE  tcy_alt_ref = p_alt_ref;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HEM_LINK_TENANCIES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
l_tcy_refno        NUMBER(10);
l_tcy_refno_is_for NUMBER(10);
--
i integer := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_link_tenancies.dataload_delete');
fsc_utils.debug_message( 's_dl_hem_link_tenancies.dataload_delete',3 );
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
cs := p1.llte_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
--
l_tcy_refno        := null;   -- This is the Child Link
l_tcy_refno_is_for := null;   -- This is the Parent Link
--
-- Get the Tenancy Refno's
--
   OPEN c_tcy_refno(p1.llte_tcy_alt_ref);
    FETCH c_tcy_refno INTO l_tcy_refno;
   CLOSE c_tcy_refno;
--
   OPEN c_tcy_refno(p1.llte_tcy_alt_ref_is_for);
    FETCH c_tcy_refno INTO l_tcy_refno_is_for;
   CLOSE c_tcy_refno;
--
    DELETE FROM link_tenancies
    WHERE  lte_tcy_refno        = l_tcy_refno
    AND    lte_tcy_refno_is_for = l_tcy_refno_is_for;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('LINK_TENANCIES');
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
END s_dl_hem_link_tenancies;
/

