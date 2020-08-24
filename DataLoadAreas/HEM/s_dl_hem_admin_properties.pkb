CREATE OR REPLACE PACKAGE BODY s_dl_hem_admin_properties
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Ver WHO  WHEN        WHY
--      1.0        PJD  05/9/2000   Dataload
--      1.1        PJD  31/10/2000  Include latest revisions to dataload process
--      1.2        PJD  20/12/2000  Alterations to errors handling
--      1.3        PJD  23/08/2002  Added check on duplicate admin unit types
--      1.4        PJD  11/09/2002  Added new check on Prop Groupings within the 
--                                  create process
--      2.0        PJD  01/12/2002  Changed the above check to limit to auy_type 
--                              of  HOU
--      2.1        PJD  20/11/2002  Move update of record status and process count
--      2.2        PH   22/03/2004  Amended Validate to get pro_refno to improve
--                                  performance.
--     3.0  5.13.0 PH   06-FEB-2008 Now includes its own 
--                                  set_record_status_flag procedure. Included
--                                  db version in this section
--
--  declare package variables AND constants
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
  UPDATE dl_hem_admin_properties
  SET lapr_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hem_admin_properties');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
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
,lapr_dlb_batch_id
,lapr_dl_seqno
,lapr_DL_LOAD_STATUS
,LAPR_PRO_PROPREF
,LAPR_AUN_CODE
FROM dl_hem_admin_properties
WHERE lapr_dlb_batch_id    = p_batch_id
AND   lapr_dl_load_status = 'V';
--
CURSOR c_pro_refno(p_propref varchar2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
CURSOR c_check_pgr(p_pro_refno VARCHAR2) IS
SELECT 'X'
FROM   dual
WHERE  EXISTS (SELECT null
              FROM   admin_unit_types,prop_groupings
              WHERE  pgr_pro_refno = p_pro_refno
              AND    pgr_aun_type  = auy_code
              AND    auy_type      = 'HOU'
              GROUP BY pgr_aun_type
              HAVING count(*) > 1);
--
-- Constants for process_summary
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'CREATE';
ct          VARCHAR2(30) := 'DL_HEM_ADMIN_PROPERTIES';
cs          INTEGER;
ce	      VARCHAR2(200);
l_id     ROWID;
l_an_tab    VARCHAR2(1);
--
-- Other variables
--
l_pro_refno NUMBER;
i           INTEGER := 0;
l_exists    VARCHAR2(1);
--
e_dup_pgr   EXCEPTION;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_admin_properties.dataload_create');
fsc_utils.debug_message('s_dl_hem_admin_properties.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lapr_dl_seqno;
l_id := p1.rec_rowid;
--
-- get the pro_refno
--
l_pro_refno := null;
 --
OPEN  c_pro_refno(p1.lapr_pro_propref);
FETCH c_pro_refno INTO l_pro_refno;
CLOSE c_pro_refno;
--
--
      SAVEPOINT SP2;
      --
      INSERT INTO admin_properties
         (
             apr_pro_refno,
             apr_aun_code ,
             apr_auy_type ,
             apr_direct_link,
             apr_creation_date,
             apr_created_by
         )
      VALUES
         (
             l_pro_refno         ,
             p1.lapr_aun_code    ,
             'HOU'               ,
             'Y'                 ,
             trunc(sysdate)      ,
             'DATALOAD'
         );
--
      l_exists := NULL;
--
      OPEN   c_check_pgr(l_pro_refno);
      FETCH  c_check_pgr INTO l_exists;
      CLOSE  c_check_pgr;
--
      IF   l_exists IS NOT NULL
      THEN 
      ROLLBACK TO SP2;
      RAISE e_dup_pgr;
      END IF;
--
-- Update record status and process count
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'C');
--
-- keep a count of the rows processed AND COMMIT after every 5000
--
i := i+1; IF MOD(i,5000)=0 THEN COMMIT; END IF;
--
 EXCEPTION
   WHEN e_dup_pgr THEN
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA','1','DUPLICATE VALUE IN PROP GROUPINGS VIEW');
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
   WHEN OTHERS THEN
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADMIN_PROPERTIES');
--
fsc_utils.proc_END;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
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
CURSOR c1 is
SELECT
rowid rec_rowid
,lapr_dlb_batch_id
,lapr_dl_seqno
,LAPR_PRO_PROPREF
,LAPR_AUN_CODE
FROM dl_hem_admin_properties
WHERE lapr_dlb_batch_id      = p_batch_id
AND   lapr_dl_load_status       in ('L','F','O');
--
CURSOR c_aun_code(p_aun_code varchar2) is
SELECT null
FROM admin_units
WHERE aun_code      = p_aun_code;
--
CURSOR c_pro_refno (p_pro_propref varchar2) IS
SELECT pro_refno
FROM   properties
where  pro_propref = p_pro_propref;
--
CURSOR c_pgr (p_aun_code VARCHAR2, p_pro_refno VARCHAR2) IS
SELECT 'X' 
FROM  prop_groupings, 
      admin_units
WHERE pgr_pro_refno = p_pro_refno
  AND pgr_aun_type  = aun_auy_code
  AND aun_code      = p_aun_code;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HEM_ADMIN_PROPERTIES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_admin_properties.dataload_validate');
fsc_utils.debug_message( 's_dl_hem_admin_properties.dataload_validate',3);
--
cb := p_batch_id;
cd := p_DATE;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lapr_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- Check the property exists on PROPERTIES
--
  IF (not s_dl_hem_utils.exists_propref(p1.lapr_pro_propref))
  THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
  END IF;
--
-- Check the admin_unit code exists on ADMIN UNITS
--
  OPEN c_aun_code(p1.lapr_aun_code);
  FETCH c_aun_code into l_exists;
  IF c_aun_code%notfound THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
  END IF;
  CLOSE c_aun_code;
--
l_pro_refno := NULL;
--
  OPEN c_pro_refno(p1.lapr_pro_propref);
   FETCH c_pro_refno into l_pro_refno;
  CLOSE c_pro_refno;
--
  OPEN c_pgr(p1.lapr_aun_code, l_pro_refno);
  FETCH c_pgr INTO l_exists;
  IF c_pgr%FOUND
  THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',045);
  END IF;
  CLOSE c_pgr;

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
PROCEDURE dataload_DELETE (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT
 a1.rowid rec_rowid
,a1.lapr_dlb_batch_id
,a1.lapr_dl_seqno
,a1.lapr_DL_LOAD_STATUS
,a1.LAPR_PRO_PROPREF
,a1.LAPR_AUN_CODE
,p1.pro_refno
FROM properties p1,  dl_hem_admin_properties a1
WHERE pro_propref           = lapr_pro_propref
  AND lapr_dlb_batch_id     = p_batch_id
  AND lapr_dl_load_status   = 'C';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HEM_ADMIN_PROPERTIES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i integer := 0;
--
BEGIN
--
-- fsc_utils.proc_start('s_dl_hem_admin_properties.dataload_DELETE');
-- fsc_utils.debug_message( 's_dl_hem_admin_properties.dataload_DELETE',3 );
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
cs := p1.lapr_dl_seqno;
l_id := p1.rec_rowid;
--
DELETE FROM admin_properties
WHERE apr_pro_refno   = p1.pro_refno
  AND apr_aun_code    = p1.lapr_aun_code;
--
-- Update record status and process count
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'V');
--
-- keep a count of the rows processed AND COMMIT after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
EXCEPTION
   WHEN OTHERS THEN
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADMIN_PROPERTIES');
--
fsc_utils.proc_END;
COMMIT;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_DELETE;
--
--
END s_dl_hem_admin_properties;
/

