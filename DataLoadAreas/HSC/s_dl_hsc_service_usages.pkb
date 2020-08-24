CREATE OR REPLACE PACKAGE BODY s_dl_hsc_service_usages
AS
-- ***********************************************************************

  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION     WHO  WHEN       WHY
  --      1.0     PJD  04-12-01   Dataload
  --      3.0     PH   07-03-03   Changed space to underscore in Validate
  --                              process ct variable.
  --      3.1     IR   09-06-04   Removed validation 884 -lsus_end_date is
  --                              null.  Field is not mandatory
  --      3.2     PH   14-07-04   Amended Admin Unit validation as it's not
  --                              a mandatory field. Only val is supplied.
  --      3.3     IR   26-07-04   Amended table names from hra_ hsc_
  --      3.4     PH   07-12-04   Amended Record Status in Create process.
  --      3.5     PH   12-01-05   Added new fields for 570 Release
  --      3.6     PH   05-05-06   Added new validate on Parent Service Assignment
--
--      2.0 5.13.0   PH   06-FEB-2008 Now includes its own 
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
  UPDATE dl_hsc_service_usages
  SET lsus_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_service_usages');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
  --
  --  declare package variables AND constants


PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) is
SELECT
rowid rec_rowid
,lsus_dlb_batch_id
,lsus_dl_seqno
,lsus_propref
,lsus_svc_att_ele_code
,lsus_svc_att_code
,lsus_start_date
,lsus_end_date
,lsus_sea_aun_code
,lsus_sea_start_date
,lsus_origine
,nvl(lsus_chargeable_ind, 'Y') lsus_chargeable_ind
FROM dl_hsc_service_usages
WHERE lsus_dlb_batch_id    = p_batch_id
AND   lsus_dl_load_status = 'V';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HSC_SERVICE_USAGES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
i                    INTEGER := 0;
l_answer             VARCHAR2(1);
l_pro_refno          NUMBER;
l_an_tab             VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_service_charge_usages.dataload_create');
fsc_utils.debug_message( 's_dl_hsc_service_charge_usages.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
for p1 in c1(p_batch_id) loop
--
BEGIN
  --
  cs := p1.lsus_dl_seqno;
  l_id := p1.rec_rowid;
  --
  l_pro_refno := null;
  --
  l_pro_refno := s_properties.get_refno_for_propref(p1.lsus_propref);
  --
  INSERT INTO SERVICE_USAGES
  (
   sus_pro_refno
  ,sus_svc_att_ele_code
  ,sus_svc_att_code
  ,sus_start_date
  ,sus_created_by
  ,sus_created_date
  ,sus_end_date
  ,sus_sea_aun_code
  ,sus_sea_start_date
  ,sus_origine
  ,sus_chargeable_ind
  )
  VALUES
  (l_pro_refno
  ,p1.lsus_svc_att_ele_code
  ,p1.lsus_svc_att_code
  ,p1.lsus_start_date
  ,'DATALOAD'
  ,sysdate
  ,p1.lsus_end_date
  ,p1.lsus_sea_aun_code
  ,p1.lsus_sea_start_date
  ,p1.lsus_origine
  ,p1.lsus_chargeable_ind
  );
  --
  --
  -- keep a count of the rows processed and commit after every 1000
  --
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
--
--
set_record_status_flag(l_id,'C');
--
 EXCEPTION
   WHEN OTHERS THEN
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
 END;
END LOOP;
--
--
fsc_utils.proc_end;
commit;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SERVICE_USAGES');
--
   EXCEPTION
      WHEN OTHERS THEN
      set_record_status_flag(l_id,'O');
      s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
--
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
,lsus_dlb_batch_id
,lsus_dl_seqno
,lsus_propref
,lsus_svc_att_ele_code
,lsus_svc_att_code
,lsus_start_date
,lsus_end_date
,lsus_sea_aun_code
,lsus_sea_start_date
,lsus_origine
,lsus_chargeable_ind
FROM dl_hsc_service_usages
WHERE lsus_dlb_batch_id    = p_batch_id
AND   lsus_dl_load_status  in ('L','F','O');
--
CURSOR c_scp(p_scp_code   VARCHAR2
            ,p_scp_start_date DATE) IS
SELECT 'X'
FROM   service_charge_periods
WHERE  scp_code       = p_scp_code
  AND  scp_start_date = p_scp_start_date;
--
--
cursor c_aun_code(p_aun_code varchar2) is
select null
from admin_units
where aun_code      = p_aun_code;
--
--
CURSOR c_att(p_ele_code VARCHAR2, p_att_code VARCHAR2) IS
SELECT null
FROM attributes
WHERE att_ele_code = p_ele_code
AND   att_code     = p_att_code;
--
CURSOR c_sea (p_aun_code   varchar2,
              p_ele_code   varchar2,
              p_att_code   varchar2,
              p_start_date date)  IS
SELECT 'X'
FROM   service_assignments
WHERE  sea_aun_code         = p_aun_code
AND    sea_svc_att_ele_code = p_ele_code
AND    sea_svc_att_code     = p_att_code
AND    sea_start_date       = p_start_date;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HSC_SERVICE_USAGES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- Other Variables
--
l_pro_refno     properties.pro_refno%TYPE;
l_exists         VARCHAR2(1);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
BEGIN
--
-- dbms_output.put_line('STARTING');
--
fsc_utils.proc_start('s_dl_hsc_service_usages.dataload_validate');
fsc_utils.debug_message('s_dl_hsc_service_usages.dataload_validate',3);
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
  cs := p1.lsus_dl_seqno;
  l_id := p1.rec_rowid;
  --
  l_errors := 'V';
  l_error_ind := 'N';
  --
  --
  -- Check the Property Reference is valid
  l_pro_refno := NULL;
  l_pro_refno := s_dl_hem_utils.pro_refno_FOR_propref
                                (p1.lsus_propref);
  --
  IF l_pro_refno is NULL 
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
  END IF;
  --
  -- Check the element and attribute combination is valid
  l_exists := NULL;
  OPEN c_att(p1.lsus_svc_att_ele_code
            ,p1.lsus_svc_att_code);
  FETCH c_att into l_exists;
  IF c_att%notfound 
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',041);
  END IF;
  CLOSE c_att;
  --
  -- Check the other mandatory columns have been supplied
  IF p1.lsus_start_date IS NULL
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',883);
  END IF;
  --
  -- Check the start date is before the end date
  IF p1.lsus_end_date < p1.lsus_start_date
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',882);
  END IF;
  --
  -- Check the admin_unit code exists on ADMIN UNITS
  --
  IF p1.lsus_sea_aun_code IS NOT NULL
   THEN
    OPEN c_aun_code(p1.lsus_sea_aun_code);
     FETCH c_aun_code INTO l_exists;
      IF c_aun_code%NOTFOUND 
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
      END IF;
    CLOSE c_aun_code;
  END IF;
--
-- Check the Parent Service Assignment Exists for the
-- Admin Unit, Element, Attribute and Start Date
--
  IF p1.lsus_sea_aun_code IS NOT NULL
   THEN
    OPEN c_sea(p1.lsus_sea_aun_code, p1.lsus_svc_att_ele_code
              ,p1.lsus_svc_att_code, p1.lsus_sea_start_date);
     FETCH c_sea INTO l_exists;
      IF c_sea%NOTFOUND 
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',101);
      END IF;
    CLOSE c_sea;
  END IF;
--
-- Check the Origine if supplied
--
  IF p1.lsus_origine IS NOT NULL
   THEN
    IF p1.lsus_origine not in ('P', 'E', 'Q')
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',397);
    END IF;
  END IF;
--
-- Check the Chargeable Ind
--
  IF nvl(p1.lsus_chargeable_ind, 'Y') not in ('Y', 'N')
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',398);
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
--
-- keep a count of the rows processed and commit after every 1000
--
  i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
-- dbms_output.put_line('count = '||i);
  s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
  set_record_status_flag(l_id,l_errors);
--
   EXCEPTION
      WHEN OTHERS THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
END;
--
END LOOP;
COMMIT;
--
fsc_utils.proc_end;
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
CURSOR c1 is
SELECT
rowid rec_rowid
,lsus_dlb_batch_id
,lsus_dl_seqno
,lsus_propref
,lsus_svc_att_ele_code
,lsus_svc_att_code
,lsus_start_date
,lsus_end_date
,lsus_sea_aun_code
,lsus_sea_start_date     
FROM dl_hsc_service_usages
WHERE lsus_dlb_batch_id    = p_batch_id
AND   lsus_dl_load_status = 'C';
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HSC_SERVICE_USAGES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
 
i                INTEGER := 0;
l_pro_refno      NUMBER;
l_an_tab         VARCHAR2(1);

BEGIN
--
fsc_utils.proc_start('s_dl_hsc_service_usages.dataload_delete');
fsc_utils.debug_message( 's_dl_hsc_service_usages.dataload_delete',3 );
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
 cs := p1.lsus_dl_seqno;
 l_id := p1.rec_rowid;
--
 i := i +1;
--
-- Get the Pro Refno
--
 l_pro_refno := s_properties.get_refno_for_propref(p1.lsus_propref);
 --
  DELETE FROM service_usages
   WHERE sus_pro_refno           = l_pro_refno
     AND sus_svc_att_ele_code    = p1.lsus_svc_att_ele_code
     AND sus_svc_att_code        = p1.lsus_svc_att_code
     AND sus_start_date          = p1.lsus_start_date;
--
--
-- keep a count of the rows processed and commit after every 1000
--
 i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
--
 s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
 set_record_status_flag(l_id,'V');
--
 EXCEPTION
   WHEN OTHERS THEN
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
   set_record_status_flag(l_id,'C');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
 END;
--
END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SERVICE_USAGES');
--
fsc_utils.proc_end;
commit;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_delete;
--
--
END s_dl_hsc_service_usages;
/

