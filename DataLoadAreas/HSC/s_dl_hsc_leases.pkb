CREATE OR REPLACE PACKAGE BODY s_dl_hsc_leases
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER  WHO  WHEN         WHY
--
--      1.0  5.2.0   PH   10-Jan-2003  Initial Creation
--      1.1  5.4.0   PH   03-SEP-2003  Made sure Package Compiles okay
--      1.2  5.4.0   PH   06-SEP-2003  Added insert into hou_prop_statuses
--                                     on create process also amended delete
--      1.3  5.5.0   IR   05-MAR-2004  Amended cursor c_ipp_refno to look at
--                                     interested_party_types for type 'ACCO'
--                                     instead of interested_parties code 'ACCO'
--      1.4  5.6.0   PJD  07-dec-2004  Added delete of existing 'C' statuses.
--      1.5  5.6.0   PJD  28-dec-2004  New Validation (HDL059) to check for overlaps
--                                     with tenancies.
--      1.6  5.7.0   PJD  17-jan-2005  Remove 'set record status statement' from 
--                                     exception handler in Delete procedure.
--                                     Corrected cursor c_chk_tho
--      1.7  5.8.0   IR   06-MAY-2005  Added llea_rtb_discount, llea_rtb_application_date
--                                     , llea_rtb_purchase_price to procedures
--      1.9  5.10.0  PH   02-NOV-2006  Added validation on hou_prop_statuses. We
--                                     would expect the status to be C. If
--                                     it's anything else causes an overlap.
--      2.0  5.11.0  PH   11-APR-2007  Added update to pro_sco_code on Create.
--      3.0  5.12.0  PH   17-JUL-2007  Added Reusable Refno into create process
--      4.0 5.13.0   PH   06-FEB-2008  Now includes its own 
--                                     set_record_status_flag procedure.
--      4.1  5.15.1  PH                Interested Party in not mandatory, 
--                                     amended validate.
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
  UPDATE dl_hsc_leases
  SET llea_dl_load_status  = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_leases');
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
,llea_dlb_batch_id
,llea_dl_seqno
,llea_dl_load_status
,llea_pro_propref
,llea_start_date
,llea_lease_record_type_ind
,llea_lco_code
,llea_ipp_shortname
,llea_rtb_reference
,llea_s125_offer_date
,llea_initial_start_date
,llea_actual_end_date
,llea_lease_duration
,llea_ref_period_start_date
,llea_hrv_lir_code
,llea_hrv_flt_code
,llea_hrv_sch_code
,llea_rtb_discount
,llea_rtb_application_date
,llea_rtb_purchase_price
FROM dl_hsc_leases
WHERE llea_dlb_batch_id    = p_batch_id
AND   llea_dl_load_status  = 'V';
--
CURSOR c_pro_refno(p_propref varchar2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
CURSOR c_ipp_refno(p_ipp_shortname varchar2) IS
SELECT ipp_refno
FROM   interested_parties, interested_party_types
WHERE  ipp_shortname = p_ipp_shortname
AND    ipp_ipt_code  = ipt_code
AND    ipt_hrv_fiy_code = 'ACCO';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HSC_LEASES';
cs       INTEGER;
ce	 VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
l_pro_refno number;
i           integer := 0;
l_ipp_refno number(10);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_leases.dataload_create');
fsc_utils.debug_message( 's_dl_hsc_leases.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.llea_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- get the pro_refno
--
l_pro_refno := null;
 --
  OPEN  c_pro_refno(p1.llea_pro_propref);
   FETCH c_pro_refno INTO l_pro_refno;
  CLOSE c_pro_refno;
--
-- get the ipp_refno
--
l_ipp_refno := null;
--
  OPEN c_ipp_refno(p1.llea_ipp_shortname);
   FETCH c_ipp_refno INTO l_ipp_refno;
  CLOSE c_ipp_refno;
--
      INSERT INTO leases
         (lea_pro_refno
         ,lea_start_date
         ,lea_lease_record_type_ind
         ,lea_created_by
         ,lea_created_date
         ,lea_lco_code
         ,lea_ipp_refno
         ,lea_rtb_reference
         ,lea_s125_offer_date
         ,lea_initial_start_date
         ,lea_actual_end_date
         ,lea_lease_duration
         ,lea_ref_period_start_date
         ,lea_hrv_lir_code
         ,lea_hrv_flt_code
         ,lea_hrv_sch_code
         ,lea_modified_by
         ,lea_modified_date
         ,lea_rtb_discount
         ,lea_rtb_application_date
         ,lea_rtb_purchase_price
         ,lea_reusable_refno
         )
      VALUES
         (l_pro_refno
         ,p1.llea_start_date
         ,p1.llea_lease_record_type_ind
         ,'DATALOAD'
         ,sysdate
         ,p1.llea_lco_code
         ,l_ipp_refno
         ,p1.llea_rtb_reference
         ,p1.llea_s125_offer_date
         ,p1.llea_initial_start_date
         ,p1.llea_actual_end_date
         ,p1.llea_lease_duration
         ,p1.llea_ref_period_start_date
         ,p1.llea_hrv_lir_code
         ,p1.llea_hrv_flt_code
         ,p1.llea_hrv_sch_code
         ,null
         ,null
         ,p1.llea_rtb_discount
         ,p1.llea_rtb_application_date
         ,p1.llea_rtb_purchase_price
         ,reusable_refno_seq.nextval
         );
--
-- Insert into hou_prop_statuses
--
-- .. but first delete any conflicting statuses
--
      DELETE from hou_prop_statuses
      WHERE hps_pro_refno = l_pro_refno
        AND hps_hpc_type  = 'C'
        AND hps_end_date  IS NULL;
--
      INSERT INTO hou_prop_statuses
         (hps_pro_refno
         ,hps_hpc_code
         ,hps_hpc_type
         ,hps_start_date
         ,hps_END_date)
      VALUES
         (l_pro_refno
         ,'OCCP'
         ,'O'
         ,p1.llea_start_date
         ,null);
--
-- Update properties to keep it in line..
--
      UPDATE properties
         SET pro_sco_code = 'OCC'
       WHERE pro_refno    = l_pro_refno;
--
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('leases');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('hou_prop_statuses');
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
,llea_dlb_batch_id
,llea_dl_seqno
,llea_dl_load_status
,llea_pro_propref
,llea_start_date
,llea_lease_record_type_ind
,llea_lco_code
,llea_ipp_shortname
,llea_rtb_reference
,llea_s125_offer_date
,llea_initial_start_date
,llea_actual_end_date
,llea_lease_duration
,llea_ref_period_start_date
,llea_hrv_lir_code
,llea_hrv_flt_code
,llea_hrv_sch_code
,llea_rtb_discount
,llea_rtb_application_date
,llea_rtb_purchase_price
FROM dl_hsc_leases
WHERE llea_dlb_batch_id    = p_batch_id
AND   llea_dl_load_status    in ('L','F','O');
--
--
CURSOR c_lco_code (p_lco_code VARCHAR2) IS
SELECT 'X'
FROM   legislation_codes
WHERE  lco_code = p_lco_code;
--
CURSOR c_ipp_refno(p_ipp_shortname VARCHAR2) IS
SELECT 'X'
FROM   interested_parties, interested_party_types 
WHERE  ipp_shortname = p_ipp_shortname
AND    ipp_ipt_code = ipt_code
AND    ipt_hrv_fiy_code  = 'ACCO';
--
CURSOR c_chk_tho(p_pro_propref VARCHAR2,p_start_date DATE, p_end_date DATE) IS
SELECT 'Y' 
FROM tenancy_holdings tho,properties pro
WHERE tho.tho_pro_refno = pro.pro_refno
AND   pro.pro_propref   = p_pro_propref
AND   (tho.tho_start_date BETWEEN p_start_date AND nvl(p_end_date,sysdate)
       OR
       p_start_date       BETWEEN tho.tho_start_date 
                              AND nvl(tho.tho_end_date,sysdate)
      )
;
--
CURSOR c_current_hps(p_pro_propref VARCHAR2) IS
SELECT hps_hpc_type
FROM   hou_prop_statuses,
       properties
WHERE  pro_propref = p_pro_propref
AND    pro_refno   = hps_pro_refno
AND    hps_end_date is null;
--
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HSC_LEASES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_hps_type       VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_leases.dataload_validate');
fsc_utils.debug_message( 's_dl_hsc_leases.dataload_validate',3);
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
cs := p1.llea_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
l_hps_type  := null;
--
-- Check the Links to Other Tables
--
-- Check the property exists on properties
--
  IF (not s_dl_hem_utils.exists_propref(p1.llea_pro_propref))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
  END IF;
--
-- New check added PJD 28 Dec 2005
-- Check this doesn't overlap with any tenancy info
--
   l_exists := 'N';
   OPEN   c_chk_tho(p1.llea_pro_propref
                   ,p1.llea_start_date
                   ,p1.llea_actual_end_date);
   FETCH  c_chk_tho into L_exists;
   CLOSE  c_chk_tho;
   IF nvl(l_exists,'N') != 'N'
   THEN 
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',059);
   END IF;   
--
--
-- Validate the reference value fields
--
-- Legislation Code
--
  OPEN c_lco_code(p1.llea_lco_code);
   FETCH c_lco_code INTO l_exists;
    IF c_lco_code%NOTFOUND
     THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',571);
    END IF;
  CLOSE c_lco_code;
--
-- IPP Shortname - Amended, not mandatory
--
  IF p1.llea_ipp_shortname IS NOT NULL
   THEN
    OPEN c_ipp_refno(p1.llea_ipp_shortname);
     FETCH c_ipp_refno INTO l_exists;
      IF c_ipp_refno%NOTFOUND
       THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',572);
      END IF;
    CLOSE c_ipp_refno;
  END IF;
--
-- Initial lease start reason code
--
  IF (NOT s_dl_hem_utils.exists_frv('LISSTRSN',p1.llea_hrv_lir_code,'Y'))
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',573);
  END IF;
--
-- Lease Termination Reason
--
  IF (NOT s_dl_hem_utils.exists_frv('LSETRMRSN',p1.llea_hrv_flt_code,'Y'))
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',574);
  END IF;
--
-- Lease Ref period start reason code
--
  IF (NOT s_dl_hem_utils.exists_frv('LRPSTRSN',p1.llea_hrv_sch_code,'Y'))
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',575);
  END IF;
--
-- Check any Other Mandatory Fields
--
-- Lease Start Date
--
  IF p1.llea_start_date is null
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',576);
  END IF;
--
-- Record Type
--
  IF nvl(p1.llea_lease_record_type_ind, '~#!*') not in ('L','F')
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',577);
  END IF;
--
-- Check the end date is not before the start date
--
  IF p1.llea_actual_end_date is not null
   THEN
    IF (p1.llea_actual_end_date <= p1.llea_start_date)
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',578);
    END IF;
  END IF;
--
-- New validate on hou_prop_statues 02-NOV-2006
--
  OPEN c_current_hps(p1.llea_pro_propref);
   FETCH c_current_hps INTO l_hps_type;
    IF nvl(l_hps_type, 'C') != 'C'
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',224);
    END IF;
  CLOSE c_current_hps;
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
,llea_dlb_batch_id
,llea_dl_seqno
,llea_dl_load_status
,llea_pro_propref
,llea_start_date
,llea_lease_record_type_ind
,llea_lco_code
,llea_ipp_shortname
,llea_rtb_reference
,llea_s125_offer_date
,llea_initial_start_date
,llea_actual_end_date
,llea_lease_duration
,llea_ref_period_start_date
,llea_hrv_lir_code
,llea_hrv_flt_code
,llea_hrv_sch_code
,llea_rtb_discount
,llea_rtb_application_date
,llea_rtb_purchase_price
FROM dl_hsc_leases
WHERE llea_dlb_batch_id    = p_batch_id
AND   llea_dl_load_status  = 'C';
--
CURSOR c_pro_refno(p_propref varchar2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HSC_LEASES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i integer := 0;
l_pro_refno number;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_leases.dataload_delete');
fsc_utils.debug_message( 's_dl_hsc_leases.dataload_delete',3 );
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
cs := p1.llea_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
--
-- get the pro_refno
--
l_pro_refno := null;
 --
  OPEN  c_pro_refno(p1.llea_pro_propref);
   FETCH c_pro_refno INTO l_pro_refno;
  CLOSE c_pro_refno;
--
--
   DELETE FROM leases
   WHERE lea_pro_refno   = l_pro_refno
   AND   lea_start_date  = p1.llea_start_date;
--
-- Delete from hou_prop_statuses
--
   DELETE FROM hou_prop_statuses
   WHERE  hps_pro_refno  = l_pro_refno
   AND    hps_start_date = p1.llea_start_date
   AND    hps_hpc_type    = 'O';
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
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
   set_record_status_flag(l_id,'C');
--
END;
--
END LOOP;
--
COMMIT;
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('leases');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('hou_prop_statuses');
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
END s_dl_hsc_leases;
/

