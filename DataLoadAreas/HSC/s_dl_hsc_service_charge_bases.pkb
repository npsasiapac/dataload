CREATE OR REPLACE PACKAGE BODY s_dl_hsc_service_charge_bases
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER   WHO  WHEN         WHY
--      1.0  5.2.0    PH   05-AUG-2002  Bespoke Dataload for NCCW
--      1.1  5.8.0    VST  01-DEC-2005  Added new table columns
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
  UPDATE dl_hsc_service_charge_bases
  SET lscb_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_service_charge_bases');
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
,lscb_dlb_batch_id
,lscb_dl_seqno
,lscb_dl_load_status
,lscb_scp_code
,lscb_scp_start_date
,lscb_svc_att_ele_code
,lscb_svc_att_code
,lscb_description
,lscb_cost_basis
,lscb_apportion_ind
,lscb_apply_cap_ind
,lscb_increase_type
,lscb_tax_ind
,lscb_charge_applicable_to
,lscb_complete_ind
,lscb_rebateable_ind
,lscb_adjustment_method
,lscb_nom_admin_period
,lscb_ele_code
,lscb_vca_code
,lscb_reapportion_actuals_ind
,lscb_default_percent_increase
,lscb_extract_from_repairs_ind
,lscb_include_prop_repairs_ind
,lscb_component_level
,lscb_auy_code
FROM dl_hsc_service_charge_bases
WHERE lscb_dlb_batch_id    = p_batch_id
AND   lscb_dl_load_status = 'V';
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HSC_SERVICE_CHARGE_BASES';
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
fsc_utils.proc_start('s_dl_hsc_service_charge_bases.dataload_create');
fsc_utils.debug_message( 's_dl_hsc_service_charge_bases.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lscb_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
--
      INSERT INTO service_charge_bases
         (scb_scp_code
         ,scb_scp_start_date
         ,scb_svc_att_ele_code
         ,scb_svc_att_code
         ,scb_description
         ,scb_cost_basis
         ,scb_apportion_ind
         ,scb_apply_cap_ind
         ,scb_increase_type
         ,scb_tax_ind
         ,scb_charge_applicable_to
         ,scb_complete_ind 
         ,scb_rebateable_ind 
         ,scb_adjustment_method
         ,scb_created_date 
         ,scb_created_by
         ,scb_nom_admin_period
         ,scb_ele_code
         ,scb_vca_code
	   ,scb_reapportion_actuals_ind
	   ,scb_default_percent_increase
	   ,scb_extract_from_repairs_ind
	   ,scb_include_prop_repairs_ind
	   ,scb_component_level
	   ,scb_auy_code
         )
      VALUES
         (p1.lscb_scp_code
         ,p1.lscb_scp_start_date
         ,p1.lscb_svc_att_ele_code
         ,p1.lscb_svc_att_code
         ,p1.lscb_description
         ,p1.lscb_cost_basis
         ,p1.lscb_apportion_ind
         ,p1.lscb_apply_cap_ind
         ,p1.lscb_increase_type
         ,p1.lscb_tax_ind
         ,p1.lscb_charge_applicable_to
         ,p1.lscb_complete_ind
         ,p1.lscb_rebateable_ind
         ,p1.lscb_adjustment_method 
         ,trunc(sysdate)
         ,'DATALOAD'
         ,p1.lscb_nom_admin_period
         ,p1.lscb_ele_code
         ,p1.lscb_vca_code
	   ,p1.lscb_reapportion_actuals_ind
	   ,p1.lscb_default_percent_increase
	   ,p1.lscb_extract_from_repairs_ind
	   ,p1.lscb_include_prop_repairs_ind
	   ,p1.lscb_component_level
	   ,p1.lscb_auy_code
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
-- Section to analyze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SERVICE_CHARGE_BASES');
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
,lscb_dlb_batch_id
,lscb_dl_seqno
,lscb_dl_load_status
,lscb_scp_code
,lscb_scp_start_date
,lscb_svc_att_ele_code
,lscb_svc_att_code
,lscb_description
,lscb_cost_basis
,lscb_apportion_ind
,lscb_apply_cap_ind
,lscb_increase_type
,lscb_tax_ind
,lscb_charge_applicable_to
,lscb_complete_ind
,lscb_rebateable_ind
,lscb_adjustment_method
,lscb_nom_admin_period
,lscb_ele_code
,lscb_vca_code
,lscb_reapportion_actuals_ind
,lscb_default_percent_increase
,lscb_extract_from_repairs_ind
,lscb_include_prop_repairs_ind
,lscb_component_level
,lscb_auy_code
FROM  dl_hsc_service_charge_bases
WHERE lscb_dlb_batch_id      = p_batch_id
AND   lscb_dl_load_status   in ('L','F','O');
--
CURSOR c_period(p_scp_code VARCHAR2, p_start_date DATE) IS
SELECT 'X'
FROM   service_charge_periods
WHERE  scp_code       = p_scp_code
AND    scp_start_date = p_start_date;
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
CURSOR c_vat(p_vat_code varchar2) IS
SELECT 'x' from vat_categories
WHERE  vca_code = p_vat_code;
--
CURSOR c_auy_type(p_auy_type varchar2) IS
SELECT 'x' from admin_unit_types
WHERE  auy_code = p_auy_type;
--
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HSC_SERVICE_CHARGE_BASES';
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
fsc_utils.proc_start('s_dl_hsc_service_charge_bases.dataload_validate');
fsc_utils.debug_message( 's_dl_hsc_service_charge_bases.dataload_validate',3);
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
cs := p1.lscb_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- Check the Links to Other Tables
--
-- Check record exists on service_charge_periods
--
   OPEN  c_period(p1.lscb_scp_code, p1.lscb_scp_start_date);
    FETCH c_period INTO l_exists;
     IF c_period%NOTFOUND
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',552);
     END IF;
   CLOSE c_period;
--
-- Check element exists
--
  OPEN c_ele_code(p1.lscb_svc_att_ele_code);
   FETCH c_ele_code INTO l_exists;
    IF c_ele_code%notfound
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',126);
    END IF;
  CLOSE c_ele_code;
--
-- Check attribute exists
--
   OPEN  c_att_code(p1.lscb_svc_att_ele_code, p1.lscb_svc_att_code);
    FETCH c_att_code INTO l_exists;
     IF c_att_code%NOTFOUND
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',540);
     END IF;
   CLOSE c_att_code;
--
-- Check service charge bases
--
   IF nvl(p1.lscb_cost_basis, '^') NOT IN ('P', 'W', 'M', 'Q', 'H', 'Y')
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',554);
   END IF;
--
-- Check Y N fields
--
-- Apportioned Ind
--
   IF (NOT s_dl_hem_utils.yorn(p1.lscb_apportion_ind))
     THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',555);
   END IF;
--
-- Capping Ind
--
   IF (NOT s_dl_hem_utils.yorn(p1.lscb_apply_cap_ind))
     THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',556);
   END IF;
--
-- Complete Ind
--
   IF (NOT s_dl_hem_utils.yorn(p1.lscb_complete_ind))
     THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',557);
   END IF;
--
-- Rebateable Ind
--
   IF (NOT s_dl_hem_utils.yorn(p1.lscb_rebateable_ind))
     THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',558);
   END IF;
--
-- Extract from Repairs Ind
--
   IF (NOT s_dl_hem_utils.yorn(p1.lscb_extract_from_repairs_ind))
     THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',27);
   END IF;
--
-- Include Repairs from Properties Ind
--
   IF (NOT s_dl_hem_utils.yorn(p1.lscb_include_prop_repairs_ind))
     THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',28);
   END IF;
--
--Check coded fields
--
-- Check Service Charge Increase Type
--
   IF nvl(p1.lscb_increase_type, '^') NOT IN ('A', 'P', 'F')
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',559);
   END IF;
--
-- Check Service Charge Tax Ind
--
   IF nvl(p1.lscb_tax_ind, '^') NOT IN ('A', 'B')
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',560);
   END IF;
--
-- Check Service Charge Applicable Ind
--
   IF nvl(p1.lscb_charge_applicable_to, '^') NOT IN ('R', 'L', 'B')
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',561);
   END IF;
--
-- Check Service Charge Adjustment Method
--
   IF nvl(p1.lscb_adjustment_method, '^') NOT IN ('SYS', 'EXT', 'NO')
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',562);
   END IF;
--
-- Check Repair Component Level
--
   IF nvl(p1.lscb_component_level, '^') NOT IN ('JOB', 'WO')
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',26);
   END IF;
--
-- Check Service Admin period Date
--
   IF p1.lscb_nom_admin_period IS NOT NULL
     THEN
       IF p1.lscb_nom_admin_period < p1.lscb_scp_start_date
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',563);
       END IF;
   END IF;
--
-- Check Vat code is valid
--
   IF p1.lscb_vca_code is not null
     THEN
       OPEN  c_vat(p1.lscb_vca_code);
        FETCH c_vat INTO l_exists;
         IF c_vat%notfound
          THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',705);
         END IF;
       CLOSE c_vat;
   END IF;
--
-- Check Description is supplied
--
   IF p1.lscb_description IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',564);
   END IF;
--
-- Check Weighting Element is Valid if supplied
--
   IF p1.lscb_ele_code IS NOT NULL
    THEN
      OPEN c_ele_code(p1.lscb_ele_code);
       FETCH c_ele_code INTO l_exists;
        IF c_ele_code%notfound
         THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',565);
        END IF;
      CLOSE c_ele_code;
   END IF;
--
-- Check Admin Unit Type is Valid if supplied
--
   IF p1.lscb_auy_code IS NOT NULL
    THEN
      OPEN c_auy_type(p1.lscb_auy_code);
       FETCH c_auy_type INTO l_exists;
        IF c_auy_type%notfound
         THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',29);
        END IF;
      CLOSE c_auy_type;
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
,lscb_dlb_batch_id
,lscb_dl_seqno
,lscb_dl_load_status
,lscb_scp_code
,lscb_scp_start_date
,lscb_svc_att_ele_code
,lscb_svc_att_code
FROM dl_hsc_service_charge_bases
WHERE lscb_dlb_batch_id     = p_batch_id
  AND lscb_dl_load_status   = 'C';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HSC_SERVICE_CHARGE_BASES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i integer := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hsc_service_charge_bases.dataload_delete');
fsc_utils.debug_message( 's_dl_hsc_service_charge_bases.dataload_delete',3 );
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
cs := p1.lscb_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
      DELETE FROM service_charge_bases
      WHERE  scb_scp_code         = p1.lscb_scp_code
      AND    scb_scp_start_date   = p1.lscb_scp_start_date
      AND    scb_svc_att_ele_code = p1.lscb_svc_att_ele_code
      AND    scb_svc_att_code     = p1.lscb_svc_att_code;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SERVICE_CHARGE_BASES');
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
END s_dl_hsc_service_charge_bases;
/


