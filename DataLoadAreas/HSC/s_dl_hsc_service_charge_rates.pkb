CREATE OR REPLACE PACKAGE BODY s_dl_hsc_service_charge_rates
AS
-- ***********************************************************************

  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION DB Ver WHO  WHEN       WHY
  --      1.0        PJD  04-12-01   Dataload
  --      2.0 5.3.0  PJD  04/02/03   Default 'Created By' field to DATALOAD
  --      3.0 5.5.0  IR   26-JUL-04  Certain areas referred to hra_ instead of hsc_
  --      3.1 5.7.0  PH   12-JAN-05  Added new fields for 570 release
  --                                 to create, no validate
  --      3.2 5.10.0 PH   21-SEP-06  Added additional validates
  --      3.3 5.10.0 PH   29-NOV-06  Amended above error codes from HDL to HD1
  --      3.4 5.10.0 PH   05-MAR-07  Amended validate on reconciled ind as
  --                                 it's a mandatory field.
  --      3.5 5.11.0 PH   16-JUL-07  Amended c_scr_exists cursor on validate as it
  --                                 passed pro_propref into pro_refno field.
  --                                 Split cursor into UNION statement.
  --      4.0 5.13.0 PH   06-FEB-2008 Now includes its own
  --                                  set_record_status_flag procedure.
  --      4.1 6.9    PJD  21-MAR-2014 Do some deletes, updates and inserts
  --                                  to Service Usages
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
  UPDATE dl_hsc_service_charge_rates
  SET lscr_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_service_charge_rates');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
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
,lscr_dlb_batch_id
,lscr_dl_seqno
,lscr_dl_load_status
,lscr_propref_auncode
,lscr_pro_aun_type
,lscr_scb_scp_code
,lscr_scb_scp_start_date
,lscr_scb_svc_att_ele_code
,lscr_scb_svc_att_code
,lscr_estimated_amount
,lscr_actual_amount
,lscr_oride_weighting_tot
,lscr_void_loss_percentage
,lscr_reconciled_ind
,lscr_actual_calc_weight_tot
,lscr_actual_oride_weight_tot
FROM dl_hsc_service_charge_rates
WHERE lscr_dlb_batch_id    = p_batch_id
AND   lscr_dl_load_status = 'V';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HSC_SERVICE_CHARGE_RATES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
i                    INTEGER := 0;
l_answer             VARCHAR2(1);
l_pro_refno          NUMBER;
l_pro_aun_code       VARCHAR2(20);
l_an_tab             VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_hsc_service_charge_rates.dataload_create');
fsc_utils.debug_message( 's_hsc_service_charge_rates.dataload_create',3);
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
  cs := p1.lscr_dl_seqno;
  l_id := p1.rec_rowid;
  --
  l_pro_aun_code := p1.lscr_propref_auncode;
  --
  IF p1.lscr_pro_aun_type = 'P'
  THEN
    --
    l_pro_refno := null;
    --
    l_pro_refno := s_properties.get_refno_for_propref
                                (p1.lscr_propref_auncode);
    l_pro_aun_code := TO_CHAR(l_pro_refno);
--
  END IF;
  --
  INSERT INTO SERVICE_CHARGE_RATES
  (
   scr_prorefno_auncode
  ,scr_pro_aun_type
  ,scr_scb_scp_code
  ,scr_scb_scp_start_date
  ,scr_scb_svc_att_ele_code
  ,scr_scb_svc_att_code
  ,scr_estimated_amount
  ,scr_created_by
  ,scr_created_date
  ,scr_actual_amount
  ,scr_oride_weighting_tot
  ,scr_void_loss_percentage
  ,scr_reconciled_ind
  ,scr_actual_calc_weight_tot
  ,scr_actual_oride_weight_tot
  )
  VALUES
  (l_pro_aun_code
  ,p1.lscr_pro_aun_type
  ,p1.lscr_scb_scp_code
  ,p1.lscr_scb_scp_start_date
  ,p1.lscr_scb_svc_att_ele_code
  ,p1.lscr_scb_svc_att_code
  ,p1.lscr_estimated_amount
  ,'DATALOAD'
  ,trunc(sysdate)
  ,p1.lscr_actual_amount
  ,p1.lscr_oride_weighting_tot
  ,p1.lscr_void_loss_percentage
  ,p1.lscr_reconciled_ind
  ,p1.lscr_actual_calc_weight_tot
  ,p1.lscr_actual_oride_weight_tot
  );
  --
  IF p1.lscr_pro_aun_type = 'P'
  THEN
    DELETE FROM service_usages
    WHERE  sus_svc_att_ele_code = p1.lscr_scb_svc_att_ele_code
      AND  sus_start_Date       > p1.lscr_scb_scp_start_date -1
      AND  sus_pro_refno        = TO_NUMBER(l_pro_aun_code)
    ;

    UPDATE service_usages
    SET    sus_end_date         =  p1.lscr_scb_scp_start_date -1
    WHERE  sus_svc_att_ele_code = p1.lscr_scb_svc_att_ele_code
      AND  NVL(sus_end_Date,p1.lscr_scb_scp_start_date ) > p1.lscr_scb_scp_start_date -1
      AND  sus_pro_refno        = TO_NUMBER(l_pro_aun_code)
    ;

    INSERT INTO service_usages
    (sus_pro_refno
    ,sus_svc_att_ele_code
    ,sus_svc_att_code
    ,sus_start_date
    ,sus_created_by
    ,sus_created_date
    ,sus_origine
    ,sus_chargeable_ind) VALUES
    (TO_NUMBER(l_pro_aun_code)
    ,p1.lscr_scb_svc_att_ele_code
    ,p1.lscr_scb_svc_att_code
    ,p1.lscr_scb_scp_start_date
    ,USER
    ,SYSDATE
    ,'P'
    ,'Y'
    );

 END IF;
  --
  --
  -- keep a count of the rows processed and commit after every 1000
  --
 i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
 s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
 set_record_status_flag(l_id,'C');
--
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SERVICE_CHARGE_RATES');
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
,lscr_dlb_batch_id
,lscr_dl_seqno
,lscr_dl_load_status
,lscr_propref_auncode
,lscr_pro_aun_type
,lscr_scb_scp_code
,lscr_scb_scp_start_date
,lscr_scb_svc_att_ele_code
,lscr_scb_svc_att_code
,lscr_estimated_amount
,lscr_actual_amount
,lscr_oride_weighting_tot
,lscr_void_loss_percentage
,lscr_reconciled_ind
FROM dl_hsc_service_charge_rates
WHERE lscr_dlb_batch_id    =   p_batch_id
AND   lscr_dl_load_status  in ('L','F','O');
--
CURSOR c_scp(p_scp_code   VARCHAR2
            ,p_scp_start_date DATE) IS
SELECT 'X'
FROM   service_charge_periods
WHERE  scp_code       = p_scp_code
  AND  scp_start_date = p_scp_start_date;
--
--
CURSOR c_att(p_ele_code VARCHAR2, p_att_code VARCHAR2) IS
SELECT null
FROM attributes
WHERE att_ele_code = p_ele_code
AND   att_code     = p_att_code;
--
CURSOR c_scb_exists (p_scr_scb_scp_start_date     DATE,
                     p_scr_scb_scp_code           VARCHAR2,
                     p_scr_scb_svc_att_ele_code   VARCHAR2,
                     p_scr_scb_svc_att_code       VARCHAR2) IS
SELECT 'X'
FROM   service_charge_bases
WHERE  scb_scp_start_date     = p_scr_scb_scp_start_date
AND    scb_scp_code           = p_scr_scb_scp_code
AND    scb_svc_att_ele_code   = p_scr_scb_svc_att_ele_code
AND    scb_svc_att_code       = p_scr_scb_svc_att_code;
--
CURSOR c_scr_exists (p_scr_propref_auncode        VARCHAR2,
                     p_scr_pro_aun_type           VARCHAR2,
                     p_scr_scb_scp_start_date     DATE,
                     p_scr_scb_scp_code           VARCHAR2,
                     p_scr_scb_svc_att_ele_code   VARCHAR2,
                     p_scr_scb_svc_att_code       VARCHAR2) IS
SELECT 'X'
FROM   service_charge_rates,
       properties
WHERE  scr_prorefno_auncode       = to_char(pro_refno)
AND    pro_propref                = p_scr_propref_auncode
AND    scr_pro_aun_type           = p_scr_pro_aun_type
AND    scr_scb_scp_start_date     = p_scr_scb_scp_start_date
AND    scr_scb_scp_code           = p_scr_scb_scp_code
AND    scr_scb_svc_att_ele_code   = p_scr_scb_svc_att_ele_code
AND    scr_scb_svc_att_code       = p_scr_scb_svc_att_code
AND    scr_pro_aun_type           = 'P'
UNION
SELECT 'X'
FROM   service_charge_rates
WHERE  scr_prorefno_auncode       = p_scr_propref_auncode
AND    scr_pro_aun_type           = p_scr_pro_aun_type
AND    scr_scb_scp_start_date     = p_scr_scb_scp_start_date
AND    scr_scb_scp_code           = p_scr_scb_scp_code
AND    scr_scb_svc_att_ele_code   = p_scr_scb_svc_att_ele_code
AND    scr_scb_svc_att_code       = p_scr_scb_svc_att_code
AND    scr_pro_aun_type           = 'A';

--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HSC_SERVICE_CHARGE_RATES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_ele_type       VARCHAR2(2);
l_ele_value_type VARCHAR2(1);
--
-- Other Variables
--
l_rac_aun_code  admin_units.aun_code%TYPE;
l_tcy_refno     tenancies.tcy_refno%TYPE;
l_pro_refno     properties.pro_refno%TYPE;
l_dbr           VARCHAR2(1);
l_ety_attr_type elements.ele_usage%TYPE;
l_ety_type      elements.ele_type%TYPE;
l_answer        VARCHAR2(1);
--
BEGIN
--
-- dbms_output.put_line('STARTING');
--
fsc_utils.proc_start('s_hsc_service_charge_rates.dataload_validate');
fsc_utils.debug_message( 's_hsc_service_charge_rates.dataload_validate',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- check if a property element may be created FROM this batch
--
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
FOR p1 IN c1 LOOP
--
BEGIN
--
  cs := p1.lscr_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors := 'V';
  l_error_ind := 'N';
--
  -- First Check is on the Propref / Auncode
  IF p1.lscr_pro_aun_type != 'A' THEN
--
    l_pro_refno := NULL;
    l_pro_refno := s_dl_hem_utils.pro_refno_FOR_propref
                                  (p1.lscr_propref_auncode);
--
    IF l_pro_refno is NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
    END IF;
  ELSE
    IF (NOT s_dl_hem_utils.exists_aun_code(p1.lscr_propref_auncode))
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
    END IF;
--
  END IF;
  --
  -- Check the Service Charge Period
  l_exists := NULL;
  OPEN c_scp(p1.lscr_scb_scp_code,p1.lscr_scb_scp_start_date);
  FETCH c_scp INTO l_exists;
  CLOSE c_scp;
  IF l_exists IS NULL
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',879);
  END IF;
  --
  --
  -- Check the element code exists on ELEMENTS
  --
  l_exists := NULL;
  OPEN c_att(p1.lscr_scb_svc_att_ele_code
            ,p1.lscr_scb_svc_att_code);
  FETCH c_att into l_exists;
  IF c_att%notfound
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',041);
  END IF;
  CLOSE c_att;
  --
  -- Check the Y/N columns
  --
  -- Reconciled Indicator
  --
  IF ( nvl(p1.lscr_reconciled_ind, 'X') NOT IN ('Y','N') )
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',880);
  END IF;
  --
 -- Check the Mandatory Columns
  --
  -- Estimated Value
  --
  IF ( p1.lscr_estimated_amount IS NULL)
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',881);
  END IF;
  --
  -- Additional Validates added 21-SEP-2006
  --
  -- Check the Service Charge Basis exists
  --
     OPEN c_scb_exists (p1.lscr_scb_scp_start_date,   p1.lscr_scb_scp_code
                       ,p1.lscr_scb_svc_att_ele_code, p1.lscr_scb_svc_att_code);
      FETCH c_scb_exists INTO l_exists;
       IF c_scb_exists%notfound
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',189);
       END IF;
     CLOSE c_scb_exists;
  --
  -- Check a record doesn't already exist on service_charge_rates
  --
     OPEN c_scr_exists (p1.lscr_propref_auncode,      p1.lscr_pro_aun_type
                       ,p1.lscr_scb_scp_start_date,   p1.lscr_scb_scp_code
                       ,p1.lscr_scb_svc_att_ele_code, p1.lscr_scb_svc_att_code);
      FETCH c_scr_exists INTO l_exists;
       IF c_scr_exists%found
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',190);
       END IF;
     CLOSE c_scr_exists;
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
--   EXCEPTION
--      WHEN OTHERS THEN
--      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
--      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--      set_record_status_flag(l_id,'O');
END;
--
END LOOP;
COMMIT;
--
fsc_utils.proc_END;
--
--   EXCEPTION
--      WHEN OTHERS THEN
--      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
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
,lscr_dlb_batch_id
,lscr_dl_seqno
,lscr_dl_load_status
,lscr_propref_auncode
,lscr_pro_aun_type
,lscr_scb_scp_code
,lscr_scb_scp_start_date
,lscr_scb_svc_att_ele_code
,lscr_scb_svc_att_code
,lscr_estimated_amount
,lscr_actual_amount
,lscr_oride_weighting_tot
,lscr_void_loss_percentage
,lscr_reconciled_ind
FROM dl_hsc_service_charge_rates
WHERE lscr_dlb_batch_id    = p_batch_id
AND   lscr_dl_load_status = 'C';
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HSC_SERVICE_CHARGE_RATES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--

i                INTEGER := 0;
l_pro_refno      NUMBER;
l_pro_aun_code   VARCHAR2(20);
l_an_tab         VARCHAR2(1);

BEGIN
--
fsc_utils.proc_start('s_hsc_service_charge_rates.dataload_delete');
fsc_utils.debug_message( 's_hsc_service_charge_rates.dataload_delete',3 );
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
 cs := p1.lscr_dl_seqno;
 l_id := p1.rec_rowid;
--
 i := i +1;
--
l_pro_aun_code := p1.lscr_propref_auncode;
  --
  IF p1.lscr_pro_aun_type = 'P'
  THEN
    --
    l_pro_refno := null;
    --
    l_pro_refno := s_properties.get_refno_for_propref
                                (p1.lscr_propref_auncode);
    l_pro_aun_code := TO_CHAR(l_pro_refno);
  --
  END IF;
  --
  DELETE FROM service_charge_rates
   WHERE scr_prorefno_auncode     = l_pro_aun_code
     AND scr_pro_aun_type         = p1.lscr_pro_aun_type
     AND scr_scb_scp_code         = p1.lscr_scb_scp_code
     AND scr_scb_scp_start_date   = p1.lscr_scb_scp_start_date
     AND scr_scb_svc_att_ele_code = p1.lscr_scb_svc_att_ele_code
     AND scr_scb_svc_att_code     = p1.lscr_scb_svc_att_code
     AND scr_scb_scp_start_date   = p1.lscr_scb_scp_start_date;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SERVICE_CHARGE_RATES');
--
fsc_utils.proc_end;
commit;
--
   EXCEPTION
      WHEN OTHERS THEN
      set_record_status_flag(l_id,'O');
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_delete;
--
--
END s_dl_hsc_service_charge_rates;

/



