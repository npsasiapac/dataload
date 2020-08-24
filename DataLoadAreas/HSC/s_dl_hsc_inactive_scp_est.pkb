CREATE OR REPLACE PACKAGE BODY s_dl_hsc_inactive_scp_est
AS
-- ***********************************************************************

  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION     WHO  WHEN       WHY
  --      1.0     MTR  23/11/01   Dataload
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
  UPDATE dl_hsc_inactive_scp_est
  SET lise_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_inactive_scp_est');
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
  rowid rec_rowid,
  lise_dlb_batch_id,
  lise_dl_seqno,
  lise_dl_load_status,
  lise_prorefno_auncode,
  lise_pro_aun_type,
  lise_scb_scp_code,
  lise_scb_scp_start_date,
  lise_scb_svc_att_ele_code,
  lise_scb_svc_att_code,
  lise_amount,
  lise_oride_weighting_tot,
  lise_reconciled_ind
FROM dl_hsc_inactive_scp_est
WHERE lise_dlb_batch_id    = p_batch_id
AND   lise_dl_load_status = 'V';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HSC_INACTIVE_SCP_EST';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
i           INTEGER := 0;
l_wgt_val_sum             NUMBER(8,2);
r_service_charge_rate     service_charge_rates%ROWTYPE;

--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_inactive_scp_est.dataload_create');
  fsc_utils.debug_message( 's_dl_hsc_inactive_scp_est.dataload_create',3);
  --
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  for p1 in c1(p_batch_id) loop
    --
    BEGIN
      --
      cs := p1.lise_dl_seqno;
      l_id := p1.rec_rowid;
     -- 
      -- Check wether Service Charge Rate exists
      IF s_service_charge_rates.service_charge_rate_exists(
           p1.lise_prorefno_auncode,
           p1.lise_pro_aun_type,
           p1.lise_scb_scp_code,
           p1.lise_scb_scp_start_date,
       p1.lise_scb_svc_att_code, 
           p1.lise_scb_svc_att_ele_code)  
      THEN
        -- UPDATE existing Service Charge Rate
        UPDATE service_charge_rates
        SET  scr_estimated_amount = p1.lise_amount
        WHERE  scr_prorefno_auncode  = p1.lise_prorefno_auncode
        AND scr_pro_aun_type         = p1.lise_pro_aun_type
        AND scr_scb_scp_code         = p1.lise_scb_scp_code
        AND scr_scb_scp_start_date   = p1.lise_scb_scp_start_date
        AND scr_scb_svc_att_ele_code = p1.lise_scb_svc_att_ele_code
        AND scr_scb_svc_att_code     = p1.lise_scb_svc_att_code;

      ELSE           
        -- Process NEW Service Charge Rate
        s_service_charge_rates.create_service_charge_rate
         (p1.lise_prorefno_auncode,
          p1.lise_pro_aun_type,
          p1.lise_scb_scp_code,
          p1.lise_scb_scp_start_date,
          p1.lise_scb_svc_att_ele_code ,
          p1.lise_scb_svc_att_code, 
          p1.lise_amount,
          'DATALOAD',                              -- created_by ,
          TRUNC(sysdate),                          -- created_date ,
          NULL,                                    -- p1.lise_actual_amount ,
          NULL,                                    -- p1.lise_calcd_weighting_tot,
          NULL,                                    -- p1.lise_oride_weighting_tot,
          NULL,                                    -- modified_by,          
          NULL,                                    -- modified_date ,
          NULL,                                    -- p1.lise_void_loss_percentage, 
          'N' );                                   -- p1.lise_reconciled_ind );        
       
        s_pro_service_charge_rates.maint_pro_service_charge_rates
         (p1.lise_prorefno_auncode,
          p1.lise_pro_aun_type, 
          p1.lise_scb_scp_code,
          p1.lise_scb_scp_start_date, 
          p1.lise_scb_svc_att_ele_code,
          p1.lise_scb_svc_att_code ); 

        -- Following procedures copied from s_service_charge_bases.calc_wght_tot 
    -- validation for SCH-40 done in validate procedure (HDL-609)

        l_wgt_val_sum := s_pro_service_charge_rates.property_wgt_val_sum (
          p1.lise_prorefno_auncode,
          p1.lise_pro_aun_type,
          p1.lise_scb_scp_code,
          p1.lise_scb_svc_att_code,
          p1.lise_scb_svc_att_ele_code,
          p1.lise_scb_scp_start_date) ;

        r_service_charge_rate:= s_service_charge_rates.get_service_charge_rate(
          p1.lise_prorefno_auncode,
          p1.lise_pro_aun_type,
          p1.lise_scb_scp_code,
          p1.lise_scb_scp_start_date,
          p1.lise_scb_svc_att_ele_code,
          p1.lise_scb_svc_att_code);  
          
       --
       --  If the calculated weighting total for the selected SERVICE CHARGE RATE differs from the
       --  sum of the PROPERTY SERVICE CHARGE RATE weighting values, then update the SERVICE CHARGE RATE
       --  to this value.
       --

       IF NVL(r_service_charge_rate.scr_calcd_weighting_tot,0) <> NVL(l_wgt_val_sum,0) THEN
         s_service_charge_rates.update_service_charge_rate (
            p1.lise_prorefno_auncode,
            p1.lise_pro_aun_type,
            p1.lise_scb_scp_code,
            p1.lise_scb_scp_start_date,
            p1.lise_scb_svc_att_ele_code,
            p1.lise_scb_svc_att_code,
            p1.lise_amount,
            NULL,              --p1.lise_created_by,
            NULL,              --p1.lise_created_date,
            NULL,              --p1.lise_actual_amount,
            l_wgt_val_sum,
            p1.lise_oride_weighting_tot,
            'DATALOAD',
            TRUNC(sysdate),
            NULL,                        -- p1.lise_void_loss_percentage,
            p1.lise_reconciled_ind) ;

       END IF ;
              
       
      END IF;

      --
      -- keep a count of the rows processed and commit after every 1000
      --
      i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
      --
      s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
      set_record_status_flag(l_id,'C');
      --
      EXCEPTION
        WHEN OTHERS THEN
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
        set_record_status_flag(l_id,'O');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
    END LOOP;
  --
  -- Section to anayze the table(s) populated by this dataload
  --
  -- l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADMIN_GROUPINGS');
  --
  fsc_utils.proc_end;
  commit;
  --
EXCEPTION
  WHEN OTHERS THEN
  set_record_status_flag(l_id,'O');
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
CURSOR c1 is
SELECT
  rowid rec_rowid,
  lise_dlb_batch_id,
  lise_dl_seqno,
  lise_dl_load_status,
  lise_prorefno_auncode,
  lise_pro_aun_type,
  lise_scb_scp_code,
  lise_scb_scp_start_date,
  lise_scb_svc_att_ele_code,
  lise_scb_svc_att_code,
  lise_amount,
  lise_oride_weighting_tot,
  lise_reconciled_ind
FROM dl_hsc_inactive_scp_est
WHERE lise_dlb_batch_id      = p_batch_id
AND   lise_dl_load_status       in ('L','F','O');
--
CURSOR c_pro (p_pro_refno VARCHAR2,
              p_svc_att_ele_code VARCHAR2,
              p_svc_att_code VARCHAR2 ) is                  
SELECT NULL FROM  service_usages   
WHERE sus_pro_refno      = p_pro_refno
AND sus_svc_att_ele_code = p_svc_att_ele_code
AND sus_svc_att_code     = p_svc_att_code
AND sus_start_date < sysdate
AND NVL(sus_end_date, sysdate+1) > sysdate; 
--
CURSOR c_scr (p_pro_refno VARCHAR2,
              p_aun_type  VARCHAR2,
              p_scp_code  VARCHAR2,
              p_scp_start_date VARCHAR2,
              p_svc_att_ele_code VARCHAR2,
              p_svc_att_code VARCHAR2 ) is                  
SELECT NULL FROM  service_charge_rates   
WHERE scr_prorefno_auncode   = p_pro_refno
AND scr_scb_scp_code         = p_scp_code
AND scr_scb_scp_start_date   = p_scp_start_date
AND scr_scb_svc_att_ele_code = p_svc_att_ele_code
AND scr_scb_svc_att_code     = p_svc_att_code
AND scr_reconciled_ind       = 'Y'; 
--
CURSOR c_scc (p_pro_refno VARCHAR2,
              p_aun_type  VARCHAR2,
              p_scp_code  VARCHAR2,
              p_scp_start_date VARCHAR2,
              p_svc_att_ele_code VARCHAR2,
              p_svc_att_code VARCHAR2 ) is                  
SELECT NULL FROM  scr_components   
WHERE scc_scr_prorefno_auncode   = p_pro_refno
AND scc_scr_pro_aun_type         = p_aun_type
AND scc_scr_scb_scp_code         = p_scp_code
AND scc_scr_scb_scp_start_date   = p_scp_start_date
AND scc_scr_scb_svc_att_ele_code = p_svc_att_ele_code
AND scc_scr_scb_svc_att_code     = p_svc_att_code
AND scc_act_est_ind              = 'E'; 
--
CURSOR c_scb (p_scp_code  VARCHAR2,
              p_scp_start_date VARCHAR2,
              p_svc_att_ele_code VARCHAR2,
              p_svc_att_code VARCHAR2 ) is                  
SELECT NULL FROM  service_charge_bases   
WHERE scb_scp_code         = p_scp_code
AND scb_scp_start_date     = p_scp_start_date
AND scb_svc_att_ele_code   = p_svc_att_ele_code
AND scb_svc_att_code       = p_svc_att_code
AND scb_complete_ind       = 'N'; 

CURSOR c_hfi (p_prorefno_auncode VARCHAR2) is
SELECT hmv_map_value
  FROM hfi_segments hs,
       hfi_mappings hm,
       hfi_segment_codes hsc,
       hfi_segment_elements hse,
       hfi_segment_details hsd,
       hfi_rents_extract_types hre,
       hfi_mapping_values
 WHERE hs.hse_no                  = hsd.sgd_hse_no
   AND hsd.sgd_rxt_refno          = hre.rxt_refno
   AND hsd.sgd_hsc_control_code   = hsc.hsc_code -- control
   AND hsc.hsc_code               = hse.hsl_hsc_code
   AND hse.hsl_hmp_code           = hm.hmp_code (+)
   AND hre.rxt_hdt_code           = 'SERVICE'
   AND hre.rxt_hrv_ate_code       = 'SER'
   AND hmv_hmp_code               = hmp_code
   AND hmp_hmt_obj_name           = 'ADMIN_UNITS'
   AND hmp_code                   = hmv_hmp_code
   AND hmv_aun_code               = p_prorefno_auncode;
     
CURSOR c_hfi2 (p_svc_att_ele_code VARCHAR2) is
SELECT hmv_map_value
  FROM hfi_segments hs,
       hfi_mappings hm,
       hfi_segment_codes hsc,
       hfi_segment_elements hse,
       hfi_segment_details hsd,
       hfi_rents_extract_types hre,
       hfi_mapping_values
 WHERE hs.hse_no                  = hsd.sgd_hse_no
   AND hsd.sgd_rxt_refno          = hre.rxt_refno
   AND hsd.sgd_hsc_control_code   = hsc.hsc_code -- control
   AND hsc.hsc_code               = hse.hsl_hsc_code
   AND hse.hsl_hmp_code           = hm.hmp_code (+)
   AND hre.rxt_hdt_code           = 'SERVICE'
   AND hre.rxt_hrv_ate_code       = 'SER'
   AND hmv_hmp_code               = hmp_code
   AND hmp_hmt_obj_name           = 'ELEMENT'
   AND hmp_code                   = hmv_hmp_code
   AND hmv_ele_code               = p_svc_att_ele_code;
   
CURSOR c_hfi3 (p_svc_att_code VARCHAR2) is
SELECT hmv_map_value
  FROM hfi_segments hs,
       hfi_mappings hm,
       hfi_segment_codes hsc,
       hfi_segment_elements hse,
       hfi_segment_details hsd,
       hfi_rents_extract_types hre,
       hfi_mapping_values
 WHERE hs.hse_no                  = hsd.sgd_hse_no
   AND hsd.sgd_rxt_refno          = hre.rxt_refno
   AND hsd.sgd_hsc_control_code   = hsc.hsc_code -- control
   AND hsc.hsc_code               = hse.hsl_hsc_code
   AND hse.hsl_hmp_code           = hm.hmp_code (+)
   AND hre.rxt_hdt_code           = 'SERVICE'
   AND hre.rxt_hrv_ate_code       = 'SER'
   AND hmv_hmp_code               = hmp_code
   AND hmp_hmt_obj_name           = 'ATTRIBUTES'
   AND hmp_code                   = hmv_hmp_code
   AND hmv_att_code               = p_svc_att_code;   

--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HSC_INACTIVE_SCP_EST';
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
l_dummy             VARCHAR2(10);
l_is_inactive       BOOLEAN DEFAULT FALSE;                      
l_auncode  hfi_mapping_values.hmv_aun_code%TYPE := NULL;
l_ele_code hfi_mapping_values.hmv_ele_code%TYPE := NULL;
l_att_code hfi_mapping_values.hmv_att_code%TYPE := NULL;

--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_inactive_scp_est.dataload_validate');
  fsc_utils.debug_message( 's_dl_hsc_inactive_scp_est.dataload_validate',3);
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
    cs := p1.lise_dl_seqno;
    l_id := p1.rec_rowid;
    --
    l_errors := 'V';
    l_error_ind := 'N';
    --
  l_auncode   := p1.lise_prorefno_auncode;
    l_ele_code  := p1.lise_scb_svc_att_ele_code;
    l_att_code  := p1.lise_scb_svc_att_code;

    -- Check the Status Of the relevant Service Charge Period is Inactive
    IF s_service_charge_periods.is_period_inactive(p1.lise_scb_scp_code, p1.lise_scb_scp_start_date)    
    THEN
    -- If applicable MAP Admin Unit/Element-Attribute before following validation
    IF fsc_utils.area_is_configured('HFI')
    THEN
      -- MAP Admin Unit
        IF l_auncode IS NOT NULL 
    AND p1.lise_pro_aun_type = 'A'
        THEN 
      l_auncode := s_dl_hsc_utils.map_admin_unit(l_auncode);
      IF l_auncode IS NULL
      THEN
        l_auncode := p1.lise_prorefno_auncode;
            l_errors  := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',610);
        END IF;           
    END IF;
  
    -- MAP Element
    IF p1.lise_scb_svc_att_ele_code IS NOT NULL
        THEN 
      l_ele_code := s_dl_hsc_utils.map_element(l_ele_code);
      IF l_ele_code IS NULL
      THEN
        l_ele_code := p1.lise_scb_svc_att_ele_code;
            l_errors   :=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',611);  
      END IF;       
    END IF;
    
    -- MAP Attribute
    IF l_att_code IS NOT NULL
        THEN 
      l_att_code := s_dl_hsc_utils.map_attribute(l_att_code);
      IF l_att_code IS NULL
      THEN
        l_att_code := p1.lise_scb_svc_att_code;
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',612);           
      END IF;     
    END IF; 
      
      END IF;
  
      -- Check Property has indicated Service
      IF p1.lise_pro_aun_type = 'P'
      AND l_auncode IS NOT NULL
      THEN
        OPEN c_pro(l_auncode,
                   l_ele_code,
                   l_att_code); 
        FETCH c_pro INTO l_dummy;    
        IF c_pro%notfound THEN          
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',601);            
        END IF;                          
        CLOSE c_pro;   
      END IF;
      
      -- Has Service Charge Rate been reconciled?
      OPEN c_scr(l_auncode,
                 p1.lise_pro_aun_type,
                 p1.lise_scb_scp_code,
                 p1.lise_scb_scp_start_date,
                 l_ele_code,
                 l_att_code); 
      FETCH c_scr INTO l_dummy;    
      IF c_scr%found THEN          
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',602);            
      END IF; 
      CLOSE c_scr;           
        
      -- Does Service Charge Rate have associated components?
      OPEN c_scc(l_auncode,
                 p1.lise_pro_aun_type,
                 p1.lise_scb_scp_code,
                 p1.lise_scb_scp_start_date,
                 l_ele_code,
                 l_att_code); 
      FETCH c_scc INTO l_dummy;    
      IF c_scc%found THEN          
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',603);            
      END IF; 
      CLOSE c_scc;  
        
      -- Has the associated Service Charge Basis been completed?
      OPEN c_scb(p1.lise_scb_scp_code,
                 p1.lise_scb_scp_start_date,
                 l_ele_code,
                 l_att_code); 
      FETCH c_scb INTO l_dummy;    
      IF c_scb%notfound THEN          
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',604);            
      END IF; 
      CLOSE c_scb;   
    
    -- Do PSCR rate conflicts exist for an Apportioned SCB
      IF s_service_charge_bases.scb_should_be_apportioned(
           p1.lise_scb_scp_code,
           p1.lise_scb_scp_start_date,
           l_ele_code,
           l_att_code )     
    THEN
      IF s_pro_service_charge_rates.rate_conflicts_exist ( 
           p1.lise_scb_scp_code,
           l_att_code,
           l_ele_code,
           p1.lise_scb_scp_start_date)       
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',609); 
      
    END IF;   
      
      END IF;        

    ELSE          
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',607); 
      l_is_inactive := FALSE;           
    END IF;
      
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
    --
    s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);    
    set_record_status_flag(l_id,l_errors);
  
  -- If record is valid, update DL table values to mapped values
    IF l_errors = 'V' AND
  fsc_utils.area_is_configured('HFI')
    THEN  
      UPDATE dl_hsc_inactive_scp_est
      SET lise_prorefno_auncode     = l_auncode,
          lise_scb_svc_att_ele_code = l_ele_code,
          lise_scb_svc_att_code     = l_att_code
      WHERE rowid = p1.rec_rowid;
    END IF;   
    --
    EXCEPTION
      WHEN OTHERS THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
    END;
    --
  END LOOP;
  --
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
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 is
SELECT
  rowid rec_rowid
  ,lise_dlb_batch_id
  ,lise_dl_seqno
  ,lise_dl_load_status,
  lise_prorefno_auncode,
  lise_pro_aun_type,
  lise_scb_scp_code,
  lise_scb_scp_start_date,
  lise_scb_svc_att_ele_code,
  lise_scb_svc_att_code,
  lise_amount,
  lise_oride_weighting_tot,
  lise_reconciled_ind
FROM  DL_HSC_INACTIVE_SCP_EST
WHERE lise_dlb_batch_id   = p_batch_id
AND   lise_dl_load_status = 'C';

-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HSC_INACTIVE_SCP_EST';
cs       INTEGER;
l_id     ROWID;
--
i        INTEGER := 0;
--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hsc_inactive_scp_est.dataload_delete');
  fsc_utils.debug_message( 's_dl_hsc_inactive_scp_est.dataload_delete',3 );
  --
  cp := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  FOR p1 IN c1 LOOP
    --
    cs := p1.lise_dl_seqno;
    l_id := p1.rec_rowid;
    i := i +1;
    --
    NULL;
    /*
    DELETE FROM ADMIN_GROUPINGS
    WHERE agr_aun_code_child    = p1.lise_aun_code_child;
    */
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
  END LOOP;
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
END s_dl_hsc_inactive_scp_est;
/

