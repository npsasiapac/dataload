CREATE OR REPLACE PACKAGE BODY s_dl_hpm_deliverable_cmpts
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VER		DB Ver	WHO	WHEN		  WHY
--  1.0		5.16	VRS  	09-SEP-2009   Product Dataload
--  1.1         5.16    PH      23-OCT-2009   Completed Dataload
--  1.2         6.9     AJ      Feb    2014   Minor Validation improvements
--  1.3         6.90    MB      20-MAR-2014   Make budget check conditional on one being supplied
--  1.4         6.11    MJK     24-AUG-2015   Reformatted for 6.11.  No logic changes
--
-- ***********************************************************************
--
  PROCEDURE set_record_status_flag
    (p_rowid  IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_hpm_deliverable_cmpts
    SET    ldcp_dl_load_status = p_status
    WHERE  rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_hpm_deliverable_cmpts');
    RAISE;
  END set_record_status_flag;
  PROCEDURE dataload_create
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  AS
    CURSOR c1(p_batch_id VARCHAR2)
    IS
      SELECT rowid rec_rowid
      ,      ldcp_dlb_batch_id
      ,      ldcp_dl_seqno
      ,      ldcp_dl_load_status
      ,      ldcp_refno
      ,      ldcp_cnt_reference
      ,      ldcp_cad_pro_aun_code
      ,      ldcp_cad_type_ind
      ,      ldcp_dve_std_code
      ,      ldcp_sco_code
      ,      ldcp_status_date
      ,      NVL(ldcp_created_by,'DATALOAD')        ldcp_created_by
      ,      NVL(ldcp_created_date,SYSDATE)         ldcp_created_date
      ,      ldcp_actual_end_date
      ,      ldcp_authorised_by
      ,      ldcp_authorised_date
      ,      ldcp_prev_sco_code
      ,      ldcp_prev_status_date
      ,      ldcp_dcv_version_number
      ,      ldcp_dcv_display_sequence
      ,      ldcp_dcv_reusable_refno
      ,      NVL(ldcp_dcv_current_ind,'Y')          ldcp_dcv_current_ind
      ,      NVL(ldcp_dcv_created_by,'DATALOAD')    ldcp_dcv_created_by
      ,      NVL(ldcp_dcv_created_date,SYSDATE)     ldcp_dcv_created_date
      ,      ldcp_dcv_sor_code
      ,      ldcp_dcv_bhe_code
      ,      ldcp_dcv_bca_year
      ,      ldcp_dcv_vca_code
      ,      ldcp_dcv_description
      ,      ldcp_dcv_quantity
      ,      ldcp_dcv_hrv_pmu_code
      ,      ldcp_dcv_unit_cost
      ,      ldcp_dcv_estimated_cost
      ,      ldcp_dcv_planned_start_date
      ,      ldcp_dcv_projected_cost
      ,      ldcp_dcv_hrv_loc_code
      FROM   dl_hpm_deliverable_cmpts
      WHERE  ldcp_dlb_batch_id = p_batch_id
      AND    ldcp_dl_load_status = 'V';
    CURSOR c_dlv_refno(p_cnt_reference VARCHAR2,p_pro_aun_code VARCHAR2,p_pro_aun_type VARCHAR2,p_std_code VARCHAR2)
    IS
      SELECT dlv_refno
      ,      dve_version_number
      FROM   deliverables
      ,      deliverable_versions
      WHERE  dlv_refno = dve_dlv_refno
      AND    dlv_cnt_reference = p_cnt_reference
      AND    dlv_cad_pro_aun_code = p_pro_aun_code
      AND    dlv_cad_type_ind = p_pro_aun_type
      AND    dve_std_code = p_std_code
      AND    dve_current_ind = 'Y';
    CURSOR c_reusable_refno
    IS
      SELECT reusable_refno_seq.nextval
      FROM   dual;
    CURSOR c_bhe_refno(p_bhe_code VARCHAR2)
    IS
      SELECT bhe_refno
      FROM   budget_heads
      WHERE  bhe_code = p_bhe_code;
    CURSOR c_bud_refno(p_bhe_refno NUMBER, p_bca_year NUMBER)
    IS
      SELECT bud_refno
      FROM   budgets
      WHERE  bud_bhe_refno = p_bhe_refno
      AND    bud_bca_year = p_bca_year;
    CURSOR get_sor_details(p_sor_code VARCHAR2)
    IS
      SELECT sor_type
      ,      sor_description
      FROM   schedule_of_rates
      WHERE  sor_code = p_sor_code;
    CURSOR c_pro_refno(p_pro_propref VARCHAR2)
    IS
      SELECT pro_refno
      FROM   properties
      WHERE  pro_propref = p_pro_propref;
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'CREATE';
    ct VARCHAR2(30) := 'DL_HPM_DELIVERABLE_CMPTS';
    cs INTEGER;
    ce VARCHAR2(200);
    ci INTEGER;
    l_pro_refno properties.pro_refno%type;
    l_cad_pro_aun_code admin_units.aun_code%type;
    l_scs_refno NUMBER(8);
    l_dlv_refno deliverables.dlv_refno%type;
    l_dve_version_number deliverable_versions.dve_version_number%type;
    l_dcv_reusable_refno deliverable_cmpt_versions.dcv_reusable_refno%type;
    l_bud_refno budgets.bud_refno%type;
    l_bhe_refno budget_heads.bhe_refno%type;
    i                 INTEGER := 0;
    l_an_tab          VARCHAR2(1);
    l_sor_type        VARCHAR2(1);
    l_sor_description VARCHAR2(4000);
    l_sor_desc        VARCHAR2(4000);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_deliverable_cmpts.dataload_create');
    fsc_utils.debug_message('s_dl_hpm_deliverable_cmpts.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.ldcp_dl_seqno;
        l_dcv_reusable_refno := NULL;
        l_dlv_refno := NULL;
        l_dve_version_number := NULL;
        l_bhe_refno := NULL;
        l_bud_refno := NULL;
        l_cad_pro_aun_code := NULL;
        OPEN c_reusable_refno;
        FETCH c_reusable_refno INTO l_dcv_reusable_refno;
        CLOSE c_reusable_refno;
        OPEN c_bhe_refno(p1.ldcp_dcv_bhe_code);
        FETCH c_bhe_refno INTO l_bhe_refno;
        CLOSE c_bhe_refno;
        OPEN c_bud_refno(l_bhe_refno,p1.ldcp_dcv_bca_year);
        FETCH c_bud_refno INTO l_bud_refno;
        CLOSE c_bud_refno;
        IF (p1.ldcp_cad_type_ind = 'P') 
        THEN
          OPEN c_pro_refno(p1.ldcp_cad_pro_aun_code);
          FETCH c_pro_refno INTO l_cad_pro_aun_code;
          CLOSE c_pro_refno;
        ELSE
          l_cad_pro_aun_code := p1.ldcp_cad_pro_aun_code;
        END IF;
        OPEN c_dlv_refno(p1.ldcp_cnt_reference,l_cad_pro_aun_code,p1.ldcp_cad_type_ind,p1.ldcp_dve_std_code);
        FETCH c_dlv_refno INTO l_dlv_refno,l_dve_version_number;
        CLOSE c_dlv_refno;
        SAVEPOINT SP1;
        INSERT INTO deliverable_components
          (dcp_refno
          ,dcp_dlv_refno
          ,dcp_sco_code
          ,dcp_status_date
          ,dcp_created_by
          ,dcp_created_date
          ,dcp_cad_cnt_reference
          ,dcp_cad_pro_aun_code
          ,dcp_cad_type_ind
          ,dcp_actual_end_date
          ,dcp_authorised_by
          ,dcp_authorised_date
          ,dcp_prev_sco_code
          ,dcp_prev_status_date
          )
          VALUES
          (p1.ldcp_refno
          ,l_dlv_refno
          ,p1.ldcp_sco_code
          ,p1.ldcp_status_date
          ,p1.ldcp_created_by
          ,p1.ldcp_created_date
          ,p1.ldcp_cnt_reference
          ,l_cad_pro_aun_code
          ,p1.ldcp_cad_type_ind
          ,p1.ldcp_actual_end_date
          ,p1.ldcp_authorised_by
          ,p1.ldcp_authorised_date
          ,p1.ldcp_prev_sco_code
          ,p1.ldcp_prev_status_date
          );
        IF p1.ldcp_created_date < TRUNC(SYSDATE) 
        THEN
          UPDATE deliverable_components
          SET    dcp_created_Date = p1.ldcp_created_date
          ,      dcp_created_by = p1.ldcp_created_by
          WHERE  dcp_refno = p1.ldcp_refno;
        END IF;
        INSERT INTO deliverable_cmpt_versions
        (dcv_dcp_refno  
        ,dcv_version_number  
        ,dcv_display_sequence  
        ,dcv_reusable_refno  
        ,dcv_current_ind  
        ,dcv_created_by  
        ,dcv_created_date  
        ,dcv_sor_code  
        ,dcv_bud_refno  
        ,dcv_vca_code  
        ,dcv_description  
        ,dcv_quantity  
        ,dcv_hrv_pmu_code  
        ,dcv_unit_cost  
        ,dcv_estimated_cost  
        ,dcv_planned_start_date  
        ,dcv_projected_cost  
        ,dcv_hrv_loc_code  
        )  
        VALUES  
        (p1.ldcp_refno  
        ,p1.ldcp_dcv_version_number  
        ,p1.ldcp_dcv_display_sequence  
        ,l_dcv_reusable_refno  
        ,p1.ldcp_dcv_current_ind  
        ,p1.ldcp_dcv_created_by  
        ,p1.ldcp_dcv_created_date  
        ,p1.ldcp_dcv_sor_code  
        ,l_bud_refno  
        ,p1.ldcp_dcv_vca_code  
        ,p1.ldcp_dcv_description  
        ,p1.ldcp_dcv_quantity  
        ,p1.ldcp_dcv_hrv_pmu_code  
        ,p1.ldcp_dcv_unit_cost  
        ,p1.ldcp_dcv_estimated_cost  
        ,p1.ldcp_dcv_planned_start_date  
        ,p1.ldcp_dcv_projected_cost  
        ,p1.ldcp_dcv_hrv_loc_code  
        );  
        IF p1.ldcp_dcv_created_date < TRUNC(SYSDATE) 
        THEN
          UPDATE deliverable_cmpt_versions
          SET    dcv_created_Date = p1.ldcp_dcv_created_date
          ,      dcv_created_by = p1.ldcp_dcv_created_by
          WHERE  dcv_dcp_refno = p1.ldcp_refno
          AND    dcv_version_number = p1.ldcp_dcv_version_number;
        END IF;
        INSERT INTO deliv_version_mappings
        (dvm_dve_dlv_refno  
        ,dvm_dve_version_number  
        ,dvm_dcv_dcp_refno  
        ,dvm_dcv_version_number  
        )  
        VALUES  
        (l_dlv_refno  
        ,l_dve_version_number  
        ,p1.ldcp_refno  
        ,p1.ldcp_dcv_version_number  
        );  
        i := i + 1;
        IF MOD(i,1000) = 0 
        THEN
          COMMIT;
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        s_dl_utils.set_record_status_flag(ct,cb,cs,'C');
      EXCEPTION
      WHEN OTHERS 
      THEN
        ROLLBACK TO SP1;
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
        s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
    END LOOP;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('DELIVERABLE_COMPONENTS');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('DELIVERABLE_CMPT_VERSIONS');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('DELIV_VERSION_MAPPINGS');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_create;
  --
  PROCEDURE dataload_validate
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  AS
    CURSOR c1
      (p_batch_id VARCHAR2)
    IS
      SELECT rowid rec_rowid
      ,      ldcp_dlb_batch_id
      ,      ldcp_dl_seqno
      ,      ldcp_dl_load_status
      ,      ldcp_refno
      ,      ldcp_cnt_reference
      ,      ldcp_cad_pro_aun_code
      ,      ldcp_cad_type_ind
      ,      ldcp_dve_std_code
      ,      ldcp_sco_code
      ,      ldcp_status_date
      ,      NVL(ldcp_created_by,'DATALOAD')        ldcp_created_by
      ,      NVL(ldcp_created_date,SYSDATE)         ldcp_created_date
      ,      ldcp_actual_end_date
      ,      ldcp_authorised_by
      ,      ldcp_authorised_date
      ,      ldcp_prev_sco_code
      ,      ldcp_prev_status_date
      ,      ldcp_dcv_version_number
      ,      ldcp_dcv_display_sequence
      ,      ldcp_dcv_reusable_refno
      ,      ldcp_dcv_current_ind
      ,      NVL(ldcp_dcv_created_by,'DATALOAD')    ldcp_dcv_created_by
      ,      NVL(ldcp_dcv_created_date,SYSDATE)     ldcp_dcv_created_date
      ,      ldcp_dcv_sor_code
      ,      ldcp_dcv_bhe_code
      ,      ldcp_dcv_bca_year
      ,      ldcp_dcv_vca_code
      ,      ldcp_dcv_description
      ,      ldcp_dcv_quantity
      ,      ldcp_dcv_hrv_pmu_code
      ,      ldcp_dcv_unit_cost
      ,      ldcp_dcv_estimated_cost
      ,      ldcp_dcv_planned_start_date
      ,      ldcp_dcv_projected_cost
      ,      ldcp_dcv_hrv_loc_code
      FROM   dl_hpm_deliverable_cmpts
      WHERE  ldcp_dlb_batch_id = p_batch_id
      AND    ldcp_dl_load_status IN('L','F','O');
    CURSOR c_dlv_refno(p_cnt_reference VARCHAR2,p_pro_aun_code VARCHAR2,p_pro_aun_type VARCHAR2,p_std_code VARCHAR2)
    IS
      SELECT dlv_refno
      ,      dve_bud_refno
      FROM   deliverables
      ,      deliverable_versions
      WHERE  dlv_refno = dve_dlv_refno
      AND    dlv_cnt_reference = p_cnt_reference
      AND    dlv_cad_pro_aun_code = p_pro_aun_code
      AND    dlv_cad_type_ind = p_pro_aun_type
      AND    dve_std_code = p_std_code
      AND    dve_current_ind = 'Y';
    CURSOR chk_contract(p_cnt_reference VARCHAR2)
    IS
      SELECT 'X'
      FROM   contracts
      WHERE  cnt_reference = p_cnt_reference;
    CURSOR chk_status_code(p_sco_code VARCHAR2)
    IS
      SELECT 'X'
      FROM status_codes
      WHERE sco_code = p_sco_code;
    CURSOR chk_sor_code(p_sor_code VARCHAR2)
    IS
      SELECT 'x'
      FROM   schedule_of_rates
      WHERE  sor_code = p_sor_code;
    CURSOR chk_std_deliverables(p_std_code VARCHAR2)
    IS
      SELECT 'x'
      FROM   standard_deliverables
      WHERE  std_code = p_std_code;
    CURSOR chk_vat_category(p_vca_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   vat_categories
      WHERE  vca_code = p_vca_code;
    CURSOR chk_bhe_code(p_bhe_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   budget_heads
      WHERE  bhe_code = p_bhe_code;
    CURSOR chk_bca_year(p_bca_year NUMBER)
    IS
      SELECT 'X'
      FROM   budget_calendars
      WHERE  bca_year = p_bca_year;
    CURSOR c_bhe_refno(p_bhe_code VARCHAR2)
    IS
      SELECT bhe_refno
      FROM   budget_heads
      WHERE  bhe_code = p_bhe_code;
    CURSOR c_bud_refno(p_bhe_refno NUMBER, p_bca_year NUMBER)
    IS
      SELECT bud_refno
      FROM   budgets
      WHERE  bud_bhe_refno = p_bhe_refno
      AND    bud_bca_year = p_bca_year;
    CURSOR c_budget_area(p_bud_refno NUMBER)
    IS
      SELECT 'X'
      FROM   budget_areas
      WHERE  bar_bud_refno = p_bud_refno
      AND    bar_arc_code = 'HPM'
      AND    bar_active_ind = 'Y';
    CURSOR chk_contact_addresses(p_cnt_reference VARCHAR2, p_cad_pro_aun_code VARCHAR2, p_cad_type_ind VARCHAR2)
    IS
      SELECT 'x'
      FROM   contract_addresses
      WHERE  cad_cnt_reference = p_cnt_reference
      AND    cad_pro_aun_code = p_cad_pro_aun_code
      AND    cad_type_ind = p_cad_type_ind;
    CURSOR chk_contract_sors(p_cnt_reference VARCHAR2, p_version_number NUMBER, p_sor_code VARCHAR2)
    IS
      SELECT 'x'
      FROM  contract_sors
      WHERE cvs_cve_cnt_reference = p_cnt_reference
      AND   cvs_cve_version_number = p_version_number
      AND   cvs_sor_code = p_sor_code
      AND   cvs_current_ind = 'Y';
    CURSOR c_pro_refno(p_pro_propref VARCHAR2)
    IS
      SELECT pro_refno
      FROM   properties
      WHERE  pro_propref = p_pro_propref;
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'VALIDATE';
    ct VARCHAR2(30) := 'DL_HPM_DELIVERABLE_CMPTS';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    l_cnt_exists         VARCHAR2(1);
    l_cad_cnt_exists     VARCHAR2(1);
    l_sco_exists         VARCHAR2(1);
    l_bud_area_exists    VARCHAR2(1);
    l_sor_exists         VARCHAR2(1);
    l_con_sor_exists     VARCHAR2(1);
    l_cad_type_valid     VARCHAR2(1);
    l_pro_aun_code_valid VARCHAR2(1);
    l_cad_exists         VARCHAR2(1);
    l_vca_exists         VARCHAR2(1);
    l_errors             VARCHAR2(1);
    l_error_ind          VARCHAR2(1);
    i                    INTEGER := 0;
    l_bhe_refno budget_heads.bhe_refno%type;
    l_bud_refno budgets.bud_refno%type;
    l_dve_bud_refno PLS_INTEGER;
    l_bhe_code_exists VARCHAR2(1);
    l_bca_year_exists VARCHAR2(1);
    l_pro_aun_code admin_units.aun_code%type;
    l_dlv_refno deliverables.dlv_refno%type;
    l_exists VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_deliverable_cmpts.dataload_validate');
    fsc_utils.debug_message('s_dl_hpm_deliverable_cmpts.dataload_validate',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.ldcp_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors := 'V';
        l_error_ind := 'N';
        l_exists := NULL;
        IF (p1.ldcp_cnt_reference IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',291);
        END IF;
        IF (p1.ldcp_cad_pro_aun_code IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',330);
        END IF;
        IF (p1.ldcp_cad_type_ind IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',331);
        END IF;
        IF (p1.ldcp_dve_std_code IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',340);
        END IF;
        IF (p1.ldcp_sco_code IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',168);
        END IF;
        IF (p1.LDCP_STATUS_DATE IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',338);
        END IF;
        l_cad_cnt_exists := NULL;
        OPEN chk_contract(p1.LDCP_CNT_REFERENCE);
        FETCH chk_contract INTO l_cad_cnt_exists;
        CLOSE chk_contract;
        IF (l_cad_cnt_exists IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',333);
        END IF;
        l_sco_exists := NULL;
        IF (p1.ldcp_sco_code IS NOT NULL) 
        THEN
          IF (p1.ldcp_sco_codE != 'RAI') 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',376);
          END IF;
        END IF;
        IF (p1.LDCP_CAD_PRO_AUN_CODE IS NOT NULL AND p1.LDCP_CAD_TYPE_IND IS NOT NULL) 
        THEN
          l_pro_aun_code_valid := 'Y';
          IF (p1.LDCP_CAD_TYPE_IND = 'A') 
          THEN
            IF (NOT s_dl_hem_utils.exists_aun_code(p1.LDCP_CAD_PRO_AUN_CODE)) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
              l_pro_aun_code_valid := 'N';
            END IF;
          ELSIF (p1.LDCP_CAD_TYPE_IND = 'P') 
          THEN
            IF (NOT s_dl_hem_utils.exists_propref(p1.LDCP_CAD_PRO_AUN_CODE)) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
              l_pro_aun_code_valid := 'N';
            END IF;
          END IF;
        END IF;
        l_cad_type_valid := 'Y';
        IF (p1.LDCP_CAD_TYPE_IND NOT IN('A','P')) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',334);
          l_cad_type_valid := 'N';
        END IF;
        IF p1.LDCP_DCV_BHE_CODE IS NOT NULL 
        THEN
          l_bhe_code_exists := NULL;
          OPEN chk_bhe_code(p1.LDCP_DCV_BHE_CODE);
          FETCH chk_bhe_code INTO l_bhe_code_exists;
          CLOSE chk_bhe_code;
          IF (l_bhe_code_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',328);
          END IF;
          l_bca_year_exists := NULL;
          OPEN chk_bca_year(p1.LDCP_DCV_BCA_YEAR);
          FETCH chk_bca_year INTO l_bca_year_exists;
          CLOSE chk_bca_year;
          IF (l_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',528);
          END IF;
        END IF;
        IF (l_bhe_code_exists IS NOT NULL AND l_bca_year_exists IS NOT NULL) 
        THEN
          l_bhe_refno := NULL;
          OPEN c_bhe_refno(p1.LDCP_DCV_BHE_CODE);
          FETCH c_bhe_refno INTO l_bhe_refno;
          CLOSE c_bhe_refno;
          l_bud_refno := NULL;
          OPEN c_bud_refno(l_bhe_refno,p1.LDCP_DCV_BCA_YEAR);
          FETCH c_bud_refno INTO l_bud_refno;
          CLOSE c_bud_refno;
          IF (l_bud_refno IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',329);
          ELSE
            l_bud_area_exists := NULL;
            OPEN c_budget_area(l_bud_refno);
            FETCH c_budget_area INTO l_bud_area_exists;
            CLOSE c_budget_area;
            IF (l_bud_area_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',626);
            END IF;
          END IF;
        END IF;
        l_cad_exists := NULL;
        l_pro_aun_code := NULL;
        IF (l_cad_cnt_exists IS NOT NULL AND l_pro_aun_code_valid = 'Y' AND l_cad_type_valid = 'Y') 
        THEN
          IF (p1.LDCP_CAD_TYPE_IND = 'P') 
          THEN
            OPEN c_pro_refno(p1.LDCP_CAD_PRO_AUN_CODE);
            FETCH c_pro_refno
            INTO l_pro_aun_code;
            CLOSE c_pro_refno;
          ELSE
            l_pro_aun_code := p1.LDCP_CAD_PRO_AUN_CODE;
          END IF;
          OPEN chk_contact_addresses(p1.LDCP_CNT_REFERENCE,l_pro_aun_code,p1.LDCP_CAD_TYPE_IND);
          FETCH chk_contact_addresses INTO l_cad_exists;
          CLOSE chk_contact_addresses;
          IF (l_cad_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',343);
          END IF;
        END IF;
        l_dlv_refno := NULL;
        l_dve_bud_refno := NULL;
        IF (l_cad_cnt_exists IS NOT NULL AND l_pro_aun_code_valid = 'Y' AND l_cad_type_valid = 'Y')
        THEN
          OPEN c_dlv_refno(p1.ldcp_cnt_reference,l_pro_aun_code,p1.ldcp_cad_type_ind,p1.ldcp_dve_std_code);
          FETCH c_dlv_refno INTO l_dlv_refno,l_dve_bud_refno;
          IF c_dlv_refno%NOTFOUND 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',569);
          END IF;
          CLOSE c_dlv_refno;
        END IF;
        l_sor_exists := NULL;
        IF (p1.ldcp_dcv_sor_code IS NOT NULL) 
        THEN
          OPEN chk_sor_code(p1.LDCP_DCV_SOR_CODE);
          FETCH chk_sor_code
          INTO l_sor_exists;
          CLOSE chk_sor_code;
          IF (l_sor_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',344);
          ELSE
            IF (p1.ldcp_dcv_quantity IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',353);
            END IF;
            IF (p1.ldcp_dcv_hrv_pmu_code IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',354);
            END IF;
            l_con_sor_exists := NULL;
            OPEN chk_contract_sors(p1.LDCP_CNT_REFERENCE,p1.ldcp_dcv_version_number,p1.LDCP_DCV_SOR_CODE);
            FETCH chk_contract_sors INTO l_con_sor_exists;
            CLOSE chk_contract_sors;
            IF (l_con_sor_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',355);
            END IF;
          END IF;
        END IF;
        l_vca_exists := NULL;
        IF (p1.LDCP_DCV_VCA_CODE IS NOT NULL) 
        THEN
          OPEN chk_vat_category(p1.LDCP_DCV_VCA_CODE);
          FETCH chk_vat_category INTO l_vca_exists;
          CLOSE chk_vat_category;
          IF (l_vca_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',345);
          END IF;
        END IF;
        IF (p1.ldcp_dcv_hrv_pmu_code IS NOT NULL) 
        THEN
          IF (NOT s_dl_hem_utils.exists_frv('UNITS',p1.LDCP_DCV_HRV_PMU_CODE,'Y')) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',347);
          END IF;
        END IF;
        IF (p1.ldcp_dcv_hrv_loc_code IS NOT NULL AND p1.ldcp_dcv_hrv_loc_code != 'PRO' AND p1.ldcp_dcv_hrv_loc_code != 'AUN') 
        THEN
          IF (NOT s_dl_hem_utils.exists_frv('LOCATION',p1.ldcp_dcv_hrv_loc_code,'Y')) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',348);
          END IF;
        END IF;
        IF (p1.ldcp_dcv_unit_cost IS NOT NULL) 
        THEN
          IF (p1.ldcp_dcv_unit_cost < 0) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',350);
          END IF;
        END IF;
        IF (p1.ldcp_dcv_estimated_cost IS NOT NULL) 
        THEN
          IF (p1.ldcp_dcv_estimated_cost < 0) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',351);
          END IF;
        END IF;
        IF (p1.ldcp_dcv_version_number IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',625);
        END IF;
        IF (p1.ldcp_dcv_display_sequence IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',339);
        END IF;
        IF (p1.ldcp_dcv_planned_start_date IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',341);
        END IF;
        IF (p1.ldcp_dcv_estimated_cost IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',342);
        END IF;
        IF (p1.ldcp_dcv_unit_cost is null) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',349);
        END IF;
        IF (p1.ldcp_dcv_bhe_code IS NULL AND l_dve_bud_refno IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',526);
        END IF;
        IF (p1.ldcp_dcv_bca_year IS NULL AND l_dve_bud_refno IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',327);
        END IF;
        IF l_errors = 'F' 
        THEN
          l_error_ind := 'Y';
        ELSE
          l_error_ind := 'N';
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
        set_record_status_flag(l_id,l_errors);
        i := i + 1;
        IF (MOD(i,1000) = 0) 
        THEN
          COMMIT;
        END IF;
      EXCEPTION
      WHEN OTHERS 
      THEN
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
        set_record_status_flag(l_id,'O');
      END;
    END LOOP;
    fsc_utils.proc_END;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
  END dataload_validate;
  --
  PROCEDURE dataload_delete
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  AS
    CURSOR c1(p_batch_id VARCHAR2)
    IS
      SELECT rowid rec_rowid
      ,      ldcp_dlb_batch_id
      ,      ldcp_dl_seqno
      ,      ldcp_dl_load_status
      ,      ldcp_refno
      ,      ldcp_cnt_reference
      ,      ldcp_cad_pro_aun_code
      ,      ldcp_cad_type_ind
      ,      ldcp_dve_std_code
      ,      ldcp_dcv_version_number
      FROM   dl_hpm_deliverable_cmpts
      WHERE  ldcp_dlb_batch_id = p_batch_id
      AND    ldcp_dl_load_status =('C');
    CURSOR c_pro_refno(p_pro_propref VARCHAR2)
    IS
      SELECT pro_refno
      FROM   properties
      WHERE  pro_propref = p_pro_propref;
    CURSOR c_dlv_refno(p_cnt_reference VARCHAR2,p_pro_aun_code VARCHAR2,p_pro_aun_type VARCHAR2,p_std_code VARCHAR2)
    IS
      SELECT dlv_refno
      ,      dve_version_number
      FROM   deliverables
      ,      deliverable_versions
      WHERE  dlv_refno = dve_dlv_refno
      AND    dlv_cnt_reference = p_cnt_reference
      AND    dlv_cad_pro_aun_code = p_pro_aun_code
      AND    dlv_cad_type_ind = p_pro_aun_type
      AND    dve_std_code = p_std_code
      AND    dve_current_ind = 'Y';
    cb          VARCHAR2(30);
    cd          DATE;
    cp          VARCHAR2(30) := 'DELETE';
    ct          VARCHAR2(30) := 'DL_HPM_DELIVERABLE_CMPTS';
    cs          INTEGER;
    ce          VARCHAR2(200);
    l_exists    VARCHAR2(1);
    l_errors    VARCHAR2(1);
    l_error_ind VARCHAR2(1);
    i           INTEGER := 0;
    l_id ROWID;
    l_pro_refno properties.pro_refno%type;
    l_cad_pro_aun_code admin_units.aun_code%type;
    l_dlv_refno deliverables.dlv_refno%type;
    l_dve_version_number deliverable_versions.dve_version_number%type;
    l_an_tab VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_deliverable_cmpts.dataload_delete');
    fsc_utils.debug_message('s_dl_hpm_deliverable_cmpts.dataload_delete',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.ldcp_dl_seqno;
        l_id := p1.rec_rowid;
        i := i + 1;
        l_dlv_refno := NULL;
        l_dve_version_number := NULL;
        l_cad_pro_aun_code := NULL;
        IF (p1.ldcp_cad_type_ind = 'P') 
        THEN
          OPEN c_pro_refno(p1.ldcp_cad_pro_aun_code);
          FETCH c_pro_refno INTO l_cad_pro_aun_code;
          CLOSE c_pro_refno;
        ELSE
          l_cad_pro_aun_code := p1.ldcp_cad_pro_aun_code;
        END IF;
        OPEN c_dlv_refno(p1.ldcp_cnt_reference,l_cad_pro_aun_code,p1.ldcp_cad_type_ind,p1.ldcp_dve_std_code);
        FETCH c_dlv_refno INTO l_dlv_refno,l_dve_version_number;
        CLOSE c_dlv_refno;
        SAVEPOINT SP1;
        DELETE
        FROM   deliv_version_mappings
        WHERE  dvm_dve_dlv_refno = l_dlv_refno
        AND    dvm_dve_version_number = l_dve_version_number
        AND    dvm_dcv_dcp_refno = p1.ldcp_refno
        AND    dvm_dcv_version_number = p1.ldcp_dcv_version_number;
        DELETE
        FROM   deliverable_cmpt_versions
        WHERE  dcv_dcp_refno = p1.ldcp_refno
        AND    dcv_version_number = p1.ldcp_dcv_version_number;
        DELETE
        FROM   deliverable_components
        WHERE  dcp_refno = p1.ldcp_refno
        AND    dcp_dlv_refno = l_dlv_refno;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        set_record_status_flag(l_id,'V');
        IF (mod(i,5000) = 0) 
        THEN
          COMMIT;
        END IF;
      EXCEPTION
      WHEN OTHERS 
      THEN
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
        set_record_status_flag(l_id,'C');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
    END LOOP;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('DELIVERABLE_COMPONENTS');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('DELIVERABLE_CMPT_VERSIONS');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_hpm_deliverable_cmpts;
/
