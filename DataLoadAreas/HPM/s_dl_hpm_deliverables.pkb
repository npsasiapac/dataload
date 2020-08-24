CREATE OR REPLACE PACKAGE BODY s_dl_hpm_deliverables
AS
-- ***********************************************************************
--  DESCRIPTION: BESPOKE QUEENSLAND - VERSION
-- ***********************************************************************
--
--  CHANGE CONTROL
--
--  VER         DB Ver  WHO     WHEN            WHY
--  1.0         5.12    VRS     14-SEP-2007     Product Dataload
--  1.1         5.12.0  PH      11-OCT-2007     Amended validate on contract
--                                              address to get the pro_refno
--  1.2         5.12.0  PH      12-OCT-2007     Amended create to get pro_refno
--  1.3         5.12.0  PH      01-NOV-2007     Added validate on description.
--  1.4         5.14.1  PH      01-MAY-2009     Added batch question to allow
--                                              Anchor to update Start Date.
--                                              Involves changes to Create and
--                                              validate.
-- 1.5          5.14.1  PH      011-AUG-2009    Amended update to include cost
--                                              as well as date
-- 1.6          5.16.1  PH      11-NOV-2009     Corrected error 551 to 557
-- 1.7          6.1.1   MB      26-OCT-2011     Reinstated a fix for Anchor that 
--                                              applies a NVL() to the update
--                                              so that if null values are supplied
--                                              the original values are kept.
-- 1.8          6.9.0   MB      12-FEB-2014     moved unit cost validation  
-- 1.9          6.9.0   MB      18-MAR-2014     removed validation on description as
--                                              non-mand except when S Sor supplied
--                                              but Create process picks up the SOR
--                                              description so no need to supply one
-- 1.9.1        6.11.0  MJK     24-AUG-2015     Reformatted for 6.11. No logic changes
--
-- BESPOKE QUEENSLAND VERSION FROM 1.9.2 ONWARDS...SEPARATE BESPOKE GUIDE ALSO from v6.13(7.2) onwards
--
-- 1.9.2        6.13.0  MJK     21-APR-2016     Deliverables of AUT and COM status can now be loaded
-- 1.9.3        6.13.0  AJ      27-JAN-2016     Contracts(RAI) Deliverable Status(RAI) only 
--                                              Contracts(AUT) Deliverable Status(AUT or COM) only 
--                                              Contracts must be at Status(RAI or AUT) only
--
-- NOTES: Deliverable_versions
--
--        First time you create this the dve_version_number = 1 and
--        the dve_current_ind = 'Y' 
--
--        The dve_bud_refno is derived from the bhe_code and bca_year.
--        Even though the field is not mandatory in the table, it is mandatory to populate 
--        the dve_bud_refno when you create a deliverable.
--
--
--
  PROCEDURE dataload_create
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  AS
    CURSOR c1(p_batch_id VARCHAR2)
    IS
      SELECT rowid rec_rowid
      ,      ldlv_dlb_batch_id
      ,      ldlv_dl_seqno
      ,      ldlv_dl_load_status
      ,      ldlv_dlv_cnt_reference
      ,      ldlv_dlv_sco_code
      ,      ldlv_dlv_status_date
      ,      ldlv_dlv_authorised_by
      ,      ldlv_dlv_actual_end_date
      ,      ldlv_dlv_cad_pro_aun_code
      ,      ldlv_dlv_cad_type_ind
      ,      ldlv_dve_display_sequence
      ,      ldlv_dve_std_code
      ,      ldlv_dve_planned_start_date
      ,      ldlv_dve_estimated_cost
      ,      ldlv_dve_sor_code
      ,      ldlv_bhe_code
      ,      ldlv_bca_year
      ,      ldlv_dve_vca_code
      ,      ldlv_dve_description
      ,      ldlv_dve_quantity
      ,      ldlv_dve_hrv_pmu_code_quantity
      ,      ldlv_dve_unit_cost
      ,      ldlv_dve_projected_cost
      ,      ldlv_dve_hrv_loc_code
      ,      ldlv_dlv_refno
      FROM   dl_hpm_deliverables
      WHERE  ldlv_dlb_batch_id = p_batch_id
      AND    ldlv_dl_load_status = 'V';
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
    CURSOR c_upd_dve_start
      (p_cnt_reference VARCHAR2
      ,p_pro_aun_code  VARCHAR2
      ,p_type_ind      VARCHAR2
      ,p_display_seq   NUMBER
      ,p_std_code      VARCHAR2
      )
    IS
      SELECT dve_dlv_refno
      ,      dve_version_number
      FROM   deliverables
      ,      deliverable_versions
      WHERE  dlv_refno = dve_dlv_refno
      AND    dlv_cnt_reference = p_cnt_reference
      AND    dlv_cad_pro_aun_code = p_pro_aun_code
      AND    dlv_cad_type_ind = p_type_ind
      AND    dve_display_sequence = p_display_seq
      AND    dve_std_code = p_std_code
      AND    dve_current_ind = 'Y';
    cb                VARCHAR2(30);
    cd                DATE;
    cp                VARCHAR2(30) := 'CREATE';
    ct                VARCHAR2(30) := 'DL_HPM_DELIVERABLES';
    cs                INTEGER;
    ce                VARCHAR2(200);
    ci                INTEGER;
    l_pro_refno       NUMBER(10);
    l_pro_aun_code    VARCHAR2(20);
    l_scs_refno       NUMBER(8);
    l_dlv_refno       NUMBER(10);
    l_reusable_refno  NUMBER(10);
    l_bud_refno       NUMBER(10);
    l_bhe_refno       NUMBER(10);
    i                 INTEGER := 0;
    l_an_tab          VARCHAR2(1);
    l_answer          VARCHAR2(1);
    l_sor_type        VARCHAR2(1);
    l_sor_description VARCHAR2(4000);
    l_sor_desc        VARCHAR2(4000);
    l_dve_dlv_refno deliverable_versions.dve_dlv_refno%type;
    l_dve_version_number deliverable_versions.dve_version_number%type;
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_deliverables.dataload_create');
    fsc_utils.debug_message('s_dl_hpm_deliverables.dataload_create',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    ci := s_dl_hem_utils.dl_orig_rows('DL_HPM_DELIVERABLES');
    l_answer := s_dl_batches.get_answer(p_batch_id,1);
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.ldlv_dl_seqno;
        IF NVL(l_answer,'N') = 'Y'
        THEN
          l_pro_aun_code := NULL;
          l_dve_dlv_refno := NULL;
          l_dve_version_number := NULL;
          IF p1.ldlv_dlv_cad_type_ind = 'P' 
          THEN
            OPEN c_pro_refno(p1.ldlv_dlv_cad_pro_aun_code);
            FETCH c_pro_refno INTO l_pro_aun_code;
            CLOSE c_pro_refno;
          ELSE
            l_pro_aun_code := p1.ldlv_dlv_cad_pro_aun_code;
          END IF;
          OPEN c_upd_dve_start(p1.ldlv_dlv_cnt_reference,l_pro_aun_code,p1.ldlv_dlv_cad_type_ind,p1.ldlv_dve_display_sequence,p1.ldlv_dve_std_code);
          FETCH c_upd_dve_start INTO l_dve_dlv_refno,l_dve_version_number;
          CLOSE c_upd_dve_start;
          UPDATE deliverable_versions
          SET    dve_planned_start_date = NVL(p1.ldlv_dve_planned_start_date,dve_planned_start_date)
          ,      dve_estimated_cost = NVL(p1.ldlv_dve_estimated_cost,dve_estimated_cost)
          WHERE  dve_dlv_refno = l_dve_dlv_refno
          AND    dve_version_number = l_dve_version_number
          AND    dve_current_ind = 'Y';
        ELSE
          l_reusable_refno := NULL;
          OPEN c_reusable_refno;
          FETCH c_reusable_refno INTO l_reusable_refno;
          CLOSE c_reusable_refno;
          l_bhe_refno := NULL;
          OPEN c_bhe_refno(p1.ldlv_bhe_code);
          FETCH c_bhe_refno INTO l_bhe_refno;
          CLOSE c_bhe_refno;
          l_bud_refno := NULL;
          OPEN c_bud_refno(l_bhe_refno,p1.ldlv_bca_year);
          FETCH c_bud_refno INTO l_bud_refno;
          CLOSE c_bud_refno;
          l_sor_type := NULL;
          l_sor_description := NULL;
          l_sor_desc := NULL;
          OPEN get_sor_details(p1.ldlv_dve_sor_code);
          FETCH get_sor_details INTO l_sor_type,l_sor_description;
          CLOSE get_sor_details;
          IF (l_sor_type = 'S') 
          THEN
            l_sor_desc := l_sor_description;
          ELSE
            l_sor_desc := p1.ldlv_dve_description;
          END IF;
          l_pro_aun_code := NULL;
          IF p1.ldlv_dlv_cad_type_ind = 'P' 
          THEN
            OPEN c_pro_refno(p1.ldlv_dlv_cad_pro_aun_code);
            FETCH c_pro_refno INTO l_pro_aun_code;
            CLOSE c_pro_refno;
          ELSE
            l_pro_aun_code := p1.ldlv_dlv_cad_pro_aun_code;
          END IF;
          SELECT dlv_refno_seq.NEXTVAL
          INTO   p1.ldlv_dlv_refno
          FROM dual;
          UPDATE dl_hpm_deliverables
          SET    ldlv_dlv_refno = p1.ldlv_dlv_refno
          WHERE  ROWID = p1.rec_rowid;
          SAVEPOINT SP1;
          INSERT INTO deliverables
          (dlv_refno  
          ,dlv_cnt_reference  
          ,dlv_sco_code  
          ,dlv_status_date
          ,dlv_authorised_date
          ,dlv_authorised_by
          ,dlv_actual_end_date
          ,dlv_cad_pro_aun_code  
          ,dlv_cad_type_ind  
          )  
          VALUES  
          (p1.ldlv_dlv_refno  
          ,p1.ldlv_dlv_cnt_reference  
          ,p1.ldlv_dlv_sco_code  
          ,p1.ldlv_dlv_status_date  
          ,DECODE(p1.ldlv_dlv_sco_code,'RAI',NULL,p1.ldlv_dlv_status_date)
          ,DECODE(p1.ldlv_dlv_sco_code,'RAI',NULL,NVL(p1.ldlv_dlv_authorised_by,USER))
          ,p1.ldlv_dlv_actual_end_date
          ,l_pro_aun_code  
          ,p1.ldlv_dlv_cad_type_ind  
          );  
          INSERT INTO deliverable_versions
          (dve_dlv_refno  
          ,dve_version_number  
          ,dve_display_sequence  
          ,dve_current_ind  
          ,dve_reusable_refno  
          ,dve_std_code  
          ,dve_planned_start_date  
          ,dve_estimated_cost  
          ,dve_sor_code  
          ,dve_bud_refno  
          ,dve_vca_code  
          ,dve_description  
          ,dve_quantity  
          ,dve_hrv_pmu_code_quantity  
          ,dve_unit_cost  
          ,dve_projected_cost  
          ,dve_hrv_loc_code  
          )  
          VALUES  
          (p1.ldlv_dlv_refno  
          ,1  
          ,p1.ldlv_dve_display_sequence  
          ,'Y'  
          ,l_reusable_refno  
          ,p1.ldlv_dve_std_code  
          ,p1.ldlv_dve_planned_start_date  
          ,p1.ldlv_dve_estimated_cost  
          ,p1.ldlv_dve_sor_code  
          ,l_bud_refno  
          ,p1.ldlv_dve_vca_code  
          ,l_sor_desc  
          ,p1.ldlv_dve_quantity  
          ,p1.ldlv_dve_hrv_pmu_code_quantity  
          ,p1.ldlv_dve_unit_cost  
          ,p1.ldlv_dve_projected_cost  
          ,p1.ldlv_dve_hrv_loc_code  
          );  
        END IF;
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
    l_an_tab := s_dl_hem_utils.dl_comp_stats('DELIVERABLES',ci,i);
    l_an_tab := s_dl_hem_utils.dl_comp_stats('DELIVERABLE_VERSIONS',ci,i);
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
      ,      ldlv_dlb_batch_id
      ,      ldlv_dl_seqno
      ,      ldlv_dl_load_status
      ,      ldlv_dlv_cnt_reference
      ,      ldlv_dlv_sco_code
      ,      ldlv_dlv_status_date
      ,      ldlv_dlv_authorised_by
      ,      ldlv_dlv_actual_end_date
      ,      ldlv_dlv_cad_pro_aun_code
      ,      ldlv_dlv_cad_type_ind
      ,      ldlv_dve_display_sequence
      ,      ldlv_dve_std_code
      ,      ldlv_dve_planned_start_date
      ,      ldlv_dve_estimated_cost
      ,      ldlv_dve_sor_code
      ,      ldlv_bhe_code
      ,      ldlv_bca_year
      ,      ldlv_dve_vca_code
      ,      ldlv_dve_description
      ,      ldlv_dve_quantity
      ,      ldlv_dve_hrv_pmu_code_quantity
      ,      ldlv_dve_unit_cost
      ,      ldlv_dve_projected_cost
      ,      ldlv_dve_hrv_loc_code
      ,      ldlv_dlv_refno
      FROM   dl_hpm_deliverables
      WHERE  ldlv_dlb_batch_id = p_batch_id
      AND    ldlv_dl_load_status IN('L','F','O');
    CURSOR chk_contract(p_cnt_reference VARCHAR2)
    IS
      SELECT 'X',cnt_sco_code
      FROM   contracts
      WHERE  cnt_reference = p_cnt_reference;
    CURSOR chk_status_code(p_sco_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   status_codes
      WHERE  sco_code = p_sco_code;
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
    CURSOR chk_contact_addresses(p_cad_cnt_reference VARCHAR2, p_cad_pro_aun_code VARCHAR2, p_cad_type_ind VARCHAR2)
    IS
      SELECT 'x'
      FROM   contract_addresses
      WHERE  cad_cnt_reference = p_cad_cnt_reference
      AND    cad_pro_aun_code = p_cad_pro_aun_code
      AND    cad_type_ind = p_cad_type_ind;
    CURSOR chk_contract_sors(p_cnt_reference VARCHAR2, p_version_number NUMBER, p_sor_code VARCHAR2)
    IS
      SELECT 'x'
      FROM   contract_sors
      WHERE  cvs_cve_cnt_reference = p_cnt_reference
      AND    cvs_cve_version_number = p_version_number
      AND    cvs_sor_code = p_sor_code
      AND    cvs_current_ind = 'Y';
    CURSOR c_pro_refno(p_pro_propref VARCHAR2)
    IS
      SELECT pro_refno
      FROM   properties
      WHERE  pro_propref = p_pro_propref;
    CURSOR c_upd_dve_start(p_cnt_reference VARCHAR2,p_pro_aun_code VARCHAR2,p_type_ind VARCHAR2,p_display_seq NUMBER,p_std_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   deliverables
      ,      deliverable_versions
      WHERE  dlv_refno = dve_dlv_refno
      AND    dlv_cnt_reference = p_cnt_reference
      AND    dlv_cad_pro_aun_code = p_pro_aun_code
      AND    dlv_cad_type_ind = p_type_ind
      AND    dve_display_sequence = p_display_seq
      AND    dve_std_code = p_std_code
      AND    dve_current_ind = 'Y';
    cb                   VARCHAR2(30);
    cd                   DATE;
    cp                   VARCHAR2(30) := 'VALIDATE';
    ct                   VARCHAR2(30) := 'DL_HPM_DELIVERABLES';
    cs                   INTEGER;
    ce                   VARCHAR2(200);
    l_cnt_exists         VARCHAR2(1);
    l_sco_exists         VARCHAR2(1);
    l_std_exists         VARCHAR2(1);
    l_sor_exists         VARCHAR2(1);
    l_con_sor_exists     VARCHAR2(1);
    l_cad_type_valid     VARCHAR2(1);
    l_pro_aun_code_valid VARCHAR2(1);
    l_cad_exists         VARCHAR2(1);
    l_vca_exists         VARCHAR2(1);
    l_errors             VARCHAR2(1);
    l_error_ind          VARCHAR2(1);
    i                    INTEGER := 0;
    l_bhe_refno          NUMBER(10);
    l_bud_refno          NUMBER(10);
    l_bhe_code_exists    VARCHAR2(1);
    l_bca_year_exists    VARCHAR2(1);
    l_pro_aun_code       VARCHAR2(20);
    l_answer             VARCHAR2(1);
    l_exists             VARCHAR2(1);
    l_cnt_status         VARCHAR2(3);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_deliverables.dataload_validate');
    fsc_utils.debug_message('s_dl_hpm_deliverables.dataload_validate',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    l_answer := s_dl_batches.get_answer(p_batch_id,1);
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.ldlv_dl_seqno;
        l_errors := 'V';
        l_error_ind := 'N';
        IF NVL(l_answer,'N') = 'Y'
        THEN
          l_pro_aun_code := NULL;
          l_exists := NULL;
          IF p1.ldlv_dlv_cad_type_ind = 'P' 
          THEN
            OPEN c_pro_refno(p1.ldlv_dlv_cad_pro_aun_code);
            FETCH c_pro_refno INTO l_pro_aun_code;
            CLOSE c_pro_refno;
          ELSE
            l_pro_aun_code := p1.ldlv_dlv_cad_pro_aun_code;
          END IF;
          OPEN c_upd_dve_start(p1.ldlv_dlv_cnt_reference,l_pro_aun_code,p1.ldlv_dlv_cad_type_ind,p1.ldlv_dve_display_sequence,p1.ldlv_dve_std_code);
          FETCH c_upd_dve_start INTO l_exists;
          IF c_upd_dve_start%NOTFOUND 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',557);
          END IF;
          CLOSE c_upd_dve_start;
        ELSE
          IF (p1.ldlv_dlv_cnt_reference IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',291);
          END IF;
          IF (p1.ldlv_dlv_sco_code IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',168);
          END IF;
          IF (p1.ldlv_dlv_status_date IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',338);
          END IF;
          IF (p1.ldlv_bhe_code IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',526);
          END IF;
          IF (p1.ldlv_bca_year IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',327);
          END IF;
          IF (p1.ldlv_dve_display_sequence IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',339);
          END IF;
          IF (p1.ldlv_dve_std_code IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',340);
          END IF;
          IF (p1.ldlv_dve_planned_start_date IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',341);
          END IF;
          IF (p1.ldlv_dve_estimated_cost IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',342);
          END IF;
          l_cnt_exists := NULL;
          l_cnt_status := NULL;
          IF (p1.ldlv_dlv_cnt_reference IS NOT NULL)   -- start of new bit
          THEN	  
            OPEN chk_contract(p1.ldlv_dlv_cnt_reference);
            FETCH chk_contract INTO l_cnt_exists, l_cnt_status;
            CLOSE chk_contract;
            IF (l_cnt_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',333);
            END IF;
            IF (l_cnt_status         IS NOT NULL  AND 
                p1.ldlv_dlv_sco_code IS NOT NULL     )
			THEN
              IF (l_cnt_status ='RAI')
              THEN
			    IF (p1.ldlv_dlv_sco_code !='RAI')
			    THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',119);
                END IF;
              END IF;
              IF (l_cnt_status ='AUT')
              THEN
			    IF (p1.ldlv_dlv_sco_code NOT IN('AUT','COM'))
			    THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',120);
                END IF;
              END IF;
              IF (l_cnt_status NOT IN ('AUT','RAI'))
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',121);
              END IF;
            END IF;
          END IF;  -- end of new bit
          IF (p1.ldlv_dlv_cad_pro_aun_code IS NOT NULL AND p1.ldlv_dlv_cad_type_ind IS NOT NULL) 
          THEN
            l_pro_aun_code_valid := 'Y';
            IF (p1.ldlv_dlv_cad_type_ind = 'A') 
            THEN
              IF (NOT s_dl_hem_utils.exists_aun_code(p1.ldlv_dlv_cad_pro_aun_code)) 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
                l_pro_aun_code_valid := 'N';
              END IF;
            ELSIF (p1.ldlv_dlv_cad_type_ind = 'P') 
            THEN
              IF (NOT s_dl_hem_utils.exists_propref(p1.ldlv_dlv_cad_pro_aun_code)) 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
                l_pro_aun_code_valid := 'N';
              END IF;
            END IF;
          END IF;
          l_cad_type_valid := 'Y';
          IF (p1.ldlv_dlv_cad_type_ind NOT IN('A','P')) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',334);
            l_cad_type_valid := 'N';
          END IF;
          l_bhe_code_exists := NULL;
          OPEN chk_bhe_code(p1.ldlv_bhe_code);
          FETCH chk_bhe_code INTO l_bhe_code_exists;
          CLOSE chk_bhe_code;
          IF (l_bhe_code_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',328);
          END IF;
          l_bca_year_exists := NULL;
          OPEN chk_bca_year(p1.ldlv_bca_year);
          FETCH chk_bca_year INTO l_bca_year_exists;
          CLOSE chk_bca_year;
          IF (l_bca_year_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',528);
          END IF;
          IF (l_bhe_code_exists IS NOT NULL AND l_bca_year_exists IS NOT NULL) 
          THEN
            l_bhe_refno := NULL;
            OPEN c_bhe_refno(p1.ldlv_bhe_code);
            FETCH c_bhe_refno INTO l_bhe_refno;
            CLOSE c_bhe_refno;
            l_bud_refno := NULL;
            OPEN c_bud_refno(l_bhe_refno,p1.ldlv_bca_year);
            FETCH c_bud_refno INTO l_bud_refno;
            CLOSE c_bud_refno;
            IF (l_bud_refno IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',329);
            END IF;
          END IF;
          l_sco_exists := NULL;
          IF (p1.ldlv_dlv_sco_code IS NOT NULL) 
          THEN
            OPEN chk_status_code(p1.ldlv_dlv_sco_code);
            FETCH chk_status_code
            INTO l_sco_exists;
            CLOSE chk_status_code;
            IF (l_sco_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',531);
            ELSIF (p1.ldlv_dlv_sco_code NOT IN ('RAI','AUT','COM')) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',900);
            ELSIF p1.ldlv_dlv_sco_code = 'COM' 
            AND p1.ldlv_dlv_actual_end_date IS NULL 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',633);
            END IF;
          END IF;
          l_cad_exists := NULL;
          l_pro_aun_code := NULL;
          IF (l_cnt_exists IS NOT NULL AND l_pro_aun_code_valid = 'Y' AND l_cad_type_valid = 'Y') 
          THEN
            IF p1.ldlv_dlv_cad_type_ind = 'P' 
            THEN
              OPEN c_pro_refno(p1.ldlv_dlv_cad_pro_aun_code);
              FETCH c_pro_refno INTO l_pro_aun_code;
              CLOSE c_pro_refno;
            ELSE
              l_pro_aun_code := p1.ldlv_dlv_cad_pro_aun_code;
            END IF;
            OPEN chk_contact_addresses(p1.ldlv_dlv_cnt_reference, l_pro_aun_code, p1.ldlv_dlv_cad_type_ind);
            FETCH chk_contact_addresses INTO l_cad_exists;
            CLOSE chk_contact_addresses;
            IF (l_cad_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',343);
            END IF;
          END IF;
          l_sor_exists := NULL;
          IF (p1.ldlv_dve_sor_code IS NOT NULL) 
          THEN
            OPEN chk_sor_code(p1.LDLV_DVE_SOR_CODE);
            FETCH chk_sor_code INTO l_sor_exists;
            CLOSE chk_sor_code;
            IF (l_sor_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',344);
            ELSE
              IF (p1.ldlv_dve_quantity IS NULL) 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',353);
              END IF;
              IF (p1.ldlv_dve_hrv_pmu_code_quantity IS NULL) 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',354);
              END IF;
              IF (p1.ldlv_dve_unit_cost IS NULL) 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',349);
              END IF;
              l_con_sor_exists := NULL;
              OPEN chk_contract_sors(p1.ldlv_dlv_cnt_reference,1,p1.ldlv_dve_sor_code);
              FETCH chk_contract_sors INTO l_con_sor_exists;
              CLOSE chk_contract_sors;
              IF (l_con_sor_exists IS NULL) 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',355);
              END IF;
            END IF;
          END IF;
          l_vca_exists := NULL;
          IF (p1.ldlv_dve_vca_code IS NOT NULL) 
          THEN
            OPEN chk_vat_category(p1.ldlv_dve_vca_code);
            FETCH chk_vat_category INTO l_vca_exists;
            CLOSE chk_vat_category;
            IF (l_vca_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',345);
            END IF;
          END IF;
          l_std_exists := NULL;
          IF (p1.ldlv_dve_std_code IS NOT NULL) 
          THEN
            OPEN chk_std_deliverables(p1.ldlv_dve_std_code);
            FETCH chk_std_deliverables INTO l_std_exists;
            CLOSE chk_std_deliverables;
            IF (l_std_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',346);
            END IF;
          END IF;
          IF (p1.ldlv_dve_hrv_pmu_code_quantity IS NOT NULL) 
          THEN
            IF (NOT s_dl_hem_utils.exists_frv('UNITS',p1.ldlv_dve_hrv_pmu_code_quantity,'Y')) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',347);
            END IF;
          END IF;
          IF (p1.ldlv_dve_hrv_loc_code IS NOT NULL AND p1.ldlv_dve_hrv_loc_code != 'PRO' AND p1.ldlv_dve_hrv_loc_code != 'AUN') 
          THEN
            IF (NOT s_dl_hem_utils.exists_frv('LOCATION',p1.ldlv_dve_hrv_loc_code,'Y')) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',348);
            END IF;
          END IF;
          IF (p1.ldlv_dve_unit_cost IS NOT NULL) 
          THEN
            IF (p1.ldlv_dve_unit_cost < 0) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',350);
            END IF;
          END IF;
          IF (p1.ldlv_dve_estimated_cost IS NOT NULL) 
          THEN
            IF (p1.ldlv_dve_estimated_cost < 0) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',351);
            END IF;
          END IF;
        END IF;
        IF l_errors = 'F' 
        THEN
          l_error_ind := 'Y';
        ELSE
          l_error_ind := 'N';
        END IF;
        i := i + 1;
        IF (MOD(i,1000) = 0) 
        THEN
          COMMIT;
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
        s_dl_utils.set_record_status_flag(ct,cb,cs,l_errors);
      EXCEPTION
      WHEN OTHERS THEN
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
        s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
      END;
    END LOOP;
    COMMIT;
    fsc_utils.proc_END;
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
      ,      ldlv_dlb_batch_id
      ,      ldlv_dl_seqno
      ,      ldlv_dl_load_status
      ,      ldlv_dlv_cnt_reference
      ,      ldlv_dlv_sco_code
      ,      ldlv_dlv_status_date
      ,      ldlv_dlv_cad_pro_aun_code
      ,      ldlv_dlv_cad_type_ind
      ,      ldlv_dve_display_sequence
      ,      ldlv_dve_std_code
      ,      ldlv_dve_planned_start_date
      ,      ldlv_dve_estimated_cost
      ,      ldlv_dve_sor_code
      ,      ldlv_bhe_code
      ,      ldlv_bca_year
      ,      ldlv_dve_vca_code
      ,      ldlv_dve_description
      ,      ldlv_dve_quantity
      ,      ldlv_dve_hrv_pmu_code_quantity
      ,      ldlv_dve_unit_cost
      ,      ldlv_dve_projected_cost
      ,      ldlv_dve_hrv_loc_code
      ,      ldlv_dlv_refno
      FROM   dl_hpm_deliverables
      WHERE  ldlv_dlb_batch_id = p_batch_id
      AND   ldlv_dl_load_status IN('C');
    cb             VARCHAR2(30);
    cd             DATE;
    cp             VARCHAR2(30) := 'DELETE';
    ct             VARCHAR2(30) := 'DL_HPM_DELIVERABLES';
    cs             INTEGER;
    ce             VARCHAR2(200);
    l_scs_refno    NUMBER(8);
    l_pro_refno    NUMBER(10);
    l_aun_code     VARCHAR2(20);
    l_pro_aun_code VARCHAR2(20);
    l_exists       VARCHAR2(1);
    l_errors       VARCHAR2(1);
    l_error_ind    VARCHAR2(1);
    i              INTEGER := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_deliverables.dataload_delete');
    fsc_utils.debug_message('s_dl_hpm_deliverables.dataload_delete',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.ldlv_dl_seqno;
        i := i + 1;
        SAVEPOINT SP1;
        DELETE 
        FROM   deliverable_versions
        WHERE  dve_dlv_refno = p1.ldlv_dlv_refno;
        DELETE
        FROM   deliverables
        WHERE  dlv_refno = p1.ldlv_dlv_refno;
        IF (mod(i,5000) = 0) 
        THEN
          COMMIT;
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        s_dl_utils.set_record_status_flag(ct,cb,cs,'V');
      EXCEPTION
      WHEN OTHERS 
      THEN
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
        s_dl_utils.set_record_status_flag(ct,cb,cs,ce);
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
    END LOOP;
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_hpm_deliverables;
/
