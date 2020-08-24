CREATE OR REPLACE PACKAGE BODY s_dl_hpm_contracts
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VER         DB Ver  WHO     WHEN            WHY
--  1.0         5.12    VRS     10-SEP-2007     Product Dataload
--  1.1         5.12.0  PH      11-OCT-2007     Added final commit to validate
--  1.2         5.12.0  PH      12-OCT-2007     Added new fields that insert into
--                                              contract_types and contract_sors
--                                              Also added additional validates
--                                              and deletes
--  1.3         5.12.0  PH      16-NOV-2007     Added procedure to check that 
--                                              the sum of the contracts does
--                                              not exceed the maximum project amount.
--                                              Amended create process to update
--                                              the project unallocated amount
--  1.4         5.12.0  PH      14-MAR-2008     Moved set record status flag to
--                                              inside IF statement
--  1.5         5.12.0  PH      17-MAR-2008     Added update to project_versions
--                                              on create process.
--  1.6         6.11.0  MJK     24-AUG-2015     Reformatted. no logic changes.
--
--  1.7         6.13.0  MJK     18-APR-2016     Contracts of status AUT can now be loaded
--
-- ***********************************************************************
  PROCEDURE validate_variation_amt
    (p_prj_reference            IN     projects.prj_reference%TYPE
    ,p_cnt_reference            IN     contracts.cnt_reference%TYPE
    ,p_cve_max_variation_amount IN     contract_versions.cve_max_variation_amount%TYPE
    ,p_rec_status               IN OUT VARCHAR2
    )
  IS
    CURSOR c_sum_exc_tax
      (cp_prj_reference IN VARCHAR2
      ,cp_cnt_reference IN VARCHAR2
      )
    IS
      SELECT NVL(SUM(CASE 
                     WHEN cve_current_ind = 'Y' AND cnt_sco_code != 'TER' 
                     THEN 
                       NVL(cve_max_variation_amount,0)
                     END
                    )
                ,0
                ) + 
             NVL(SUM(CASE 
                     WHEN cnt_sco_code = 'TER' 
                     THEN 
                       NVL(cnt_accrued_amount + cnt_invoiced_amount + cnt_expended_amount,0) 
                     END
                    )
                ,0
                )
      FROM   contract_versions
      ,      contracts
      WHERE  cnt_prj_reference = cp_prj_reference
      AND    cnt_reference = cve_cnt_reference
      AND    cnt_sco_code != 'CAN'
      AND    cve_cnt_reference != cp_cnt_reference;
    l_sum_cve_var_amt contract_versions.cve_max_variation_amount%TYPE;
    r_pjv project_versions%ROWTYPE;
    l_progvat parameter_values.pva_char_value%TYPE := fsc_utils.get_sys_param('PROGVAT');
    l_cnt_rec contracts%ROWTYPE;
    l_sum_cnt_expend contract_versions.cve_max_variation_amount%TYPE;
  BEGIN
    fsc_utils.proc_start('s_contract_versions.validate_variation_amt');
    fsc_utils.debug_message('s_contract_versions.validate_variation_amt '||
                            ' p_prj_reference : '|| p_prj_reference||
                            ' p_cnt_reference : '|| p_cnt_reference||
                            ' p_cve_max_variation_amount : '|| 
                            TO_CHAR(p_cve_max_variation_amount)
                           ,3
                           );
    r_pjv := s_project_versions.get_current_pjv(p_prj_reference);
    OPEN c_sum_exc_tax(p_prj_reference, p_cnt_reference);
    FETCH c_sum_exc_tax INTO l_sum_cve_var_amt;
    CLOSE c_sum_exc_tax;
    IF l_sum_cve_var_amt IS NULL 
    THEN
      l_sum_cve_var_amt := 0;
    END IF;
    l_sum_cve_var_amt := l_sum_cve_var_amt + NVL(p_cve_max_variation_amount,0);
    fsc_utils.debug_message('l_sum_cve_var_amt : '|| TO_CHAR(l_sum_cve_var_amt),4);
    fsc_utils.debug_message('r_pjv.pjv_maximum_value : '|| TO_CHAR(r_pjv.pjv_maximum_value),4);
    IF l_sum_cve_var_amt > NVL(r_pjv.pjv_maximum_value,0) 
    THEN
      p_rec_status := 'F';
    END IF;
    fsc_utils.proc_end;
  EXCEPTION
  WHEN OTHERS THEN
    fsc_utils.handle_exception;
  END validate_variation_amt;
  --
  PROCEDURE dataload_create
    (p_batch_id IN VARCHAR2
    ,p_date IN DATE
    )
  AS
    CURSOR c1(p_batch_id VARCHAR2)
    IS
      SELECT rowid rec_rowid
      ,      lcnt_dlb_batch_id
      ,      lcnt_dl_seqno
      ,      lcnt_dl_load_status
      ,      lcnt_reference
      ,      lcnt_prj_reference
      ,      lcnt_aun_code
      ,      lcnt_sco_code
      ,      lcnt_status_date
      ,      lcnt_authorised_by
      ,      NVL(lcnt_warn_hrm_users_ind,'N')     lcnt_warn_hrm_users_ind
      ,      NVL(lcnt_drawings_ind,'N')           lcnt_drawings_ind
      ,      lcnt_cos_code
      ,      lcnt_file_ref
      ,      lcnt_alternative_reference
      ,      lcnt_comments
      ,      NVL(lcnt_reschedule_allowed_ind,'N') lcnt_reschedule_allowed_ind
      ,      NVL(lcve_version_number,1)           lcve_version_number
      ,      NVL(lcve_current_ind,'N')            lcve_current_ind
      ,      lcve_description
      ,      lcve_rpt_in_planned_work_ind
      ,      lcve_bca_year
      ,      lcve_bhe_code
      ,      lcve_cnt_ref_associated_with
      ,      lcve_hrv_pyr_code
      ,      lcve_estimated_start_date
      ,      lcve_estimated_end_date
      ,      lcve_projected_cost
      ,      lcve_projected_cost_tax
      ,      lcve_contract_value
      ,      lcve_max_variation_amount
      ,      lcve_max_variation_tax_amt
      ,      lcve_non_comp_damages_amt
      ,      lcve_non_comp_damages_unit
      ,      lcve_liability_period
      ,      lcve_penult_retention_pct
      ,      lcve_interim_retention_pct
      ,      lcve_interim_pymnt_interval
      ,      lcve_interim_pymnt_int_unit
      ,      lcve_final_measure_period
      ,      lcve_max_no_of_repeats
      ,      lcve_repeat_period
      ,      lcve_repeat_period_unit
      ,      NVL(lcve_retentions_ind,'N')         lcve_retentions_ind
      ,      lcve_final_measure_period_unit
      ,      lcvs_sor_code
      ,      lcvs_repeat_unit
      ,      lcvs_repeat_period_ind
      ,      lctt_hrv_ctp_code1
      ,      lctt_hrv_ctp_code2
      ,      lctt_hrv_ctp_code3
      ,      lctt_hrv_ctp_code4
      FROM   dl_hpm_contracts
      WHERE  lcnt_dlb_batch_id = p_batch_id
      AND    lcnt_dl_load_status = 'V';
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
    CURSOR c_proj_vers(p_prj_reference VARCHAR2)
    IS
      SELECT pjv_version_number
      FROM   project_versions
      WHERE  pjv_prj_reference = p_prj_reference
      AND    pjv_current_ind = 'Y';
    cb               VARCHAR2(30) ;
    cd               DATE;
    cp               VARCHAR2(30) := 'CREATE';
    ct               VARCHAR2(30) := 'DL_HPM_CONTRACTS';
    cs               INTEGER;
    ce               VARCHAR2(200) ;
    ci               INTEGER;
    l_reusable_refno NUMBER(10) ;
    l_bud_refno      NUMBER(10) ;
    l_bhe_refno      NUMBER(10) ;
    i                INTEGER := 0;
    l_an_tab         VARCHAR2(1) ;
    l_rec_status     VARCHAR2(1) ;
    l_pjv_vers project_versions.pjv_version_number%type;
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_contracts.dataload_create');
    fsc_utils.debug_message('s_dl_hpm_contracts.dataload_create',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    ci := s_dl_hem_utils.dl_orig_rows('DL_HPM_CONTRACTS');
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.lcnt_dl_seqno;
        SAVEPOINT SP1;
        l_rec_status := 'V';
        validate_variation_amt(p1.lcnt_prj_reference, p1.lcnt_reference, p1.lcve_max_variation_amount, l_rec_status);
        IF l_rec_status = 'F' 
        THEN
          ROLLBACK TO SP1;
          ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',508);
          s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
          s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
        ELSE
          INSERT INTO contracts
          (cnt_reference  
          ,cnt_prj_reference   
          ,cnt_aun_code   
          ,cnt_sco_code   
          ,cnt_status_date 
          ,cnt_authorised_datetime
          ,cnt_authorised_by
          ,cnt_warn_hrm_users_ind   
          ,cnt_drawings_ind   
          ,cnt_cos_code   
          ,cnt_file_ref   
          ,cnt_alternative_reference   
          ,cnt_comments   
          ,cnt_reschedule_allowed_ind   
          )  
          VALUES  
          (p1.lcnt_reference
          ,p1.lcnt_prj_reference   
          ,p1.lcnt_aun_code   
          ,p1.lcnt_sco_code   
          ,p1.lcnt_status_date
          ,DECODE(p1.lcnt_sco_code,'RAI',NULL,p1.lcnt_status_date)
          ,DECODE(p1.lcnt_sco_code,'RAI',NULL,NVL(p1.lcnt_authorised_by,USER))
          ,p1.lcnt_warn_hrm_users_ind   
          ,p1.lcnt_drawings_ind   
          ,p1.lcnt_cos_code   
          ,p1.lcnt_file_ref   
          ,p1.lcnt_alternative_reference   
          ,p1.lcnt_comments   
          ,p1.lcnt_reschedule_allowed_ind   
          );  
          l_reusable_refno := NULL;
          OPEN c_reusable_refno;
          FETCH c_reusable_refno INTO l_reusable_refno;
          CLOSE c_reusable_refno;
          l_bhe_refno := NULL;
          OPEN c_bhe_refno(p1.LCVE_BHE_CODE);
          FETCH c_bhe_refno INTO l_bhe_refno;
          CLOSE c_bhe_refno;
          l_bud_refno := NULL;
          OPEN c_bud_refno(l_bhe_refno,p1.LCVE_BCA_YEAR) ;
          FETCH c_bud_refno INTO l_bud_refno;
          CLOSE c_bud_refno;
          INSERT INTO contract_versions
          (cve_cnt_reference  
          ,cve_version_number   
          ,cve_current_ind   
          ,cve_description   
          ,cve_rpt_in_planned_work_ind   
          ,cve_reusable_refno   
          ,cve_bud_refno   
          ,cve_cnt_ref_associated_with   
          ,cve_hrv_pyr_code   
          ,cve_estimated_start_date   
          ,cve_estimated_end_date   
          ,cve_projected_cost   
          ,cve_projected_cost_tax   
          ,cve_contract_value   
          ,cve_max_variation_amount   
          ,cve_max_variation_tax_amt   
          ,cve_non_comp_damages_amt   
          ,cve_non_comp_damages_unit   
          ,cve_liability_period   
          ,cve_penult_retention_pct   
          ,cve_interim_retention_pct   
          ,cve_interim_pymnt_interval   
          ,cve_interim_pymnt_int_unit   
          ,cve_final_measure_period   
          ,cve_max_no_of_repeats   
          ,cve_repeat_period   
          ,cve_repeat_period_unit   
          ,cve_retentions_ind   
          ,cve_final_measure_period_unit   
          )  
          VALUES  
          (p1.lcnt_reference  
          ,p1.lcve_version_number   
          ,p1.lcve_current_ind   
          ,p1.lcve_description   
          ,p1.lcve_rpt_in_planned_work_ind   
          ,l_reusable_refno   
          ,l_bud_refno   
          ,p1.lcve_cnt_ref_associated_with   
          ,p1.lcve_hrv_pyr_code   
          ,p1.lcve_estimated_start_date   
          ,p1.lcve_estimated_end_date   
          ,p1.lcve_projected_cost   
          ,p1.lcve_projected_cost_tax   
          ,p1.lcve_contract_value   
          ,p1.lcve_max_variation_amount   
          ,p1.lcve_max_variation_tax_amt   
          ,p1.lcve_non_comp_damages_amt   
          ,p1.lcve_non_comp_damages_unit   
          ,p1.lcve_liability_period   
          ,p1.lcve_penult_retention_pct   
          ,p1.lcve_interim_retention_pct   
          ,p1.lcve_interim_pymnt_interval   
          ,p1.lcve_interim_pymnt_int_unit   
          ,p1.lcve_final_measure_period   
          ,p1.lcve_max_no_of_repeats   
          ,p1.lcve_repeat_period   
          ,p1.lcve_repeat_period_unit   
          ,p1.lcve_retentions_ind   
          ,p1.lcve_final_measure_period_unit   
          );  
          IF p1.lcvs_sor_code IS NOT NULL 
          THEN
            INSERT INTO contract_sors
            (cvs_cve_cnt_reference  
            ,cvs_cve_version_number   
            ,cvs_sor_code   
            ,cvs_current_ind   
            ,cvs_created_by   
            ,cvs_created_date   
            ,cvs_repeat_unit   
            ,cvs_repeat_period_ind   
            )  
            VALUES  
            (p1.lcnt_reference  
            ,p1.lcve_version_number   
            ,p1.lcvs_sor_code   
            ,'Y'   
            ,'DATALOAD'   
            ,SYSDATE   
            ,p1.lcvs_repeat_unit   
            ,p1.lcvs_repeat_period_ind   
            );  
          END IF;
          IF p1.LCTT_HRV_CTP_CODE1 IS NOT NULL 
          THEN
            INSERT INTO contract_types
            (ctt_cve_cnt_reference  
            ,ctt_cve_version_number   
            ,ctt_hrv_ctp_code   
            ,ctt_primary_ind   
            ,ctt_created_by   
            ,ctt_created_date   
            )  
            VALUES  
            (p1.lcnt_reference  
            ,p1.lcve_version_number   
            ,p1.lctt_hrv_ctp_code1   
            ,'Y'   
            ,'DATALOAD'   
            ,SYSDATE   
            );  
          END IF;
          IF p1.lctt_hrv_ctp_code2 IS NOT NULL 
          THEN
            INSERT INTO contract_types
              (CTT_CVE_CNT_REFERENCE
              ,CTT_CVE_VERSION_NUMBER 
              ,CTT_HRV_CTP_CODE 
              ,CTT_PRIMARY_IND 
              ,CTT_CREATED_BY 
              ,CTT_CREATED_DATE 
              )
              VALUES
              (p1.lcnt_reference
              ,p1.lcve_version_number 
              ,p1.lctt_hrv_ctp_code2 
              ,'N' 
              ,'DATALOAD' 
              ,SYSDATE 
              ); 
          END IF;
          IF p1.lctt_hrv_ctp_code3 IS NOT NULL 
          THEN
            INSERT INTO contract_types
            (ctt_cve_cnt_reference  
            ,ctt_cve_version_number   
            ,ctt_hrv_ctp_code   
            ,ctt_primary_ind   
            ,ctt_created_by   
            ,ctt_created_date   
            )  
            VALUES  
            (p1.lcnt_reference  
            ,p1.lcve_version_number   
            ,p1.lctt_hrv_ctp_code3   
            ,'N'   
            ,'DATALOAD'   
            ,SYSDATE   
            );   
          END IF;
          IF p1.lctt_hrv_ctp_code4 IS NOT NULL 
          THEN
            INSERT INTO contract_types
            (ctt_cve_cnt_reference  
            ,ctt_cve_version_number   
            ,ctt_hrv_ctp_code   
            ,ctt_primary_ind   
            ,ctt_created_by   
            ,ctt_created_date   
            )  
            VALUES  
            (p1.lcnt_reference  
            ,p1.lcve_version_number   
            ,p1.lctt_hrv_ctp_code4   
            ,'N'   
            ,'DATALOAD'   
            ,SYSDATE   
            );   
          END IF;
          UPDATE projects
          SET    prj_net_allocated_amount = NVL(prj_net_allocated_amount,0) + NVL(p1.lcve_max_variation_amount,0)
          WHERE  prj_reference = p1.lcnt_prj_reference;
          l_pjv_vers := NULL;
          OPEN c_proj_vers(p1.lcnt_prj_reference) ;
          FETCH c_proj_vers INTO l_pjv_vers;
          CLOSE c_proj_vers;
          UPDATE project_versions
          SET    pjv_planned_cost = NVL(pjv_planned_cost,0) + NVL(p1.lcve_projected_cost,0)
          WHERE  pjv_prj_reference = p1.lcnt_prj_reference
          AND    pjv_version_number = l_pjv_vers;
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N') ;
          s_dl_utils.set_record_status_flag(ct,cb,cs,'C') ;
        END IF;
        i := i + 1;
        IF MOD(i,1000) = 0 
        THEN
          COMMIT;
        END IF;
      EXCEPTION
      WHEN OTHERS 
      THEN
        ROLLBACK TO SP1;
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM) ;
        s_dl_utils.set_record_status_flag(ct,cb,cs,'O') ;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y') ;
      END;
    END LOOP;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('CONTRACTS',ci,i) ;
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED') ;
    RAISE;
  END dataload_create;
  --
  PROCEDURE dataload_validate
    (p_batch_id IN VARCHAR2
    ,p_date IN DATE
    )
  AS
    CURSOR c1(p_batch_id VARCHAR2)
    IS
      SELECT rowid rec_rowid
      ,      lcnt_dlb_batch_id
      ,      lcnt_dl_seqno
      ,      lcnt_dl_load_status
      ,      lcnt_reference
      ,      lcnt_prj_reference
      ,      lcnt_aun_code
      ,      lcnt_sco_code
      ,      lcnt_status_date
      ,      lcnt_authorised_by
      ,      NVL(lcnt_warn_hrm_users_ind,'N')     lcnt_warn_hrm_users_ind
      ,      NVL(lcnt_drawings_ind,'N')           lcnt_drawings_ind
      ,      lcnt_cos_code
      ,      lcnt_file_ref
      ,      lcnt_alternative_reference
      ,      lcnt_comments
      ,      NVL(lcnt_reschedule_allowed_ind,'N') lcnt_reschedule_allowed_ind
      ,      NVL(lcve_version_number,1)           lcve_version_number
      ,      NVL(lcve_current_ind,'N')            lcve_current_ind
      ,      lcve_description
      ,      lcve_rpt_in_planned_work_ind
      ,      lcve_bca_year
      ,      lcve_bhe_code
      ,      lcve_cnt_ref_associated_with
      ,      lcve_hrv_pyr_code
      ,      lcve_estimated_start_date
      ,      lcve_estimated_end_date
      ,      lcve_projected_cost
      ,      lcve_projected_cost_tax
      ,      lcve_contract_value
      ,      lcve_max_variation_amount
      ,      lcve_max_variation_tax_amt
      ,      lcve_non_comp_damages_amt
      ,      lcve_non_comp_damages_unit
      ,      lcve_liability_period
      ,      lcve_penult_retention_pct
      ,      lcve_interim_retention_pct
      ,      lcve_interim_pymnt_interval
      ,      lcve_interim_pymnt_int_unit
      ,      lcve_final_measure_period
      ,      lcve_max_no_of_repeats
      ,      lcve_repeat_period
      ,      lcve_repeat_period_unit
      ,      NVL(lcve_retentions_ind,'N')         lcve_retentions_ind
      ,      lcve_final_measure_period_unit
      ,      lcvs_sor_code
      ,      lcvs_repeat_unit
      ,      lcvs_repeat_period_ind
      ,      lctt_hrv_ctp_code1
      ,      lctt_hrv_ctp_code2
      ,      lctt_hrv_ctp_code3
      ,      lctt_hrv_ctp_code4
      FROM   dl_hpm_contracts
      WHERE  lcnt_dlb_batch_id = p_batch_id
      AND    lcnt_dl_load_status IN('L','F','O');
    CURSOR chk_contract(p_cnt_reference VARCHAR2)
    IS
      SELECT 'X'
      FROM   contracts
      WHERE  cnt_reference = p_cnt_reference;
    CURSOR chk_projects(p_prj_reference VARCHAR2)
    IS
      SELECT 'X'
      FROM   projects
      WHERE  prj_reference = p_prj_reference;
    CURSOR chk_sco_code(p_sco_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   status_codes
      WHERE  sco_code = p_sco_code;
    CURSOR chk_cos_code(p_cos_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   contractor_sites
      WHERE  cos_code = p_cos_code;
    CURSOR chk_mgmt_cos(p_aun_code VARCHAR2, p_cos_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   management_area_cos
      WHERE  mcs_aun_code = p_aun_code
      AND    mcs_cos_code = p_cos_code;
    CURSOR chk_contract_versions(p_cnt_reference VARCHAR2, p_version_number NUMBER)
    IS
      SELECT 'X'
      FROM   contract_versions
      WHERE  cve_cnt_reference = p_cnt_reference
      AND    cve_version_number = p_version_number;
    CURSOR chk_noof_current_cve(p_cnt_reference VARCHAR2)
    IS
      SELECT COUNT(*)
      FROM   contract_versions
      WHERE  cve_cnt_reference = p_cnt_reference
      AND    cve_current_ind = 'Y';
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
    CURSOR chk_sor_code(p_sor_code VARCHAR2)
    IS
      SELECT 'x'
      FROM   schedule_of_rates
      WHERE  sor_code = p_sor_code;
    cb                 VARCHAR2(30);
    cd                 DATE;
    cp                 VARCHAR2(30) := 'VALIDATE';
    ct                 VARCHAR2(30) := 'DL_HPM_CONTRACTS';
    cs                 INTEGER;
    ce                 VARCHAR2(200);
    l_cnt_exists       VARCHAR2(1);
    l_prj_exists       VARCHAR2(1);
    l_sco_exists       VARCHAR2(1);
    l_cos_exists       VARCHAR2(1);
    l_cve_exists       VARCHAR2(1);
    l_mgmt_cos_exists  VARCHAR2(1);
    l_noof_current_cve INTEGER;
    l_errors           VARCHAR2(1);
    l_error_ind        VARCHAR2(1);
    i                  INTEGER := 0;
    l_bhe_refno        NUMBER(10);
    l_bud_refno        NUMBER(10);
    l_bhe_code_exists  VARCHAR2(1);
    l_bca_year_exists  VARCHAR2(1);
    l_sor_exists       VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_contracts.dataload_validate');
    fsc_utils.debug_message('s_dl_hpm_contracts.dataload_validate',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.lcnt_dl_seqno;
        l_errors := 'V';
        l_error_ind := 'N';
        IF p1.lcnt_reference IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',291);
        END IF;
        IF p1.lcnt_prj_reference IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',292);
        END IF;
        IF p1.lcnt_aun_code IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',144);
        END IF;
        IF p1.lcnt_sco_code IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',168);
        END IF;
        IF p1.lcnt_status_date IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',293);
        END IF;
        IF p1.lcnt_warn_hrm_users_ind IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',294);
        END IF;
        IF p1.lcnt_drawings_ind IS NULL
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',295);
        END IF;
        IF p1.lcnt_reschedule_allowed_ind IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',296);
        END IF;
        IF p1.lcve_version_number IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',297);
        END IF;
        IF p1.lcve_current_ind IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',298);
        END IF;
        IF p1.lcve_description IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',299);
        END IF;
        IF p1.lcve_rpt_in_planned_work_ind IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',300);
        END IF;
        IF p1.lcve_bca_year IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',327);
        END IF;
        IF p1.lcve_bhe_code IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',526);
        END IF;
        l_cnt_exists := NULL;
        OPEN chk_contract(p1.lcnt_reference);
        FETCH chk_contract INTO l_cnt_exists;
        CLOSE chk_contract;
        IF l_cnt_exists IS NOT NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',301);
        END IF;
        l_prj_exists := NULL;
        OPEN chk_projects(p1.lcnt_prj_reference);
        FETCH chk_projects INTO l_prj_exists;
        CLOSE chk_projects;
        IF l_prj_exists IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',302);
        END IF;
        IF p1.lcnt_aun_code IS NOT NULL 
        THEN
          IF NOT s_dl_hem_utils.exists_aun_code(p1.lcnt_aun_code) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
          END IF;
        END IF;
        l_sco_exists := NULL;
        IF p1.lcnt_sco_code IS NOT NULL 
        THEN
          OPEN chk_sco_code(p1.lcnt_sco_code);
          FETCH chk_sco_code INTO l_sco_exists;
          CLOSE chk_sco_code;
          IF l_sco_exists IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',531);
          END IF;
          IF p1.lcnt_sco_code NOT IN ('RAI','AUT') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',900);
          END IF;
        END IF;
        IF p1.lcnt_warn_hrm_users_ind NOT IN ('Y','N') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',303);
        END IF;
        IF p1.lcnt_drawings_ind NOT IN ('Y','N') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',304);
        END IF;
        IF p1.lcnt_reschedule_allowed_ind NOT IN ('Y','N') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',305);
        END IF;
        IF p1.lcnt_cos_code IS NOT NULL 
        THEN
          l_cos_exists := NULL;
          OPEN chk_cos_code(p1.lcnt_cos_code);
          FETCH chk_cos_code
          INTO l_cos_exists;
          CLOSE chk_cos_code;
          IF l_cos_exists IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',244);
          END IF;
        END IF;
        IF p1.lcnt_aun_code IS NOT NULL AND p1.lcnt_cos_code IS NOT NULL 
        THEN
          l_mgmt_cos_exists := NULL;
          OPEN chk_mgmt_cos(p1.lcnt_aun_code,p1.lcnt_cos_code);
          FETCH chk_mgmt_cos INTO l_mgmt_cos_exists;
          CLOSE chk_mgmt_cos;
          IF l_mgmt_cos_exists IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',306);
          END IF;
        END IF;
        IF p1.lcve_current_ind NOT IN ('Y','N') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',307);
        END IF;
        IF p1.lcve_rpt_in_planned_work_ind NOT IN ('Y','N') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',308);
        END IF;
        IF p1.lcve_cnt_ref_associated_with IS NOT NULL 
        THEN
          l_cnt_exists := NULL;
          OPEN chk_contract(p1.lcve_cnt_ref_associated_with);
          FETCH chk_contract INTO l_cnt_exists;
          CLOSE chk_contract;
          IF l_cnt_exists IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',309);
          END IF;
        END IF;
        IF p1.lcve_hrv_pyr_code IS NOT NULL 
        THEN
          IF NOT s_dl_hem_utils.exists_frv('PENRULE',p1.lcve_hrv_pyr_code,'Y') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',395);
          END IF;
        END IF;
        IF p1.lcve_non_comp_damages_unit IS NOT NULL 
        THEN
          IF p1.lcve_non_comp_damages_unit NOT IN ('DAY','WDAY','WEEK','MNTH','YEAR') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',310);
          END IF;
        END IF;
        IF p1.lcve_interim_pymnt_int_unit IS NOT NULL 
        THEN
          IF p1.lcve_interim_pymnt_int_unit NOT IN ('DAY','WDAY','WEEK','MNTH','YEAR') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',311);
          END IF;
        END IF;
        IF p1.lcve_repeat_period_unit IS NOT NULL 
        THEN
          IF p1.lcve_repeat_period_unit NOT IN ('DAY','WDAY','WEEK','MNTH','YEAR') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',312);
          END IF;
        END IF;
        IF p1.lcnt_reference IS NOT NULL AND p1.lcve_version_number IS NOT NULL 
        THEN
          l_cve_exists := NULL;
          OPEN chk_contract_versions(p1.lcnt_reference,p1.lcve_version_number);
          FETCH chk_contract_versions INTO l_cve_exists;
          CLOSE chk_contract_versions;
          IF l_cve_exists IS NOT NULL
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',313);
          END IF;
        END IF;
        IF p1.lcve_projected_cost IS NOT NULL 
        THEN
          IF p1.lcve_projected_cost < 0 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',314);
          END IF;
        END IF;
        IF p1.lcve_projected_cost_tax IS NOT NULL 
        THEN
          IF p1.lcve_projected_cost_tax < 0 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',315);
          END IF;
        END IF;
        IF p1.lcve_contract_value IS NOT NULL 
        THEN
          IF p1.lcve_contract_value < 0 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',316);
          END IF;
        END IF;
        IF p1.lcve_max_variation_amount IS NOT NULL 
        THEN
          IF p1.lcve_max_variation_amount < 0 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',317);
          END IF;
        END IF;
        IF p1.lcve_max_variation_tax_amt IS NOT NULL 
        THEN
          IF p1.lcve_max_variation_tax_amt < 0 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',318);
          END IF;
        END IF;
        IF p1.lcve_non_comp_damages_amt IS NOT NULL 
        THEN
          IF (p1.lcve_non_comp_damages_amt < 0) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',319);
          END IF;
        END IF;
        IF p1.lcve_penult_retention_pct IS NOT NULL 
        THEN
          IF p1.lcve_penult_retention_pct < 0 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',320);
          END IF;
        END IF;
        IF p1.lcve_interim_retention_pct IS NOT NULL 
        THEN
          IF p1.lcve_interim_retention_pct < 0 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',321);
          END IF;
        END IF;
        IF p1.lcve_liability_period IS NOT NULL 
        THEN
          IF p1.lcve_liability_period NOT BETWEEN 1 AND 999 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',322);
          END IF;
        END IF;
        IF p1.lcve_interim_pymnt_interval IS NOT NULL 
        THEN
          IF p1.lcve_interim_pymnt_interval NOT BETWEEN 1 AND 999 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',323);
          END IF;
        END IF;
        IF p1.lcve_final_measure_period IS NOT NULL 
        THEN
          IF p1.lcve_final_measure_period NOT BETWEEN 1 AND 999 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',324);
          END IF;
        END IF;
        IF p1.lcve_max_no_of_repeats IS NOT NULL 
        THEN
          IF p1.lcve_max_no_of_repeats NOT BETWEEN 0 AND 999 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',325);
          END IF;
        END IF;
        IF p1.lcve_current_ind = 'Y' 
        THEN
          l_noof_current_cve := NULL;
          OPEN chk_noof_current_cve(p1.lcnt_reference);
          FETCH chk_noof_current_cve INTO l_noof_current_cve;
          CLOSE chk_noof_current_cve;
          IF l_noof_current_cve > 0 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',326);
          END IF;
        END IF;
        l_bhe_code_exists := NULL;
        OPEN chk_bhe_code(p1.lcve_bhe_code);
        FETCH chk_bhe_code INTO l_bhe_code_exists;
        CLOSE chk_bhe_code;
        IF l_bhe_code_exists IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',328);
        END IF;
        l_bca_year_exists := NULL;
        OPEN chk_bca_year(p1.lcve_bca_year);
        FETCH chk_bca_year INTO l_bca_year_exists;
        CLOSE chk_bca_year;
        IF l_bca_year_exists IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',528);
        END IF;
        IF l_bhe_code_exists IS NOT NULL AND l_bca_year_exists IS NOT NULL 
        THEN
          l_bhe_refno := NULL;
          OPEN c_bhe_refno(p1.lcve_bhe_code);
          FETCH c_bhe_refno INTO l_bhe_refno;
          CLOSE c_bhe_refno;
          l_bud_refno := NULL;
          OPEN c_bud_refno(l_bhe_refno,p1.lcve_bca_year);
          FETCH c_bud_refno INTO l_bud_refno;
          CLOSE c_bud_refno;
          IF l_bud_refno IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',329);
          END IF;
        END IF;
        IF p1.lcve_retentions_ind NOT IN ('Y','N') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',393);
        END IF;
        IF p1.lcve_final_measure_period_unit IS NOT NULL 
        THEN
          IF p1.lcve_final_measure_period_unit NOT IN ('DAY','WDAY','WEEK','MNTH','YEAR') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',394);
          END IF;
        END IF;
        l_sor_exists := NULL;
        IF p1.lcvs_sor_code IS NOT NULL 
        THEN
          OPEN chk_sor_code(p1.lcvs_sor_code);
          FETCH chk_sor_code INTO l_sor_exists;
          CLOSE chk_sor_code;
          IF l_sor_exists IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',344);
          END IF;
          IF p1.lcvs_repeat_period_ind IS NOT NULL 
          THEN
            IF p1.lcvs_repeat_period_ind NOT IN ('D','W','M','Y') 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',399);
            END IF;
          END IF;
        END IF;
        IF NOT s_dl_hem_utils.exists_frv('CNTTYPE',p1.lctt_hrv_ctp_code1,'N') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',400);
        END IF;
        IF p1.lctt_hrv_ctp_code2 IS NOT NULL 
        THEN
          IF NOT s_dl_hem_utils.exists_frv('CNTTYPE',p1.lctt_hrv_ctp_code2,'Y') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',401);
          END IF;
        END IF;
        IF p1.lctt_hrv_ctp_code3 IS NOT NULL 
        THEN
          IF NOT s_dl_hem_utils.exists_frv('CNTTYPE',p1.lctt_hrv_ctp_code3,'Y') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',402);
          END IF;
        END IF;
        IF p1.lctt_hrv_ctp_code4 IS NOT NULL 
        THEN
          IF NOT s_dl_hem_utils.exists_frv('CNTTYPE',p1.lctt_hrv_ctp_code4,'Y') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',403);
          END IF;
        END IF;
        IF p1.lctt_hrv_ctp_code1 IS NULL 
        THEN
          IF p1.lctt_hrv_ctp_code2 IS NOT NULL 
          OR p1.lctt_hrv_ctp_code3 IS NOT NULL 
          OR p1.lctt_hrv_ctp_code4 IS NOT NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',404);
          END IF;
        END IF;
        IF l_errors = 'F' 
        THEN
          l_error_ind := 'Y';
        ELSE
          l_error_ind := 'N';
        END IF;
        i := i + 1;
        IF MOD(i,1000) = 0 
        THEN
          COMMIT;
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
        s_dl_utils.set_record_status_flag(ct,cb,cs,l_errors);
      EXCEPTION
      WHEN OTHERS 
      THEN
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
        s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
      END;
    END LOOP;
    COMMIT;
    fsc_utils.proc_end;
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
      ,      lcnt_dlb_batch_id
      ,      lcnt_dl_seqno
      ,      lcnt_dl_load_status
      ,      lcnt_reference
      ,      lcnt_prj_reference
      ,      lcnt_aun_code
      ,      lcnt_sco_code
      ,      lcnt_status_date
      ,      NVL(lcnt_warn_hrm_users_ind,'N')     lcnt_warn_hrm_users_ind
      ,      NVL(lcnt_drawings_ind,'N')           lcnt_drawings_ind
      ,      lcnt_cos_code
      ,      lcnt_file_ref
      ,      lcnt_alternative_reference
      ,      lcnt_comments
      ,      NVL(lcnt_reschedule_allowed_ind,'N') lcnt_reschedule_allowed_ind
      ,      NVL(lcve_version_number,1)           lcve_version_number
      ,      NVL(lcve_current_ind,'N')            lcve_current_ind
      ,      lcve_description
      ,      lcve_rpt_in_planned_work_ind
      ,      lcve_bca_year
      ,      lcve_bhe_code
      ,      lcve_cnt_ref_associated_with
      ,      lcve_hrv_pyr_code
      ,      lcve_estimated_start_date
      ,      lcve_estimated_end_date
      ,      lcve_projected_cost
      ,      lcve_projected_cost_tax
      ,      lcve_contract_value
      ,      lcve_max_variation_amount
      ,      lcve_max_variation_tax_amt
      ,      lcve_non_comp_damages_amt
      ,      lcve_non_comp_damages_unit
      ,      lcve_liability_period
      ,      lcve_penult_retention_pct
      ,      lcve_interim_retention_pct
      ,      lcve_interim_pymnt_interval
      ,      lcve_interim_pymnt_int_unit
      ,      lcve_final_measure_period
      ,      lcve_max_no_of_repeats
      ,      lcve_repeat_period
      ,      lcve_repeat_period_unit
      ,      NVL(lcve_retentions_ind,'N')         lcve_retentions_ind
      ,      lcve_final_measure_period_unit
      ,      lcvs_sor_code
      ,      lcvs_repeat_unit
      ,      lcvs_repeat_period_ind
      ,      lctt_hrv_ctp_code1
      ,      lctt_hrv_ctp_code2
      ,      lctt_hrv_ctp_code3
      ,      lctt_hrv_ctp_code4
      FROM   dl_hpm_contracts
      WHERE  lcnt_dlb_batch_id = p_batch_id
      AND    lcnt_dl_load_status IN ('C');
    cb          VARCHAR2(30);
    cd          DATE;
    cp          VARCHAR2(30) := 'DELETE';
    ct          VARCHAR2(30) := 'DL_HPM_CONTRACTS';
    cs          INTEGER;
    ce          VARCHAR2(200);
    l_exists    VARCHAR2(1);
    l_errors    VARCHAR2(1);
    l_error_ind VARCHAR2(1);
    i           INTEGER := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_contracts.dataload_delete');
    fsc_utils.debug_message('s_dl_hpm_contracts.dataload_delete',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.lcnt_dl_seqno;
        i := i + 1;
        SAVEPOINT SP1;
        DELETE 
        FROM  contract_sors
        WHERE cvs_cve_cnt_reference = p1.lcnt_reference
        AND   cvs_cve_version_number = p1.lcve_version_number;
        DELETE 
        FROM  contract_types
        WHERE ctt_cve_cnt_reference = p1.lcnt_reference
        AND   ctt_cve_version_number = p1.lcve_version_number;
        DELETE
        FROM  contract_versions
        WHERE cve_cnt_reference = p1.lcnt_reference
        AND   cve_version_number = p1.lcve_version_number;
        DELETE
        FROM  contracts
        WHERE cnt_reference = p1.lcnt_reference;
        UPDATE projects
        SET    prj_net_allocated_amount = NVL(prj_net_allocated_amount,0) - NVL(p1.lcve_max_variation_amount,0)
        WHERE  prj_reference = p1.lcnt_prj_reference;
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
END s_dl_hpm_contracts;
/
