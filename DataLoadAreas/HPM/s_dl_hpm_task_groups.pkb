CREATE OR REPLACE PACKAGE BODY s_dl_hpm_task_groups
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VER     DB Ver   WHO   WHEN         WHY
--  1.0     5.12     VRS   17-SEP-2007  Product Dataload
--  1.1     5.12.0   PH    14-NOV-2007  Added final commit to validate
--  1.2     6.9.0    MB    18-MAR-2014  only validate stm_code if supplied
--  1.3     6.11.0   MJK   24-AUG-2015  Reformatted for 6.11.  No logic changes
--
--  declare package variables AND constants
--
--
  PROCEDURE dataload_create
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  AS
    CURSOR c1(p_batch_id VARCHAR2)
    IS
      SELECT ltkg_dlb_batch_id
      ,      ltkg_dl_seqno
      ,      ltkg_dl_load_status
      ,      ltkg_src_reference
      ,      ltkg_code
      ,      ltkg_src_type
      ,      ltkg_description
      ,      ltkg_start_date
      ,      ltkg_group_type
      ,      ltkg_stm_code
      FROM   dl_hpm_task_groups
      WHERE  ltkg_dlb_batch_id = p_batch_id
      AND    ltkg_dl_load_status = 'V';
    cb               VARCHAR2(30);
    cd               DATE;
    cp               VARCHAR2(30) := 'CREATE';
    ct               VARCHAR2(30) := 'DL_HPM_TASK_GROUPS';
    cs               INTEGER;
    ce               VARCHAR2(200);
    ci               INTEGER;
    l_reusable_refno NUMBER(10);
    l_bud_refno      NUMBER(10);
    l_bhe_refno      NUMBER(10);
    i                INTEGER := 0;
    l_an_tab         VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_task_groups.dataload_create');
    fsc_utils.debug_message('s_dl_hpm_task_groups.dataload_create',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    ci := s_dl_hem_utils.dl_orig_rows('DL_HPM_TASK_GROUPS');
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.ltkg_dl_seqno;
        SAVEPOINT SP1;
        INSERT INTO task_groups
        (tkg_src_reference  
        ,tkg_code   
        ,tkg_src_type   
        ,tkg_description   
        ,tkg_start_date   
        ,tkg_group_type   
        ,tkg_stm_code   
        )  
        VALUES  
        (p1.ltkg_src_reference  
        ,p1.ltkg_code  
        ,p1.ltkg_src_type  
        ,p1.ltkg_description  
        ,p1.ltkg_start_date  
        ,p1.ltkg_group_type  
        ,p1.ltkg_stm_code  
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
    l_an_tab := s_dl_hem_utils.dl_comp_stats('TASK_GROUPS',ci,i);
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
    ,p_date IN DATE
    )
  AS
    CURSOR c1
      (p_batch_id VARCHAR2)
    IS
      SELECT ltkg_dlb_batch_id
      ,      ltkg_dl_seqno
      ,      ltkg_dl_load_status
      ,      ltkg_src_reference
      ,      ltkg_code
      ,      ltkg_src_type
      ,      ltkg_description
      ,      ltkg_start_date
      ,      ltkg_group_type
      ,      ltkg_stm_code
      FROM   dl_hpm_task_groups
      WHERE  ltkg_dlb_batch_id = p_batch_id
      AND    ltkg_dl_load_status IN('L','F','O');
    CURSOR chk_task_groups(p_tkg_src_reference VARCHAR2, p_tkg_code VARCHAR2, p_tkg_src_type VARCHAR2)
    IS
      SELECT 'X'
      FROM   task_groups
      WHERE  tkg_src_reference = p_tkg_src_reference
      AND    tkg_code = p_tkg_code
      AND    tkg_src_type = p_tkg_src_type;
    CURSOR chk_stm_code(p_stm_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   standard_templates
      WHERE  stm_code = p_stm_code;
    CURSOR chk_contract(p_cnt_reference VARCHAR2)
    IS
      SELECT 'X'
      FROM   contracts
      WHERE  cnt_reference = p_cnt_reference;
    CURSOR chk_programmes(p_prg_reference VARCHAR2)
    IS
      SELECT 'X'
      FROM   programmes
      WHERE  prg_reference = p_prg_reference;
    CURSOR chk_projects(p_prj_reference VARCHAR2)
    IS
      SELECT 'X'
      FROM   projects
      WHERE  prj_reference = p_prj_reference;
    cb                 VARCHAR2(30);
    cd                 DATE;
    cp                 VARCHAR2(30) := 'VALIDATE';
    ct                 VARCHAR2(30) := 'DL_HPM_TASK_GROUPS';
    cs                 INTEGER;
    ce                 VARCHAR2(200);
    l_tkg_exists       VARCHAR2(1);
    l_stm_exists       VARCHAR2(1);
    l_cnt_exists       VARCHAR2(1);
    l_prg_exists       VARCHAR2(1);
    l_prj_exists       VARCHAR2(1);
    l_mgmt_cos_exists  VARCHAR2(1);
    l_noof_current_cve INTEGER;
    l_errors           VARCHAR2(1);
    l_error_ind        VARCHAR2(1);
    i                  INTEGER := 0;
    l_bhe_refno        NUMBER(10);
    l_bud_refno        NUMBER(10);
    l_bhe_code_exists  VARCHAR2(1);
    l_bca_year_exists  VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_task_groups.dataload_validate');
    fsc_utils.debug_message('s_dl_hpm_task_groups.dataload_validate',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.ltkg_dl_seqno;
        l_errors := 'V';
        l_error_ind := 'N';
        IF (p1.ltkg_src_reference IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',356);
        END IF;
        IF (p1.ltkg_code IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',357);
        END IF;
        IF (p1.ltkg_src_type IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',358);
        END IF;
        IF (p1.ltkg_description IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',359);
        END IF;
        IF (p1.ltkg_start_date IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',360);
        END IF;
        IF (p1.ltkg_group_type IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',361);
        END IF;
        l_tkg_exists := NULL;
        OPEN chk_task_groups(p1.ltkg_src_reference,p1.ltkg_code,p1.ltkg_src_type);
        FETCH chk_task_groups INTO l_tkg_exists;
        CLOSE chk_task_groups;
        IF (l_tkg_exists IS NOT NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',362);
        END IF;
        IF p1.ltkg_stm_code IS NOT NULL 
        THEN
          l_stm_exists := NULL;
          OPEN chk_stm_code(p1.ltkg_stm_code);
          FETCH chk_stm_code INTO l_stm_exists;
          CLOSE chk_stm_code;
          IF (l_stm_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',363);
          END IF;
        END IF;
        IF (p1.ltkg_group_type IS NOT NULL) 
        THEN
          IF (p1.ltkg_group_type NOT IN('NPAY','BUDG','PAYT')) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',364);
          END IF;
        END IF;
        IF (p1.ltkg_src_type IS NOT NULL) 
        THEN
          IF (p1.ltkg_src_type NOT IN('CNT','PRG','PRJ')) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',365);
          ELSIF (p1.ltkg_src_type = 'CNT') 
          THEN
            l_cnt_exists := NULL;
            OPEN chk_contract(p1.ltkg_src_reference);
            FETCH chk_contract INTO l_cnt_exists;
            CLOSE chk_contract;
            IF (l_cnt_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',333);
            END IF;
          ELSIF (p1.ltkg_src_type = 'PRG') 
          THEN
            l_prg_exists := NULL;
            OPEN chk_programmes(p1.ltkg_src_reference);
            FETCH chk_programmes INTO l_prg_exists;
            CLOSE chk_programmes;
            IF (l_prg_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',366);
            END IF;
          ELSIF (p1.ltkg_src_type = 'PRJ') 
          THEN
            l_prj_exists := NULL;
            OPEN chk_projects(p1.ltkg_src_reference);
            FETCH chk_projects INTO l_prj_exists;
            CLOSE chk_projects;
            IF (l_prj_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',302);
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
      WHEN OTHERS 
      THEN
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
    ,p_date IN DATE
    )
  AS
    CURSOR c1(p_batch_id VARCHAR2)
    IS
      SELECT ltkg_dlb_batch_id
      ,      ltkg_dl_seqno
      ,      ltkg_dl_load_status
      ,      ltkg_src_reference
      ,      ltkg_code
      ,      ltkg_src_type
      ,      ltkg_description
      ,      ltkg_start_date
      ,      ltkg_group_type
      ,      ltkg_stm_code
      FROM   dl_hpm_task_groups
      WHERE  ltkg_dlb_batch_id = p_batch_id
      AND    ltkg_dl_load_status IN('C');
    cb          VARCHAR2(30);
    cd          DATE;
    cp          VARCHAR2(30) := 'DELETE';
    ct          VARCHAR2(30) := 'DL_HPM_TASK_GROUPS';
    cs          INTEGER;
    ce          VARCHAR2(200);
    l_exists    VARCHAR2(1);
    l_errors    VARCHAR2(1);
    l_error_ind VARCHAR2(1);
    i           INTEGER := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_task_groups.dataload_delete');
    fsc_utils.debug_message('s_dl_hpm_task_groups.dataload_delete',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.ltkg_dl_seqno;
        i := i + 1;
        SAVEPOINT SP1;
        DELETE
        FROM   TASK_GROUPS
        WHERE  tkg_src_reference = p1.ltkg_src_reference
        AND    tkg_code = p1.ltkg_code
        AND    tkg_src_type = p1.ltkg_src_type;
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
END s_dl_hpm_task_groups;
/
