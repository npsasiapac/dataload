CREATE OR REPLACE PACKAGE BODY s_dl_hpm_tasks
AS
-- ***********************************************************************
--  DESCRIPTION: BESPOKE QUEENSLAND - VERSION
-- ***********************************************************************
--
--  CHANGE CONTROL
--
--  VER   DB Ver   WHO    WHEN         WHY
--  1.0   5.12     VRS    20-SEP-2007  Product Dataload
--  1.1   5.12.0   PH     29-OCT-2007  Amended validate for bca_year
--                                     only for payment tasks
--  1.2   5.12.1   PH     14-NOV-2007  Tidied up validate on Financial 
--                                     and non financial tasks for budgets.
--  1.3   5.12.1   PH     20-NOV-2007  Amended validate, moved setting of
--                                     local variables to null to start of
--                                     loop. Some were in IF statements and
--                                     did not get reset for next record.
--                                     Moved the get task aid from validate to
--                                     create process as there could be records
--                                     within the same batch.
--  1.4   5.12.1   PH     11-AUG-2009  Added batch question to allow
--                                     Anchor to update existing task, involves
--                                     changes to create and validate
--  1.5   6.11.0   MJK    29-AUG-2015  Reformatted for 6.11.  No logic changes
--  1.6   6.13.0   AJ     19-OCT-2016  1)VAT validation rules amended
--                                     2)Batch Question "Update existing Task?" commented out
--                                     just in case Anchor need new version but batch question
--                                     has been removed from hdl_dlas_in sql also l_answer set
--                                     to "N" in validate
--                                     3)limited tasks on where Contracts are at a RAI status only
--
-- BESPOKE QUEENSLAND VERSION FROM 1.7 ONWARDS...SEPARATE BESPOKE GUIDE ALSO from v6.13(7.2) onwards
--
--  1.7   6.13.0   AJ     27-JAN-2016  limited tasks on where Contracts are at a RAI status only
--                                     removed and replaced with
--                                     Contracts(RAI) Tsk Type = N then Tsks Status(RAI or COM)
--                                     Contracts(RAI) Task Type (B or P) then Tsk Status(RAI) only 
--                                     Contracts(AUT) All Task Types at Status(AUT or COM) only 
--                                     Contracts must be at Status(RAI or AUT) only
--                                     Projects must be at Status(RAI) only
--                                     Programmes must be at Status(RAI) only
--  1.8   6.15.0   AJ     27-FEB-2017  added validation to make ltsk_alt_reference mandatory as
--                                     needed for new task payments and deliverable valuations
--                                     loaded to link payment task in tasks table. Also needs to
--                                     be unique so checks added for duplicates on tasks and batch
--
--  declare package variables AND constants
--
--
-- ***********************************************************************
  PROCEDURE dataload_create
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  AS
    CURSOR c1(p_batch_id VARCHAR2)
    IS
      SELECT rowid                      ltsk_rowid
      ,      ltsk_dlb_batch_id
      ,      ltsk_dl_seqno
      ,      ltsk_dl_load_status
      ,      ltsk_tkg_src_reference
      ,      ltsk_tkg_code
      ,      ltsk_tkg_src_type
      ,      ltsk_type_ind
      ,      ltsk_stk_code
      ,      ltsk_sco_code
      ,      ltsk_status_date
      ,      ltsk_alt_reference
      ,      ltsk_actual_end_date
      ,      ltsk_id
      ,      NVL(ltve_version_number,1) ltve_version_number
      ,      NVL(ltve_current_ind,'N')  ltve_current_ind
      ,      ltve_display_sequence
      ,      ltve_hrv_tus_code
      ,      ltve_bca_year
      ,      ltve_vca_code
      ,      ltve_planned_start_date
      ,      ltve_net_amount
      ,      ltve_tax_amount
      ,      ltve_retention_percent
      ,      ltve_retention_period
      ,      ltve_retention_period_units
      ,      ltve_comments
      ,      ltba_bhe_code
      ,      ltba_bca_year
      ,      ltba_net_amount
      ,      ltba_tax_amount
      FROM   dl_hpm_tasks
      WHERE  ltsk_dlb_batch_id = p_batch_id
      AND    ltsk_dl_load_status = 'V';
    CURSOR c_reusable_refno
    IS
      SELECT reusable_refno_seq.nextval
      FROM   dual;
    CURSOR get_task_id(p_tsk_tkg_src_reference VARCHAR2, p_tsk_tsk_tkg_code VARCHAR2, p_tsk_tsk_tkg_src_type VARCHAR2)
    IS
      SELECT MAX(tsk_id)
      FROM   tasks
      WHERE  tsk_tkg_src_reference = p_tsk_tkg_src_reference
      AND    tsk_tkg_code = p_tsk_tsk_tkg_code
      AND    tsk_tkg_src_type = p_tsk_tsk_tkg_src_type;
    CURSOR get_task_id_upd
      (p_tve_tsk_tkg_src_reference VARCHAR2
      ,p_tve_tsk_tsk_tkg_code      VARCHAR2
      ,p_tve_tsk_tsk_tkg_src_type  VARCHAR2
      ,p_tsk_stk_code              VARCHAR2
      ,p_tve_display_sequence      NUMBER
      )
    IS
      SELECT MAX(tve_tsk_id)
      FROM   task_versions
      ,      tasks
      WHERE  tve_tsk_tkg_src_reference = p_tve_tsk_tkg_src_reference
      AND    tve_tsk_tkg_code = p_tve_tsk_tsk_tkg_code
      AND    tve_tsk_tkg_src_type = p_tve_tsk_tsk_tkg_src_type
      AND    tsk_stk_code = p_tsk_stk_code
      AND    tve_tsk_id = tsk_id
      AND    tve_tsk_tkg_src_reference = tsk_tkg_src_reference
      AND    tve_tsk_tkg_code = tsk_tkg_code
      AND    tve_tsk_tkg_src_type = tsk_tkg_src_type
      AND    tve_display_sequence = p_tve_display_sequence;
    cb               VARCHAR2(30);
    cd               DATE;
    cp               VARCHAR2(30) := 'CREATE';
    ct               VARCHAR2(30) := 'DL_HPM_TASKS';
    cs               INTEGER;
    ce               VARCHAR2(200);
    ci               INTEGER;
    l_reusable_refno NUMBER(10);
    l_bud_refno      NUMBER(10);
    l_bhe_refno      NUMBER(10);
    i                INTEGER := 0;
    l_an_tab         VARCHAR2(1);
    l_answer         VARCHAR2(1);
    l_task_id        NUMBER(8);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_tasks.dataload_create');
    fsc_utils.debug_message('s_dl_hpm_tasks.dataload_create',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    ci := s_dl_hem_utils.dl_orig_rows('DL_HPM_TASKS');
    l_answer := s_dl_batches.get_answer(p_batch_id,1);
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.ltsk_dl_seqno;
        SAVEPOINT SP1;
-- Batch question update existing bespoke for Anchor only
-- DONT USE for Other Sites so commented out for standard version		
/*      IF NVL(l_answer,'N') = 'Y'
        THEN
          l_task_id := NULL;
          OPEN get_task_id_upd(p1.LTSK_TKG_SRC_REFERENCE, p1.LTSK_TKG_CODE, p1.LTSK_TKG_SRC_TYPE, p1.LTSK_STK_CODE, p1.LTVE_DISPLAY_SEQUENCE);
          FETCH get_task_id INTO l_task_id;
          CLOSE get_task_id;
          UPDATE tasks
          SET    tsk_sco_code = p1.ltsk_sco_code
          WHERE  tsk_id = l_task_id;
          UPDATE task_versions
          SET    tve_planned_start_date = p1.ltve_planned_start_date
          ,      tve_net_amount = p1.ltve_net_amount
          ,      tve_tax_amount = p1.ltve_tax_amount
          ,      tve_comments = p1.ltve_comments
          WHERE  tve_tsk_id = l_task_id
          AND    tve_version_number = p1.ltve_version_number
          AND    tve_display_sequence = p1.ltve_display_sequence;
        ELSE
*/        l_reusable_refno := NULL;
          l_task_id := NULL;
          OPEN c_reusable_refno;
          FETCH c_reusable_refno INTO l_reusable_refno;
          CLOSE c_reusable_refno;
          --
          OPEN get_task_id(p1.LTSK_TKG_SRC_REFERENCE, p1.LTSK_TKG_CODE, p1.LTSK_TKG_SRC_TYPE);
          FETCH get_task_id INTO l_task_id;
          CLOSE get_task_id;
          --
          IF (l_task_id IS NULL) 
          THEN
            l_task_id := 1;
          ELSE
            l_task_id := l_task_id + 1;
          END IF;
          --
          UPDATE dl_hpm_tasks
          SET    ltsk_id = l_task_id
          WHERE  ltsk_dlb_batch_id = p_batch_id
          AND    rowid = p1.ltsk_rowid;
          --
          INSERT INTO tasks
          (tsk_tkg_src_reference  
          ,tsk_tkg_code   
          ,tsk_tkg_src_type   
          ,tsk_id   
          ,tsk_type_ind   
          ,tsk_stk_code   
          ,tsk_sco_code   
          ,tsk_status_date   
          ,tsk_alt_reference   
          ,tsk_actual_end_date   
          )  
          VALUES  
          (p1.ltsk_tkg_src_reference  
          ,p1.ltsk_tkg_code  
          ,p1.ltsk_tkg_src_type  
          ,l_task_id  
          ,p1.ltsk_type_ind  
          ,p1.ltsk_stk_code  
          ,p1.ltsk_sco_code  
          ,p1.ltsk_status_date  
          ,p1.ltsk_alt_reference  
          ,p1.ltsk_actual_end_date  
          );  
          INSERT INTO task_versions
          (tve_tsk_tkg_src_reference  
          ,tve_tsk_tkg_code  
          ,tve_tsk_tkg_src_type  
          ,tve_tsk_id  
          ,tve_version_number  
          ,tve_current_ind  
          ,tve_display_sequence  
          ,tve_reusable_refno  
          ,tve_hrv_tus_code  
          ,tve_bca_year  
          ,tve_vca_code  
          ,tve_planned_start_date  
          ,tve_net_amount  
          ,tve_tax_amount  
          ,tve_retention_percent  
          ,tve_retention_period  
          ,tve_retention_period_units  
          ,tve_comments  
          )  
          VALUES  
          (p1.ltsk_tkg_src_reference  
          ,p1.ltsk_tkg_code  
          ,p1.ltsk_tkg_src_type  
          ,l_task_id  
          ,p1.ltve_version_number  
          ,p1.ltve_current_ind  
          ,p1.ltve_display_sequence  
          ,l_reusable_refno  
          ,p1.ltve_hrv_tus_code  
          ,p1.ltve_bca_year  
          ,p1.ltve_vca_code  
          ,p1.ltve_planned_start_date  
          ,p1.ltve_net_amount  
          ,p1.ltve_tax_amount  
          ,p1.ltve_retention_percent  
          ,p1.ltve_retention_period  
          ,p1.ltve_retention_period_units  
          ,p1.ltve_comments  
          );  
--        END IF;
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
    l_an_tab := s_dl_hem_utils.dl_comp_stats('TASKS',ci,i);
    l_an_tab := s_dl_hem_utils.dl_comp_stats('TASK_VERSIONS',ci,i);
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
      SELECT rowid                       ltsk_rowid
      ,      ltsk_dlb_batch_id
      ,      ltsk_dl_seqno
      ,      ltsk_dl_load_status
      ,      ltsk_tkg_src_reference
      ,      ltsk_tkg_code
      ,      ltsk_tkg_src_type
      ,      ltsk_type_ind
      ,      ltsk_stk_code
      ,      ltsk_sco_code
      ,      ltsk_status_date
      ,      ltsk_alt_reference
      ,      ltsk_actual_end_date
      ,      ltsk_id
      ,      NVL(ltve_version_number,1) ltve_version_number
      ,      NVL(ltve_current_ind,'N')  ltve_current_ind
      ,      ltve_display_sequence
      ,      ltve_hrv_tus_code
      ,      ltve_bca_year
      ,      ltve_vca_code
      ,      ltve_planned_start_date
      ,      ltve_net_amount
      ,      ltve_tax_amount
      ,      ltve_retention_percent
      ,      ltve_retention_period
      ,      ltve_retention_period_units
      ,      ltve_comments
      ,      ltba_bhe_code
      ,      ltba_bca_year
      ,      ltba_net_amount
      ,      ltba_tax_amount
      FROM   dl_hpm_tasks
      WHERE  ltsk_dlb_batch_id = p_batch_id
      AND    ltsk_dl_load_status IN('L','F','O');
    CURSOR chk_task_exists
      (p_tsk_tkg_src_reference VARCHAR2
      ,p_tsk_tkg_code          VARCHAR2
      ,p_tsk_tkg_src_type      VARCHAR2
      ,p_tsk_id                NUMBER
      )
    IS
      SELECT 'X'
      FROM   tasks
      WHERE  tsk_tkg_src_reference = p_tsk_tkg_src_reference
      AND    tsk_tkg_code = p_tsk_tkg_code
      AND    tsk_tkg_src_type = p_tsk_tkg_src_type
      AND    tsk_id = p_tsk_id;
    CURSOR chk_task_version_exists
      (p_tsk_tkg_src_reference VARCHAR2
      ,p_tsk_tkg_code          VARCHAR2
      ,p_tsk_tkg_src_type      VARCHAR2
      ,p_tsk_id                NUMBER
      ,p_version_number        NUMBER
      )
    IS
      SELECT 'X'
      FROM   task_versions
      WHERE  tve_tsk_tkg_src_reference = p_tsk_tkg_src_reference
      AND    tve_tsk_tkg_code = p_tsk_tkg_code
      AND    tve_tsk_tkg_src_type = p_tsk_tkg_src_type
      AND    tve_tsk_id = p_tsk_id
      AND    tve_version_number = p_version_number;
    CURSOR chk_task_groups
      (p_tkg_src_reference VARCHAR2
      ,p_tkg_code          VARCHAR2
      ,p_tkg_src_type      VARCHAR2
      )
    IS
      SELECT 'X'
      FROM   task_groups
      WHERE  tkg_src_reference = p_tkg_src_reference
      AND    tkg_code = p_tkg_code
      AND    tkg_src_type = p_tkg_src_type;
    CURSOR chk_stk_code
      (p_stk_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   standard_tasks
      WHERE  stk_code = p_stk_code;
    CURSOR chk_contract
      (p_cnt_reference VARCHAR2)
    IS
      SELECT 'X',cnt_sco_code
      FROM   contracts
      WHERE  cnt_reference = p_cnt_reference;
    CURSOR chk_programmes
      (p_prg_reference VARCHAR2)
    IS
      SELECT 'X', prg_sco_code
      FROM   programmes
      WHERE  prg_reference = p_prg_reference;
    CURSOR chk_projects
      (p_prj_reference VARCHAR2)
    IS
      SELECT 'X', prj_sco_code
      FROM   projects
      WHERE  prj_reference = p_prj_reference;
    CURSOR c_bhe_refno
      (p_bhe_code VARCHAR2)
    IS
      SELECT bhe_refno
      FROM   budget_heads
      WHERE  bhe_code = p_bhe_code;
    CURSOR chk_bca_year
      (p_bca_year NUMBER)
    IS
      SELECT 'X'
      FROM   budget_calendars
      WHERE  bca_year = p_bca_year;
    CURSOR chk_bhe_code
      (p_bhe_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   budget_heads
      WHERE  bhe_code = p_bhe_code;
    CURSOR c_bud_refno
      (p_bhe_refno NUMBER
      ,p_bca_year  NUMBER
      )
    IS
      SELECT bud_refno
      FROM   budgets
      WHERE  bud_bhe_refno = p_bhe_refno
      AND    bud_bca_year = p_bca_year;
    CURSOR chk_vat_category
      (p_vca_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   vat_categories
      WHERE  vca_code = p_vca_code;
    CURSOR chk_status_code
      (p_sco_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   status_codes
      WHERE  sco_code = p_sco_code;
    CURSOR get_stk_type_ind
      (p_stk_code VARCHAR2)
    IS
      SELECT stk_type_ind
      FROM   standard_tasks
      WHERE  stk_code = p_stk_code;
    CURSOR get_tkg_group_type
      (p_tsk_tkg_src_reference VARCHAR2
      ,p_tsk_tkg_code          VARCHAR2
      ,p_tsk_tkg_src_type      VARCHAR2
      )
    IS
      SELECT tkg_group_type
      FROM   task_groups
      WHERE  tkg_src_reference = p_tsk_tkg_src_reference
      AND    tkg_code = p_tsk_tkg_code
      AND    tkg_src_type = p_tsk_tkg_src_type;
    CURSOR get_tba_net_amount
      (p_tve_tsk_tkg_src_reference VARCHAR2
      ,p_tve_tsk_tkg_code          VARCHAR2
      ,p_tve_tsk_tkg_src_type      VARCHAR2
      ,p_tve_tsk_id                NUMBER
      ,p_tve_version_number        NUMBER
      ,p_bud_refno                 NUMBER
      )
    IS
      SELECT SUM(tba_net_amount)
      FROM   task_budget_amounts
      WHERE  tba_tve_tsk_tkg_src_ref = p_tve_tsk_tkg_src_reference
      AND    tba_tve_tsk_tkg_code = p_tve_tsk_tkg_code
      AND    tba_tve_tsk_tkg_src_type = p_tve_tsk_tkg_src_type
      AND    tba_tve_tsk_id = p_tve_tsk_id
      AND    tba_tve_version_number = p_tve_version_number
      AND    tba_bud_refno = p_bud_refno;
    CURSOR chk_noof_current_tve
      (p_tve_tkg_src_reference VARCHAR2
      ,p_tve_tsk_tkg_code      VARCHAR2
      ,p_tve_tsk_tkg_src_type  VARCHAR2
      ,p_tsk_id                NUMBER
      )
    IS
      SELECT COUNT(*)
      FROM   task_versions
      WHERE  tve_tsk_tkg_src_reference = p_tve_tkg_src_reference
      AND    tve_tsk_tkg_code = p_tve_tsk_tkg_code
      AND    tve_tsk_tkg_src_type = p_tve_tsk_tkg_src_type
      AND    tve_tsk_id = p_tsk_id
      AND    tve_current_ind = 'Y';
    CURSOR get_task_id
      (p_tsk_tkg_src_reference VARCHAR2
      ,p_tsk_tsk_tkg_code      VARCHAR2
      ,p_tsk_tsk_tkg_src_type  VARCHAR2
      )
    IS
      SELECT MAX(tsk_id)
      FROM   tasks
      WHERE  tsk_tkg_src_reference = p_tsk_tkg_src_reference
      AND    tsk_tkg_code = p_tsk_tsk_tkg_code
      AND    tsk_tkg_src_type = p_tsk_tsk_tkg_src_type;
    CURSOR get_task_id_upd
      (p_tve_tsk_tkg_src_reference VARCHAR2
      ,p_tve_tsk_tsk_tkg_code      VARCHAR2
      ,p_tve_tsk_tsk_tkg_src_type  VARCHAR2
      ,p_tsk_stk_code              VARCHAR2
      ,p_tve_display_sequence      NUMBER
      )
    IS
      SELECT 'X'
      FROM   task_versions
      ,      tasks
      WHERE  tve_tsk_tkg_src_reference = p_tve_tsk_tkg_src_reference
      AND    tve_tsk_tkg_code = p_tve_tsk_tsk_tkg_code
      AND    tve_tsk_tkg_src_type = p_tve_tsk_tsk_tkg_src_type
      AND    tsk_stk_code = p_tsk_stk_code
      AND    tve_tsk_id = tsk_id
      AND    tve_tsk_tkg_src_reference = tsk_tkg_src_reference
      AND    tve_tsk_tkg_code = tsk_tkg_code
      AND    tve_tsk_tkg_src_type = tsk_tkg_src_type
      AND    tve_display_sequence = p_tve_display_sequence;
--
    CURSOR chk_tsk_altref
      (p_ltsk_alt_reference VARCHAR2
      )
    IS
      SELECT count(*)
      FROM   tasks
      WHERE  tsk_alt_reference = p_ltsk_alt_reference;
    CURSOR chk_ltsk_altref
      (p_ltsk_alt_reference VARCHAR2
      ,p_ltsk_dlb_batch_id  VARCHAR2
      )
    IS
      SELECT count(*)
      FROM   dl_hpm_tasks
      WHERE  ltsk_alt_reference = p_ltsk_alt_reference
      and    ltsk_dlb_batch_id = p_ltsk_dlb_batch_id;
--
    cb                 VARCHAR2(30);
    cd                 DATE;
    cp                 VARCHAR2(30) := 'VALIDATE';
    ct                 VARCHAR2(30) := 'DL_HPM_TASKS';
    cs                 INTEGER;
    ce                 VARCHAR2(200);
    l_tsk_exists       VARCHAR2(1);
    l_tkg_exists       VARCHAR2(1);
    l_tkg_group_type   VARCHAR2(4);
    l_tve_exists       VARCHAR2(1);
    l_stk_exists       VARCHAR2(1);
    l_sco_exists       VARCHAR2(1);
    l_stk_type_ind     VARCHAR2(1);
    l_cnt_exists       VARCHAR2(1);
    l_prg_exists       VARCHAR2(1);
    l_prj_exists       VARCHAR2(1);
    l_vca_exists       VARCHAR2(1);
    l_mgmt_cos_exists  VARCHAR2(1);
    l_noof_current_tve INTEGER;
    l_errors           VARCHAR2(1);
    l_error_ind        VARCHAR2(1);
    i                  INTEGER := 0;
    l_bhe_refno        NUMBER(10);
    l_bud_refno        NUMBER(10);
    l_bhe_code_exists  VARCHAR2(1);
    l_bca_year_exists  VARCHAR2(1);
    l_bca_year2_exists VARCHAR2(1);
    l_task_id          NUMBER(8);
    l_tba_net_amount   NUMBER(14,2);
    l_answer           VARCHAR2(1);
    l_exists           VARCHAR2(1);
    l_cnt_status       VARCHAR2(3);
    l_prg_status       VARCHAR2(3);
    l_prj_status       VARCHAR2(3);
    l_chk_tskaltref    INTEGER;
    l_chk_ltskaltref   INTEGER;
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_tasks.dataload_validate');
    fsc_utils.debug_message('s_dl_hpm_tasks.dataload_validate',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
--	
-- Batch question update existing bespoke for Anchor only
-- DONT USE for Other Sites so commented out in create		
-- so setting l_answer to N so not used in validate
--    l_answer := s_dl_batches.get_answer(p_batch_id,1);
    l_answer := 'N';

    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.ltsk_dl_seqno;
        l_errors := 'V';
        l_error_ind := 'N';

        IF NVL(l_answer,'N') = 'Y'
        THEN
          OPEN get_task_id_upd
            (p1.ltsk_tkg_src_reference
            ,p1.ltsk_tkg_code
            ,p1.ltsk_tkg_src_type
            ,p1.ltsk_stk_code
            ,p1.ltve_display_sequence
            );
          FETCH get_task_id_upd INTO l_exists;
          IF get_task_id_upd%NOTFOUND 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',568);
          END IF;
          CLOSE get_task_id_upd;
        ELSE
          l_tsk_exists := NULL;
          l_tkg_exists := NULL;
          l_tkg_group_type := NULL;
          l_tve_exists := NULL;
          l_stk_exists := NULL;
          l_sco_exists := NULL;
          l_stk_type_ind := NULL;
          l_cnt_exists := NULL;
          l_prg_exists := NULL;
          l_prj_exists := NULL;
          l_vca_exists := NULL;
          l_mgmt_cos_exists := NULL;
          l_noof_current_tve := NULL;
          l_bhe_refno := NULL;
          l_bud_refno := NULL;
          l_bhe_code_exists := NULL;
          l_bca_year_exists := NULL;
          l_bca_year2_exists := NULL;
          l_task_id := NULL;
          l_tba_net_amount := NULL;
          l_cnt_status := NULL;
          l_prg_status := NULL;
          l_prj_status := NULL;
          l_chk_tskaltref := NULL;
          l_chk_ltskaltref := NULL;
          IF (p1.ltsk_tkg_src_reference IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',356);
          END IF;
          IF (p1.ltsk_tkg_code IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',357);
          END IF;
          IF (p1.ltsk_tkg_src_type IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',358);
          END IF;
          IF (p1.ltsk_type_ind is null) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',367);
          END IF;
          IF (p1.ltsk_stk_code IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',368);
          END IF;
          IF (p1.ltsk_sco_code IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',168);
          END IF;
          IF (p1.ltsk_status_date IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',338);
          END IF;
          IF (p1.ltve_version_number IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',369);
          END IF;
          IF (p1.ltve_display_sequence IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',339);
          END IF;
          OPEN chk_task_groups(p1.ltsk_tkg_src_reference,p1.ltsk_tkg_code,p1.ltsk_tkg_src_type);
          FETCH chk_task_groups INTO l_tkg_exists;
          CLOSE chk_task_groups;
          IF (l_tkg_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',375);
          END IF;
          IF (p1.ltsk_sco_code IS NOT NULL) 
          THEN
            OPEN chk_status_code(p1.LTSK_SCO_CODE);
            FETCH chk_status_code INTO l_sco_exists;
            CLOSE chk_status_code;
            IF (l_sco_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',531);
            ELSIF (p1.LTSK_SCO_CODE NOT IN('RAI','AUT','COM')) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',112);
            END IF;
          END IF;
          OPEN chk_stK_code(p1.ltsk_stk_code);
          FETCH chk_stK_code INTO l_stk_exists;
          CLOSE chk_stK_code;
          IF (l_stk_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',377);
          END IF;
          IF (p1.ltsk_tkg_src_type IS NOT NULL) 
          THEN
            IF (p1.ltsk_tkg_src_type NOT IN('CNT','PRG','PRJ')) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',365);
            ELSIF (p1.ltsk_tkg_src_type = 'CNT') 
            THEN
              OPEN chk_contract(p1.ltsk_tkg_src_reference);
              FETCH chk_contract
              INTO l_cnt_exists,l_cnt_status;
              CLOSE chk_contract;
              IF (l_cnt_exists IS NULL) 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',333);
              END IF;
            ELSIF (p1.ltsk_tkg_src_type = 'PRG') 
            THEN
              OPEN chk_programmes(p1.ltsk_tkg_src_reference);
              FETCH chk_programmes
              INTO l_prg_exists,l_prg_status;
              CLOSE chk_programmes;
              IF (l_prg_exists IS NULL) 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',366);
              END IF;
            ELSIF (p1.ltsk_tkg_src_type = 'PRJ') 
            THEN
              OPEN chk_projects(p1.ltsk_tkg_src_reference);
              FETCH chk_projects
              INTO l_prj_exists,l_prj_status;
              CLOSE chk_projects;
              IF (l_prj_exists IS NULL) 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',302);
              END IF;
            END IF;
            IF (l_prg_status IS NOT NULL AND l_prg_status !='RAI')
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',113);
            END IF;
            IF (l_prj_status IS NOT NULL AND l_prj_status !='RAI')
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',114);
            END IF;
            IF (l_cnt_status IS NOT NULL     AND 
                p1.ltsk_sco_code IS NOT NULL AND 
                p1.ltsk_type_ind IS NOT NULL     )
			THEN
              IF (l_cnt_status ='RAI')
              THEN
			    IF (p1.ltsk_type_ind = 'N' AND p1.ltsk_sco_code NOT IN('RAI','COM'))
			    THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',115);
                END IF;
			    IF (p1.ltsk_type_ind IN('P','B') AND p1.ltsk_sco_code !='RAI')
			    THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',116);
                END IF;
              END IF;
              IF (l_cnt_status ='AUT')
              THEN
			    IF (p1.ltsk_sco_code NOT IN('AUT','COM'))
			    THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',117);
                END IF;
              END IF;
              IF (l_cnt_status NOT IN ('AUT','RAI'))
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',118);
              END IF;
            END IF;
          END IF;
          IF (p1.ltsk_type_ind IS NOT NULL) 
          THEN
            IF (p1.ltsk_type_ind NOT IN('P','B','N')) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',378);
            END IF;
          END IF;
          IF (p1.ltsk_type_ind IS NOT NULL) 
          THEN
            IF (p1.ltsk_type_ind = 'P') 
            THEN
              OPEN get_stk_type_ind(p1.ltsk_stk_code);
              FETCH get_stk_type_ind INTO l_stk_type_ind;
              CLOSE get_stk_type_ind;
              IF (l_stk_type_ind != 'P') 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',379);
              END IF;
            ELSIF (p1.ltsk_type_ind = 'N') 
            THEN
              OPEN get_stk_type_ind(p1.ltsk_stk_code);
              FETCH get_stk_type_ind INTO l_stk_type_ind;
              CLOSE get_stk_type_ind;
              IF (l_stk_type_ind != 'N') 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',380);
              END IF;
            ELSIF (p1.ltsk_type_ind = 'B') 
            THEN
              OPEN get_stk_type_ind(p1.ltsk_stk_code);
              FETCH get_stk_type_ind INTO l_stk_type_ind;
              CLOSE get_stk_type_ind;
              IF (l_stk_type_ind != 'B') 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',381);
              END IF;
            END IF;
          END IF;
          IF (l_tkg_exists IS NOT NULL) 
          THEN
            IF (p1.ltsk_type_ind = 'P') 
            THEN
              OPEN get_tkg_group_type(p1.ltsk_tkg_src_reference,p1.ltsk_tkg_code,p1.ltsk_tkg_src_type);
              FETCH get_tkg_group_type INTO l_tkg_group_type;
              CLOSE get_tkg_group_type;
              IF (l_tkg_group_type NOT IN('PAYT','BUDG')) 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',382);
              END IF;
            ELSIF (p1.ltsk_type_ind = 'N') 
            THEN
              OPEN get_tkg_group_type(p1.ltsk_tkg_src_reference,p1.ltsk_tkg_code,p1.ltsk_tkg_src_type);
              FETCH get_tkg_group_type INTO l_tkg_group_type;
              CLOSE get_tkg_group_type;
              IF (l_tkg_group_type NOT IN('PAYT','BUDG','NPAY')) 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',383);
              END IF;
            ELSIF (p1.ltsk_type_ind = 'B') 
            THEN
              OPEN get_tkg_group_type(p1.ltsk_tkg_src_reference,p1.ltsk_tkg_code,p1.ltsk_tkg_src_type);
              FETCH get_tkg_group_type INTO l_tkg_group_type;
              CLOSE get_tkg_group_type;
              IF (l_tkg_group_type NOT IN('BUDG')) 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',384);
              END IF;
            END IF;
          END IF;
          IF (p1.ltsk_type_ind = 'N') 
          THEN
            IF (p1.LTVE_BCA_YEAR IS NOT NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',387);
            END IF;
            IF (p1.ltve_net_amount IS NOT NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',388);
            END IF;
            IF (p1.ltve_tax_amount IS NOT NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',389);
            END IF;
            IF (p1.ltba_bhe_code IS NOT NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',502);
            END IF;
            IF (p1.ltba_bca_year IS NOT NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',503);
            END IF;
            IF (p1.ltba_net_amount IS NOT NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',504);
            END IF;
            IF (p1.ltba_tax_amount IS NOT NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',505);
            END IF;
            IF (p1.ltve_vca_code IS NOT NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',386);
            END IF;
          END IF;
          IF (p1.ltsk_type_ind != 'N') 
          THEN
            OPEN chk_bca_year(p1.ltve_bca_year);
            FETCH chk_bca_year INTO l_bca_year_exists;
            CLOSE chk_bca_year;
            IF (l_bca_year_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',528);
            END IF;
            OPEN chk_bhe_code(p1.ltba_bhe_code);
            FETCH chk_bhe_code INTO l_bhe_code_exists;
            CLOSE chk_bhe_code;
            IF (l_bhe_code_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',328);
            END IF;
            OPEN chk_bca_year(p1.ltba_bca_year);
            FETCH chk_bca_year INTO l_bca_year2_exists;
            CLOSE chk_bca_year;
            IF (l_bca_year2_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',506);
            END IF;
            OPEN c_bhe_refno(p1.ltba_bhe_code);
            FETCH c_bhe_refno INTO l_bhe_refno;
            CLOSE c_bhe_refno;
            OPEN c_bud_refno(l_bhe_refno,p1.ltba_bca_year);
            FETCH c_bud_refno INTO l_bud_refno;
            CLOSE c_bud_refno;
            IF (l_bud_refno IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',329);
            END IF;
            IF (p1.ltve_vca_code IS NOT NULL)
			THEN
              OPEN chk_vat_category(p1.ltve_vca_code);
              FETCH chk_vat_category INTO l_vca_exists;
              CLOSE chk_vat_category;
               IF (l_vca_exists IS NULL) 
               THEN
                 l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',345);
               END IF;
            END IF;
          END IF;
          IF (p1.ltsk_type_ind != 'P') 
          THEN
            IF (p1.ltve_retention_percent IS NOT NULL OR p1.ltve_retention_period IS NOT NULL OR p1.ltve_retention_period_units IS NOT NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',390);
            END IF;
          END IF;
          IF (p1.ltve_hrv_tus_code IS NOT NULL) 
          THEN
            IF (NOT s_dl_hem_utils.exists_frv('MILSTATUS',p1.LTVE_HRV_TUS_CODE,'Y')) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',391);
            END IF;
          END IF;
		  --
          --Checks below will always return Null as l_task_id not found in validate but as create will find current max
		  --task id and add one on create and you can have two tasks with the same detail then not sure if checks
		  --are required but code left in for the time being
		  --
          OPEN chk_task_exists(p1.ltsk_tkg_src_reference, p1.ltsk_tkg_code, p1.ltsk_tkg_src_type, l_task_id);
          FETCH chk_task_exists INTO l_tsk_exists;
          CLOSE chk_task_exists;
          IF (l_tsk_exists IS NOT NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',374);
          END IF;
          IF (p1.ltve_current_ind = 'Y') 
          THEN
            OPEN chk_noof_current_tve(p1.ltsk_tkg_src_reference, p1.ltsk_tkg_code, p1.ltsk_tkg_src_type, l_task_id);
            FETCH chk_noof_current_tve INTO l_noof_current_tve;
            CLOSE chk_noof_current_tve;
            IF (l_noof_current_tve > 0) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',392);
            END IF;
          END IF;
          OPEN chk_task_version_exists(p1.ltsk_tkg_src_reference, p1.ltsk_tkg_code, p1.ltsk_tkg_src_type, l_task_id, p1.ltve_version_number);
          FETCH chk_task_version_exists INTO l_tve_exists;
          CLOSE chk_task_version_exists;
          IF (l_tve_exists IS NOT NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',385);
          END IF;
          IF  l_cnt_exists IS NOT NULL 
          AND p1.ltsk_tkg_code IS NOT NULL 
          AND p1.ltsk_tkg_src_type IN('CNT','PRG','PRJ') 
          AND l_task_id IS NOT NULL 
          AND p1.ltve_version_number IS NOT NULL 
          AND l_bud_refno IS NOT NULL 
          THEN
            OPEN get_tba_net_amount(p1.ltsk_tkg_src_reference, p1.ltsk_tkg_code, p1.ltsk_tkg_src_type, l_task_id, p1.ltve_version_number, l_bud_refno);
            FETCH get_tba_net_amount INTO l_tba_net_amount;
            CLOSE get_tba_net_amount;
            IF (l_tba_net_amount IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',398);
            ELSIF (l_tba_net_amount != p1.ltve_net_amount) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',385);
            END IF;
          END IF;
--
          IF (p1.ltsk_alt_reference IS NULL)
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',134);
          ELSE 
           OPEN chk_tsk_altref(p1.ltsk_alt_reference);
           FETCH chk_tsk_altref INTO l_chk_tskaltref;
           CLOSE chk_tsk_altref;
           IF (l_chk_tskaltref > 0) 
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',135);
           END IF;
--
           OPEN chk_ltsk_altref(p1.ltsk_alt_reference,p_batch_id);
           FETCH chk_ltsk_altref INTO l_chk_ltskaltref;
           CLOSE chk_ltsk_altref;
           IF (l_chk_ltskaltref > 1) 
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',136);
            END IF;
          END IF;
--
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
    fsc_utils.proc_end;
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
      SELECT ltsk_dlb_batch_id
      ,      ltsk_dl_seqno
      ,      ltsk_dl_load_status
      ,      ltsk_tkg_src_reference
      ,      ltsk_tkg_code
      ,      ltsk_tkg_src_type
      ,      ltsk_type_ind
      ,      ltsk_stk_code
      ,      ltsk_sco_code
      ,      ltsk_status_date
      ,      ltsk_alt_reference
      ,      ltsk_actual_end_date
      ,      ltsk_id
      ,      NVL(ltve_version_number,1) ltve_version_number
      ,      NVL(ltve_current_ind,'N')  ltve_current_ind
      ,      ltve_display_sequence
      ,      ltve_hrv_tus_code
      ,      ltve_bca_year
      ,      ltve_vca_code
      ,      ltve_planned_start_date
      ,      ltve_net_amount
      ,      ltve_tax_amount
      ,      ltve_retention_percent
      ,      ltve_retention_period
      ,      ltve_retention_period_units
      ,      ltve_comments
      ,      ltba_bhe_code
      ,      ltba_bca_year
      ,      ltba_net_amount
      ,      ltba_tax_amount
      FROM   dl_hpm_tasks
      WHERE  ltsk_dlb_batch_id = p_batch_id
      AND    ltsk_dl_load_status IN('C');
    cb          VARCHAR2(30);
    cd          DATE;
    cp          VARCHAR2(30) := 'DELETE';
    ct          VARCHAR2(30) := 'DL_HPM_TASKS';
    cs          INTEGER;
    ce          VARCHAR2(200);
    l_exists    VARCHAR2(1);
    l_errors    VARCHAR2(1);
    l_error_ind VARCHAR2(1);
    i           INTEGER := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_tasks.dataload_delete');
    fsc_utils.debug_message('s_dl_hpm_tasks.dataload_delete',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.ltsk_dl_seqno;
        i := i + 1;
        SAVEPOINT SP1;
        DELETE
        FROM   task_versions
        WHERE  tve_tsk_tkg_src_reference = p1.ltsk_tkg_src_reference
        AND    tve_tsk_tkg_code = p1.ltsk_tkg_code
        AND    tve_tsk_tkg_src_type = p1.ltsk_tkg_src_type
        AND    tve_tsk_id = p1.ltsk_id
        AND    tve_version_number = p1.ltve_version_number;
        DELETE
        FROM   tasks
        WHERE  tsk_tkg_src_reference = p1.ltsk_tkg_src_reference
        AND    tsk_tkg_code = p1.ltsk_tkg_code
        AND    tsk_tkg_src_type = p1.ltsk_tkg_src_type
        AND    tsk_id = p1.ltsk_id;
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
END s_dl_hpm_tasks;
/
