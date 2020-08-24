CREATE OR REPLACE PACKAGE BODY s_dl_hpm_payment_task_dets
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VER		DB Ver	WHO	WHEN		WHY
--  1.0		5.12	VRS  	28-SEP-2007 	Product Dataload
--  1.1         5.12.0  PH      14-NOV-2007     Added final commit to validate
--                                              also amended chk_task_groups
--                                              cursor to check for PAYT types
--  1.2         5.12.0  PH      03-DEC-2007     Amended validate on task budget
--                                              changed is null to not null.
--                                              Moved the get task id from validate to
--                                              create process as there could be records
--                                              within the same batch.
--  1.3         5.12.0  PH      31-MAR-2008     Added extra field to make sure we
--                                              assign the record to correct task
--  1.4         5.12.0  PH      24-APR-2008     Amended Validate and Create on 
--                                              segment codes, now do IF, END IF on
--                                              each rather than an else. This prevented
--                                              segments 2-10 being created. Also
--                                              no need to update dl table with
--                                              sequence for delete.
--  1.5         5.12.0  PH      05-JUN-2009     Added task Vesion display seq to
--                                              further ensure we get the right task.
--  1.6         6.11.0  MJK     25-AUG-2015     Reformatted for 6.11. No logic changes
--  1.7         6.13.0  AJ      26-JAN-2017     Duplicate validates for the following
--                                              HDL369(ltba_tve_version_number)
--                                              HDL370(ltba_bhe_code) HD1371(ltba_bca_year)
--                                              and HDL372(ltba_net_amount) removed
--
-- ***********************************************************************
  PROCEDURE dataload_create
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  AS
    CURSOR c1
      (p_batch_id VARCHAR2)
    IS
      SELECT rowid                     ltba_rowid
      ,      ltba_dlb_batch_id
      ,      ltba_dl_seqno
      ,      ltba_dl_load_status
      ,      ltba_tve_tsk_tkg_src_ref
      ,      ltba_tve_tsk_tkg_code
      ,      ltba_tve_tsk_tkg_src_type
      ,      ltba_tve_version_number
      ,      ltba_bca_year
      ,      ltba_bhe_code
      ,      ltba_display_sequence
      ,      NVL(ltba_current_ind,'Y') ltba_current_ind
      ,      ltba_net_amount
      ,      ltba_tsk_id
      ,      NVL(ltba_tax_amount,0.00) ltba_tax_amount
      ,      ltgc_hgl_segment1
      ,      ltgc_hgl_segment2
      ,      ltgc_hgl_segment3
      ,      ltgc_hgl_segment4
      ,      ltgc_hgl_segment5
      ,      ltgc_hgl_segment6
      ,      ltgc_hgl_segment7
      ,      ltgc_hgl_segment8
      ,      ltgc_hgl_segment9
      ,      ltgc_hgl_segment10
      ,      ltba_tsk_stk_code
      ,      ltba_tve_display_sequence
      FROM   dl_hpm_payment_task_dets
      WHERE  ltba_dlb_batch_id = p_batch_id
      AND    ltba_dl_load_status = 'V';
    CURSOR get_task_id
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
    CURSOR c_bud_refno
      (p_bhe_refno NUMBER
      ,p_bca_year  NUMBER
      )
    IS
      SELECT bud_refno
      FROM   budgets
      WHERE  bud_bhe_refno = p_bhe_refno
      AND    bud_bca_year = p_bca_year;
    CURSOR c_hgf_seqno
      (p_vgv_code VARCHAR2)
    IS
      SELECT vgv_hgf_seqno
      FROM   hpm_valid_gl_values
      WHERE  vgv_code = p_vgv_code;
    cb          VARCHAR2(30);
    cd          DATE;
    cp          VARCHAR2(30) := 'CREATE';
    ct          VARCHAR2(30) := 'DL_HPM_PAYMENT_TASK_DETS';
    cs          INTEGER;
    ce          VARCHAR2(200);
    ci          INTEGER;
    l_bud_refno NUMBER(10);
    l_bhe_refno NUMBER(10);
    l_task_id   NUMBER(8);
    l_hgf_seqno NUMBER(4);
    i           INTEGER := 0;
    l_an_tab    VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_payment_task_dets.dataload_create');
    fsc_utils.debug_message('s_dl_hpm_payment_task_dets.dataload_create',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    ci := s_dl_hem_utils.dl_orig_rows('DL_HPM_PAYMENT_TASK_DETS');
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.ltba_dl_seqno;
        l_bhe_refno := NULL;
        l_task_id := NULL;
        OPEN get_task_id
          (p1.ltba_tve_tsk_tkg_src_ref
          ,p1.ltba_tve_tsk_tkg_code
          ,p1.ltba_tve_tsk_tkg_src_type
          ,p1.ltba_tsk_stk_code
          ,p1.ltba_tve_display_sequence
          );
        FETCH get_task_id INTO l_task_id;
        CLOSE get_task_id;
        UPDATE dl_hpm_payment_task_dets
        SET    ltba_tsk_id = l_task_id
        WHERE  ltba_dlb_batch_id = p_batch_id
        AND    rowid = p1.ltba_rowid;
        OPEN c_bhe_refno(p1.LTBA_BHE_CODE);
        FETCH c_bhe_refno INTO l_bhe_refno;
        CLOSE c_bhe_refno;
        l_bud_refno := NULL;
        OPEN c_bud_refno(l_bhe_refno,p1.LTBA_BCA_YEAR);
        FETCH c_bud_refno INTO l_bud_refno;
        CLOSE c_bud_refno;
        SAVEPOINT SP1;
        INSERT INTO task_budget_amounts
        (tba_tve_tsk_tkg_src_ref  
        ,tba_tve_tsk_tkg_code  
        ,tba_tve_tsk_tkg_src_type  
        ,tba_tve_tsk_id  
        ,tba_tve_version_number  
        ,tba_bud_refno  
        ,tba_display_sequence  
        ,tba_current_ind  
        ,tba_net_amount  
        ,tba_tax_amount  
        )  
        VALUES  
        (p1.ltba_tve_tsk_tkg_src_ref  
        ,p1.ltba_tve_tsk_tkg_code  
        ,p1.ltba_tve_tsk_tkg_src_type  
        ,l_task_id  
        ,p1.ltba_tve_version_number  
        ,l_bud_refno  
        ,p1.ltba_display_sequence  
        ,p1.ltba_current_ind  
        ,p1.ltba_net_amount  
        ,p1.ltba_tax_amount  
        );  
        IF (p1.ltgc_hgl_segment1 IS NOT NULL) 
        THEN
          l_hgf_seqno := NULL;
          OPEN c_hgf_seqno(p1.ltgc_hgl_segment1);
          FETCH c_hgf_seqno INTO l_hgf_seqno;
          CLOSE c_hgf_seqno;
          INSERT INTO task_gl_classifications
          (tgc_tba_tve_tsk_tkg_src_ref  
          ,tgc_tba_tve_tsk_tkg_code  
          ,tgc_tba_tve_tsk_tkg_src_type  
          ,tgc_tba_tve_tsk_id  
          ,tgc_tba_tve_version_number  
          ,tgc_tba_bud_refno  
          ,tgc_hgf_seqno  
          ,tgc_vgv_code  
          )  
          VALUES  
          (p1.ltba_tve_tsk_tkg_src_ref  
          ,p1.ltba_tve_tsk_tkg_code  
          ,p1.ltba_tve_tsk_tkg_src_type  
          ,l_task_id  
          ,p1.ltba_tve_version_number  
          ,l_bud_refno  
          ,l_hgf_seqno  
          ,p1.ltgc_hgl_segment1  
          );  
        END IF;
        IF (p1.ltgc_hgl_segment2 IS NOT NULL) 
        THEN
          l_hgf_seqno := NULL;
          OPEN c_hgf_seqno(p1.ltgc_hgl_segment2);
          FETCH c_hgf_seqno INTO l_hgf_seqno;
          CLOSE c_hgf_seqno;
          INSERT INTO task_gl_classifications
          (tgc_tba_tve_tsk_tkg_src_ref  
          ,tgc_tba_tve_tsk_tkg_code  
          ,tgc_tba_tve_tsk_tkg_src_type  
          ,tgc_tba_tve_tsk_id  
          ,tgc_tba_tve_version_number  
          ,tgc_tba_bud_refno  
          ,tgc_hgf_seqno  
          ,tgc_vgv_code  
          )  
          values  
          (p1.ltba_tve_tsk_tkg_src_ref  
          ,p1.ltba_tve_tsk_tkg_code  
          ,p1.ltba_tve_tsk_tkg_src_type  
          ,l_task_id  
          ,p1.ltba_tve_version_number  
          ,l_bud_refno  
          ,l_hgf_seqno  
          ,p1.ltgc_hgl_segment2  
          );  
        END IF;
        IF (p1.ltgc_hgl_segment3 IS NOT NULL) 
        THEN
          l_hgf_seqno := NULL;
          OPEN c_hgf_seqno(p1.ltgc_hgl_segment3);
          FETCH c_hgf_seqno INTO l_hgf_seqno;
          CLOSE c_hgf_seqno;
          INSERT INTO task_gl_classifications
          (tgc_tba_tve_tsk_tkg_src_ref  
          ,tgc_tba_tve_tsk_tkg_code  
          ,tgc_tba_tve_tsk_tkg_src_type  
          ,tgc_tba_tve_tsk_id  
          ,tgc_tba_tve_version_number  
          ,tgc_tba_bud_refno  
          ,tgc_hgf_seqno  
          ,tgc_vgv_code  
          )  
          Values  
          (p1.ltba_tve_tsk_tkg_src_ref  
          ,p1.ltba_tve_tsk_tkg_code  
          ,p1.ltba_tve_tsk_tkg_src_type  
          ,l_task_id  
          ,p1.ltba_tve_version_number  
          ,l_bud_refno  
          ,l_hgf_seqno  
          ,p1.ltgc_hgl_segment3  
          );  
        END IF;
        IF (p1.ltgc_hgl_segment4 IS NOT NULL) 
        THEN
          l_hgf_seqno := NULL;
          OPEN c_hgf_seqno(p1.ltgc_hgl_segment4);
          FETCH c_hgf_seqno INTO l_hgf_seqno;
          CLOSE c_hgf_seqno;
          INSERT INTO task_gl_classifications
          (tgc_tba_tve_tsk_tkg_src_ref  
          ,tgc_tba_tve_tsk_tkg_code  
          ,tgc_tba_tve_tsk_tkg_src_type  
          ,tgc_tba_tve_tsk_id  
          ,tgc_tba_tve_version_number  
          ,tgc_tba_bud_refno  
          ,tgc_hgf_seqno  
          ,tgc_vgv_code  
          )  
          VALUES  
          (p1.ltba_tve_tsk_tkg_src_ref  
          ,p1.ltba_tve_tsk_tkg_code  
          ,p1.ltba_tve_tsk_tkg_src_type  
          ,l_task_id  
          ,p1.ltba_tve_version_number  
          ,l_bud_refno  
          ,l_hgf_seqno  
          ,p1.ltgc_hgl_segment4  
          );  
        END IF;
        IF (p1.ltgc_hgl_segment5 IS NOT NULL) 
        THEN
          l_hgf_seqno := NULL;
          OPEN c_hgf_seqno(p1.ltgc_hgl_segment5);
          FETCH c_hgf_seqno INTO l_hgf_seqno;
          CLOSE c_hgf_seqno;
          INSERT INTO task_gl_classifications
          (tgc_tba_tve_tsk_tkg_src_ref  
          ,tgc_tba_tve_tsk_tkg_code  
          ,tgc_tba_tve_tsk_tkg_src_type  
          ,tgc_tba_tve_tsk_id  
          ,tgc_tba_tve_version_number  
          ,tgc_tba_bud_refno  
          ,tgc_hgf_seqno  
          ,tgc_vgv_code  
          )  
          VALUES  
          (p1.ltba_tve_tsk_tkg_src_ref  
          ,p1.ltba_tve_tsk_tkg_code  
          ,p1.ltba_tve_tsk_tkg_src_type  
          ,l_task_id  
          ,p1.ltba_tve_version_number  
          ,l_bud_refno  
          ,l_hgf_seqno  
          ,p1.ltgc_hgl_segment5  
          );  
        END IF;
        IF (p1.ltgc_hgl_segment6 IS NOT NULL) 
        THEN
          l_hgf_seqno := NULL;
          OPEN c_hgf_seqno(p1.ltgc_hgl_segment6);
          FETCH c_hgf_seqno INTO l_hgf_seqno;
          CLOSE c_hgf_seqno;
          INSERT INTO task_gl_classifications
          (tgc_tba_tve_tsk_tkg_src_ref  
          ,tgc_tba_tve_tsk_tkg_code  
          ,tgc_tba_tve_tsk_tkg_src_type  
          ,tgc_tba_tve_tsk_id  
          ,tgc_tba_tve_version_number  
          ,tgc_tba_bud_refno  
          ,tgc_hgf_seqno  
          ,tgc_vgv_code  
          )  
          VALUES  
          (p1.ltba_tve_tsk_tkg_src_ref  
          ,p1.ltba_tve_tsk_tkg_code  
          ,p1.ltba_tve_tsk_tkg_src_type  
          ,l_task_id  
          ,p1.ltba_tve_version_number  
          ,l_bud_refno  
          ,l_hgf_seqno  
          ,p1.ltgc_hgl_segment6  
          );
        END IF;
        IF (p1.ltgc_hgl_segment7 IS NOT NULL) 
        THEN
          l_hgf_seqno := NULL;
          OPEN c_hgf_seqno(p1.ltgc_hgl_segment7);
          FETCH c_hgf_seqno INTO l_hgf_seqno;
          CLOSE c_hgf_seqno;
          INSERT INTO task_gl_classifications
          (tgc_tba_tve_tsk_tkg_src_ref
          ,tgc_tba_tve_tsk_tkg_code  
          ,tgc_tba_tve_tsk_tkg_src_type  
          ,tgc_tba_tve_tsk_id  
          ,tgc_tba_tve_version_number  
          ,tgc_tba_bud_refno  
          ,tgc_hgf_seqno  
          ,tgc_vgv_code  
          )  
          VALUES  
          (p1.ltba_tve_tsk_tkg_src_ref  
          ,p1.ltba_tve_tsk_tkg_code  
          ,p1.ltba_tve_tsk_tkg_src_type  
          ,l_task_id  
          ,p1.ltba_tve_version_number  
          ,l_bud_refno  
          ,l_hgf_seqno  
          ,p1.ltgc_hgl_segment7  
          );  
        END IF;
        IF (p1.ltgc_hgl_segment8 IS NOT NULL) 
        THEN
          l_hgf_seqno := NULL;
          OPEN c_hgf_seqno(p1.ltgc_hgl_segment8);
          FETCH c_hgf_seqno INTO l_hgf_seqno;
          CLOSE c_hgf_seqno;
          INSERT INTO task_gl_classifications
          (tgc_tba_tve_tsk_tkg_src_ref
          ,tgc_tba_tve_tsk_tkg_code  
          ,tgc_tba_tve_tsk_tkg_src_type  
          ,tgc_tba_tve_tsk_id  
          ,tgc_tba_tve_version_number  
          ,tgc_tba_bud_refno  
          ,tgc_hgf_seqno  
          ,tgc_vgv_code  
          )  
          VALUES  
          (p1.ltba_tve_tsk_tkg_src_ref  
          ,p1.ltba_tve_tsk_tkg_code  
          ,p1.ltba_tve_tsk_tkg_src_type  
          ,l_task_id  
          ,p1.ltba_tve_version_number  
          ,l_bud_refno  
          ,l_hgf_seqno  
          ,p1.ltgc_hgl_segment8  
          );  
        END IF;
        IF (p1.ltgc_hgl_segment9 IS NOT NULL) 
        THEN
          l_hgf_seqno := NULL;
          OPEN c_hgf_seqno(p1.ltgc_hgl_segment9);
          FETCH c_hgf_seqno INTO l_hgf_seqno;
          CLOSE c_hgf_seqno;
          INSERT INTO task_gl_classifications
          (tgc_tba_tve_tsk_tkg_src_ref
          ,tgc_tba_tve_tsk_tkg_code  
          ,tgc_tba_tve_tsk_tkg_src_type  
          ,tgc_tba_tve_tsk_id  
          ,tgc_tba_tve_version_number  
          ,tgc_tba_bud_refno  
          ,tgc_hgf_seqno  
          ,tgc_vgv_code  
          )  
          VALUES  
          (p1.ltba_tve_tsk_tkg_src_ref  
          ,p1.ltba_tve_tsk_tkg_code  
          ,p1.ltba_tve_tsk_tkg_src_type  
          ,l_task_id  
          ,p1.ltba_tve_version_number  
          ,l_bud_refno  
          ,l_hgf_seqno  
          ,p1.ltgc_hgl_segment9  
          );  
        END IF;
        IF (p1.ltgc_hgl_segment10 IS NOT NULL) 
        THEN
          l_hgf_seqno := NULL;
          OPEN c_hgf_seqno(p1.ltgc_hgl_segment10);
          FETCH c_hgf_seqno INTO l_hgf_seqno;
          CLOSE c_hgf_seqno;
          INSERT INTO task_gl_classifications
          (tgc_tba_tve_tsk_tkg_src_ref
          ,tgc_tba_tve_tsk_tkg_code  
          ,tgc_tba_tve_tsk_tkg_src_type  
          ,tgc_tba_tve_tsk_id  
          ,tgc_tba_tve_version_number  
          ,tgc_tba_bud_refno  
          ,tgc_hgf_seqno  
          ,tgc_vgv_code  
          )  
          VALUES  
          (p1.ltba_tve_tsk_tkg_src_ref  
          ,p1.ltba_tve_tsk_tkg_code  
          ,p1.ltba_tve_tsk_tkg_src_type  
          ,l_task_id  
          ,p1.ltba_tve_version_number  
          ,l_bud_refno  
          ,l_hgf_seqno  
          ,p1.ltgc_hgl_segment10  
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
    l_an_tab := s_dl_hem_utils.dl_comp_stats('TASK_BUDGET_AMOUNTS',ci,i);
    l_an_tab := s_dl_hem_utils.dl_comp_stats('TASK_GL_CLASSIFICATIONS',ci,i);
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
      (
        p_batch_id VARCHAR2
      )
    IS
      SELECT rowid                     ltba_rowid
      ,      ltba_dlb_batch_id
      ,      ltba_dl_seqno
      ,      ltba_dl_load_status
      ,      ltba_tve_tsk_tkg_src_ref
      ,      ltba_tve_tsk_tkg_code
      ,      ltba_tve_tsk_tkg_src_type
      ,      ltba_tve_version_number
      ,      ltba_bca_year
      ,      ltba_bhe_code
      ,      ltba_display_sequence
      ,      NVL(ltba_current_ind,'Y') ltba_current_ind
      ,      ltba_net_amount
      ,      ltba_tsk_id
      ,      NVL(ltba_tax_amount,0.00) ltba_tax_amount
      ,      ltgc_hgl_segment1
      ,      ltgc_hgl_segment2
      ,      ltgc_hgl_segment3
      ,      ltgc_hgl_segment4
      ,      ltgc_hgl_segment5
      ,      ltgc_hgl_segment6
      ,      ltgc_hgl_segment7
      ,      ltgc_hgl_segment8
      ,      ltgc_hgl_segment9
      ,      ltgc_hgl_segment10
      ,      ltba_tsk_stk_code
      ,      ltba_tve_display_sequence
      FROM   dl_hpm_payment_task_dets
      WHERE  ltba_dlb_batch_id = p_batch_id
      AND    ltba_dl_load_status IN('L','F','O');
    CURSOR chk_task_version_exists
      (p_tsk_tkg_src_reference VARCHAR2
      ,p_tsk_tkg_code          VARCHAR2
      ,p_tsk_tkg_src_type      VARCHAR2
      ,p_tsk_id                NUMBER
      ,p_version_number        NUMBER
      ,p_tve_display_sequence  NUMBER
      )
    IS
      SELECT 'X'
      FROM   task_versions
      WHERE  tve_tsk_tkg_src_reference = p_tsk_tkg_src_reference
      AND    tve_tsk_tkg_code = p_tsk_tkg_code
      AND    tve_tsk_tkg_src_type = p_tsk_tkg_src_type
      AND    tve_tsk_id = p_tsk_id
      AND    tve_version_number = p_version_number
      AND    tve_display_sequence = p_tve_display_sequence;
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
      AND    tkg_src_type = p_tkg_src_type
      AND    tkg_group_type = 'PAYT';
    CURSOR chk_contract
      (p_cnt_reference VARCHAR2)
    IS
      SELECT 'X'
      FROM   contracts
      WHERE  cnt_reference = p_cnt_reference;
    CURSOR chk_programmes
      (p_prg_reference VARCHAR2)
    IS
      SELECT 'X'
      FROM   programmes
      WHERE  prg_reference = p_prg_reference;
    CURSOR chk_projects
      (p_prj_reference VARCHAR2)
    IS
      SELECT 'X'
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
    CURSOR get_task_id
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
    CURSOR chk_tba_exists
      (p_tve_tsk_tkg_src_reference VARCHAR2
      ,p_tve_tsk_tkg_code          VARCHAR2
      ,p_tve_tsk_tkg_src_type      VARCHAR2
      ,p_tve_tsk_id                NUMBER
      ,p_tve_version_number        NUMBER
      ,p_bud_refno                 NUMBER
      )
    IS
      SELECT 'X'
      FROM   task_budget_amounts
      WHERE  tba_tve_tsk_tkg_src_ref = p_tve_tsk_tkg_src_reference
      AND    tba_tve_tsk_tkg_code = p_tve_tsk_tkg_code
      AND    tba_tve_tsk_tkg_src_type = p_tve_tsk_tkg_src_type
      AND    tba_tve_tsk_id = p_tve_tsk_id
      AND    tba_tve_version_number = p_tve_version_number
      AND    tba_bud_refno = p_bud_refno;
    CURSOR chk_hgl_segment_exists
      (p_vgv_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   hpm_valid_gl_values
      WHERE  vgv_code = p_vgv_code
      AND    vgv_current_ind = 'Y';
    cb                   VARCHAR2(30);
    cd                   DATE;
    cp                   VARCHAR2(30) := 'VALIDATE';
    ct                   VARCHAR2(30) := 'DL_HPM_PAYMENT_TASK_DETS';
    cs                   INTEGER;
    ce                   VARCHAR2(200);
    l_tsk_exists         VARCHAR2(1);
    l_tkg_exists         VARCHAR2(1);
    l_tve_exists         VARCHAR2(1);
    l_tba_exists         VARCHAR2(1);
    l_cnt_exists         VARCHAR2(1);
    l_prg_exists         VARCHAR2(1);
    l_prj_exists         VARCHAR2(1);
    l_task_id            NUMBER(8);
    l_errors             VARCHAR2(1);
    l_error_ind          VARCHAR2(1);
    i                    INTEGER := 0;
    l_bhe_refno          NUMBER(10);
    l_bud_refno          NUMBER(10);
    l_bhe_code_exists    VARCHAR2(1);
    l_bca_year_exists    VARCHAR2(1);
    l_hgl_segment_exists VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_payment_task_dets.dataload_validate');
    fsc_utils.debug_message('s_dl_hpm_payment_task_dets.dataload_validate',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.ltba_dl_seqno;
        l_errors := 'V';
        l_error_ind := 'N';
        IF (p1.ltba_tve_tsk_tkg_src_ref IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',356);
        END IF;
        IF (p1.ltba_tve_tsk_tkg_code is null) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',357);
        END IF;
        IF (p1.ltba_tve_tsk_tkg_src_type IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',358);
        END IF;
        IF (p1.ltba_tve_version_number IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',369);
        END IF;
        IF (p1.ltba_bhe_code IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',370);
        END IF;
        IF (p1.ltba_bca_year IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',371);
        END IF;
        IF (p1.ltba_net_amount IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',372);
        END IF;
        IF (p1.ltba_display_sequence IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',339);
        END IF;
        IF (p1.ltba_tax_amount IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',373);
        END IF;
        l_tkg_exists := NULL;
        OPEN chk_task_groups(p1.ltba_tve_tsk_tkg_src_ref, p1.ltba_tve_tsk_tkg_code, p1.ltba_tve_tsk_tkg_src_type);
        FETCH chk_task_groups INTO l_tkg_exists;
        CLOSE chk_task_groups;
        IF (l_tkg_exists IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',375);
        END IF;
        IF (p1.ltba_tve_tsk_tkg_src_type IS NOT NULL) 
        THEN
          IF (p1.ltba_tve_tsk_tkg_src_type NOT IN('CNT','PRG','PRJ')) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',365);
          ELSIF (p1.ltba_tve_tsk_tkg_src_type = 'CNT') 
          THEN
            l_cnt_exists := NULL;
            OPEN chk_contract(p1.ltba_tve_tsk_tkg_src_ref);
            FETCH chk_contract INTO l_cnt_exists;
            CLOSE chk_contract;
            IF (l_cnt_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',333);
            END IF;
          ELSIF (p1.ltba_tve_tsk_tkg_src_type = 'PRG') 
          THEN
            l_prg_exists := NULL;
            OPEN chk_programmes(p1.ltba_tve_tsk_tkg_src_ref);
            FETCH chk_programmes INTO l_prg_exists;
            CLOSE chk_programmes;
            IF (l_prg_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',366);
            END IF;
          ELSIF (p1.ltba_tve_tsk_tkg_src_type = 'PRJ') 
          THEN
            l_prj_exists := NULL;
            OPEN chk_projects(p1.ltba_tve_tsk_tkg_src_ref);
            FETCH chk_projects INTO l_prj_exists;
            CLOSE chk_projects;
            IF (l_prj_exists IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',302);
            END IF;
          END IF;
        END IF;
        l_bhe_code_exists := NULL;
        OPEN chk_bhe_code(p1.ltba_bhe_code);
        FETCH chk_bhe_code INTO l_bhe_code_exists;
        CLOSE chk_bhe_code;
        IF (l_bhe_code_exists IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',328);
        END IF;
        l_bca_year_exists := NULL;
        OPEN chk_bca_year(p1.ltba_bca_year);
        FETCH chk_bca_year INTO l_bca_year_exists;
        CLOSE chk_bca_year;
        IF (l_bca_year_exists IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',528);
        END IF;
        IF (l_bhe_code_exists IS NOT NULL AND l_bca_year_exists IS NOT NULL)
        THEN
          l_bhe_refno := NULL;
          OPEN c_bhe_refno(p1.ltba_bhe_code);
          FETCH c_bhe_refno INTO l_bhe_refno;
          CLOSE c_bhe_refno;
          l_bud_refno := NULL;
          OPEN c_bud_refno(l_bhe_refno,p1.ltba_bca_year);
          FETCH c_bud_refno INTO l_bud_refno;
          CLOSE c_bud_refno;
          IF (l_bud_refno IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',329);
          END IF;
        END IF;
        IF (p1.LTBA_CURRENT_IND NOT IN('Y','N')) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',307);
        END IF;
        l_task_id := NULL;
        OPEN get_task_id
          (p1.ltba_tve_tsk_tkg_src_ref
          ,p1.ltba_tve_tsk_tkg_code
          ,p1.ltba_tve_tsk_tkg_src_type
          ,p1.ltba_tsk_stk_code
          ,p1.ltba_tve_display_sequence
          );
        FETCH get_task_id INTO l_task_id;
        CLOSE get_task_id;
        l_tve_exists := NULL;
        OPEN chk_task_version_exists
          (p1.ltba_tve_tsk_tkg_src_ref
          ,p1.ltba_tve_tsk_tkg_code
          ,p1.ltba_tve_tsk_tkg_src_type
          ,l_task_id, p1.ltba_tve_version_number
          ,p1.ltba_tve_display_sequence
          );
        FETCH chk_task_version_exists INTO l_tve_exists;
        CLOSE chk_task_version_exists;
        IF (l_tve_exists IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',405);
        END IF;
        l_tba_exists := NULL;
        OPEN chk_tba_exists
          (p1.ltba_tve_tsk_tkg_src_ref
          ,p1.ltba_tve_tsk_tkg_code
          ,p1.ltba_tve_tsk_tkg_src_type
          ,l_task_id
          ,p1.ltba_tve_version_number
          ,l_bud_refno
          );
        FETCH chk_tba_exists INTO l_tba_exists;
        CLOSE chk_tba_exists;
        IF (l_tba_exists IS NOT NULL) THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',406);
        END IF;
        IF (p1.ltgc_hgl_segment1 IS NOT NULL) 
        THEN
          l_hgl_segment_exists := NULL;
          OPEN chk_hgl_segment_exists(p1.ltgc_hgl_segment1);
          FETCH chk_hgl_segment_exists INTO l_hgl_segment_exists;
          CLOSE chk_hgl_segment_exists;
          IF (l_hgl_segment_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',407);
          END IF;
        END IF;
        IF (p1.ltgc_hgl_segment2 IS NOT NULL) 
        THEN
          l_hgl_segment_exists := NULL;
          OPEN chk_hgl_segment_exists(p1.ltgc_hgl_segment2);
          FETCH chk_hgl_segment_exists INTO l_hgl_segment_exists;
          CLOSE chk_hgl_segment_exists;
          IF (l_hgl_segment_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',408);
          END IF;
        END IF;
        IF (p1.ltgc_hgl_segment3 IS NOT NULL) 
        THEN
          l_hgl_segment_exists := NULL;
          OPEN chk_hgl_segment_exists(p1.ltgc_hgl_segment3);
          FETCH chk_hgl_segment_exists INTO l_hgl_segment_exists;
          CLOSE chk_hgl_segment_exists;
          IF (l_hgl_segment_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',409);
          END IF;
        END IF;
        IF (p1.ltgc_hgl_segment4 IS NOT NULL) 
        THEN
          l_hgl_segment_exists := NULL;
          OPEN chk_hgl_segment_exists(p1.ltgc_hgl_segment4);
          FETCH chk_hgl_segment_exists INTO l_hgl_segment_exists;
          CLOSE chk_hgl_segment_exists;
          IF (l_hgl_segment_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',410);
          END IF;
        END IF;
        IF (p1.ltgc_hgl_segment5 IS NOT NULL) 
        THEN
          l_hgl_segment_exists := NULL;
          OPEN chk_hgl_segment_exists(p1.ltgc_hgl_segment5);
          FETCH chk_hgl_segment_exists INTO l_hgl_segment_exists;
          CLOSE chk_hgl_segment_exists;
          IF (l_hgl_segment_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',411);
          END IF;
        END IF;
        IF (p1.LTGC_HGL_SEGMENT6 IS NOT NULL) 
        THEN
          l_hgl_segment_exists := NULL;
          OPEN chk_hgl_segment_exists(p1.ltgc_hgl_segment6);
          FETCH chk_hgl_segment_exists INTO l_hgl_segment_exists;
          CLOSE chk_hgl_segment_exists;
          IF (l_hgl_segment_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',412);
          END IF;
        END IF;
        IF (p1.ltgc_hgl_segment7 IS NOT NULL) 
        THEN
          l_hgl_segment_exists := NULL;
          OPEN chk_hgl_segment_exists(p1.ltgc_hgl_segment7);
          FETCH chk_hgl_segment_exists INTO l_hgl_segment_exists;
          CLOSE chk_hgl_segment_exists;
          IF (l_hgl_segment_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',413);
          END IF;
        END IF;
        IF (p1.ltgc_hgl_segment8 IS NOT NULL) 
        THEN
          l_hgl_segment_exists := NULL;
          OPEN chk_hgl_segment_exists(p1.ltgc_hgl_segment8);
          FETCH chk_hgl_segment_exists INTO l_hgl_segment_exists;
          CLOSE chk_hgl_segment_exists;
          IF (l_hgl_segment_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',414);
          END IF;
        END IF;
        IF (p1.ltgc_hgl_segment9 IS NOT NULL) 
        THEN
          l_hgl_segment_exists := NULL;
          OPEN chk_hgl_segment_exists(p1.ltgc_hgl_segment9);
          FETCH chk_hgl_segment_exists INTO l_hgl_segment_exists;
          CLOSE chk_hgl_segment_exists;
          IF (l_hgl_segment_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',415);
          END IF;
        END IF;
        IF (p1.ltgc_hgl_segment10 IS NOT NULL) 
        THEN
          l_hgl_segment_exists := NULL;
          OPEN chk_hgl_segment_exists(p1.ltgc_hgl_segment10);
          FETCH chk_hgl_segment_exists INTO l_hgl_segment_exists;
          CLOSE chk_hgl_segment_exists;
          IF (l_hgl_segment_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',416);
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
    CURSOR c1
      (p_batch_id VARCHAR2)
    IS
      SELECT rowid                     ltba_rowid
      ,      ltba_dlb_batch_id
      ,      ltba_dl_seqno
      ,      ltba_dl_load_status
      ,      ltba_tve_tsk_tkg_src_ref
      ,      ltba_tve_tsk_tkg_code
      ,      ltba_tve_tsk_tkg_src_type
      ,      ltba_tve_version_number
      ,      ltba_bca_year
      ,      ltba_bhe_code
      ,      ltba_display_sequence
      ,      NVL(ltba_current_ind,'Y') ltba_current_ind
      ,      ltba_net_amount
      ,      ltba_tsk_id
      ,      NVL(ltba_tax_amount,0.00) ltba_tax_amount
      ,      ltgc_hgl_segment1
      ,      ltgc_hgl_segment2
      ,      ltgc_hgl_segment3
      ,      ltgc_hgl_segment4
      ,      ltgc_hgl_segment5
      ,      ltgc_hgl_segment6
      ,      ltgc_hgl_segment7
      ,      ltgc_hgl_segment8
      ,      ltgc_hgl_segment9
      ,      ltgc_hgl_segment10
      ,      ltba_tsk_stk_code
      ,      ltgc_hgf_seqno
      FROM   dl_hpm_payment_task_dets
      WHERE  ltba_dlb_batch_id = p_batch_id
      AND    ltba_dl_load_status IN('C');
    CURSOR c_bhe_refno
      (p_bhe_code VARCHAR2)
    IS
      SELECT bhe_refno
      FROM   budget_heads
      WHERE  bhe_code = p_bhe_code;
    CURSOR c_bud_refno
      (p_bhe_refno NUMBER, p_bca_year NUMBER)
    IS
      SELECT bud_refno
      FROM   budgets
      WHERE  bud_bhe_refno = p_bhe_refno
      AND    bud_bca_year = p_bca_year;
    cb          VARCHAR2(30);
    cd          DATE;
    cp          VARCHAR2(30) := 'DELETE';
    ct          VARCHAR2(30) := 'DL_HPM_PAYMENT_TASK_DETS';
    cs          INTEGER;
    ce          VARCHAR2(200);
    l_exists    VARCHAR2(1);
    l_errors    VARCHAR2(1);
    l_error_ind VARCHAR2(1);
    i           INTEGER := 0;
    l_bhe_refno NUMBER(10);
    l_bud_refno NUMBER(10);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_payment_task_dets.dataload_delete');
    fsc_utils.debug_message('s_dl_hpm_payment_task_dets.dataload_delete',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.ltba_dl_seqno;
        l_bhe_refno := NULL;
        OPEN c_bhe_refno(p1.ltba_bhe_code);
        FETCH c_bhe_refno INTO l_bhe_refno;
        CLOSE c_bhe_refno;
        l_bud_refno := NULL;
        OPEN c_bud_refno(l_bhe_refno,p1.ltba_bca_year);
        FETCH c_bud_refno INTO l_bud_refno;
        CLOSE c_bud_refno;
        i := i + 1;
        SAVEPOINT SP1;
        DELETE
        FROM   task_gl_classifications
        WHERE  tgc_tba_tve_tsk_tkg_src_ref = p1.ltba_tve_tsk_tkg_src_ref
        AND    tgc_tba_tve_tsk_tkg_code = p1.ltba_tve_tsk_tkg_code
        AND    tgc_tba_tve_tsk_tkg_src_type = p1.ltba_tve_tsk_tkg_src_type
        AND    tgc_tba_tve_tsk_id = p1.ltba_tsk_id
        AND    tgc_tba_tve_version_number = p1.ltba_tve_version_number
        AND    tgc_tba_bud_refno = l_bud_refno;
        DELETE
        FROM   task_budget_amounts
        WHERE  tba_tve_tsk_tkg_src_ref = p1.ltba_tve_tsk_tkg_src_ref
        AND    tba_tve_tsk_tkg_code = p1.ltba_tve_tsk_tkg_code
        AND    tba_tve_tsk_tkg_src_type = p1.ltba_tve_tsk_tkg_src_type
        AND    tba_tve_tsk_id = p1.ltba_tsk_id
        AND    tba_tve_version_number = p1.ltba_tve_version_number
        AND    tba_bud_refno = l_bud_refno;
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
END s_dl_hpm_payment_task_dets;
/
