CREATE OR REPLACE PACKAGE BODY s_dl_hpm_man_area_budgets
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VER     DB Ver  WHO  WHEN         WHY
--  1.0     6.9.0   AJ   04-NOV-2013  Initial Creation for Alberta
--  1.1     6.13.0  MJK  17-NOV-2015  Reformatted - no logic changes
--
--
-- **************************************************************************************************
--
--
  PROCEDURE set_record_status_flag
    (p_rowid  IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_hpm_man_area_budgets
    SET    lmab_dl_load_status = p_status
    WHERE  ROWID = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_hpm_man_area_budgets');
    RAISE;
  END set_record_status_flag;
  --
  -- **************************************************************************************************
  --
  PROCEDURE dataload_create
    (p_batch_id    IN VARCHAR2
    ,p_date        IN DATE
    )
  AS
  CURSOR c1
    (p_batch_id  VARCHAR2) 
  IS
    SELECT ROWID rec_rowid
    ,      lmab_dlb_batch_id
    ,      lmab_dl_seqno
    ,      lmab_dl_load_status
    ,      lmab_aun_code
    ,      lmab_bhe_code
    ,      lmab_bca_year
    ,      lmab_del_bud_refno
    FROM   dl_hpm_man_area_budgets
    WHERE  lmab_dlb_batch_id = p_batch_id
    AND    lmab_dl_load_status = 'V';
  CURSOR c_get_bhe_refno
    (p_bhe_code VARCHAR2) 
  IS
    SELECT bhe_refno
    FROM   budget_heads
    WHERE  bhe_code = p_bhe_code;
  CURSOR c_get_bud_refno
    (p_bhe_refno NUMBER
    ,p_bca_year  NUMBER
    ) 
  IS
    SELECT bud_refno
    FROM   budgets
    WHERE  bud_bhe_refno = p_bhe_refno
    AND    bud_bca_year = p_bca_year;
  --
  -- Constants for process_summary
  --
  cb             VARCHAR2(30);
  cd             DATE;
  cp             VARCHAR2(30) := 'CREATE';
  ct             VARCHAR2(30) := 'DL_HPM_MAN_AREA_BUDGETS';
  cs             INTEGER;
  ce             VARCHAR2(200);
  ci             INTEGER;
  l_id           ROWID;
  i              INTEGER := 0;
  l_an_tab       VARCHAR2(1);
  --
  -- Other variables
  --
  l_bhe_refno   NUMBER(10);
  l_bud_refno   NUMBER(10);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_man_area_budgets.dataload_create');
    fsc_utils.debug_message('s_dl_hpm_man_area_budgets.dataload_create',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    FOR p1 in c1(p_batch_id) 
    LOOP
      BEGIN
        cs   := p1.lmab_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        --
        -- get the bhe_refno then get the bud_refno from budget_heads and budgets tables
        --
        l_bhe_refno := NULL;
        l_bud_refno := NULL;
        --
        -- get bhe_refno first
        --
        OPEN c_get_bhe_refno(p1.lmab_bhe_code);
        FETCH c_get_bhe_refno into l_bhe_refno;
        CLOSE c_get_bhe_refno;
        --
        -- now use l_bhe_refno and l_mab_bca_year to get bud_refno
        --
        OPEN c_get_bud_refno(l_bhe_refno, p1.lmab_bca_year);
        FETCH c_get_bud_refno into l_bud_refno;
        CLOSE c_get_bud_refno;
        --
        -- Insert into relevant table
        --
        INSERT INTO management_area_budgets
        (mab_aun_code
        ,mab_bud_refno
        )
        VALUES  
        (p1.lmab_aun_code
        ,l_bud_refno
        );
        --
        -- use l_bud_refno and store it in lmab_del_bud_refno to use for delete
        -- not checked if field is NULL before update to allow for create delete
        -- create delete without deleting batch
        --
        UPDATE dl_hpm_man_area_budgets
        SET    lmab_del_bud_refno = l_bud_refno
        WHERE  lmab_dlb_batch_id = p1.lmab_dlb_batch_id
        AND    lmab_dl_seqno = p1.lmab_dl_seqno
        AND    lmab_aun_code = p1.lmab_aun_code
        AND    lmab_bhe_code = p1.lmab_bhe_code
        AND    lmab_bca_year = p1.lmab_bca_year;
        --
        -- keep a count of the rows processed and commit after every 1000
        --
        i := i + 1; 
        IF MOD(i,1000) = 0 
        THEN 
          COMMIT; 
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        set_record_status_flag(l_id,'C');
      EXCEPTION
      WHEN OTHERS 
      THEN
        ROLLBACK TO SP1;
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
        set_record_status_flag(l_id,'O');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
    END LOOP;
    --
    -- Section to analyse the table(s) populated by this data load
    --
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('MANAGEMENT_AREA_BUDGETS');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_create;
  --
  -- **************************************************************************************************
  --
  PROCEDURE dataload_validate
    (p_batch_id  IN VARCHAR2
    ,p_date      IN DATE
    )
  AS
  CURSOR c1
    (p_batch_id  VARCHAR2) 
  IS
    SELECT ROWID rec_rowid
    ,      lmab_dlb_batch_id
    ,      lmab_dl_seqno
    ,      lmab_dl_load_status
    ,      lmab_aun_code
    ,      lmab_bhe_code
    ,      lmab_bca_year
    ,      lmab_del_bud_refno
    FROM   dl_hpm_man_area_budgets
    WHERE  lmab_dlb_batch_id = p_batch_id
    AND    lmab_dl_load_status IN ('L','F','O');
  CURSOR chk_aun_exists
    (p_aun_code VARCHAR2) 
  IS
    SELECT 'X'
    FROM   admin_units
    WHERE  aun_code = p_aun_code;
  CURSOR chk_bhe_exists(p_bhe_code VARCHAR2) 
  IS
    SELECT 'X'
    FROM   budget_heads
    WHERE  bhe_code = p_bhe_code;
  CURSOR chk_bca_yr_exists
    (p_bca_year NUMBER) 
  IS
    SELECT 'X'
    FROM   budget_calendars
    WHERE  bca_year = p_bca_year;
  CURSOR c_get_bhe_refno
    (p_bhe_code VARCHAR2) 
  IS
    SELECT bhe_refno
    FROM   budget_heads
    WHERE  bhe_code = p_bhe_code;
  CURSOR c_get_bud_refno
    (p_bhe_refno NUMBER
    ,p_bca_year  NUMBER
    ) 
  IS
    SELECT bud_refno
    FROM   budgets
    WHERE  bud_bhe_refno = p_bhe_refno
    AND    bud_bca_year = p_bca_year;
  CURSOR chk_mab_exists
    (p_aun_code VARCHAR2
    ,p_bud_refno NUMBER
    ) 
  IS
    SELECT 'X'
    FROM   management_area_budgets
    WHERE  mab_aun_code = p_aun_code
    AND    mab_bud_refno = p_bud_refno;
  --
  -- Constants FOR summary reporting
  --
  cb                 VARCHAR2(30);
  cd                 DATE;
  cp                 VARCHAR2(30) := 'VALIDATE';
  ct                 VARCHAR2(30) := 'DL_HPM_MAN_AREA_BUDGETS';
  cs                 INTEGER;
  ce                 VARCHAR2(200);
  --
  -- other variables
  --
  l_errors           VARCHAR2(1);
  l_error_ind        VARCHAR2(1);
  i                  INTEGER :=0;
  l_id               ROWID;
  l_aun_exists       VARCHAR2(1);
  l_bhe_exists       VARCHAR2(1);
  l_bca_yr_exists    VARCHAR2(1);
  l_bhe_refno        NUMBER(10);
  l_bud_refno        NUMBER(10);
  l_mab_exists       VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_man_area_budgets.dataload_validate');
    fsc_utils.debug_message('s_dl_hpm_man_area_budgets.dataload_validate',3 );
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    FOR p1 in c1(p_batch_id) 
    LOOP  
      BEGIN
        cs := p1.lmab_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors    := 'V';
        l_error_ind := 'N';
        l_aun_exists := NULL;
        l_bhe_exists := NULL;
        l_bca_yr_exists := NULL;
        l_bhe_refno := NULL;
        l_bud_refno := NULL;
        l_mab_exists := NULL;
        --
        -- Check that the Admin Unit Code has been supplied does not exceed the max
        -- length of 20 Char and exists in admin_units table 
        --
        IF p1.lmab_aun_code IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',189);
        ELSIF p1.lmab_aun_code IS NOT NULL
        AND   LENGTH(p1.lmab_aun_code) > 20
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',614);
        ELSE
          OPEN chk_aun_exists(p1.lmab_aun_code);
          FETCH chk_aun_exists INTO l_aun_exists;
          IF chk_aun_exists%NOTFOUND 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',614);
          END IF;
          CLOSE chk_aun_exists;
        END IF;
        --
        -- Check that the Budget Heads Code has been supplied does not exceed the max
        -- length of 30 Char and exists in budget_heads table 
        --
        IF p1.lmab_bhe_code IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',648);
        ELSIF p1.lmab_bhe_code IS NOT NULL
        AND   LENGTH(p1.lmab_bhe_code) > 30
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',646);
        ELSE
          OPEN chk_bhe_exists(p1.lmab_bhe_code);
          FETCH chk_bhe_exists INTO l_bhe_exists;
          IF chk_bhe_exists%NOTFOUND 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',646);
          END IF;
          CLOSE chk_bhe_exists;
        END IF;
        --
        -- Check that the Budget Year has been supplied does not exceed the max
        -- length of 4 Char and exists in budget_calendars table 
        --
        IF p1.lmab_bca_year IS NULL 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',645);
        ELSIF p1.lmab_bca_year IS NOT NULL
        AND   LENGTH(p1.lmab_bca_year) > 4
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',649);
        ELSE
          OPEN chk_bca_yr_exists(p1.lmab_bca_year);
          FETCH chk_bca_yr_exists INTO l_bca_yr_exists;
          IF chk_bca_yr_exists%NOTFOUND 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',649);
          END IF;
          CLOSE chk_bca_yr_exists;
        END IF;
        --
        -- If the Admin Unit Budget Heads Code and Budget Year all exist then Check
        -- that the combination supplied exists in the budgets table and does not 
        -- already exist in the Management Area Budgets table
        --
        IF  l_aun_exists IS NOT NULL
        AND l_bhe_exists IS NOT NULL
        AND l_bca_yr_exists IS NOT NULL
        THEN
          --
          -- get bhe_refno first
          --
          OPEN c_get_bhe_refno(p1.lmab_bhe_code);
          FETCH c_get_bhe_refno into l_bhe_refno;
          CLOSE c_get_bhe_refno;
          --
          -- now use l_bhe_refno and l_mab_bca_year to check bud_refnoc exists
          -- and get it if it does
          --
          OPEN c_get_bud_refno(l_bhe_refno,p1.lmab_bca_year);
          FETCH c_get_bud_refno into l_bud_refno;
        IF c_get_bud_refno%NOTFOUND 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',647);
          END IF;
          CLOSE c_get_bud_refno;
          --
          -- now use l_bud_refno and l_mab_aun_code to check if the combination already
          -- exists in the Management Area Budgets table
          --
          OPEN chk_mab_exists(p1.lmab_aun_code,l_bud_refno);
          FETCH chk_mab_exists INTO l_mab_exists;
          IF chk_mab_exists%FOUND 
          THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',672);
          END IF;
          CLOSE chk_mab_exists;
        END IF;
        --
        -- Now UPDATE the record count AND error code
        --
        IF l_errors = 'F' 
        THEN
          l_error_ind := 'Y';
        ELSE
          l_error_ind := 'N';
        END IF;
        --
        -- keep a count of the rows processed and commit after every 1000
        --
        i := i + 1; 
        IF MOD(i,1000) = 0
        THEN 
          COMMIT; 
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
        set_record_status_flag(l_id,l_errors);
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
  -- **************************************************************************************************
  --
  PROCEDURE dataload_delete
    (p_batch_id  IN VARCHAR2
    ,p_date      IN DATE
    )
  AS
  CURSOR c1
    (p_batch_id  VARCHAR2) 
  IS
    SELECT ROWID rec_rowid
    ,      lmab_dlb_batch_id
    ,      lmab_dl_seqno
    ,      lmab_dl_load_status
    ,      lmab_aun_code
    ,      lmab_bhe_code
    ,      lmab_bca_year
    ,      lmab_del_bud_refno
    FROM   dl_hpm_man_area_budgets
    WHERE  lmab_dlb_batch_id = p_batch_id
    AND    lmab_dl_load_status = 'C';
  --
  -- Constants for process_summary
  --
  cb             VARCHAR2(30);
  cd             DATE;
  cp             VARCHAR2(30) := 'DELETE';
  ct             VARCHAR2(30) := 'DL_HPM_MAN_AREA_BUDGETS';
  cs             INTEGER;
  ce             VARCHAR2(200);
  --
  -- Other variables
  --
  l_id           ROWID;
  i              INTEGER := 0;
  l_an_tab       VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_man_area_budgets.dataload_delete');
    fsc_utils.debug_message('s_dl_hpm_man_area_budgets.dataload_delete',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    FOR p1 in c1(p_batch_id) 
    LOOP
      BEGIN
        cs   := p1.lmab_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        --
        -- Deletion of record INDEXE/CONSTRAINT "MAB PK" used to identify the record
        -- that has been inserted.  The 2 Fields are mab_aun_code and mab_bud_refno
        -- mab_bud_refno was loaded into lmab_del_bud_refno by Create process
        --
        DELETE 
        FROM   management_area_budgets
        WHERE  mab_aun_code = p1.lmab_aun_code
        AND    mab_bud_refno = p1.lmab_del_bud_refno;
        --
        -- keep a count of the rows processed and commit after every 1000
        --
        i := i + 1; 
        IF MOD(i,1000) = 0 
        THEN 
          COMMIT; 
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        set_record_status_flag(l_id,'V');
      EXCEPTION
      WHEN OTHERS 
      THEN
        ROLLBACK TO SP1;
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
        set_record_status_flag(l_id,'C');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
    END LOOP;
    COMMIT;
    --
    -- Section to analyse the table(s) populated by this data load
    --
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('MANAGEMENT_AREA_BUDGETS');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_hpm_man_area_budgets;
/