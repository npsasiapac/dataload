CREATE OR REPLACE PACKAGE BODY s_dl_hrm_budgets
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      1.0  5.1.6   PJD  19-JUN-2002  Bespoke Dataload for NCCW
--      2.0  5.2.0   PJD  10-Sep-2002  Corrections to BHE Insert Proc
--      2.1  5.2.0   PJD  19-Sep-2002  Insert and Delete Budget Areas
--      2.2  5.6.0   PH   27-OCT-2004  Added nvl clause to cursor c_parent_bud
--      2.2  5.9.0   PJD  29-mar-2006  Picking up wrong parent as not taking
--                                     year into account
--      2.3  6.5.1   MB   16-FEB-2012  Added in field for arc_Code
--      2.4  6.5.1   MB   01-AUG-2012  Allow BTH in arc_Code for HRM and HPM
--                                     for WA
--      2.5  6.13.0  MJK  16-NOV-2015  Reformatted - no logic changes
-- ***********************************************************************     
--
--  declare package variables and constants
--
--
  PROCEDURE dataload_create
    (p_batch_id  IN VARCHAR2
    ,p_date      IN DATE
    )
  AS
  CURSOR c1 
  IS
    SELECT ROWID rec_rowid
    ,      lbud_dlb_batch_id             
    ,      lbud_dl_seqno                 
    ,      lbud_dl_load_status           
    ,      lbud_bhe_code                 
    ,      lbud_bhe_description          
    ,      lbud_bhe_created_by           
    ,      lbud_bhe_created_date         
    ,      lbud_bca_year                 
    ,      lbud_type                     
    ,      lbud_aun_code                 
    ,      lbud_amount                   
    ,      lbud_allow_negative_ind       
    ,      lbud_repeat_warning_ind       
    ,      lbud_warning_issued_ind       
    ,      lbud_sco_code                 
    ,      lbud_created_by               
    ,      lbud_created_date             
    ,      lbud_bud_bhe_code             
    ,      lbud_bpca_bpr_code            
    ,      lbud_warning_percent          
    ,      lbud_comments                 
    ,      lbud_committed                
    ,      lbud_accrued                  
    ,      lbud_invoiced                 
    ,      lbud_expended                 
    ,      lbud_credited                 
    ,      lbud_tax_committed            
    ,      lbud_tax_accrued              
    ,      lbud_tax_invoiced             
    ,      lbud_tax_expended             
    ,      lbud_tax_credited
    ,      lbud_arc_code
    FROM   dl_hrm_budgets
    WHERE  lbud_dlb_batch_id = p_batch_id
    AND    lbud_dl_load_status = 'V';
  CURSOR c_bhe_check
    (p_bhe_code varchar2) 
  IS
    SELECT bhe_refno
    FROM   budget_heads
    WHERE  bhe_code = p_bhe_code;
  CURSOR c_bhe_refno_new 
  IS
    SELECT bhe_refno_seq.nextval
    FROM   dual;
  CURSOR c_bud_refno 
  IS
    SELECT bud_refno_seq.nextval
    FROM   dual;
  CURSOR c_bud_parent
    (p_bud_bhe_code varchar2
    ,p_bud_year NUMBER
    ) 
  IS
    SELECT bud_refno
    FROM   budgets, budget_heads
    WHERE  bud_bhe_refno = bhe_refno
    AND    bhe_code      = p_bud_bhe_code
    AND    bud_bca_year  = p_bud_year;
  --
  -- Constants for process_summary
  --
  cb           VARCHAR2(30);
  cd           DATE;
  cp           VARCHAR2(30) := 'CREATE';
  ct           VARCHAR2(30) := 'DL_HRM_BUDGETS';
  cs           INTEGER;
  ce           VARCHAR2(200);
  l_an_tab     VARCHAR2(1);
  --
  -- Other variables
  --
  i            INTEGER := 0;
  l_bhe_refno  NUMBER(10);
  l_bud_refno  NUMBER(10);
  l_bud_parent NUMBER(10);
  BEGIN
    fsc_utils.proc_start('s_dl_hrm_budgets.dataload_create');
    fsc_utils.debug_message( 's_dl_hrm_budgets.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 in c1 
    LOOP
      BEGIN
        cs := p1.lbud_dl_seqno;
        SAVEPOINT SP1;
        l_bhe_refno  := null;
        l_bud_refno  := null;
        l_bud_parent := null;
        --
        -- First check to see if Budget Header record already exists
        -- If not get the next value, if so use the value returned
        --
        OPEN c_bhe_check(p1.lbud_bhe_code);
        FETCH c_bhe_check INTO l_bhe_refno;
        CLOSE c_bhe_check; 
        IF l_bhe_refno IS NULL
        THEN
          OPEN c_bhe_refno_new;  
          FETCH c_bhe_refno_new INTO l_bhe_refno;  
          CLOSE c_bhe_refno_new;  
          INSERT INTO budget_heads
          (bhe_refno        
          ,bhe_code        
          ,bhe_description        
          ,bhe_created_by        
          ,bhe_created_date        
          )        
          VALUES
          (l_bhe_refno        
          ,p1.lbud_bhe_code        
          ,p1.lbud_bhe_description        
          ,nvl(p1.lbud_bhe_created_by, 'DATALOAD')        
          ,nvl(p1.lbud_bhe_created_date, TRUNC(SYSDATE))        
          );        
        END IF;
        OPEN c_bud_refno;
        FETCH c_bud_refno INTO l_bud_refno;
        CLOSE c_bud_refno;
        IF p1.lbud_bud_bhe_code IS NOT NULL
        THEN
          OPEN c_bud_parent(p1.lbud_bud_bhe_code,p1.lbud_bca_year);
          FETCH c_bud_parent INTO l_bud_parent;
          CLOSE c_bud_parent;
        END IF;
        INSERT INTO budgets
        (bud_refno
        ,bud_bhe_refno
        ,bud_bca_year
        ,bud_type
        ,bud_aun_code
        ,bud_amount
        ,bud_allow_negative_ind
        ,bud_repeat_warning_ind
        ,bud_warning_issued_ind
        ,bud_sco_code
        ,bud_created_by
        ,bud_created_date
        ,bud_bud_refno
        ,bud_bpca_bpr_code
        ,bud_warning_percent
        ,bud_comments
        ,bud_committed
        ,bud_accrued
        ,bud_invoiced
        ,bud_expended
        ,bud_credited
        ,bud_tax_committed
        ,bud_tax_accrued
        ,bud_tax_invoiced
        ,bud_tax_expended
        ,bud_tax_credited
        )
        VALUES
        (l_bud_refno
        ,l_bhe_refno
        ,p1.lbud_bca_year
        ,p1.lbud_type
        ,p1.lbud_aun_code 
        ,p1.lbud_amount
        ,p1.lbud_allow_negative_ind 
        ,p1.lbud_repeat_warning_ind
        ,p1.lbud_warning_issued_ind
        ,p1.lbud_sco_code
        ,NVL(p1.lbud_created_by, 'DATALOAD')
        ,NVL(p1.lbud_created_date, TRUNC(SYSDATE))
        ,l_bud_parent
        ,p1.lbud_bpca_bpr_code
        ,p1.lbud_warning_percent 
        ,p1.lbud_comments
        ,p1.lbud_committed
        ,p1.lbud_accrued 
        ,p1.lbud_invoiced
        ,p1.lbud_expended
        ,p1.lbud_credited
        ,p1.lbud_tax_committed  
        ,p1.lbud_tax_accrued
        ,p1.lbud_tax_invoiced              
        ,p1.lbud_tax_expended              
        ,p1.lbud_tax_credited
        );
        IF p1.lbud_arc_code = 'BTH'
        THEN
          --
          -- HRM
          --
          INSERT INTO budget_areas
          (bar_bud_refno      
          ,bar_arc_code       
          ,bar_arc_sys_code   
          ,bar_active_ind     
          ,bar_created_by     
          ,bar_created_date 
          )  
          VALUES
          (l_bud_refno
          ,'HRM'
          ,'HOU'
          ,'Y'
          ,USER
          ,SYSDATE
          );
          --
          -- and HPM
          --
          INSERT INTO budget_areas
          (bar_bud_refno      
          ,bar_arc_code       
          ,bar_arc_sys_code   
          ,bar_active_ind     
          ,bar_created_by     
          ,bar_created_date 
          )  
          VALUES
          (l_bud_refno
          ,'HPM'
          ,'HOU'
          ,'Y'
          ,USER
          ,SYSDATE
          );
        ELSIF p1.lbud_arc_code IN ('HRM','HPM')
        THEN
          INSERT INTO budget_areas
          (bar_bud_refno      
          ,bar_arc_code       
          ,bar_arc_sys_code   
          ,bar_active_ind     
          ,bar_created_by     
          ,bar_created_date 
          )  
          VALUES
          (l_bud_refno
          ,p1.lbud_arc_code
          ,'HOU'
          ,'Y'
          ,USER
          ,SYSDATE
          );
        END IF;
        --
        -- keep a count of the rows processed and commit after every 5000
        --
        i := i + 1; 
        IF MOD(i,5000) = 0 
        THEN 
          COMMIT; 
        END If;
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
    COMMIT;
    --
    -- Section to anayze the table(s) populated by this dataload
    --
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('BUDGETS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('BUDGET_HEADS');
    --
    -- Set the chargeable flag based on whether a child record exists
    --
    UPDATE budgets b1
    SET    b1.bud_type = 'N'
    WHERE  EXISTS 
             (SELECT NULL 
              FROM budgets b2
              where b2.bud_bud_refno = b1.bud_refno
             )
    AND  bud_type = 'C';
    fsc_utils.proc_end;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_create;
  --
  --  **************************************************************************
  --
  PROCEDURE dataload_validate
    (p_batch_id  IN VARCHAR2
    ,p_date      IN DATE
    )
  AS
  CURSOR c1 
  IS
    SELECT ROWID rec_rowid
    ,      lbud_dlb_batch_id              
    ,      lbud_dl_seqno                  
    ,      lbud_dl_load_status            
    ,      lbud_bhe_code                  
    ,      lbud_bhe_description           
    ,      lbud_bhe_created_by            
    ,      lbud_bhe_created_date          
    ,      lbud_bca_year                  
    ,      lbud_type                      
    ,      lbud_aun_code                  
    ,      lbud_amount                    
    ,      lbud_allow_negative_ind        
    ,      lbud_repeat_warning_ind        
    ,      lbud_warning_issued_ind        
    ,      lbud_sco_code                  
    ,      lbud_created_by                
    ,      lbud_created_date              
    ,      lbud_bud_bhe_code              
    ,      lbud_bpca_bpr_code             
    ,      lbud_warning_percent           
    ,      lbud_comments                  
    ,      lbud_committed                 
    ,      lbud_accrued                   
    ,      lbud_invoiced                  
    ,      lbud_expended                  
    ,      lbud_credited                  
    ,      lbud_tax_committed             
    ,      lbud_tax_accrued               
    ,      lbud_tax_invoiced              
    ,      lbud_tax_expended              
    ,      lbud_tax_credited
    ,      lbud_arc_code
    FROM   dl_hrm_budgets
    WHERE  lbud_dlb_batch_id = p_batch_id
    AND    lbud_dl_load_status   in ('L','F','O');
  CURSOR c_aun_code
    (p_aun_code VARCHAR2) 
  IS
    SELECT NULL
    FROM   admin_units
    WHERE  aun_code = p_aun_code;
  CURSOR c_bud_year
    (p_bud_year number) 
  IS
    SELECT 'X'
    FROM   budget_calendars
    WHERE  bca_year = p_bud_year;
  CURSOR c_sco
    (p_sco_code VARCHAR2) 
  IS
    SELECT 'X'
    FROM   status_codes
    WHERE  sco_code = p_sco_code;
  CURSOR c_parent_bud
    (p_bud_bhe_code VARCHAR2
    ,p_batch_id     VARCHAR2
    ) 
  IS
    SELECT 'X'
    FROM   budgets
    ,      budget_heads
    WHERE  bud_bhe_refno = bhe_refno
    AND    bhe_code = p_bud_bhe_code
    UNION
    SELECT 'X'
    FROM   dl_hrm_budgets 
    WHERE  lbud_bhe_code = p_bud_bhe_code
    AND    NVL(lbud_bud_bhe_code, '!*~') != p_bud_bhe_code
    AND    lbud_dlb_batch_id = p_batch_id
    AND    lbud_dl_load_status = 'V';
  CURSOR c_profile
    (p_profile_code VARCHAR2
    ,p_year         NUMBER
    )  
  IS
    SELECT 'X'
    FROM   budget_profile_calendars
    WHERE  bpca_bpr_code = p_profile_code
    AND    bpca_bca_year = p_year;
  --
  -- Constants for process_summary
  --
  cb       VARCHAR2(30);
  cd       DATE;
  cp       VARCHAR2(30) := 'VALIDATE';
  ct       VARCHAR2(30) := 'DL_HRM_BUDGETS';
  cs       INTEGER;
  ce       VARCHAR2(200);
  --
  -- Other variables
  --
  l_exists         VARCHAR2(1);
  l_errors         VARCHAR2(10);
  l_error_ind      VARCHAR2(10);
  i                INTEGER :=0;
  BEGIN
    fsc_utils.proc_start('s_dl_hrm_budgets.dataload_validate');
    fsc_utils.debug_message( 's_dl_hrm_budgets.dataload_validate',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1 
    LOOP
      BEGIN
        cs := p1.lbud_dl_seqno;
        l_errors := 'V';
        l_error_ind := 'N';
        --
        -- Check the Links to Other Tables
        --
        -- Check the admin_unit code exists on ADMIN UNITS
        --
        OPEN c_aun_code(p1.lbud_aun_code);
        FETCH c_aun_code INTO l_exists;
        IF c_aun_code%NOTFOUND 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
        END IF;
        CLOSE c_aun_code;
        --
        -- Check the Y/N fields
        --
        --  Allow Negative Indicator
        --
        IF NOT s_dl_hem_utils.yorn(p1.lbud_allow_negative_ind)
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',523);
        END IF;
        --
        --  Repeat Warning Indicator
        --
        IF NOT s_dl_hem_utils.yorn(p1.lbud_repeat_warning_ind)
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',524);
        END IF;
        --
        --  Warning Issued Indicator
        --
        IF NOT s_dl_hem_utils.yorn(p1.lbud_warning_issued_ind)
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',525);
        END IF;
        --
        --  Check Mandatory Fields
        --
        --  Budget Head Code
        --
        IF p1.lbud_bhe_code IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',526);
        END IF;
        --
        --  Budget Head Description
        --
        IF p1.lbud_bhe_description IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',527);
        END IF;
        --
        --  Budget Year
        --
        OPEN c_bud_year(p1.lbud_bca_year);
        FETCH c_bud_year INTO l_exists;
        IF c_bud_year%NOTFOUND
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',528);
        END IF;
        CLOSE c_bud_year;
        --
        --  Budget Type
        --
        IF NVL(p1.lbud_type, 'X') NOT IN ('C','N')
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',529);
        END IF;
        --
        --  Budget Amount
        --
        IF p1.lbud_amount IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',530);
        END IF;
        --
        --  Status Code
        --
        OPEN  c_sco(p1.lbud_sco_code);
        FETCH c_sco INTO l_exists;
        IF c_sco%NOTFOUND
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',531);
        END IF;
        CLOSE c_sco;
        --
        -- Other Validation Checks
        --
        --  If parent code supplied check it already exists on budgets, budget_heads
        --  or is a validated record
        --
        IF p1.lbud_bud_bhe_code IS NOT NULL
        THEN
          OPEN c_parent_bud(p1.lbud_bud_bhe_code, p1.lbud_dlb_batch_id);
          FETCH c_parent_bud INTO l_exists;
          IF c_parent_bud%NOTFOUND
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',532);
          END IF;
          CLOSE c_parent_bud;
        END IF;
        --
        --  Check Budget Profile Code
        --
        IF p1.lbud_bpca_bpr_code IS NOT NULL
        THEN
          OPEN c_profile(p1.lbud_bpca_bpr_code, p1.lbud_bca_year);
          FETCH c_profile INTO l_exists;
          IF c_profile%NOTFOUND
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',533);
          END IF;
          CLOSE c_profile;
        END IF;
        IF p1.lbud_arc_code NOT IN ('HRM','HPM','BTH')
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',435);
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
  --  **************************************************************************
  --
  PROCEDURE dataload_delete 
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    ) 
  IS
  CURSOR c1 
  IS
    SELECT ROWID rec_rowid
    ,      lbud_dlb_batch_id              
    ,      lbud_dl_seqno                  
    ,      lbud_dl_load_status            
    ,      lbud_bhe_code                  
    ,      lbud_bhe_description           
    ,      lbud_bhe_created_by            
    ,      lbud_bhe_created_date          
    ,      lbud_bca_year                  
    ,      lbud_type                      
    ,      lbud_aun_code                  
    ,      lbud_amount                    
    ,      lbud_allow_negative_ind        
    ,      lbud_repeat_warning_ind        
    ,      lbud_warning_issued_ind        
    ,      lbud_sco_code                  
    ,      lbud_created_by                
    ,      lbud_created_date              
    ,      lbud_bud_bhe_code              
    ,      lbud_bpca_bpr_code             
    ,      lbud_warning_percent           
    ,      lbud_comments                  
    ,      lbud_committed                 
    ,      lbud_accrued                   
    ,      lbud_invoiced                  
    ,      lbud_expended                  
    ,      lbud_credited                  
    ,      lbud_tax_committed             
    ,      lbud_tax_accrued               
    ,      lbud_tax_invoiced              
    ,      lbud_tax_expended              
    ,      lbud_tax_credited
    FROM   dl_hrm_budgets
    WHERE  lbud_dlb_batch_id = p_batch_id
    AND    lbud_dl_load_status   = 'C';
  --
  -- Cursor to get bhe_refno
  --
  CURSOR c_bhe_refno
    (p_bhe_code VARCHAR2) 
  IS
    SELECT bhe_refno
    FROM   budget_heads
    WHERE  bhe_code = p_bhe_code;
  --
  -- CURSOR to get bud_refno
  --
  CURSOR c_bud_refno
    (p_bhe_refno NUMBER
    ,p_bud_year  NUMBER
    ) 
  IS
    SELECT bud_refno
    FROM   budgets
    WHERE  bud_bhe_refno = p_bhe_refno
    AND    bud_bca_year = p_bud_year;
  --
  -- Constants for process_summary
  --
  cb       VARCHAR2(30);
  cd       DATE;
  cp       VARCHAR2(30) := 'DELETE';
  ct       VARCHAR2(30) := 'DL_HRM_BUDGETS';
  cs       INTEGER;
  ce       VARCHAR2(200);
  --
  -- Other variables
  --
  l_an_tab VARCHAR2(1);
  l_bhe_refno  number(10);
  l_bud_refno  number(10);
  i integer := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_hrm_budgets.dataload_delete');
    fsc_utils.debug_message( 's_dl_hrm_budgets.dataload_delete',3 );
    cb := p_batch_id;
    cd := p_date;
    FOR p1 IN c1 
    LOOP
      BEGIN
        cs := p1.lbud_dl_seqno;
        SAVEPOINT SP1;
        l_bhe_refno := NULL;
        l_bud_refno := NULL;
        OPEN c_bhe_refno(p1.lbud_bhe_code);
        FETCH c_bhe_refno INTO l_bhe_refno;
        CLOSE c_bhe_refno;
        OPEN c_bud_refno(l_bhe_refno,p1.lbud_bca_year);
        FETCH c_bud_refno INTO l_bud_refno;
        CLOSE c_bud_refno;
        DELETE 
        FROM   budget_areas
        WHERE  bar_bud_refno = l_bud_refno;
        DELETE 
        FROM   budgets
        WHERE  bud_refno = l_bud_refno;
        DELETE 
        FROM   budget_heads
        WHERE  bhe_refno = l_bhe_refno
        AND    NOT EXISTS 
                 (SELECT NULL 
                  FROM   budgets
                  WHERE  bud_bhe_refno = l_bhe_refno
                 );
        --
        -- keep a count of the rows processed and commit after every 1000
        --
        i := i + 1; 
        IF MOD(i,1000) = 0 
        THEN 
          COMMIT; 
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        s_dl_utils.set_record_status_flag(ct,cb,cs,'V');
      EXCEPTION
      WHEN OTHERS 
      THEN
        ROLLBACK TO SP1;
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
        -- s_dl_utils.set_record_status_flag(ct,cb,cs,'C');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
    END LOOP;
    COMMIT;
    --
    -- Section to anayze the table(s) populated by this dataload
    --
    l_an_tab := s_dl_hem_utils.dl_comp_stats('BUDGETS');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('BUDGET_HEADS');
    fsc_utils.proc_end;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_hrm_budgets;
/
