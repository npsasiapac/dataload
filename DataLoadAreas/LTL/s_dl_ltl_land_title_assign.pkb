CREATE OR REPLACE PACKAGE BODY s_dl_ltl_land_title_assign
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER  WHO  WHEN        WHY
--      1.0          IR  29/10/2007  Initial Creation
--
--      2.0          VS  20/04/2009  Change in the way the ltl_reference
--                                   is constructed. Section Number is 
--                                   not always supplied for DP type
--
--      2.0          VS  20/04/2009  character to number conversion failure
--                                   on column LLTA_HRV_FACR_CODE in VALIDATE.
--                                   process
--
--      2.0          VS  20/04/2009  Changed HDL errors codes to refer to HD1
--                                   in the VALIDATE process.
--
--      2.0          VS  20/04/2009  Check for valid ltl_reference c_ltl_ref_exists
--                                   was not being checked correctly. It should have been 
--                                   %NOTFOUND instead of %FOUND in the VALIDATE process.
--
--      3.0          VS  23/04/2009  Fix for the performance issue. Bring
--                                   set_record_status_flag locally.
--
--      4.0          VS  04/02/2010  Defect Id 3261 Fix. ltl_refno not derived correctly.
--
--      4.1  6.13    MJK 12/11/2015  Reformatted for 6.13. No logic changes
--
--      4.2          PLL 03/07/2018  FIXED DEFECT WITH ASSIGNMENTS TO ADMIN UNITS
-- ***********************************************************************
--
--
  PROCEDURE set_record_status_flag
    (p_rowid  IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_ltl_land_title_assign
    SET    llta_dl_load_status = p_status
    WHERE  rowid               = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_ltl_land_title_assign');
    RAISE;
  END set_record_status_flag;
  --
  -- ***********************************************************************
  --
  PROCEDURE dataload_create
    (p_batch_id  IN VARCHAR2
    ,p_date      IN DATE  
    )
  AS
  CURSOR c1 
  IS 
    SELECT ROWID   rec_rowid
    ,      llta_dl_seqno
    ,      llta_lltl_plan_number
    ,      llta_lltl_lot_number
    ,      llta_lltl_ltt_code
    ,      llta_start_date
    ,      llta_pro_refno 
    ,      llta_aun_code
    ,      llta_end_date
    ,      llta_hrv_facr_code
    ,      llta_comments
    ,      llta_created_by
    ,      llta_created_date
    ,      llta_modified_by
    ,      llta_modified_date
    ,      llta_section_number
    FROM   dl_ltl_land_title_assign
    WHERE  llta_dlb_batch_id   = p_batch_id
    AND    llta_dl_load_status = 'V';
  CURSOR c_plan_type(p_lltl_ltt_code varchar2) 
  IS
    SELECT ltt_hrv_fltt_type 
    FROM   land_title_types
    WHERE  ltt_code = p_lltl_ltt_code;
  CURSOR c_pro 
    (p_propref VARCHAR2) 
  IS
    SELECT pro_refno 
    FROM   properties
    WHERE  pro_propref = p_propref;
  CURSOR c_ltl_refno
    (p_lltl_ref          VARCHAR2
    ,p_ltl_hrv_fltt_type VARCHAR2
    ) 
  IS
    SELECT ltl_refno
    FROM land_titles
    WHERE ltl_reference = p_lltl_ref
    AND ltl_hrv_fltt_type = p_ltl_hrv_fltt_type;
  --
  -- Constants for process_summary
  --
  cb       VARCHAR2(30);
  cd       DATE;
  cp       VARCHAR2(30) := 'CREATE';
  ct       VARCHAR2(30) := 'DL_LTL_LAND_TITLE_ASSIGN';
  cs       INTEGER;
  ce       VARCHAR2(200);
  --
  -- Other variables
  --
  ai                  PLS_INTEGER := 100;
  l_plan_type         vARCHAR2(10);
  l_ltl_reference     VARCHAR2(17);
  l_ltl_refno         NUMBER(10);
  l_an_tab            VARCHAR2(1);
  i                   PLS_INTEGER := 0;
  l_pro_refno         NUMBER;
  l_id                ROWID;
  BEGIN
    execute immediate 'alter trigger LTA_BR_I disable';
    fsc_utils.proc_start('s_dl_ltl_land_title_assign.dataload_create');
    fsc_utils.debug_message( 's_dl_ltl_land_title_assign.dataload_create',3);
    cb := p_batch_id;
    cd := p_date; 
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1 
    LOOP
      BEGIN
        cs := p1.llta_dl_seqno;
        l_id := p1.rec_rowid;
        l_ltl_reference := null;
        l_pro_refno     := null; -- 4.2 fix 
        l_plan_type     := null; -- 4.2 better to do this here.
        OPEN c_plan_type(p1.llta_lltl_ltt_code);
        FETCH c_plan_type into l_plan_type;
        CLOSE c_plan_type;
        IF l_plan_type = 'SP' 
        THEN
          l_ltl_reference := p1.llta_lltl_lot_number||'/'||p1.llta_lltl_plan_number;
        ELSIF l_plan_type = 'DP' 
        THEN
          IF p1.llta_section_number IS NULL
          THEN
            l_ltl_reference := p1.llta_lltl_lot_number||'/'||p1.llta_lltl_plan_number;
          ELSE
            l_ltl_reference := p1.llta_lltl_lot_number||'/'||p1.llta_section_number||'/'||p1.llta_lltl_plan_number;
          END IF; 
        END IF;
        OPEN c_ltl_refno(l_ltl_reference, l_plan_type);
        FETCH c_ltl_refno into l_ltl_refno;
        CLOSE c_ltl_refno;
        OPEN c_pro(p1.llta_pro_refno);
        FETCH c_pro INTO l_pro_refno;
        CLOSE c_pro;
        SAVEPOINT SP1;
        INSERT INTO land_title_assignments
        (LTA_REFNO
        ,LTA_LTL_REFNO
        ,LTA_START_DATE
        ,LTA_PRO_REFNO
        ,LTA_AUN_CODE
        ,LTA_CREATED_BY
        ,LTA_CREATED_DATE
        ,LTA_HRV_FACR_CODE
        ,LTA_END_DATE
        ,LTA_MODIFIED_BY
        ,LTA_MODIFIED_DATE
        ,LTA_COMMENTS
        )
        VALUES
        (LTA_REFNO_SEQ.NEXTVAL
        ,l_ltl_refno
        ,p1.LLTA_START_DATE
        ,l_pro_refno
        ,p1.LLTA_AUN_CODE
        ,p1.LLTA_CREATED_BY
        ,p1.LLTA_CREATED_DATE
        ,p1.LLTA_HRV_FACR_CODE
        ,p1.LLTA_END_DATE
        ,p1.LLTA_MODIFIED_BY
        ,p1.LLTA_MODIFIED_DATE 
        ,p1.LLTA_COMMENTS
        );
        --
        -- Update Record Status and Process Count
        --
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        set_record_status_flag(l_id,'C');
        --
        -- keep a count of the rows processed and commit after every 1000
        --
        i := i + 1;
        IF MOD(i,1000) = 0  
        THEN 
          COMMIT;
          --
          -- Do a regular analyze table based on 10 times as many records as last time
          --
          IF i >= (ai*5)  -- changed from 10* to 5*
          THEN
            ai := i;
            l_an_tab := s_dl_hem_utils.dl_comp_stats('LAND_TITLE_ASSIGN');
          END IF;
        END IF;
      EXCEPTION
      WHEN OTHERS 
      THEN
        ROLLBACK TO SP1;
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
        set_record_status_flag(l_id,'O');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
      COMMIT;
    END LOOP;
    --
    -- Section to anayze the table(s) populated by this dataload
    --
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('LAND_TITLE_ASSIGN');
    execute immediate 'alter trigger LTA_BR_I enable';
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_create;
  --
  -- ***********************************************************************
  --
  PROCEDURE dataload_validate
    (p_batch_id  IN VARCHAR2
    ,p_date      IN DATE
    )
  AS
  CURSOR C1 
  IS
    SELECT ROWID rec_rowid
    ,      llta_dl_seqno
    ,      llta_lltl_plan_number
    ,      llta_lltl_lot_number
    ,      llta_lltl_ltt_code
    ,      llta_start_date
    ,      llta_pro_refno    --Need to get the pro_refno
    ,      llta_aun_code
    ,      llta_end_date
    ,      llta_hrv_facr_code
    ,      llta_comments
    ,      llta_created_by
    ,      llta_created_date
    ,      llta_modified_by
    ,      llta_modified_date 
    ,      llta_section_number
    FROM   dl_ltl_land_title_assign
    WHERE  llta_dlb_batch_id = p_batch_id
    AND    llta_dl_load_status in ('L','F','O');
  CURSOR c_plan_type
    (p_lltl_ltt_code varchar2) 
  IS
    SELECT ltt_hrv_fltt_type 
    FROM   land_title_types
    WHERE  ltt_code = p_lltl_ltt_code;
  CURSOR c_ltl_ref_exists
    (p_ltl_reference     VARCHAR2
    ,p_ltl_hrv_fltt_type VARCHAR2
    ) 
  IS
    SELECT ltl_refno
    FROM   land_titles
    WHERE  ltl_reference = p_ltl_reference
    AND    ltl_hrv_fltt_type = p_ltl_hrv_fltt_type;
  CURSOR c_pro
    (p_propref VARCHAR2) 
  IS
    SELECT pro_refno 
    FROM   properties
    WHERE  pro_propref = p_propref;
  CURSOR c_aun (p_aun_code VARCHAR2) 
  IS
    SELECT aun_code 
      FROM admin_units
     WHERE aun_code = p_aun_code;
    
  CURSOR c_facr_code(p_llta_hrv_facr_code  varchar2) 
  IS
    SELECT frv_code
    FROM   first_ref_values
    WHERE  frv_frd_domain = 'ASSIGNMENT_CLOSE_RSN'
    AND    frv_code = p_llta_hrv_facr_code;
  --
  -- constants FOR error process
  --
  cb VARCHAR2(30);
  cd DATE;
  cp VARCHAR2(30) := 'VALIDATE';
  ct VARCHAR2(30) := 'DL_LTL_LAND_TITLE_ASSIGN';
  cs INTEGER;
  ce VARCHAR2(200);
  --
  -- other variables
  --
  l_answer            VARCHAR2(1);
  l_num_chk           VARCHAR2(3);
  l_errors            VARCHAR2(10);
  l_error_ind         VARCHAR2(10);
  l_ltl_reference     VARCHAR2(17);
  l_plan_type         VARCHAR2(10);
  l_exists            NUMBER(10);
  l_aun_exists        VARCHAR2(20);
  l_facr_exists       VARCHAR2(10);
  i                   INTEGER := 0;
  l_id                ROWID;
  BEGIN
    fsc_utils.proc_start('s_dl_ltl_land_title_assign.dataload_validate');
    fsc_utils.debug_message( 's_dl_ltl_land_title_assign.dataload_validate',3 );
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    --
    -- Get the answer to the 'Property Update Allowed' Question'
    --
    l_answer := s_dl_batches.get_answer(p_batch_id, 1);
    FOR p1 IN c1 
    LOOP
      BEGIN
        cs := p1.llta_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors := 'V';
        l_error_ind := 'N';
        --
        -- Get the plan type for the land title
        --
        l_ltl_reference := null;
        OPEN c_plan_type(p1.llta_lltl_ltt_code);
        FETCH c_plan_type into l_plan_type;
        CLOSE c_plan_type;
        IF l_plan_type = 'SP' 
        THEN
          l_ltl_reference := p1.llta_lltl_lot_number||'/'||TO_CHAR(p1.llta_lltl_plan_number);
        ELSIF l_plan_type = 'DP' 
        THEN
          IF p1.llta_section_number IS NULL 
          THEN
            l_ltl_reference := p1.llta_lltl_lot_number||'/'|| TO_CHAR(p1.llta_lltl_plan_number);
          ELSE
            l_ltl_reference := p1.llta_lltl_lot_number||'/'||p1.llta_section_number||'/'|| TO_CHAR(p1.llta_lltl_plan_number);
          END IF;
        END IF;
        --
        -- Check Land Type does not exist
        --
        OPEN c_ltl_ref_exists(l_ltl_reference, l_plan_type);
        FETCH  c_ltl_ref_exists INTO l_exists;
        IF c_ltl_ref_exists%NOTFOUND 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',459);
        END IF;
        CLOSE c_ltl_ref_exists;
        
        IF (p1.llta_aun_code IS NOT NULL AND p1.llta_pro_refno IS NOT NULL)
        THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',899);
        END IF;

        IF (p1.llta_aun_code IS NULL AND p1.llta_pro_refno IS NULL)
        THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',900);
        END IF;
        
        --
        -- Check Property Reference Exists
        --
        IF p1.llta_pro_refno IS NOT NULL
        THEN
           OPEN c_pro(p1.llta_pro_refno);
           FETCH  c_pro INTO l_exists;
           IF c_pro%NOTFOUND 
           THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',468);
           END IF;
           CLOSE c_pro;
        END IF;
        
        --
        -- Check Admin Unit Reference Exists
        --
        IF p1.llta_aun_code IS NOT NULL
        THEN
           OPEN c_aun(p1.llta_aun_code);
           FETCH  c_aun INTO l_aun_exists;
           IF c_aun%NOTFOUND 
           THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',802);
           END IF;
           CLOSE c_aun;
        END IF;
        --
        -- Check All Mandatory Fields have been supplied
        --
        -- Plan Number
        --
        IF p1.llta_lltl_plan_number IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',449);
        END IF;
        --
        -- Lot Number
        --
        IF p1.llta_lltl_lot_number IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',450);
        END IF;
        --
        -- Land Title Type
        --
        IF p1.llta_lltl_lot_number IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',451);
        END IF;
        --
        -- Assignment Start Date
        --
        IF p1.llta_start_date IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',452);
        END IF;
        --
        -- Check Assignment End Date (where supplied) is not earlier than Assignment Start Date
        --
        IF p1.llta_end_date IS NOT NULL 
        THEN
          IF p1.llta_end_date < p1.llta_start_date  
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',453);
          END IF;
        END IF;
        --
        -- Check Assignment Reason is supplied when Assignment End Date is
        --
        IF p1.llta_end_date IS NOT NULL 
        THEN
          IF p1.llta_hrv_facr_code IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',454);
          END IF;
        END IF;
        --
        -- Check end reason code exists where supplied.
        --
        IF p1.llta_hrv_facr_code IS NOT NULL 
        THEN 
          OPEN c_facr_code(p1.llta_hrv_facr_code);
          FETCH c_facr_code INTO l_facr_exists;
          IF c_facr_code%NOTFOUND 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',470);
          END IF;
          CLOSE c_facr_Code;
        END IF;
        --
        -- Now UPDATE the record count and error code
        --
        IF l_errors = 'F' 
        THEN
          l_error_ind := 'Y';
        ELSE
          l_error_ind := 'N';
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind); 
        set_record_status_flag(l_id,l_errors);
        --
        -- keep a count of the rows processed and commit after every 1000
        --
        i := i+1; 
        IF MOD(i,1000) = 0 
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
    COMMIT;
    fsc_utils.proc_END;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
  END dataload_validate;
  --
  -- **************************************************************
  --
  PROCEDURE dataload_delete 
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE    
    ) 
  IS
  CURSOR c1 
  IS
    SELECT ROWID   rec_rowid
    ,      LLTA_DL_SEQNO
    ,      LLTA_LLTL_PLAN_NUMBER
    ,      LLTA_LLTL_LOT_NUMBER
    ,      LLTA_LLTL_LTT_CODE
    ,      LLTA_START_DATE
    ,      LLTA_PRO_REFNO    --Need to get the pro_refno
    ,      LLTA_AUN_CODE
    ,      LLTA_END_DATE
    ,      LLTA_HRV_FACR_CODE
    ,      LLTA_COMMENTS
    ,      LLTA_CREATED_BY
    ,      LLTA_CREATED_DATE
    ,      LLTA_MODIFIED_BY
    ,      LLTA_MODIFIED_DATE
    ,      LLTA_SECTION_NUMBER
    FROM   dl_ltl_land_title_assign
    WHERE  llta_dlb_batch_id = p_batch_id
    AND    llta_dl_load_status = 'C';
  CURSOR c_plan_type
    (p_lltl_ltt_code VARCHAR2) 
  IS
    SELECT ltt_hrv_fltt_type 
    FROM   land_title_types
    WHERE  ltt_code = p_lltl_ltt_code;
  CURSOR c_ltl_refno
    (p_ltl_reference     VARCHAR2
    ,p_ltl_hrv_fltt_type VARCHAR2
    ) 
  IS
    SELECT ltl_refno
    FROM   land_titles
    WHERE  ltl_reference = p_ltl_reference
    AND    ltl_hrv_fltt_type = p_ltl_hrv_fltt_type;
  --
  -- Constants FOR process_summary
  --
  cb       VARCHAR2(30);
  cd       DATE;
  cp       VARCHAR2(30) := 'DELETE';
  ct       VARCHAR2(30) := 'DL_LTL_LAND_TITLE_ASSIGN';
  cs       INTEGER;
  ce       VARCHAR2(200);
  --
  -- Other variables
  --
  i                   INTEGER := 0;
  l_an_tab            VARCHAR2(1);
  l_ltl_reference     VARCHAR2(17);
  l_plan_type         VARCHAR2(10);
  l_ltl_ref           NUMBER(10);
  l_id                ROWID;
  BEGIN
    fsc_utils.proc_start('s_dl_ltl_land_title_assign.dataload_delete');
    fsc_utils.debug_message( 's_dl_ltl_land_title_assign.dataload_delete',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1 
    LOOP
      BEGIN
        cs   := p1.llta_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        OPEN c_plan_type(p1.llta_lltl_ltt_code);
        FETCH c_plan_type into l_plan_type;
        CLOSE c_plan_type;
        IF l_plan_type = 'SP' 
        THEN
          l_ltl_reference := p1.llta_lltl_lot_number||'/'||p1.llta_lltl_plan_number;
        ELSIF l_plan_type = 'DP' 
        THEN
          IF p1.llta_section_number IS NULL 
          THEN
            l_ltl_reference := p1.llta_lltl_lot_number||'/'||p1.llta_lltl_plan_number;
          ELSE
            l_ltl_reference := p1.llta_lltl_lot_number||'/'||p1.llta_section_number||'/'||p1.llta_lltl_plan_number;
          END IF;
        END IF;
        OPEN c_ltl_refno(l_ltl_reference,l_plan_type);
        FETCH c_ltl_refno INTO l_ltl_ref;
        CLOSE c_ltl_refno;
        DELETE 
        FROM   land_title_assignments
        WHERE  lta_ltl_refno = l_ltl_ref;
        --
        -- keep a count of the rows processed and commit after every 1000
        --
        i := i+1; 
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
    --
    --Section to analyse the table populated by this dataload
    --
    l_an_tab := s_dl_hem_utils.dl_comp_stats('LAND_TITLE_ASSIGN');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION                                                 
  WHEN OTHERS 
  THEN                                       
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');  
    RAISE;                                                                                                         
  END dataload_delete;
END s_dl_ltl_land_title_assign;
/
