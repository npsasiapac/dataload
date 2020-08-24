CREATE OR REPLACE PACKAGE BODY s_dl_ltl_land_title_releases
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
--      3.0          VS  23/04/2009  Fix for the performance issue. Bring
--                                   set_record_status_flag locally.
--
--      4.0          VS  04/02/2010  Defect Id 3261 Fix. ltl_refno not derived correctly.
--
--      4.1  6.13    MJK 11/11/2015  Reformatted for 6.13. No logic changes
--      4.2  6.14    AJ  16/03/2016  lltl_reference amended from (17) to (22)
--                                   and lltl_lot_number from (4) to (6) 
--
-- ***********************************************************************
--
--
  PROCEDURE set_record_status_flag
    (p_rowid  IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_ltl_land_title_releases
    SET    lltr_dl_load_status = p_status
    WHERE  rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_ltl_land_title_releases');
    RAISE;
  END set_record_status_flag;
  --
  -- ***********************************************************************
  --
  PROCEDURE dataload_create
    (p_batch_id         IN VARCHAR2
    ,p_date             IN DATE  
    )
  AS
  CURSOR c1 
  IS 
    SELECT ROWID   rec_rowid
    ,      lltr_dl_seqno
    ,      lltr_lltl_plan_number
    ,      lltr_lltl_lot_number
    ,      lltr_lltl_ltt_code
    ,      lltr_released_date
    ,      lltr_released_to
    ,      lltr_matter_number
    ,      lltr_returned_date
    ,      lltr_hrv_ftrr_code
    ,      lltr_created_by
    ,      lltr_created_date
    ,      lltr_modified_by
    ,      lltr_modified_date
    ,      lltr_comments
    ,      lltr_section_number
    FROM   dl_ltl_land_title_releases
    WHERE  lltr_dlb_batch_id = p_batch_id
    AND    lltr_dl_load_status = 'V';
  CURSOR c_plan_type
    (p_lltl_ltt_code varchar2) 
  IS
    SELECT ltt_hrv_fltt_type 
    FROM   land_title_types
    WHERE  ltt_code = p_lltl_ltt_code;
  CURSOR c_ltl_refno
    (p_lltl_ref          VARCHAR2
    ,p_ltl_hrv_fltt_type VARCHAR2
    ) 
  IS
    SELECT ltl_refno
    FROM   land_titles
    WHERE  ltl_reference = p_lltl_ref
    AND    ltl_hrv_fltt_type = p_ltl_hrv_fltt_type;
  --
  -- Constants for process_summary
  --
  cb       VARCHAR2(30);
  cd       DATE;
  cp       VARCHAR2(30) := 'CREATE';
  ct       VARCHAR2(30) := 'DL_LTL_LAND_TITLE_RELEASES';
  cs       INTEGER;
  ce       VARCHAR2(200);
  --
  -- Other variables
  --
  ai                  PLS_INTEGER:=100;
  l_plan_type         vARCHAR2(10);
  l_ltl_reference     VARCHAR2(22);
  l_ltl_refno         NUMBER(10);
  l_an_tab            VARCHAR2(1);
  i                   PLS_INTEGER:=0;
  l_id                ROWID;
  BEGIN
    execute immediate 'alter trigger LTR_BR_I disable';
    fsc_utils.proc_start('s_dl_ltl_land_title_releases.dataload_create');
    fsc_utils.debug_message( 's_dl_ltl_land_title_releases.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1 
    LOOP
      BEGIN
      cs := p1.lltr_dl_seqno;
      l_id := p1.rec_rowid;
      OPEN c_plan_type(p1.lltr_lltl_ltt_code);
      FETCH c_plan_type into l_plan_type;
      CLOSE c_plan_type;
      IF l_plan_type = 'SP' 
      THEN
        l_ltl_reference := p1.lltr_lltl_lot_number||'/'||p1.lltr_lltl_plan_number;
      ELSIF l_plan_type = 'DP' 
      THEN
        IF p1.lltr_section_number IS NULL 
        THEN
          l_ltl_reference := p1.lltr_lltl_lot_number||'/'||p1.lltr_lltl_plan_number;
        ELSE
          l_ltl_reference := p1.lltr_lltl_lot_number||'/'||p1.lltr_section_number||'/'||p1.lltr_lltl_plan_number;
        END IF;
      END IF;
      OPEN c_ltl_refno(l_ltl_reference, l_plan_type);
      FETCH c_ltl_refno into l_ltl_refno;
      CLOSE c_ltl_refno;
      SAVEPOINT SP1;
      INSERT INTO land_title_releases
      (ltr_ltl_refno
      ,ltr_released_date
      ,ltr_released_to
      ,ltr_matter_number
      ,ltr_returned_date
      ,ltr_hrv_ftrr_code
      ,ltr_created_by
      ,ltr_created_date
      ,ltr_modified_by
      ,ltr_modified_date
      ,ltr_comments
      )
      VALUES
      (l_ltl_refno
      ,p1.lltr_released_date
      ,p1.lltr_released_to
      ,p1.lltr_matter_number
      ,p1.lltr_returned_date
      ,p1.lltr_hrv_ftrr_code
      ,NVL(p1.lltr_created_by,'DATALOAD')
      ,NVL(p1.lltr_created_date,TRUNC(SYSDATE) )
      ,p1.lltr_modified_by
      ,p1.lltr_modified_date
      ,p1.lltr_comments 
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
      IF MOD(i,1000)=0 
      THEN 
        COMMIT;
        -- Do a regular analyze table based on 10 times as many records as last time
        IF i >= (ai*5)  -- changed from 10* to 5*
        THEN
          ai := i;
          l_an_tab:=s_dl_hem_utils.dl_comp_stats('LAND_TITLE_RELEASES');
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('LAND_TITLE_RELEASES');
    execute immediate 'alter trigger LTR_BR_I enable';
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
    (p_batch_id          IN VARCHAR2
    ,p_date              IN DATE
    )
  AS
  CURSOR c1 
  IS 
    SELECT ROWID   rec_rowid
    ,      lltr_dl_seqno
    ,      lltr_lltl_plan_number
    ,      lltr_lltl_lot_number
    ,      lltr_lltl_ltt_code
    ,      lltr_released_date
    ,      lltr_released_to
    ,      lltr_hrv_ftrr_code
    ,      lltr_matter_number
    ,      lltr_returned_date
    ,      lltr_comments
    ,      lltr_created_by
    ,      lltr_created_date
    ,      lltr_modified_by
    ,      lltr_modified_date
    ,      lltr_section_number
    FROM   dl_ltl_land_title_releases
    WHERE  lltr_dlb_batch_id = p_batch_id
    AND    lltr_dl_load_status in ('L','F','O');
  CURSOR c_plan_type(p_lltl_ltt_code VARCHAR2) 
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
    ,      land_title_releases
    WHERE  ltl_reference = p_ltl_reference
    AND    ltl_hrv_fltt_type = p_ltl_hrv_fltt_type
    AND    ltl_refno = ltr_ltl_refno;
  CURSOR c_ltl_rel_exists(p_rel_code VARCHAR2) 
  IS 
    SELECT frv_code                                    
    FROM   first_ref_values                                     
    WHERE  frv_code = p_rel_code           
    AND    frv_frd_domain = 'LANDTITLE_REL_RSN';
  --
  -- constants FOR error process
  --
  cb VARCHAR2(30);
  cd DATE;
  cp VARCHAR2(30) := 'VALIDATE';
  ct VARCHAR2(30) := 'DL_LTL_LAND_TITLE_RELEASES';
  cs INTEGER;
  ce VARCHAR2(200);
  --
  -- other variables
  --
  l_answer            VARCHAR2(1);
  l_errors            VARCHAR2(10);
  l_error_ind         VARCHAR2(10);
  l_ltl_reference     VARCHAR2(22);
  l_plan_type         VARCHAR2(10);
  l_exists            VARCHAR2(20);
  i                   INTEGER := 0;
  l_id     ROWID;
  BEGIN
    fsc_utils.proc_start('s_dl_ltl_land_title_releases.dataload_validate');
    fsc_utils.debug_message( 's_dl_ltl_land_title_releases.dataload_validate',3 );
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    --
    -- Get the answer to the 'Property Update Allowed' Question'
    --
    l_answer := s_dl_batches.get_answer(p_batch_id, 1);
    FOR p1 IN c1 
    LOOP
      cs := p1.lltr_dl_seqno;
      l_id := p1.rec_rowid;
      l_errors := 'V';
      l_error_ind := 'N';
      --
      -- Get the plan type for the land title
      --
      OPEN c_plan_type(p1.lltr_lltl_ltt_code);
      FETCH c_plan_type into l_plan_type;
      CLOSE c_plan_type;
      IF l_plan_type = 'SP' 
      THEN
        l_ltl_reference := p1.lltr_lltl_lot_number||'/'||p1.lltr_lltl_plan_number;
      ELSIF l_plan_type = 'DP' 
      THEN
        IF p1.lltr_section_number IS NULL
        THEN
          l_ltl_reference := p1.lltr_lltl_lot_number||'/'||p1.lltr_lltl_plan_number;
        ELSE
          l_ltl_reference := p1.lltr_lltl_lot_number||'/'||p1.lltr_section_number||'/'||p1.lltr_lltl_plan_number;
        END IF;
      END IF;
      --
      -- Check Land Type does not exist
      --
      OPEN c_ltl_ref_exists(l_ltl_reference, l_plan_type);
      FETCH  c_ltl_ref_exists INTO l_exists;
      IF c_ltl_ref_exists%FOUND
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',459);
      END IF;
      CLOSE c_ltl_ref_exists;
      --
      -- Check All Mandatory Fields have been supplied
      --
      -- Plan Number
      --
      IF p1.lltr_lltl_plan_number is null
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',438);
      END IF;
      --
      -- Lot Number
      --
      IF p1.lltr_lltl_lot_number is null
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',439);
      END IF;
      --
      -- Land Title Type
      --
      IF p1.lltr_lltl_ltt_code is null
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',440);
      END IF;
      --
      -- Released Date
      --
      IF p1.lltr_released_date is null
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',441);
      END IF;
      --
      -- Released To
      --
      IF p1.lltr_released_to is null
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',442);
      END IF;
      --
      -- Released Reason
      --
      IF p1.lltr_hrv_ftrr_code is null
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',443);
      END IF;
      --
      -- Check Release Date is not in Future
      --
      IF p1.lltr_released_date > SYSDATE
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',444);
      END IF;
      --
      -- Check Returned Date (where supplied) is not earlier than Release Date
      --
      IF p1.lltr_returned_date IS NOT NULL
      THEN
        IF (p1.lltr_returned_date < p1.lltr_released_date)
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',445);
        END IF;
      END IF;
      --
      -- Check Returned Date (where supplied) is not in the future
      --
      IF p1.lltr_returned_date IS NOT NULL
      THEN
        IF p1.lltr_returned_date > SYSDATE 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',446);
        END IF;
      END IF;
      --
      -- Check Modified Date is supplied when modified by is supplied
      --
      IF p1.lltr_modified_by IS NOT NULL
      THEN
        IF p1.lltr_modified_date IS NULL
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',447);
        END IF;
      END IF;
      --
      -- Check Modified By is supplied when Modified Date is supplied
      --
      IF p1.lltr_modified_date IS NOT NULL
      THEN
        IF p1.lltr_modified_by IS NULL
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',448);
        END IF;
      END IF;
      --
      -- Check Release Reason Code exists
      --
      OPEN c_ltl_rel_exists(p1.lltr_hrv_ftrr_code);
      FETCH c_ltl_rel_exists into l_exists;
      IF c_ltl_rel_exists%NOTFOUND
      THEN  
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',465);
      END IF;
      CLOSE c_ltl_rel_exists;
      --
      -- Now UPDATE the record count and error code
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
      IF MOD(i,1000)=0 
      THEN 
        COMMIT; 
      END IF;
      s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
      set_record_status_flag(l_id,l_errors);
    END LOOP;
    COMMIT;
    fsc_utils.proc_END;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
    set_record_status_flag(l_id,'O');
  END dataload_validate;
  --
  -- ***********************************************************************
  --
  PROCEDURE dataload_delete 
    (p_batch_id        IN VARCHAR2
    ,p_date            IN DATE    
    ) 
  IS
  CURSOR c1 
  IS
    SELECT ROWID   rec_rowid
    ,      lltr_dl_seqno
    ,      lltr_lltl_plan_number
    ,      lltr_lltl_lot_number
    ,      lltr_lltl_ltt_code
    ,      lltr_released_date
    ,      lltr_released_to
    ,      lltr_hrv_ftrr_code
    ,      lltr_matter_number
    ,      lltr_returned_date
    ,      lltr_comments
    ,      lltr_created_by
    ,      lltr_created_date
    ,      lltr_modified_by
    ,      lltr_modified_date
    ,      lltr_section_number
    FROM   dl_ltl_land_title_releases
    WHERE  lltr_dlb_batch_id = p_batch_id
    AND    lltr_dl_load_status = 'C';
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
  --Other Variables
  --
  i INTEGER := 0;
  l_an_tab  VARCHAR2(1);
  l_ltl_reference     VARCHAR2(22);
  l_plan_type         VARCHAR2(10);
  l_ltl_ref           NUMBER(10);
  --
  -- Constants FOR process_summary
  --
  cb       VARCHAR2(30);
  cd       DATE;
  cp       VARCHAR2(30) := 'DELETE';
  ct       VARCHAR2(30) := 'DL_LTL_LAND_TITLE_RELEASES';
  cs       INTEGER;
  ce       VARCHAR2(200);
  l_id     ROWID;
  BEGIN
    fsc_utils.proc_start('s_dl_ltl_land_title_releases.dataload_delete');
    fsc_utils.debug_message( 's_dl_ltl_land_title_releases.dataload_delete',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1 
    LOOP
      BEGIN
        cs   := p1.lltr_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        OPEN c_plan_type(p1.lltr_lltl_ltt_code);
        FETCH c_plan_type into l_plan_type;
        CLOSE c_plan_type;
        IF l_plan_type = 'SP' 
        THEN
          l_ltl_reference := p1.lltr_lltl_lot_number||'/'||p1.lltr_lltl_plan_number;
        ELSIF l_plan_type = 'DP' 
        THEN
          IF p1.lltr_section_number IS NULL 
          THEN
            l_ltl_reference := p1.lltr_lltl_lot_number||'/'||p1.lltr_lltl_plan_number;
          ELSE
            l_ltl_reference := p1.lltr_lltl_lot_number||'/'||p1.lltr_section_number||'/'||p1.lltr_lltl_plan_number;
          END IF;
        END IF;
        OPEN c_ltl_refno(l_ltl_reference, l_plan_type);
        FETCH c_ltl_refno INTO l_ltl_ref;
        CLOSE c_ltl_refno;
        DELETE 
        FROM   land_title_releases
        WHERE  ltr_ltl_refno = l_ltl_ref;
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
    --
    --Section to analyse the table populated by this dataload
    --
    l_an_tab := s_dl_hem_utils.dl_comp_stats('LAND_TITLE_RELEASES');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION                                                 
  WHEN OTHERS 
  THEN                                       
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');  
    RAISE;                                                                                                           
  END dataload_delete;
END s_dl_ltl_land_title_releases;
/
