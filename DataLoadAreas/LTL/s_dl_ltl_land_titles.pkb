create or replace PACKAGE BODY s_dl_ltl_land_titles
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER  WHO  WHEN        WHY
--
--      1.0          IR  29/10/2007  Initial Creation
--
--      2.0          VS  20/04/2009  Change in the way the ltl_reference
--                                   is constructed. Section Number is 
--                                   not always supplied for DP type
--
--      3.0          VS  23/04/2009  Fix for the performance issue. Bring
--                                   set_record_status_flag locally.
--
--      4.0          VS  12/10/2009  Change Validation comment for 458
--                                   From Land Type Ref to Land Title Ref
--
--      5.0          VS  04/02/2010  Defect Id 3261. ltl_refno nor derived
--                                   correctly. Tidied code up and removed call
--                                   to s_dl_ltl_utils for Y/N validations
--
--      6.0   6.13   MJK 11/11/2015  Changed to allow party or organisation
--                                   to be specified.
--      6.1   6.13   AJ  29/01/2016  ltl_reusable_refno added to create as LTL_BR_I
--                                   is disabled on create needs adding at v6.13
--      6.2   6.14   AJ  16/03/2016  1) lltl_reference amended from (17) to (22)
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
    UPDATE dl_ltl_land_titles
    SET    lltl_dl_load_status = p_status
    WHERE  rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_ltl_land_titles');
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
    SELECT ROWID rec_rowid
    ,      lltl_dl_seqno
    ,      lltl_plan_number
    ,      lltl_lot_number
    ,      lltl_ltt_code
    ,      lltl_par_per_alt_ref        
    ,      lltl_par_org_short_name     
    ,      lltl_par_org_frv_oty_code   
    ,      lltl_area_measurement
    ,      lltl_start_date
    ,      lltl_date_type_ind
    ,      lltl_affect_ease_ind
    ,      lltl_appurt_ease_ind
    ,      lltl_residual_ind
    ,      lltl_volume_number
    ,      lltl_created_by
    ,      lltl_created_date
    ,      lltl_section_number
    ,      lltl_consolidation_number
    ,      lltl_folio_number
    ,      lltl_book_number
    ,      lltl_book_sequence_number
    ,      lltl_closed_date
    ,      lltl_hrv_fltc_code
    ,      lltl_closed_by
    ,      lltl_num_properties
    ,      lltl_num_properties_owned
    ,      lltl_lltl_plan_number
    ,      lltl_lltl_lot_number
    ,      lltl_lltl_ltt_code
    ,      lltl_comments
    ,      lltl_modified_by
    ,      lltl_modified_date
    FROM   dl_ltl_land_titles
    WHERE  lltl_dlb_batch_id = p_batch_id
    AND    lltl_dl_load_status = 'V';
-- *************************************
  CURSOR c_plan_type
    (p_lltl_ltt_code VARCHAR2) 
  IS
    SELECT ltt_hrv_fltt_type 
    FROM   land_title_types
    WHERE ltt_code = p_lltl_ltt_code;
-- *************************************
  CURSOR c_par_ref
    (p_par_per_alt_ref      VARCHAR2
    ,p_par_org_short_name   VARCHAR2
    ,p_par_org_frv_oty_code VARCHAR2
    ) 
  IS
    SELECT par_refno
    FROM   parties
    WHERE  p_par_per_alt_ref IS NOT NULL
    AND    par_per_alt_ref = p_par_per_alt_ref
    AND    par_org_frv_oty_code = NVL(p_par_org_frv_oty_code,par_org_frv_oty_code)
    UNION
    SELECT par_refno
    FROM   parties
    WHERE  p_par_org_short_name IS NOT NULL
    AND    p_par_org_frv_oty_code IS NOT NULL
    AND    par_org_short_name   = p_par_org_short_name
    AND    par_org_frv_oty_code = p_par_org_frv_oty_code;
-- *************************************
  CURSOR c_prev_ltt_no                                  
    (p_lltl_lltl_plan_number NUMBER
    ,p_lltl_lltl_lot_number  VARCHAR2
    ,p_lltl_lltl_ltt_code    VARCHAR2
    ) 
  IS
    SELECT ltl_refno
    FROM   land_titles
    WHERE  ltl_plan_number = p_lltl_lltl_plan_number
    AND    ltl_lot_number = p_lltl_lltl_lot_number
    AND    ltl_hrv_fltt_type = p_lltl_lltl_ltt_code;
-- *************************************
  --
  --
  --
  -- Constants for process_summary
  --
  cb       VARCHAR2(30);
  cd       DATE;
  cp       VARCHAR2(30) := 'CREATE';
  ct       VARCHAR2(30) := 'DL_LTL_LAND_TITLES';
  cs       INTEGER;
  ce       VARCHAR2(200);
  --
  -- Other variables
  --
  i                   PLS_INTEGER :=0;
  ai                  PLS_INTEGER :=100;
  l_plan_type         vARCHAR2(10);
  l_ltl_reference     VARCHAR2(22);
  l_an_tab            VARCHAR2(1);
  l_par_ref           NUMBER(8);
  l_prev_ltt_no       NUMBER(10);
  l_reusable_refno    NUMBER(20);
  l_id                ROWID;
  BEGIN
    execute immediate 'alter trigger LTL_BR_I disable';  
    fsc_utils.proc_start('s_dl_ltl_land_titles.dataload_create');  
    fsc_utils.debug_message( 's_dl_ltl_land_titles.dataload_create',3);  
    cb := p_batch_id;  
    cd := p_date;  
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');  
    FOR p1 IN c1   
    LOOP  
      BEGIN  
          cs := p1.lltl_dl_seqno;  
          l_id := p1.rec_rowid;  
          l_prev_ltt_no := null;
          l_reusable_refno := null;
          SAVEPOINT SP1;
          --		  
          OPEN c_plan_type(p1.lltl_ltt_code);  
          FETCH c_plan_type INTO l_plan_type;  
          CLOSE c_plan_type;
          --		  
          IF l_plan_type = 'SP'   
          THEN  
            l_ltl_reference := p1.lltl_lot_number||'/'||p1.lltl_plan_number;  
          ELSIF l_plan_type = 'DP'   
          THEN  
            IF p1.lltl_section_number IS NULL  
            THEN  
              l_ltl_reference := p1.lltl_lot_number||'/'||p1.lltl_plan_number;  
            ELSE  
              l_ltl_reference := p1.lltl_lot_number||'/'||p1.lltl_section_number||'/'||p1.lltl_plan_number;  
            END IF;  
          END IF;
          --		  
          OPEN c_par_ref(p1.lltl_par_per_alt_ref,p1.lltl_par_org_short_name,p1.lltl_par_org_frv_oty_code);  
          FETCH c_par_ref INTO l_par_ref;  
          CLOSE c_par_ref;
          --		  
          IF (p1.lltl_lltl_plan_number IS NOT NULL)   
          THEN   
            OPEN c_prev_ltt_no(p1.lltl_lltl_plan_number,p1.lltl_lltl_lot_number,p1.lltl_lltl_ltt_code);  
            FETCH c_prev_ltt_no INTO l_prev_ltt_no;  
            CLOSE c_prev_ltt_no;  
          END IF;
          --
          SELECT reusable_refno_seq.nextval
          INTO   l_reusable_refno
          FROM   dual;
          --		  
          INSERT INTO land_titles  
          (ltl_refno  
          ,ltl_reference                         
          ,ltl_plan_number                         
          ,ltl_lot_number                         
          ,ltl_hrv_fltt_type                         
          ,ltl_ltt_code                         
          ,ltl_par_refno                             
          ,ltl_area_measurement                         
          ,ltl_start_date                         
          ,ltl_date_type_ind                         
          ,ltl_affect_ease_ind                         
          ,ltl_appurt_ease_ind                         
          ,ltl_residual_ind                         
          ,ltl_volume_number                         
          ,ltl_created_by                         
          ,ltl_created_date                         
          ,ltl_section_number                         
          ,ltl_consolidation_number                         
          ,ltl_folio_number                         
          ,ltl_book_number                         
          ,ltl_book_sequence_number                         
          ,ltl_hrv_fltc_code                         
          ,ltl_closed_date                         
          ,ltl_closed_by                         
          ,ltl_num_properties                         
          ,ltl_num_properties_owned                         
          ,ltl_ltl_refno                         
          ,ltl_comments                         
          ,ltl_modified_by                         
          ,ltl_modified_date
          ,ltl_reusable_refno		  
          )                         
          VALUES   
          (ltl_refno_seq.nextval  
          ,l_ltl_reference                         
          ,p1.lltl_plan_number                         
          ,p1.lltl_lot_number                         
          ,l_plan_type                         
          ,p1.lltl_ltt_code                         
          ,l_par_ref                         
          ,p1.lltl_area_measurement                         
          ,p1.lltl_start_date                         
          ,p1.lltl_date_type_ind                         
          ,p1.lltl_affect_ease_ind                         
          ,p1.lltl_appurt_ease_ind                         
          ,p1.lltl_residual_ind                         
          ,p1.lltl_volume_number                         
          ,p1.lltl_created_by                         
          ,p1.lltl_created_date                         
          ,p1.lltl_section_number                         
          ,p1.lltl_consolidation_number                         
          ,p1.lltl_folio_number                         
          ,p1.lltl_book_number                         
          ,p1.lltl_book_sequence_number                         
          ,p1.lltl_hrv_fltc_code                         
          ,p1.lltl_closed_date                         
          ,p1.lltl_closed_by                         
          ,p1.lltl_num_properties                         
          ,p1.lltl_num_properties_owned                         
          ,l_prev_ltt_no                         
          ,p1.lltl_comments                         
          ,p1.lltl_modified_by                         
          ,p1.lltl_modified_date
          ,l_reusable_refno		  
          );                         
          -- Update Record Status and Process Count  
          --  
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');  
          set_record_status_flag(l_id,'C');  
          --  
          -- keep a count of the rows processed and commit after every 1000  
          --  
          i := i+1;   
          IF MOD(i,1000) = 0   
          THEN   
            COMMIT;  
            --  
            -- Do a regular analyze table based on 10 times as many records as last time  
            --  
            IF i >= (ai*5)  -- changed from 10* to 5*  
            THEN   
              ai := i;  
              l_an_tab:=s_dl_hem_utils.dl_comp_stats('LAND_TITLES');  
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
      END LOOP;  
      COMMIT;  
      --  
      -- Section to anayze the table(s) populated by this dataload  
      --  
      l_an_tab:=s_dl_hem_utils.dl_comp_stats('LAND_TITLES');  
      execute immediate 'alter trigger LTL_BR_I enable';  
      fsc_utils.proc_end;  
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
    (p_batch_id     IN VARCHAR2
    ,p_date         IN DATE
    )
  AS
  CURSOR c1 
  IS
    SELECT ROWID rec_rowid
    ,      lltl_dl_seqno
    ,      lltl_plan_number
    ,      lltl_lot_number
    ,      lltl_ltt_code
    ,      lltl_par_per_alt_ref        
    ,      lltl_par_org_short_name     
    ,      lltl_par_org_frv_oty_code   
    ,      lltl_area_measurement
    ,      lltl_start_date
    ,      lltl_date_type_ind
    ,      lltl_affect_ease_ind
    ,      lltl_appurt_ease_ind
    ,      lltl_residual_ind
    ,      lltl_volume_number
    ,      lltl_created_by
    ,      lltl_created_date
    ,      lltl_section_number
    ,      lltl_consolidation_number
    ,      lltl_folio_number
    ,      lltl_book_number
    ,      lltl_book_sequence_number
    ,      lltl_closed_date
    ,      lltl_hrv_fltc_code
    ,      lltl_closed_by
    ,      lltl_num_properties
    ,      lltl_num_properties_owned
    ,      lltl_lltl_plan_number
    ,      lltl_lltl_lot_number
    ,      lltl_lltl_ltt_code
    ,      lltl_comments
    ,      lltl_modified_by
    ,      lltl_modified_date
    FROM   dl_ltl_land_titles
    WHERE  lltl_dlb_batch_id = p_batch_id
    AND    lltl_dl_load_status IN ('L','F','O');
  CURSOR c_ltype(p_lltl_ltt_code VARCHAR2) 
  IS
    SELECT NULL 
    FROM   land_title_types
    WHERE  ltt_code        = p_lltl_ltt_code
    AND    ltt_current_ind = 'Y';
  CURSOR c_close(p_lltl_hrv_fltc_code VARCHAR2) 
  IS
    SELECT NULL 
    FROM   first_ref_values
    WHERE  frv_code = p_lltl_hrv_fltc_code
    AND   frv_frd_domain = 'LANDTITLE_CLOSE_RSN';
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
    SELECT ltl_reference
    FROM   land_titles
    WHERE  ltl_reference = p_ltl_reference
    AND    ltl_hrv_fltt_type = p_ltl_hrv_fltt_type;
  CURSOR c_par_ref_exists
    (p_par_per_alt_ref      VARCHAR2
    ,p_par_org_short_name   VARCHAR2
    ,p_par_org_frv_oty_code VARCHAR2
    ) 
  IS
    SELECT par_refno
    FROM   parties
    WHERE  p_par_per_alt_ref IS NOT NULL
    AND    par_per_alt_ref = p_par_per_alt_ref
    AND    par_org_frv_oty_code = NVL(p_par_org_frv_oty_code,par_org_frv_oty_code)
    UNION
    SELECT par_refno
    FROM   parties
    WHERE  p_par_org_short_name IS NOT NULL
    AND    p_par_org_frv_oty_code IS NOT NULL
    AND    par_org_short_name   = p_par_org_short_name
    AND    par_org_frv_oty_code = p_par_org_frv_oty_code;
  CURSOR c_prev_ltt_no
    (p_lltl_lltl_plan_number NUMBER
    ,p_lltl_lltl_lot_number  VARCHAR2
    ,p_lltl_lltl_ltt_code    VARCHAR2
    ) 
  IS
    SELECT ltl_refno
    FROM   land_titles
    WHERE  ltl_plan_number = p_lltl_lltl_plan_number
    AND    ltl_lot_number = p_lltl_lltl_lot_number
    AND    ltl_hrv_fltt_type = p_lltl_lltl_ltt_code;
  --
  -- constants FOR error process
  --
  cb    VARCHAR2(30);
  cd    DATE;
  cp    VARCHAR2(30) := 'VALIDATE';
  ct    VARCHAR2(30) := 'DL_LTL_LAND_TITLES';
  cs    INTEGER;
  ce    VARCHAR2(200);
  --
  -- other variables
  --
  l_num_chk           VARCHAR2(3);
  l_dummy             VARCHAR2(10);
  l_errors            VARCHAR2(10);
  l_error_ind         VARCHAR2(10);
  l_plan_type         vARCHAR2(10);
  l_ltl_reference     VARCHAR2(22);
  l_exists            VARCHAR2(20);
  i                   INTEGER := 0;
  l_id                ROWID;
  BEGIN
    fsc_utils.proc_start('s_dl_ltl_land_titles.dataload_validate');
    fsc_utils.debug_message( 's_dl_ltl_land_titles.dataload_validate',3 );
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1 
    LOOP
      BEGIN
        cs := p1.lltl_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors := 'V';
        l_error_ind := 'N';
        --
        -- Get the plan type for the land title
        --
        OPEN c_plan_type(p1.lltl_ltt_code);
        FETCH c_plan_type INTO l_plan_type;
        CLOSE c_plan_type;
        IF l_plan_type = 'SP' 
        THEN
          l_ltl_reference := p1.lltl_lot_number||'/'||p1.lltl_plan_number;
        ELSIF l_plan_type = 'DP' 
        THEN
          IF p1.lltl_section_number IS NULL
          THEN
            l_ltl_reference := p1.lltl_lot_number||'/'||p1.lltl_plan_number;
          ELSE
            l_ltl_reference := p1.lltl_lot_number||'/'||p1.lltl_section_number||'/'||p1.lltl_plan_number;
          END IF; 
        END IF;
        --
        -- Check Land Title Reference does not exist
        --
        OPEN c_ltl_ref_exists(l_ltl_reference,l_plan_type);
        FETCH c_ltl_ref_exists INTO l_exists;
        IF (c_ltl_ref_exists%FOUND) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',458);
        END IF;
        CLOSE c_ltl_ref_exists;
        --
        -- Check Party exists
        --
        OPEN c_par_ref_exists(p1.lltl_par_per_alt_ref,p1.lltl_par_org_short_name,p1.lltl_par_org_frv_oty_code);  
        FETCH c_par_ref_exists INTO l_exists;  
        IF c_par_ref_exists%NOTFOUND  
        THEN  
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',461); -- 'Land Title Party Does Not Exist on Parties Table' 
        ELSE
          --
          -- perform a further fetch.  If it returns a row then supplied data matches more than one party.  
          --
          FETCH c_par_ref_exists INTO l_exists;  
          IF c_par_ref_exists%FOUND  
          THEN  
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',471);  -- 'Supplied data does not identify a unique Party'
          END if;  
        END IF;  
        CLOSE c_par_ref_exists;
        --
        -- Check Reference Data
        --
        -- Check Land Types
        --
        OPEN c_ltype(p1.lltl_ltt_code);
        FETCH c_ltype INTO l_dummy;
        IF c_ltype%NOTFOUND 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',455);
        END IF;
        CLOSE c_ltype;
        --
        -- Check Closed reason code where supplied
        --
        IF (p1.lltl_closed_date IS NOT NULL) 
        THEN
          OPEN c_close(p1.lltl_hrv_fltc_code);
          FETCH c_close INTO l_dummy;
          IF c_close%NOTFOUND 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',456);
          END IF;
          CLOSE c_close;
        END IF;
        --
        -- Check No of Properties and Properties Owned are populated where plan type is SP
        --
        IF (l_plan_type = 'SP') 
        THEN
          IF p1.lltl_num_properties IS NULL  
          OR  p1.lltl_num_properties_owned IS NULL   
          THEN  
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',430);  
          END IF;  
          --  
          --   Check Section NUMBER IS NULL where plan type is SP   
          --  
          IF p1.lltl_section_number IS NOT NULL  
          THEN  
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',457);  
          END IF;  
        END IF;  
        --  
        -- Check all the YN columns  
        --  
        -- Affecting Easement Indicator  
        --  
        IF p1.lltl_affect_ease_ind NOT IN ('Y','N')  
        THEN  
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',417);   
        END IF;  
        --  
        -- Appurtenant Easement Indicator  
        --  
        IF p1.lltl_appurt_ease_ind NOT IN ('Y','N')   
        THEN  
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',418);  
        END IF;  
        --  
        -- Residual Indicator  
        --  
        IF p1.lltl_residual_ind NOT IN ('Y','N')   
        THEN  
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',419);  
        END IF;  
        --  
        -- Check All Mandatory Fields have been supplied  
        --  
        -- Plan Number  
        --  
        IF p1.lltl_plan_number IS NULL    
        THEN   
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',420);   
        END IF;   
        --   
        -- Lot Number   
        --   
        IF p1.lltl_lot_number IS NULL   
        THEN   
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',421);   
        END IF;   
        --   
        -- Land Title Type   
        --   
        IF p1.lltl_lot_number IS NULL   
        THEN   
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',422);   
        END IF;   
        --   
        -- Area Measurement   
        --   
        IF p1.lltl_area_measurement IS NULL   
        THEN   
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',423);   
        END IF;   
        --   
        -- Start Date   
        --   
        IF p1.lltl_start_date IS NULL    
        THEN   
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',424);   
        END IF;   
        --   
        -- Check Start Date is not in Future   
        --   
        IF TRUNC(p1.lltl_start_date) > TRUNC(SYSDATE)    
        THEN   
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',425);   
        END IF;   
        --   
        -- Start Date Type is 'A' or 'R'   
        --   
        IF p1.lltl_date_type_ind not in ('A','R','U')    
        THEN   
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',426);   
        END IF;   
        --   
        -- Check only one of Consolidation, Volume or Book Number are Populated   
        --   
        l_num_chk :=NULL;   
        --   
        -- Set l_num_chk value based on the information supplied   
        --   
        IF p1.lltl_volume_number IS NOT NULL   
        THEN    
          l_num_chk := l_num_chk||'V';   
        END IF;   
        IF p1.lltl_consolidation_number IS NOT NULL   
        THEN    
          l_num_chk := l_num_chk||'C';   
        END IF;   
        IF p1.lltl_book_number IS NOT NULL   
        THEN    
          l_num_chk := l_num_chk||'B';   
        END IF;   
        --   
        -- So l_num_chk will either be null, V, C or B   
        --   
        IF l_num_chk IS NOT NULL   
        AND l_num_chk NOT IN ('V','C','B')    
        THEN   
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',427);   
        END IF;   
        --   
        -- Check Book Number Sequence supplied when Book Number is   
        --   
        IF p1.lltl_book_number IS NOT NULL   
        AND p1.lltl_book_sequence_number IS NULL    
        THEN   
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',428);   
        END IF;   
        --   
        -- Check Book Number Sequence is not supplied when Book Number is not   
        --   
        IF p1.lltl_book_sequence_number IS NOT NULL   
        AND p1.lltl_book_number IS NULL    
        THEN   
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',429);   
        END IF;   
        --   
        -- Check Section Number not supplied when type is SP   
        --   
        IF p1.lltl_ltt_code = 'SP'   
        THEN   
          IF p1.lltl_section_number IS NOT NULL   
          THEN   
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',431);   
          END IF;   
        END IF;   
        --   
        -- Check Properties owned is not greater than NUMBER of properties   
        --   
        IF p1.lltl_num_properties IS NOT NULL   
        AND p1.lltl_num_properties_owned IS NOT NULL    
        THEN   
          IF p1.lltl_num_properties_owned > p1.lltl_num_properties    
          THEN   
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',432);   
          END IF;   
        END IF;   
        --   
        -- Check CLosed Reason and Closed By are supplied when Closed Date is   
        --   
        IF p1.lltl_closed_date IS NOT NULL    
        THEN   
          IF p1.lltl_hrv_fltc_code IS NULL   
          OR p1.lltl_closed_by IS NULL   
          THEN   
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',433);   
          END IF;   
        END IF;   
        --   
        -- Check Closed Date and Closed By are supplied when Closed Reason is   
        --   
        IF p1.lltl_hrv_fltc_code IS NOT NULL    
        THEN   
          IF p1.lltl_closed_date IS NULL   
          OR p1.lltl_closed_by IS NULL    
          THEN   
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',462);   
          END IF;   
        END IF;   
        --   
        -- Check Closed Date and Closed Reason are supplied when Closed Date is   
        --   
        IF p1.lltl_closed_by IS NOT NULL   
        THEN   
          IF p1.lltl_closed_date IS NULL   
          OR p1.lltl_hrv_fltc_code IS NULL    
          THEN   
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',463);   
          END IF;   
        END IF;   
        --   
        -- Check CLosed Date (where supplied) is not earlier than Land Title Start Date   
        --   
        IF p1.lltl_closed_date IS NOT NULL    
        THEN   
          IF p1.lltl_closed_date < p1.lltl_start_date    
          THEN   
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',434);   
          END IF;   
        END IF;   
        --   
        -- Check Previous Land Title Lot and Previous Type are supplied when Previous Plan Number is   
        --   
        IF p1.lltl_lltl_plan_number IS NOT NULL    
        THEN   
          IF p1.lltl_lltl_lot_number IS NULL   
          OR p1.lltl_lltl_ltt_code IS NULL   
          THEN   
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',435);   
          END IF;   
        END IF;   
        --   
        -- Check Modified Date is supplied when modified by is supplied   
        --   
        IF p1.lltl_modified_by IS NOT NULL    
        THEN   
          IF p1.lltl_modified_date IS NULL    
          THEN   
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',436);   
          END IF;   
        END IF;   
        --   
        -- Check Modified By is supplied when Modified Date is supplied   
        --   
        IF p1.lltl_modified_date IS NOT NULL    
        THEN   
          IF p1.lltl_modified_by IS NULL   
          THEN   
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',437);   
          END IF;   
        END IF;   
        --   
        -- Check Folio Number supplied when Volume Number Supplied   
        --   
        IF p1.lltl_folio_number IS NULL   
        AND p1.lltl_volume_number IS NOT NULL   
        THEN   
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',466);   
        END IF;   
        --   
        -- Check Volume Number supplied when Folio Number Supplied   
        --   
        IF p1.lltl_volume_number IS NULL   
        AND p1.lltl_folio_number IS NOT NULL   
        THEN   
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',467);   
        END IF;   
        --   
        -- Check previous Land Title reference exists where supplied   
        --   
        IF p1.lltl_lltl_plan_number IS NOT NULL    
        THEN    
          OPEN c_prev_ltt_no(p1.lltl_lltl_plan_number,p1.lltl_lltl_lot_number,p1.lltl_lltl_ltt_code);   
          FETCH c_prev_ltt_no INTO l_dummy;   
          IF c_prev_ltt_no%NOTFOUND    
          THEN   
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',469);   
          END IF;   
          CLOSE c_prev_ltt_no;   
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
      END;
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
    SELECT ROWID rec_rowid
    ,      lltl_dlb_batch_id
    ,      lltl_dl_seqno
    ,      lltl_ltt_code
    ,      lltl_lot_number
    ,      lltl_plan_number
    ,      lltl_section_number
    FROM   dl_ltl_land_titles
    WHERE  lltl_dlb_batch_id   = p_batch_id
    AND    lltl_dl_load_status = 'C';
  CURSOR c_plan_type
    (p_lltl_ltt_code VARCHAR2) 
  IS 
    SELECT ltt_hrv_fltt_type                        
    FROM   land_title_types                           
    WHERE  ltt_code = p_lltl_ltt_code;              
  --
  -- Constants FOR process_summary
  --
  cb       VARCHAR2(30);
  cd       DATE;
  cp       VARCHAR2(30) := 'DELETE';
  ct       VARCHAR2(30) := 'DL_LTL_LAND_TITLES';
  cs       INTEGER;
  ce       VARCHAR2(200);
  --
  --Other Variables
  --
  i INTEGER := 0;
  l_plan_type         VARCHAR2(10);
  l_ltl_reference     VARCHAR2(22);
  l_an_tab            VARCHAR2(1);
  l_id     ROWID;
  BEGIN
    fsc_utils.proc_start('s_dl_ltl_land_titles.dataload_delete');       
    fsc_utils.debug_message( 's_dl_ltl_land_titles.dataload_delete',3); 
    cb := p_batch_id;                                                   
    cd := p_date;         
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');             
    FOR p1 in c1 
    LOOP
      BEGIN
        cs := p1.lltl_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        OPEN c_plan_type(p1.lltl_ltt_code);
        FETCH c_plan_type INTO l_plan_type;
        CLOSE c_plan_type;
        IF l_plan_type = 'SP' 
        THEN
          l_ltl_reference := p1.lltl_lot_number||'/'||p1.lltl_plan_number;
        ELSIF l_plan_type = 'DP' 
        THEN
          IF p1.lltl_section_number IS NULL
          THEN
            l_ltl_reference := p1.lltl_lot_number||'/'||p1.lltl_plan_number;
          ELSE
            l_ltl_reference := p1.lltl_lot_number||'/'||p1.lltl_section_number||'/'||p1.lltl_plan_number;
          END IF;
        END IF;
        DELETE 
        FROM   land_titles
        WHERE  ltl_reference = l_ltl_reference
        AND    ltl_hrv_fltt_type = l_plan_type;
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
    l_an_tab := s_dl_hem_utils.dl_comp_stats('LAND_TITLES');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION                                                 
  WHEN OTHERS 
  THEN                                       
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');  
    RAISE;                                                                                                           
  END dataload_delete;
END s_dl_ltl_land_titles;
/
