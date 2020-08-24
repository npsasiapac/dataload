CREATE OR REPLACE PACKAGE BODY s_dl_hem_void_events
AS
-- **************************************************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      1.0  6.13.0    MJK  19-NOV-2015  Bespoke Dataload for Void Events
--      1.1  6.13      AJ   30-JAN-2017  1)c_get_hps_vin amended as incorrectly closed as c1
--                                       2)In Validate l_errors and l_error_ind incorrectly set to null
--                                         after being set to V and N respectively so record status is
--                                         not being set to V correctly removed null setting
--                                       3)set_record_status_flag changed from calling the utils version
--                                         to the actual function in the package itself under SP1 for
--                                         ora error
--      1.2  6.13      AJ   02-FEB-2017  l_id not being set in validate to p1 row id 
--
-- **************************************************************************************************
--
  PROCEDURE set_record_status_flag
    (p_rowid  IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_hem_void_events
    SET    lvev_dl_load_status = p_status
    WHERE  rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_hem_void_events');
    RAISE;
  END set_record_status_flag;
  --
  -- **************************************************************************************************
  --
  PROCEDURE dataload_create
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  AS
  CURSOR c1 
    (p_batch_id VARCHAR2)
  IS
    SELECT ROWID rec_rowid
    ,      lvev_dlb_batch_id
    ,      lvev_dl_seqno
    ,      lvev_dl_load_status
    ,      lvev_pro_propref
    ,      lvev_vin_hps_start_date
    ,      lvev_vin_hps_end_date
    ,      lvev_vin_hps_hpc_code
    ,      lvev_int_seqno
    ,      lvev_order_seqno
    ,      lvev_evt_code
    ,      lvev_event_date
    ,      lvev_target_date
    ,      lvev_calc_ext
    ,      lvev_dev_vpa_code
    ,      lvev_dev_seqno
    ,      lvev_sys_updated
    ,      lvev_username
    ,      lvev_source_type
    ,      lvev_off_refno
    ,      lvev_vin_effective_date
    ,      lvev_upd_vin_refno
    ,      lvev_text
    FROM   dl_hem_void_events
    WHERE  lvev_dlb_batch_id = p_batch_id
    AND    lvev_dl_load_status = 'V';
  CURSOR c_pro_refno
    (p_propref VARCHAR2)
  IS
    SELECT pro_refno
    FROM   properties
    WHERE  pro_propref = p_propref;
  --
  -- Constants for process_summary
  --
  cb          VARCHAR2(30);
  cd          DATE;
  cp          VARCHAR2(30) := 'CREATE';
  ct          VARCHAR2(30) := 'DL_HEM_VOID_EVENTS';
  cs          INTEGER;
  ce          VARCHAR2(200);
  --
  -- Other variables
  --
  l_id        ROWID;
  i           INTEGER := 0;
  l_an_tab    VARCHAR2(1);
  l_pro_refno NUMBER(10);
  l_vin_refno NUMBER(8);
  BEGIN
    fsc_utils.proc_start('s_dl_hem_void_events.dataload_create');
    fsc_utils.debug_message( 's_dl_hem_void_events.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 in c1(p_batch_id) 
    LOOP
      BEGIN
        cs := p1.lvev_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        l_pro_refno := NULL;
        l_vin_refno := NULL;
        --
        -- get the pro_refno
        --
        OPEN c_pro_refno(p1.lvev_pro_propref);
        FETCH c_pro_refno INTO l_pro_refno;
        CLOSE c_pro_refno;
        --
        -- get the vin_refno that was put in lvev_upd_vin_refno during validation
        --
        l_vin_refno := p1.lvev_upd_vin_refno;
        --
        -- Need to delete any conflicting VEVs from the prop status dataload
        --
        DELETE from void_events
        WHERE  vev_vin_refno = l_vin_refno
        AND    vev_text = 'DATALOAD';
        INSERT INTO void_events
        (vev_vin_refno
        ,vev_int_seqno 
        ,vev_evt_code
        ,vev_event_date
        ,vev_target_date
        ,vev_calc_ext
        ,vev_text
        ,vev_dev_vpa_code
        ,vev_dev_seqno
        ,vev_order_seqno    
        ,vev_sys_updated
        ,vev_username
        ,vev_source_type
        ,vev_reusable_refno                      
        )
        VALUES
        (l_vin_refno
        ,p1.lvev_int_seqno
        ,p1.lvev_evt_code
        ,p1.lvev_event_date
        ,p1.lvev_target_date
        ,p1.lvev_calc_ext
        ,DECODE(p1.lvev_text,'DATALOAD','VEV DATALOAD',p1.lvev_text)
        ,p1.lvev_dev_vpa_code
        ,p1.lvev_dev_seqno
        ,DECODE(p1.lvev_order_seqno,NULL,(p1.lvev_int_seqno * 10),p1.lvev_order_seqno)                   
        ,DECODE(p1.lvev_sys_updated,NULL,TRUNC(SYSDATE))
        ,DECODE(p1.lvev_username,NULL,'VEV DATALOAD','DATALOAD','VEV DATALOAD',p1.lvev_username)
        ,p1.lvev_source_type
        ,reusable_refno_seq.NEXTVAL
        );
        --
        -- keep a count of the rows processed and commit after every 5000
        --
        i := i + 1;
        IF MOD(i,5000) = 0 
        THEN
          COMMIT;
        END If;
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
    -- Section to anayze the table(s) populated by this dataload
    --
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('VOID_EVENTS');
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
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  AS
  CURSOR c1 
    (p_batch_id VARCHAR2)
  IS
    SELECT ROWID rec_rowid
    ,      lvev_dlb_batch_id
    ,      lvev_dl_seqno
    ,      lvev_dl_load_status
    ,      lvev_pro_propref
    ,      lvev_vin_hps_start_date
    ,      lvev_vin_hps_end_date
    ,      lvev_vin_hps_hpc_code
    ,      lvev_int_seqno
    ,      lvev_order_seqno
    ,      lvev_evt_code
    ,      lvev_event_date
    ,      lvev_target_date
    ,      lvev_calc_ext
    ,      lvev_dev_vpa_code
    ,      lvev_dev_seqno
    ,      lvev_sys_updated
    ,      lvev_username
    ,      lvev_source_type
    ,      lvev_off_refno
    ,      lvev_vin_effective_date
    ,      lvev_upd_vin_refno
    ,      lvev_text
    FROM   dl_hem_void_events
    WHERE  lvev_dlb_batch_id = p_batch_id
    AND    lvev_dl_load_status IN ('L','F','O');
  CURSOR c_evt_code 
    (p_evt_code VARCHAR2) 
  IS
    SELECT 'X'
    FROM   event_types
    WHERE  evt_code = p_evt_code;
  CURSOR c_dup
    (p_vin_refno INTEGER
    ,p_int_seqno INTEGER
    ) 
  IS
    SELECT 'X'
    FROM   void_events
    WHERE  vev_vin_refno = p_vin_refno
    AND    vev_int_seqno = p_int_seqno
    AND    vev_text != 'DATALOAD';
  CURSOR c_get_prop
    (p_propref VARCHAR2) 
  IS
    SELECT pro_refno
    FROM   properties
    WHERE  pro_propref = p_propref;
  CURSOR c_get_hps_vin 
    (p_pro_refno  NUMBER
    ,p_start_date DATE
    ,p_hpc_code   VARCHAR2
    )
  IS
    SELECT TO_NUMBER(SUBSTR(hps_comments,14,INSTR(hps_comments,')',1,1) -14)) hps_vin_refno
    FROM   hou_prop_statuses
    WHERE  hps_pro_refno = p_pro_refno
    AND    hps_start_date = p_start_date
    AND    hps_hpc_code = p_hpc_code
    AND    hps_comments LIKE '(VIN REFNO = %';
  CURSOR c_dev_vpa_seqno
    (p_vev_evt_code VARCHAR2
	,p_dev_vpa_code VARCHAR2
    ,p_dev_seqno    NUMBER
    ) 
  IS
    SELECT 'X'
    FROM   default_events
    WHERE  dev_vpa_code = p_dev_vpa_code
    AND    dev_seqno = p_dev_seqno
    AND    dev_evt_code = p_vev_evt_code;
  --
  -- Constants for process_summary
  --
  cb          VARCHAR2(30);
  cd          DATE;
  cp          VARCHAR2(30) := 'VALIDATE';
  ct          VARCHAR2(30) := 'DL_HEM_VOID_EVENTS';
  cs          INTEGER;
  ce          VARCHAR2(200);
  --
  -- Other variables
  --
  l_id        ROWID;
  i           INTEGER := 0;
  l_an_tab    VARCHAR2(1);
  l_exists         VARCHAR2(1);
  l_pro_refno      NUMBER(10);
  l_errors         VARCHAR2(10);
  l_error_ind      VARCHAR2(10);
  l_curr_void      VARCHAR2(1);
  l_vin_refno      INTEGER := 0;
  l_vpa_exists     VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hem_void_events.dataload_validate');
    fsc_utils.debug_message( 's_dl_hem_void_events.dataload_validate',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1(p_batch_id) 
    LOOP
      BEGIN
        cs := p1.lvev_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors := 'V';
        l_error_ind := 'N';
        l_exists := NULL;
        l_pro_refno := NULL;
        l_curr_void := NULL;
        l_vin_refno := NULL;
        l_vpa_exists := NULL;
        --
        -- Check the Links to Other Tables
        --
        -- Check the property exists on properties
        --
        OPEN c_get_prop(p1.lvev_pro_propref);
        FETCH c_get_prop INTO l_pro_refno;
        IF c_get_prop%NOTFOUND 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
        END IF;
        CLOSE c_get_prop;
        --
        -- Check the Event code is valid
        --
        OPEN  c_evt_code(p1.lvev_evt_code);
        FETCH c_evt_code INTO l_exists;
        IF c_evt_code%NOTFOUND
        THEN 
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',946);
        END IF;
        CLOSE c_evt_code;
        --
        -- Check the Target date is not before the Void Instance
        --
        IF p1.lvev_target_date > p1.lvev_vin_effective_date
        THEN 
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',947);
        END IF;
        --
        -- Check the Event Date is not before the Void Instance
        --
        IF p1.lvev_event_date > p1.lvev_vin_effective_date
        THEN 
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',947);
        END IF;
        --
        -- Check the Last Updated Date is not before the Void Instance
        --
        IF p1.lvev_sys_updated > p1.lvev_vin_effective_date
        THEN 
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',947);
        END IF;
        --
        -- Find the vin_refno from the comment on the matching hou_prop_statuses record 
        --
        OPEN c_get_hps_vin(l_pro_refno,p1.lvev_vin_hps_start_date,p1.lvev_vin_hps_hpc_code);
        FETCH c_get_hps_vin INTO l_vin_refno;
        IF c_get_hps_vin%NOTFOUND
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',945);
        END IF;
        CLOSE c_get_hps_vin;
        --
        -- Update the dataload table with the vin_refno so that this can be used in the dataload create process
        --
        UPDATE dl_hem_void_events
        SET    lvev_upd_vin_refno = l_vin_refno
        WHERE  ROWID = p1.rec_rowid;
        --
        -- Check Mandatory Fields
        --
        -- Sequence Number
        --
        IF p1.lvev_int_seqno IS NULL
        THEN 
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',948);
        ELSE    
          --
          -- Check the lvev_int_seq is not a duplicate
          --
          OPEN c_dup(l_vin_refno,p1.lvev_int_seqno);
          FETCH c_dup INTO l_exists;
          IF c_dup%FOUND
          THEN 
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',948);
          END IF;
          CLOSE c_dup;
        END IF;
        --
        -- Check default void path seqno and event combination if supplied
        --
        IF (p1.lvev_evt_code     IS NOT NULL AND
            p1.lvev_dev_vpa_code IS NOT NULL AND
            p1.lvev_dev_seqno    IS NOT NULL     )
        THEN        
          OPEN c_dev_vpa_seqno(p1.lvev_evt_code,p1.lvev_dev_vpa_code,p1.lvev_dev_seqno);
          FETCH c_dev_vpa_seqno INTO l_vpa_exists;
          IF c_dev_vpa_seqno%NOTFOUND 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',122);
          END IF;
          CLOSE c_dev_vpa_seqno;
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
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    ) 
  IS
  CURSOR c1 
    (p_batch_id VARCHAR2)
  IS
    SELECT ROWID rec_rowid
    ,      lvev_dlb_batch_id
    ,      lvev_dl_seqno
    ,      lvev_dl_load_status
    ,      lvev_pro_propref
    ,      lvev_vin_hps_start_date
    ,      lvev_vin_hps_end_date
    ,      lvev_vin_hps_hpc_code
    ,      lvev_int_seqno
    ,      lvev_order_seqno
    ,      lvev_evt_code
    ,      lvev_event_date
    ,      lvev_target_date
    ,      lvev_calc_ext
    ,      lvev_dev_vpa_code
    ,      lvev_dev_seqno
    ,      lvev_sys_updated
    ,      lvev_username
    ,      lvev_source_type
    ,      lvev_off_refno
    ,      lvev_vin_effective_date
    ,      lvev_upd_vin_refno
    ,      lvev_text
    FROM   dl_hem_void_events
    WHERE  lvev_dlb_batch_id = p_batch_id
    AND    lvev_dl_load_status = 'C';
  --
  -- Constants for process_summary
  --
  cb       VARCHAR2(30);
  cd       DATE;
  cp       VARCHAR2(30) := 'DELETE';
  ct       VARCHAR2(30) := 'DL_HEM_VOID_EVENTS';
  cs       INTEGER;
  ce       VARCHAR2(200);
  --
  -- Other variables
  --
  l_id           ROWID;
  i              INTEGER := 0;
  l_an_tab       VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hem_void_events.dataload_delete');
    fsc_utils.debug_message( 's_dl_hem_void_events.dataload_delete',3 );
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1(p_batch_id) 
    LOOP
      BEGIN
        cs := p1.lvev_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        DELETE 
        FROM   void_events
        WHERE  vev_vin_refno = p1.lvev_upd_vin_refno
        AND    vev_evt_code = p1.lvev_evt_code
        AND    vev_int_seqno = p1.lvev_int_seqno;
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
    -- Section to anayze the table(s) populated by this dataload
    --
    l_an_tab := s_dl_hem_utils.dl_comp_stats('VOID_EVENTS');
    fsc_utils.proc_end;
    COMMIT;  
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_hem_void_events;
/

