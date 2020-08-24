CREATE OR REPLACE PACKAGE BODY s_dl_hrm_works_order_versions
AS
-- ***********************************************************************  
--  DESCRIPTION:  
--  
--  CHANGE CONTROL  
--  VERSION DB Ver  WHO  WHEN        WHY  
--  1.0             PJD  22/05/2003  New Dataload for WF  
--  1.1             PH   04/07/2003  Changed error code on validate for W/O.  
--  1.2     5.3.0   PH   23/07/2003  Changed create so it populates the  
--                                   version field correctly.  
--  1.3     5.9.0   PH   10/04/2006  Amended validate on Statuses (HDL790)  
--                                   Commented out orig code and added new  
--  1.4     5.9.0   PJD  22/04/2006  Ammended typo in above change  
--  
--  2.0     5.13.0  PH   06-FEB-2008 Now includes its own  
--                                   set_record_status_flag procedure.  
--  2.1     5.15.1  PH   24-FEB-2010 commented out validate on Invoice date  
--                                   as wouldn't expect this version to be  
--                                   invoiced  
--  2.2     6.11.0  MJK  03-SEP-2015 Reformatted, Added wov_authorised_by  
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
    UPDATE dl_hrm_works_order_versions
    SET    lwov_dl_load_status = p_status
    WHERE  rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_hrm_works_order_versions');
    RAISE;
  END set_record_status_flag;
  --
  PROCEDURE dataload_create
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  AS
    CURSOR c1
    IS
      SELECT rowid                       rec_rowid
      ,      lwov_dlb_batch_id
      ,      lwov_dl_seqno
      ,      lwov_dl_load_status
      ,      lwov_wor_srq_legacy_refno
      ,      lwov_wor_seqno
      ,      lwov_type
      ,      lwov_sco_code
      ,      lwov_status_date
      ,      lwov_pri_code
      ,      lwov_hrv_vre_code
      ,      lwov_access_am
      ,      lwov_access_pm
      ,      lwov_hrv_acc_code
      ,      lwov_access_notes
      ,      lwov_hrv_loc_code
      ,      lwov_location_notes
      ,      lwov_rtr_ind
      ,      lwov_held_date
      ,      lwov_authorised_date
      ,      lwov_invoiced_date
      ,      lwov_estimated_cost
      ,      lwov_estimated_tax_amount
      ,      lwov_invoiced_cost
      ,      lwov_invoiced_tax_amount
      ,      lwov_raised_date
      ,      lwov_target_date
      ,      lwov_spr_printer_name
      ,      lwov_contractor_extract_date
      ,      lwov_financials_extract_date
      ,      lwov_hrv_ust_code
      ,      lwov_description
      ,      lwov_comments
      ,      lwov_version_no
      ,      lwov_authorised_by
      FROM   dl_hrm_works_order_versions
      WHERE  lwov_dlb_batch_id = p_batch_id
      AND    lwov_dl_load_status = 'V';
    -- Cursor to get Service Request No
    CURSOR c_srq_no
      (p_code VARCHAR2)
    IS
      SELECT srq_no
      FROM   service_requests
      WHERE  srq_legacy_refno = p_code;
    -- Cursor to get the location notes if none supplied
    CURSOR c_loc_notes
      (p_code VARCHAR2)
    IS
      SELECT frv_name
      FROM   first_ref_values
      WHERE  frv_frd_domain = 'LOCATION'
      AND   frv_code = p_code;
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'CREATE';
    ct VARCHAR2(30) := 'DL_HRM_WORKS_ORDER_VERSIONS';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    l_pro_refno  NUMBER;
    l_an_tab     VARCHAR2(1);
    i            INTEGER := 0;
    l_srq_no     NUMBER;
    l_ppp_start  DATE;
    l_cspg_refno NUMBER;
    l_cspg_start DATE;
    l_reuse      NUMBER;
    l_loc_note   VARCHAR2(40);
  BEGIN
    fsc_utils.proc_start('s_dl_hrm_works_order_versions.dataload_create');
    fsc_utils.debug_message('s_dl_hrm_works_order_versions.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.lwov_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        l_srq_no := NULL;
        OPEN c_srq_no(p1.lwov_wor_srq_legacy_refno);
        FETCH c_srq_no INTO l_srq_no;
        CLOSE c_srq_no;
        -- Set the location note if null
        l_loc_note := p1.lwov_location_notes;
        IF l_loc_note IS NULL 
        THEN
          OPEN c_loc_notes(p1.lwov_hrv_loc_code);
          FETCH c_loc_notes INTO l_loc_note;
          CLOSE c_loc_notes;
        END IF;
        --
        -- Insert into Works Order Versions
        --
        INSERT INTO works_order_versions
        (wov_wor_srq_no  
        ,wov_wor_seqno  
        ,wov_version_no  
        ,wov_type  
        ,wov_sco_code  
        ,wov_pri_code  
        ,wov_raised_datetime  
        ,wov_status_date  
        ,wov_target_datetime  
        ,wov_hrv_loc_code  
        ,wov_sundry_cleared_ind  
        ,wov_rtr_ind  
        ,wov_access_am  
        ,wov_access_pm  
        ,wov_created_by  
        ,wov_created_date  
        ,wov_spr_printer_name  
        ,wov_held_datetime  
        ,wov_authorised_datetime  
        ,wov_invoiced_datetime  
        ,wov_estimated_cost  
        ,wov_estimated_tax_amount  
        ,wov_invoiced_cost  
        ,wov_invoiced_tax_amount  
        ,wov_financials_extract_date  
        ,wov_contractor_extract_date  
        ,wov_hrv_vre_code  
        ,wov_hrv_ust_code  
        ,wov_hrv_acc_code  
        ,wov_description  
        ,wov_comments  
        ,wov_location_notes  
        ,wov_access_notes 
        ,wov_authorised_by
        )  
        VALUES  
        (l_srq_no  
        ,p1.lwov_wor_seqno  
        ,p1.lwov_version_no  
        ,p1.lwov_type  
        ,p1.lwov_sco_code  
        ,p1.lwov_pri_code  
        ,p1.lwov_raised_date  
        ,p1.lwov_status_date  
        ,p1.lwov_target_date  
        ,p1.lwov_hrv_loc_code  
        ,'N'  
        ,p1.lwov_rtr_ind  
        ,p1.lwov_access_am  
        ,p1.lwov_access_pm  
        ,'DATALOAD'  
        ,TRUNC(sysdate)  
        ,p1.lwov_spr_printer_name  
        ,p1.lwov_held_date  
        ,p1.lwov_authorised_date  
        ,p1.lwov_invoiced_date  
        ,p1.lwov_estimated_cost  
        ,p1.lwov_estimated_tax_amount  
        ,p1.lwov_invoiced_cost  
        ,p1.lwov_invoiced_tax_amount  
        ,p1.lwov_financials_extract_date  
        ,p1.lwov_contractor_extract_date  
        ,p1.lwov_hrv_vre_code  
        ,p1.lwov_hrv_ust_code  
        ,p1.lwov_hrv_acc_code  
        ,p1.lwov_description  
        ,p1.lwov_comments  
        ,l_loc_note  
        ,p1.lwov_access_notes
        ,p1.lwov_authorised_by
        );  
        i := i + 1;
        IF MOD(i,2000) = 0 
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
    COMMIT;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('WORKS_ORDER_VERSIONS');
    fsc_utils.proc_end;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_create;
  --
  PROCEDURE dataload_validate
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  AS
    CURSOR c1
    IS
      SELECT rowid                   rec_rowid
      ,      lwov_dlb_batch_id
      ,      lwov_dl_seqno
      ,      lwov_dl_load_status
      ,      lwov_wor_srq_legacy_refno
      ,      lwov_wor_seqno
      ,      lwov_type
      ,      lwov_sco_code
      ,      lwov_status_date
      ,      lwov_pri_code
      ,      lwov_hrv_vre_code
      ,      lwov_access_am
      ,      lwov_access_pm
      ,      lwov_hrv_acc_code
      ,      lwov_access_notes
      ,      lwov_hrv_loc_code
      ,      lwov_location_notes
      ,      lwov_rtr_ind
      ,      lwov_held_date
      ,      lwov_authorised_date
      ,      lwov_invoiced_date
      ,      lwov_estimated_cost
      ,      lwov_estimated_tax_amount
      ,      lwov_invoiced_cost
      ,      lwov_invoiced_tax_amount
      ,      lwov_raised_date
      ,      lwov_target_date
      ,      lwov_spr_printer_name
      ,      lwov_contractor_extract_date
      ,      lwov_financials_extract_date
      ,      lwov_hrv_ust_code
      ,      lwov_description
      ,      lwov_comments
      ,      lwov_authorised_by
      FROM   dl_hrm_works_order_versions
      WHERE  lwov_dlb_batch_id = p_batch_id
      AND    lwov_dl_load_status IN('L','F','O');
    -- Cursor for Service Request
    CURSOR c_srq_no
      (p_code VARCHAR2)
    IS
      SELECT srq_no
      FROM   service_requests
      WHERE  srq_legacy_refno = p_code;
    --
    -- Cursor to check Works Order Seqno
    --
    CURSOR c_wor_no
      (p_seq_no    NUMBER
      ,p_wor_seqno NUMBER
      )
    IS
      SELECT 'X'
      FROM   works_orders
      WHERE  wor_srq_no = p_seq_no
      AND    wor_seqno = p_wor_seqno;
    CURSOR c_wor_status
      (p_seq_no    NUMBER
      ,p_wor_seqno NUMBER
      )
    IS
      SELECT wor_sco_code
      FROM   works_orders
      WHERE  wor_srq_no = p_seq_no
      AND    wor_seqno = p_wor_seqno;
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'VALIDATE';
    ct VARCHAR2(30) := 'DL_HRM_WORKS_ORDER_VERSIONS';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    l_exists    VARCHAR2(1);
    l_errors    VARCHAR2(10);
    l_error_ind VARCHAR2(10);
    i           INTEGER := 0;
    l_suberror  VARCHAR2(1);
    l_srq_no    NUMBER(8);
    l_wor_sco   VARCHAR2(10);
    --
  BEGIN
    fsc_utils.proc_start('s_dl_hrm_works_order_versions.dataload_validate');
    fsc_utils.debug_message('s_dl_hrm_works_order_versions.dataload_validate',3);
    cb := p_batch_id;
    cd := p_DATE;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.lwov_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors := 'V';
        l_error_ind := 'N';
        --
        -- Check the Service Requests Exists
        --
        l_srq_no := NULL;
        OPEN c_srq_no(p1.lwov_wor_srq_legacy_refno);
        FETCH c_srq_no INTO l_srq_no;
        IF c_srq_no%notfound 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',779);
        END IF;
        CLOSE c_srq_no;
        --
        -- Check the Works Order Exists
        --
        OPEN c_wor_no(l_srq_no,p1.lwov_wor_seqno);
        FETCH c_wor_no INTO l_exists;
        IF c_wor_no%notfound 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',994);
        END IF;
        CLOSE c_wor_no;
        --
        -- Check all the hou_ref_values
        --
        -- Version Raising Reason Code
        --
        IF(NOT s_dl_hem_utils.exists_frv('VARREASON',p1.lwov_hrv_vre_code,'Y')) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',784);
        END IF;
        --
        -- Access Code
        --
        IF(NOT s_dl_hem_utils.exists_frv('ACCESS',p1.lwov_hrv_acc_code,'Y')) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',727);
        END IF;
        --
        -- Location Code
        --
        IF(NOT s_dl_hem_utils.exists_frv('LOCATION',p1.lwov_hrv_loc_code,'Y')) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',773);
        END IF;
        --
        -- User Defined Status
        --
        IF(NOT s_dl_hem_utils.exists_frv('WOSTATUS',p1.lwov_hrv_ust_code,'Y')) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',737);
        END IF;
        --
        -- Validate Status (works order version)
        --
        IF p1.lwov_sco_code NOT IN('RAI','AUT','ISS','COM','CAN','CLO','HLD') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',894);
        END IF;
        l_wor_sco := NULL;
        --
        OPEN c_wor_status(l_srq_no,p1.lwov_wor_seqno);
        FETCH c_wor_status INTO l_wor_sco;
        IF l_wor_sco = 'RAI' 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',790);
        END IF;
        CLOSE c_wor_status;
        --
        -- Validate Authorised Date if status (Works Order Version)
        --
        IF p1.lwov_sco_code IN('AUT','ISS','COM','CLO') 
        THEN
          IF p1.lwov_authorised_date IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',989);
          END IF;
        END IF;
        --
        -- Validate Version Type
        --
        IF p1.lwov_type NOT IN('R','H') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',798);
        END IF;
        --
        --
        -- Validate Status Date
        --
        IF p1.lwov_status_date IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',777);
        END IF;
        --
        -- Validate am pm access
        --
        IF p1.lwov_access_am IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',728);
        END IF;
        --
        IF p1.lwov_access_pm IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',729);
        END IF;
        --
        l_suberror := 'N';
        --
        IF p1.lwov_access_am IS NOT NULL 
        THEN
          FOR i IN 1..7
          LOOP
            IF((SUBSTR(p1.lwov_access_am,i,1) NOT IN('Y','N')) AND (l_suberror = 'N')) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',728);
              l_suberror := 'Y';
            END IF;
          END LOOP;
        END IF;
        l_suberror := 'N';
        IF p1.lwov_access_pm IS NOT NULL 
        THEN
          FOR i IN 1..7
          LOOP
            IF((SUBSTR(p1.lwov_access_pm,i,1) NOT IN('Y','N')) AND (l_suberror = 'N')) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',729);
              l_suberror := 'Y';
            END IF;
          END LOOP;
        END IF;
        --
        -- If the location Code is Null then the location note must be supplied
        --
        IF(p1.lwov_location_notes IS NULL AND p1.lwov_hrv_loc_code IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',783);
        END IF;
        --
        -- Validate Right to Repair
        --
        IF p1.lwov_rtr_ind NOT IN('Y','N') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',733);
        END IF;
        --
        -- Validate Raised Date
        --
        IF p1.lwov_raised_date IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',788);
        END IF;
        --
        -- Validate Target Date
        --
        IF p1.lwov_target_date IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',789);
        END IF;
        --
        -- Validate Tax fields
        --
        IF p1.lwov_estimated_cost IS NULL 
        THEN
          IF p1.lwov_estimated_tax_amount IS NOT NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',799);
          END IF;
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
        -- keep a count of the rows processed and commit after every 2000
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
    COMMIT;
    fsc_utils.proc_END;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_validate;
  --
  PROCEDURE dataload_delete
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  IS
    CURSOR c1
    IS
      SELECT a1.rowid rec_rowid
      ,      a1.lwov_dlb_batch_id
      ,      a1.lwov_dl_seqno
      ,      a1.lwov_DL_LOAD_STATUS
      ,      b1.rowid wov_rowid
      ,      c1.rowid wo_rowid
      FROM  dl_hrm_works_order_versions a1
      ,     works_order_versions b1
      ,     works_orders c1
      ,     service_requests s1
      WHERE a1.lwov_dlb_batch_id = p_batch_id
      AND   a1.lwov_dl_load_status = 'C'
      AND   a1.lwov_wor_legacy_ref = c1.wor_legacy_ref
      AND   c1.wor_seqno = b1.wov_wor_seqno
      AND   a1.lwov_wor_seqno = c1.wor_seqno
      AND   b1.wov_version_no = a1.lwov_version_no
      AND   s1.srq_no = b1.wov_wor_srq_no
      AND   s1.srq_no = c1.wor_srq_no
      AND   s1.srq_legacy_refno = a1.lwov_wor_srq_legacy_refno;
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'DELETE';
    ct VARCHAR2(30) := 'DL_HRM_WORKS_ORDER_VERSIONS';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    l_an_tab VARCHAR2(1);
    i INTEGER := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_hrm_works_order_versions.dataload_delete');
    fsc_utils.debug_message('s_dl_hrm_works_order_versions.dataload_delete',3);
    cb := p_batch_id;
    cd := p_date;
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.lwov_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        DELETE
        FROM   works_order_versions
        WHERE  rowid = p1.wov_rowid;
        i := i + 1;
        IF MOD(i,2000) = 0 
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
    l_an_tab := s_dl_hem_utils.dl_comp_stats('WORKS_ORDER_VERSIONS');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_hrm_works_order_versions;
/
show errors
