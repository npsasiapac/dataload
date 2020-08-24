CREATE OR REPLACE PACKAGE BODY s_dl_hrm_works_orders
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Ver  WHO  WHEN        WHY
--  1.0             PH   15/07/2001  Product Dataload
--  1.1     5.1.4   PJD  02/02/2002  Added error Code 894 (for 110)
--                                   Added Savepoints
--                                   Location now uses domain ELELOC
--                                   Location Notes now derivable from
--                                    location code
--                                   Close Cursor c_cspg
--                                   Check on Work Prog now uses table
--                                    instead of a domain
--  2.0     5.2.0   SB   04-12-2002  Changed ELELOC back to LOCATION
--  2.1     5.2.0   PH   10/12/2002  Changed incorrect error codes on
--                                    issued and authorised dates.
--  3.0     5.3.0   PH   28/03/2003  Set l_cspg_refno and l_cspg_start
--                                    to null on insert.
--  3.1     5.3.4   PH   20/05/2003  Amended cursor on delete, linked
--                                    using wor_srq_no and wov_wor_srq_no
--  3.2     5.3.5   PH   04/07/2003  Changed error code on previous status date
--                                   from 896 to 995 as 896 used by contractors.
--  3.3     5.5.0   PH   19/02/2004  Amended Validate on works order version status
--                                   valid value is AUT as it's the first version.
--                                   Previously was 'RAI','AUT','ISS','COM',
--                                   'CAN','CLO','HLD'
--  3.4     5.6.0   PJD  28/12/2004  Now added own set_record_status_flag procedure.
--  3.5     5.10.0  PH   13/10/2006  Amended create, was putting in lwor_sco_code
--                                   instead of lwov_sco_code.
--  3.6     5.10.0  PH   03/11/2006  Amended create, now defaults lwov_version_no
--                                   to 1 if not supplied.
--  3.7     5.13.0  PH   05/02/2008  Added HLD to list in validation error 988.
--  3.8     5.14.0  PH   26/01/2009  Added nvl to validate on sco_code
--  3.9     5.16.0  MB   31/08/2010  Put trunc around p_rai on call to c_cspg
--                                   as wors raised during last day of a cspg failing validation
--  4.0     6.11.0  MJK  03/09/2015  Reformatted for v6.11 Authorised_by added, wov_hrv_cby_code updated.
--  5.0     6.11.1  PN   05/07/2018  works_orders.wor_authorised_by recorded in dataload_create
-- ***********************************************************************
  PROCEDURE set_record_status_flag
    (p_rowid  IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_hrm_works_orders
    SET    lwor_dl_load_status = p_status
    WHERE  rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_hrm_works_orders');
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
      SELECT rowid rec_rowid
      ,      lwor_dlb_batch_id
      ,      lwor_dl_seqno
      ,      lwor_dl_load_status
      ,      lwor_srq_legacy_refno
      ,      lwor_seqno
      ,      lwor_legacy_ref
      ,      lwor_pro_propref
      ,      lwor_aun_code
      ,      lwor_ppc_ppp_ppg_code
      ,      lwor_ppc_ppp_wpr_code
      ,      lwor_sco_code
      ,      lwor_prev_status_code
      ,      lwor_prev_status_date
      ,      lwor_confirmation_ind
      ,      lwor_alternative_ref
      ,      lwor_ppc_cos_code
      ,      lwor_print_ind
      ,      lwor_tenant_ticket_print_ind
      ,      lwor_reassign_ind
      ,      lwor_def_contr_ind
      ,      lwor_authorised_date
      ,      lwor_issued_date
      ,      lwor_reported_comp_date
      ,      lwor_system_comp_date
      ,      lwor_hrv_cby_code
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
      ,      NVL(lwov_version_no,1) lwov_version_no
      ,      lwov_authorised_by
      FROM   dl_hrm_works_orders
      WHERE  lwor_dlb_batch_id = p_batch_id
      AND    lwor_dl_load_status = 'V';
    -- Cursor for Service Request Number
    CURSOR c_srq_no
      (p_code VARCHAR2)
    IS
      SELECT srq_no
      FROM   service_requests
      WHERE  srq_legacy_refno = p_code;
    -- Cursor for Policy Start Date
    CURSOR c_ppp_start
      (p_ppg_code VARCHAR2
      ,p_wpr_code VARCHAR2
      )
    IS
      SELECT ppp_start_date
      FROM   pricing_policy_programmes
      WHERE  ppp_ppg_code = p_ppg_code
      AND    ppp_wpr_code = p_wpr_code;
    -- Cursor for propref
    CURSOR c_pro_refno
      (p_propref VARCHAR2)
    IS
      SELECT pro_refno
      FROM   properties
      WHERE  pro_propref = p_propref;
    -- Cursor for reusable_refno
    CURSOR c_reuse
    IS
      SELECT reusable_refno_seq.nextval
      FROM   dual;
    -- Cursor to get cspg_refno and Start Date
    CURSOR c_cspg
      (p_cos VARCHAR2
      ,p_ppg VARCHAR2
      ,p_wpr VARCHAR2
      ,p_rai DATE
      )
    IS
      SELECT cspg_refno
      ,      cspg_start_date
      FROM   con_site_price_groups
      WHERE  cspg_ppc_cos_code = p_cos
      AND    cspg_ppc_ppp_ppg_code = p_ppg
      AND    cspg_ppc_ppp_wpr_code = p_wpr
      AND    p_rai BETWEEN cspg_start_date AND NVL(cspg_end_date,sysdate);
    -- Cursor to get the location notes if none supplied
    CURSOR c_loc_notes
      (p_code VARCHAR2)
    IS
      SELECT frv_name
      FROM   first_ref_values
      WHERE  frv_frd_domain = 'LOCATION'
      AND    frv_code = p_code;
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'CREATE';
    ct VARCHAR2(30) := 'DL_HRM_WORKS_ORDERS';
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
    fsc_utils.proc_start('s_dl_hrm_works_orders.dataload_create');
    fsc_utils.debug_message('s_dl_hrm_works_orders.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.lwor_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        l_pro_refno := NULL;
        OPEN c_pro_refno(p1.lwor_pro_propref);
        FETCH c_pro_refno INTO l_pro_refno;
        CLOSE c_pro_refno;
        l_srq_no := NULL;
        OPEN c_srq_no(p1.lwor_srq_legacy_refno);
        FETCH c_srq_no INTO l_srq_no;
        CLOSE c_srq_no;
        l_ppp_start := NULL;
        OPEN c_ppp_start(p1.lwor_ppc_ppp_ppg_code,p1.lwor_ppc_ppp_wpr_code);
        FETCH c_ppp_start INTO l_ppp_start;
        CLOSE c_ppp_start;
        OPEN c_reuse;
        FETCH c_reuse
        INTO l_reuse;
        CLOSE c_reuse;
        l_cspg_refno := NULL;
        l_cspg_start := NULL;
        OPEN c_cspg(p1.lwor_ppc_cos_code,p1.lwor_ppc_ppp_ppg_code, p1.lwor_ppc_ppp_wpr_code,TRUNC(p1.lwov_raised_date));
        FETCH c_cspg INTO l_cspg_refno,l_cspg_start;
        CLOSE c_cspg;
        l_loc_note := p1.lwov_location_notes;
        IF l_loc_note IS NULL 
        THEN
          OPEN c_loc_notes(p1.lwov_hrv_loc_code);
          FETCH c_loc_notes INTO l_loc_note;
          CLOSE c_loc_notes;
        END IF;
        --
        -- Insert into Works Orders
        --
        INSERT INTO works_orders
        (wor_srq_no  
        ,wor_seqno  
        ,wor_raised_datetime  
        ,wor_status_date  
        ,wor_update_child_elems_ind  
        ,wor_confirmation_ind  
        ,wor_print_ind  
        ,wor_tenant_ticket_print_ind  
        ,wor_reassign_ind  
        ,wor_def_contr_ind  
        ,wor_created_by  
        ,wor_created_date  
        ,wor_reusable_refno  
        ,wor_sco_code  
        ,wor_ppc_cos_code  
        ,wor_ppc_ppp_ppg_code  
        ,wor_ppc_ppp_wpr_code  
        ,wor_ppc_ppp_start_date  
        ,wor_aun_code  
        ,wor_pro_refno  
        ,wor_authorised_datetime 
        ,wor_authorised_by
        ,wor_held_datetime  
        ,wor_issued_datetime  
        ,wor_legacy_ref  
        ,wor_alternative_ref  
        ,wor_reported_comp_datetime  
        ,wor_system_comp_datetime  
        ,wor_hrv_cby_code  
        ,wor_prev_status_code  
        ,wor_prev_status_date  
        ,wor_cspg_refno  
        ,wor_cspg_start_date  
        )  
        VALUES  
        (l_srq_no  
        ,p1.lwor_seqno  
        ,p1.lwov_raised_date  
        ,p1.lwov_status_date  
        ,'N'  
        ,p1.lwor_confirmation_ind  
        ,p1.lwor_print_ind  
        ,p1.lwor_tenant_ticket_print_ind  
        ,p1.lwor_reassign_ind  
        ,p1.lwor_def_contr_ind  
        ,'DATALOAD'  
        ,TRUNC(sysdate)  
        ,l_reuse  
        ,p1.lwor_sco_code  
        ,p1.lwor_ppc_cos_code  
        ,p1.lwor_ppc_ppp_ppg_code  
        ,p1.lwor_ppc_ppp_wpr_code  
        ,l_ppp_start  
        ,p1.lwor_aun_code  
        ,l_pro_refno  
        ,p1.lwor_authorised_date  
        ,p1.lwov_authorised_by
        ,p1.lwov_held_date  
        ,p1.lwor_issued_date  
        ,p1.lwor_legacy_ref  
        ,p1.lwor_alternative_ref  
        ,p1.lwor_reported_comp_date  
        ,p1.lwor_system_comp_date  
        ,p1.lwor_hrv_cby_code  
        ,p1.lwor_prev_status_code  
        ,p1.lwor_prev_status_date  
        ,l_cspg_refno  
        ,l_cspg_start  
        );  
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
        ,wov_printed_ind  
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
        ,wov_hrv_cby_code
        )  
        VALUES  
        (l_srq_no  
        ,p1.lwor_seqno  
        ,p1.lwov_version_no  
        ,p1.lwov_type  
        ,p1.lwov_sco_code  
        ,p1.lwov_pri_code  
        ,p1.lwov_raised_date  
        ,p1.lwov_status_date  
        ,p1.lwov_target_date  
        ,p1.lwor_print_ind  
        ,p1.lwov_hrv_loc_code  
        ,'N'  
        ,p1.lwov_rtr_ind  
        ,p1.lwov_access_am  
        ,p1.lwov_access_pm  
        ,'DATALOAD'  
        ,TRUNC(sysdate)  
        ,p1.lwov_spr_printer_name  
        ,p1.lwov_held_date  
        ,p1.lwor_authorised_date  
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
        ,p1.lwor_hrv_cby_code
        );  
        IF p1.lwor_sco_code IN('CAN','COM','CLO','CVR') 
        THEN
          s_contractor_sites.decrement_cost_and_wo_count(p1.lwor_ppc_cos_code,s_works_order_versions.get_current_wov_estimate(l_srq_no,p1.lwor_seqno));
        END IF;
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
    l_an_tab := s_dl_hem_utils.dl_comp_stats('WORKS_ORDERS');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('WORKS_ORDER_VERSIONS');
    fsc_utils.proc_end;
  EXCEPTION
  WHEN OTHERS 
  THEN
    set_record_status_flag(l_id,'O');
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
      SELECT rowid                       rec_rowid
      ,      lwor_dlb_batch_id
      ,      lwor_dl_seqno
      ,      lwor_srq_legacy_refno
      ,      lwor_seqno
      ,      lwor_pro_propref
      ,      lwor_aun_code
      ,      lwor_ppc_ppp_ppg_code
      ,      lwor_ppc_ppp_wpr_code
      ,      lwor_sco_code
      ,      lwor_prev_status_code
      ,      lwor_prev_status_date
      ,      lwor_confirmation_ind
      ,      lwor_alternative_ref
      ,      lwor_ppc_cos_code
      ,      lwor_print_ind
      ,      lwor_tenant_ticket_print_ind
      ,      lwor_reassign_ind
      ,      lwor_def_contr_ind
      ,      lwor_authorised_date
      ,      lwor_issued_date
      ,      lwor_reported_comp_date
      ,      lwor_system_comp_date
      ,      lwor_hrv_cby_code
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
      FROM   dl_hrm_works_orders
      WHERE  lwor_dlb_batch_id = p_batch_id
      AND    lwor_dl_load_status IN('L','F','O');
    -- Cursor for Service Request
    CURSOR c_srq_no
      (p_code VARCHAR2)
    IS
      SELECT 'x'
      FROM   service_requests
      WHERE  srq_legacy_refno = p_code;
    -- Cursor for Admin Unit
    CURSOR c_aun_code
      (p_aun_code VARCHAR2)
    IS
      SELECT 'x'
      FROM   admin_units
      WHERE  aun_code = p_aun_code;
    -- Cursor for Pricing Policy Code
    CURSOR c_pol
      (p_pol_code VARCHAR2)
    IS
      SELECT 'x'
      FROM   pricing_policy_groups
      WHERE  ppg_code = p_pol_code;
    -- Cursor for Contractor site
    CURSOR c_con
      (p_con_code VARCHAR2)
    IS
      SELECT 'x'
      FROM   contractor_sites
      WHERE  cos_code = p_con_code;
    -- Cursor for Con Site price groups
    CURSOR c_cspg
      (p_cos VARCHAR2
      ,p_ppg VARCHAR2
      ,p_wpr VARCHAR2
      ,p_rai DATE
      )
    IS
      SELECT 'x'
      FROM   con_site_price_groups
      WHERE  cspg_ppc_cos_code = p_cos
      AND    cspg_ppc_ppp_ppg_code = p_ppg
      AND    cspg_ppc_ppp_wpr_code = p_wpr
      AND    p_rai BETWEEN cspg_start_date AND NVL(cspg_end_date,sysdate);
    -- Cursor for Work Programme;
    CURSOR c_wpr(p_wpr VARCHAR2)
    IS
      SELECT 'X'
      FROM   work_programmes
      WHERE  wpr_code = p_wpr;
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'VALIDATE';
    ct VARCHAR2(30) := 'DL_HRM_WORKS_ORDERS';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    l_exists    VARCHAR2(1);
    l_pro_refno NUMBER(10);
    l_errors    VARCHAR2(10);
    l_error_ind VARCHAR2(10);
    i           INTEGER := 0;
    l_suberror  VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hrm_works_orders.dataload_validate');
    fsc_utils.debug_message('s_dl_hrm_works_orders.dataload_validate',3);
    cb := p_batch_id;
    cd := p_DATE;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.lwor_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors := 'V';
        l_error_ind := 'N';
        --
        -- Check The service Requests Exists
        --
        OPEN c_srq_no(p1.lwor_srq_legacy_refno);
        FETCH c_srq_no INTO l_exists;
        IF c_srq_no%notfound 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',779);
        END IF;
        CLOSE c_srq_no;
        --
        -- Check that at least a prop_ref or admin_unit has been supplied
        --
        IF p1.lwor_pro_propref IS NULL AND p1.lwor_aun_code IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',723);
        END IF;
        --
        -- Check that only one of prop_ref and admin_unit has been supplied
        --
        IF p1.lwor_pro_propref IS NOT NULL AND p1.lwor_aun_code IS NOT NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',724);
        END IF;
        --
        -- Check that the prop_ref supplied exists on PROPERTIES
        --
        IF p1.lwor_pro_propref IS NOT NULL 
        THEN
          IF (NOT s_dl_hem_utils.exists_propref(p1.lwor_pro_propref)) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',726);
          END IF;
        END IF;
        --
        -- Check that the admin unit exists on ADMIN_UNITS
        --
        IF p1.lwor_aun_code IS NOT NULL 
        THEN
          OPEN c_aun_code(p1.lwor_aun_code);
          FETCH c_aun_code INTO l_exists;
          IF c_aun_code%notfound 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',725);
          END IF;
          CLOSE c_aun_code;
        END IF;
        --
        -- Check all the hou_ref_values
        --
        -- Pricing Policy
        --
        OPEN c_pol(p1.lwor_ppc_ppp_ppg_code);
        FETCH c_pol INTO l_exists;
        IF c_pol%notfound 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',717);
        END IF;
        CLOSE c_pol;
        --
        -- Work Program
        --
        l_exists := NULL;
        OPEN c_wpr(p1.lwor_ppc_ppp_wpr_code);
        FETCH c_wpr INTO l_exists;
        IF c_wpr%notfound 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',716);
        END IF;
        CLOSE c_wpr;
        --
        -- Caused By Code
        --
        IF(NOT s_dl_hem_utils.exists_frv('FAU_CAUSE',p1.lwor_hrv_cby_code,'Y')) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',721);
        END IF;
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
        -- Validate Status (works order)
        --
        IF NVL(p1.lwor_sco_code,'^*!') NOT IN('RAI','AUT','ISS','COM','CAN','CLO','HLD') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',734);
        END IF;
        --
        -- Validate Status (works order version)
        --
        IF p1.lwov_sco_code != 'AUT' 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',894);
        END IF;
        --
        -- Validate Relationship between works order and version
        --
        IF p1.lwor_sco_code = 'RAI' 
        THEN
          IF p1.lwov_sco_code != 'AUT' 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',790);
          END IF;
        END IF;
        --
        IF p1.lwor_sco_code IN('AUT','ISS','COM','CLO','CAN') 
        THEN
          IF p1.lwov_sco_code != 'AUT' 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',790);
          END IF;
        END IF;
        --
        -- Validate Confirmation Ind
        --
        IF p1.lwor_confirmation_ind NOT IN('Y','N') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',735);
        END IF;
        --
        -- Validate Print Ind
        --
        IF p1.lwor_print_ind NOT IN('Y','N') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',763);
        END IF;
        --
        -- Validate Contractor Site
        --
        OPEN c_con(p1.lwor_ppc_cos_code);
        FETCH c_con INTO l_exists;
        IF c_con%notfound 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',718);
        END IF;
        CLOSE c_con;
        --
        -- Validate Tenant Ticket Printed
        --
        IF p1.lwor_tenant_ticket_print_ind NOT IN('Y','N') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',785);
        END IF;
        --
        -- Validate Reassign Indicator
        --
        IF p1.lwor_reassign_ind NOT IN('Y','N') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',786);
        END IF;
        --
        -- Validate Default Contractor Indicator
        --
        IF p1.lwor_def_contr_ind NOT IN('Y','N','X') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',787);
        END IF;
        --
        -- Validate Authorised Date if status (Works Order)
        --
        IF p1.lwor_sco_code IN('AUT','ISS','COM','CLO') 
        THEN
          IF p1.lwor_authorised_date IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',739);
          END IF;
        END IF;
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
        -- Validate Issued Date
        --
        IF p1.lwor_sco_code NOT IN('RAI','AUT','HLD') 
        THEN
          IF p1.lwor_issued_date IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',988);
          END IF;
        END IF;
        --
        -- Validate System Complete Date
        --
        IF p1.lwor_sco_code IN('COM','CLO') 
        THEN
          IF p1.lwor_system_comp_date IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',740);
          END IF;
        END IF;
        --
        -- Validate Previous status
        --
        IF p1.lwor_sco_code = 'HLD' 
        THEN
          IF p1.lwor_prev_status_code IS NULL OR p1.lwor_prev_status_code NOT IN('RAI','AUT','ISS','COM','CAN','CLO','HLD') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',744);
          END IF;
        END IF;
        --
        -- Valiate previous Status Date
        --
        IF p1.lwor_sco_code = 'HLD' 
        THEN
          IF p1.lwor_prev_status_date IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',995);
          END IF;
        END IF;
        --
        -- Validate Held Date
        --
        IF p1.lwov_sco_code = 'HLD' 
        THEN
          IF p1.lwov_held_date IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',738);
          END IF;
        END IF;
        --
        -- Validate Version Type
        --
        IF p1.lwov_type != 'C' 
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
        --
        l_suberror := 'N';
        --
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
        -- Validate Invoiced Date
        --
        IF p1.lwov_sco_code = 'CLO' 
        THEN
          IF p1.lwov_invoiced_date IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',741);
          END IF;
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
        -- Validate existance of con site price groups
        --
        OPEN c_cspg(p1.lwor_ppc_cos_code,p1.lwor_ppc_ppp_ppg_code, p1.lwor_ppc_ppp_wpr_code,TRUNC(p1.lwov_raised_date));
        FETCH c_cspg INTO l_exists;
        IF c_cspg%notfound 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',791);
        END IF;
        CLOSE c_cspg;
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
        IF p1.lwov_invoiced_cost IS NULL 
        THEN
          IF p1.lwov_invoiced_tax_amount IS NOT NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',863);
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
      SELECT a1.rowid                  rec_rowid
      ,      a1.lwor_dlb_batch_id
      ,      a1.lwor_dl_seqno
      ,      a1.lwor_DL_LOAD_STATUS
      ,      b1.rowid wov_rowid
      ,      c1.rowid wo_rowid
      FROM   dl_hrm_works_orders a1
      ,      works_order_versions b1
      ,      works_orders c1
      WHERE  a1.lwor_dlb_batch_id = p_batch_id
      AND    a1.lwor_dl_load_status = 'C'
      AND    a1.lwor_legacy_ref = c1.wor_legacy_ref
      AND    c1.wor_srq_no = b1.wov_wor_srq_no
      AND    c1.wor_seqno = b1.wov_wor_seqno;
    cb       VARCHAR2(30);
    cd       DATE;
    cp       VARCHAR2(30) := 'DELETE';
    ct       VARCHAR2(30) := 'DL_HRM_WORKS_ORDERS';
    cs       INTEGER;
    ce       VARCHAR2(200);
    l_an_tab VARCHAR2(1);
    l_id ROWID;
    i INTEGER := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_hrm_works_orders.dataload_delete');
    fsc_utils.debug_message('s_dl_hrm_works_orders.dataload_delete',3);
    cb := p_batch_id;
    cd := p_date;
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.lwor_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        DELETE
        FROM   works_order_versions
        WHERE  rowid = p1.wov_rowid;
        DELETE
        FROM   works_orders
        WHERE  rowid = p1.wo_rowid;
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
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
    END LOOP;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('WORKS_ORDERS');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('WORKS_ORDER_VERSIONS');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_hrm_works_orders;
/