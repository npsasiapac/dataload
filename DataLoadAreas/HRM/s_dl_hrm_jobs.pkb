CREATE OR REPLACE 
PACKAGE BODY s_dl_hrm_jobs
AS
-- ***********************************************************************
--  DESCRIPTION:  
--  
--  CHANGE CONTROL  
--  VERSION  DB VER   WHO  WHEN        WHY  
--      1.0           PH   16-JUL-2001  Product Dataload  
--      1.1  5.1.4    PJD  03-FEB-2002  Changes to Location and VAT Code Processing  
--      1.2  5.1.4    PJD  07-FEB-2002  Combined update of job refno and load status  
--      2.0  5.2.0    PH   08-JUL-2002  Changes for 520, new fields added.  
--                                      Amended create process.  
--      2.1  5.2.0    PJD  15-JUL-2002  Reset l_inv within Validate Proc  
--      2.2  5.3.0    PH   20-DEC-2002  Added nvl to validate on sor_code also only  
--                                      validate vca_code if its not null  
--      2.3  5.3.0    PJD  07-JAN-2003  New column, Budget Year added to help link  
--                                      to the correct budget_year    
--      2.4  5.4.0    PJD  20-MAY-2003  New columns for wov_version and  
--                                      Job Created Date  
--      3.0  5.3.0    PH   03-JUN-2003  Added validate on ljob_liability_type_ind  
--                                      if supplied. Also added value to insert   
--                                      instead of default 'O'  
--      4.0  5.10.0   PH   19-MAY-2006  Added new field job_sco_code to   
--                                      validate and create  
--      4.01 5.10.0   PJD  13-FEB-2007  In Validate proc - Amalgamated three  
--                                      cursors into c_srq.  
--                                      In Validate and Create procs - Default  
--                                      job_Sco_code to wor_sco_code in certain  
--                                      circumstances.  
--      4.2  5.13.0   PH   05-FEB-2008  Added new field ljob_dts_dty_code,  
--                                      included validation. Amended domain  
--                                      for validate on Location from ELELOC to  
--                                      LOCATION. Added additional validate on  
--                                      uniqueness of srq, wor, display seqno.  
--                                      Now includes its own   
--                                      set_record_status_flag procedure.  
--      4.3  5.13.0   PH   21-APR-2008  Corrected c_job_exists cursor  
--                                      (incorrect close statement)  
--      4.4   5.16.0 MB  14-JUL-2010    Amended c_loc_notes cursor to use   
--                                      LOCATION not ELELOC  
--      4.5   6.9.0  MB  31-MAR-2014    Addition of budgets update via batch  
--                                      questions  
--      4.6   6.10.0 MB  03-JUN-2015    corrected handling of null value tax fields
--      4.7   6.10.0 MJK 02-SEP-2015    reformatted for 6.10
--
-- ***********************************************************************
--
--
  PROCEDURE set_record_status_flag
    (p_rowid IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_hrm_jobs
    SET ljob_dl_load_status = p_status
    WHERE rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS THEN
    dbms_output.put_line('Error updating status of dl_hrm_jobs');
    RAISE;
  END set_record_status_flag;
  --
  PROCEDURE dataload_create
    (p_batch_id IN VARCHAR2
    ,p_date IN DATE
    )
  AS
    CURSOR c1
    IS
      SELECT rowid                           rec_rowid
      ,      ljob_dlb_batch_id
      ,      ljob_dl_seqno
      ,      ljob_srq_legacy_refno
      ,      ljob_lwor_legacy_ref
      ,      ljob_order_seqno
      ,      ljob_type
      ,      ljob_hrv_lia_code
      ,      ljob_hrv_trd_code
      ,      ljob_sor_code
      ,      ljob_quantity
      ,      ljob_bhe_code
      ,      ljob_estimated_cost
      ,      ljob_estimated_tax_amount
      ,      ljob_hrv_uom_code
      ,      ljob_pri_code
      ,      ljob_hrv_loc_code
      ,      ljob_description
      ,      ljob_location_notes
      ,      ljob_reported_comp_date
      ,      ljob_system_comp_date
      ,      ljob_target_date
      ,      ljob_vca_code
      ,      ljob_invoiced_cost
      ,      ljob_invoiced_tax_amount
      ,      ljob_invoiced_date
      ,      ljob_hrv_jcl_code
      ,      ljob_comments
      ,      ljob_coverage_amount
      ,      ljob_budget_year
      ,      ljob_liability_type_ind
      ,      ljob_wov_version_no
      ,      ljob_created_date
      ,      ljob_sco_code
      ,      ljob_dts_dty_code
      FROM   dl_hrm_jobs
      WHERE  ljob_dlb_batch_id = p_batch_id
      AND    ljob_dl_load_status = 'V';
    CURSOR c_srq
      (p_srq_no         VARCHAR2
      ,p_leg_ref        VARCHAR2
      ,p_wov_version_no NUMBER
      )
    IS
      SELECT wov_wor_srq_no
      ,      wov_wor_seqno
      ,      wov_version_no
      ,      wov_raised_datetime
      ,      wor_sco_code
      FROM   works_orders
      ,      works_order_versions
      ,      service_requests
      WHERE  wov_wor_srq_no = wor_srq_no
      AND    wor_seqno = wov_wor_seqno
      AND    wor_srq_no = srq_no
      AND    srq_legacy_refno = p_srq_no
      AND    wor_legacy_ref = p_leg_ref
      AND    wov_version_no = NVL(p_wov_version_no,1);
    CURSOR c_bud_calendar
      (p_code VARCHAR
      ,p_date DATE
      )
    IS
      SELECT bud_refno
      FROM   budgets bud
      ,      budget_heads bhe
      ,      budget_calendars bca
      WHERE  bud.bud_bhe_refno = bhe.bhe_refno
      AND    bud.bud_bca_year = bca.bca_year
      AND    p_date >= bca_start_date
      AND    p_date <= bca_end_date
      AND    bhe.bhe_code = p_code;
    CURSOR c_bud_refno
      (p_code VARCHAR2
      ,p_year NUMBER
      )
    IS
      SELECT bud_refno
      FROM   budgets
      ,      budget_heads
      WHERE  bud_bhe_refno = bhe_refno
      AND    bhe_code = p_code
      AND    bud_bca_year = p_year;
    CURSOR c_job_refno
    IS
      SELECT job_refno_seq.nextval
      FROM   dual;
    CURSOR c_loc_notes(p_code VARCHAR2)
    IS
      SELECT frv_name
      FROM   first_ref_values
      WHERE  frv_frd_domain = 'LOCATION'
      AND    frv_code = p_code;
    CURSOR c_invoice
    IS
      SELECT pva_char_value
      FROM   parameter_values
      WHERE  pva_pdu_pdf_name = 'INVOICE'
      AND    pva_pdu_pdf_param_type = 'SYSTEM';
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'CREATE';
    ct VARCHAR2(30) := 'DL_HRM_JOBS';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    l_pro_refno    NUMBER;
    l_an_tab       VARCHAR2(1);
    i              INTEGER := 0;
    l_srq_no       NUMBER;
    l_wov_refno    NUMBER;
    l_version      NUMBER;
    l_wov_raised   DATE;
    l_bud_refno    NUMBER;
    l_job_refno    NUMBER;
    l_loc_note     VARCHAR2(40);
    l_bud_date     DATE;
    l_job_sco_code VARCHAR2(3);
    l_wor_sco_code VARCHAR2(3);
    l_answer       VARCHAR2(1);
    l_ret_type     VARCHAR2(1);
    l_invoice      fsc.parameter_values.pva_char_value%TYPE;
    l_amount       NUMBER(11,2);
    l_tax_amount   NUMBER(11,2);
  BEGIN
    fsc_utils.proc_start('s_dl_hrm_jobs.dataload_create');
    fsc_utils.debug_message('s_dl_hrm_jobs.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    l_answer := s_dl_batches.get_answer(p_batch_id,1);
    l_invoice := 'N';
    OPEN c_invoice;
    FETCH c_invoice INTO l_invoice;
    CLOSE c_invoice;
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.ljob_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        OPEN c_srq(p1.ljob_srq_legacy_refno,p1.ljob_lwor_legacy_ref,p1.ljob_wov_version_no);
        FETCH c_srq INTO l_srq_no,l_wov_refno,l_version,l_wov_raised,l_wor_sco_code;
        CLOSE c_srq;
        l_job_sco_code := NULL;
        IF p1.ljob_sco_code IS NULL 
        THEN
          IF l_wor_sco_code IN('COM','CLO') 
          THEN
            l_job_sco_code := l_wor_sco_code;
          ELSIF l_wor_sco_code IN('CAN') 
          THEN
            l_job_sco_code := 'UNC';
          END IF;
        ELSE
          l_job_sco_code := p1.ljob_sco_code;
        END IF;
        IF p1.ljob_budget_year IS NULL 
        THEN
          l_bud_date := p1.ljob_system_comp_date;
          IF l_bud_date IS NULL 
          THEN
            l_bud_date := l_wov_raised;
          END IF;
          OPEN c_bud_calendar(p1.ljob_bhe_code,l_bud_date);
          FETCH c_bud_calendar INTO l_bud_refno;
          CLOSE c_bud_calendar;
        ELSE
          OPEN c_bud_refno(p1.ljob_bhe_code,p1.ljob_budget_year);
          FETCH c_bud_refno INTO l_bud_refno;
          CLOSE c_bud_refno;
        END IF;
        OPEN c_job_refno;
        FETCH c_job_refno INTO l_job_refno;
        CLOSE c_job_refno;
        l_loc_note := p1.ljob_location_notes;
        IF l_loc_note IS NULL 
        THEN
          OPEN c_loc_notes(p1.ljob_hrv_loc_code);
          FETCH c_loc_notes INTO l_loc_note;
          CLOSE c_loc_notes;
        END IF;
        INSERT
        INTO jobs
        (job_refno
        ,job_wov_wor_srq_no
        ,job_wov_wor_seqno
        ,job_wov_version_no
        ,job_no
        ,job_sor_code
        ,job_type
        ,job_pri_code
        ,job_quantity
        ,job_order_seqno
        ,job_use_preferred_con_ind
        ,job_target_datetime
        ,job_hfi_match_ind
        ,job_hrv_loc_code
        ,job_created_by
        ,job_created_date
        ,job_bud_refno
        ,job_hrv_uom_code
        ,job_hrv_trd_code
        ,job_hrv_lia_code
        ,job_hrv_jcl_code
        ,job_description
        ,job_location_notes
        ,job_estimated_cost
        ,job_estimated_tax_amount
        ,job_reported_comp_datetime
        ,job_system_comp_datetime
        ,job_comments
        ,job_invoiced_cost
        ,job_invoiced_tax_amount
        ,job_invoiced_date
        ,job_vca_code
        ,job_liability_type_ind
        ,job_coverage_amount
        ,job_sco_code
        ,job_dts_dty_code
        )
        VALUES
        (l_job_refno
        ,l_srq_no
        ,l_wov_refno
        ,l_version
        ,p1.ljob_order_seqno
        ,p1.ljob_sor_code
        ,p1.ljob_type
        ,p1.ljob_pri_code
        ,p1.ljob_quantity
        ,p1.ljob_order_seqno
        ,'N'
        ,p1.ljob_target_date
        ,'N'
        ,p1.ljob_hrv_loc_code
        ,'DATALOAD'
        ,TRUNC(sysdate)
        ,l_bud_refno
        ,p1.ljob_hrv_uom_code
        ,p1.ljob_hrv_trd_code
        ,p1.ljob_hrv_lia_code
        ,p1.ljob_hrv_jcl_code
        ,p1.ljob_description
        ,l_loc_note
        ,p1.ljob_estimated_cost
        ,p1.ljob_estimated_tax_amount
        ,p1.ljob_reported_comp_date
        ,p1.ljob_system_comp_date
        ,p1.ljob_comments
        ,p1.ljob_invoiced_cost
        ,p1.ljob_invoiced_tax_amount
        ,p1.ljob_invoiced_date
        ,p1.ljob_vca_code
        ,NVL(p1.ljob_liability_type_ind,'O')
        ,p1.ljob_coverage_amount
        ,l_job_sco_code
        ,p1.ljob_dts_dty_code
        );
        IF l_answer = 'Y' 
        THEN
          l_ret_type := NULL;
          l_amount := NULL;
          l_tax_amount := NULL;
          IF l_wor_sco_code IN('ISS','AUT') 
          THEN
            l_ret_type := 'C';
            l_amount := NVL(p1.ljob_estimated_cost,0);
            l_tax_amount := NVL(p1.ljob_estimated_tax_amount,0);
            UPDATE budgets
            SET    bud_committed = NVL(bud_committed,0) + l_amount
            ,      bud_tax_committed = NVL(bud_tax_committed,0) + l_tax_amount
            WHERE  bud_refno = l_bud_refno;
          ELSIF l_wor_sco_code = 'COM' 
          THEN
            l_ret_type := 'A';
            l_amount := NVL(p1.ljob_estimated_cost,0);
            l_tax_amount := NVL(p1.ljob_estimated_tax_amount,0);
            UPDATE budgets
            SET    bud_accrued = NVL(bud_accrued,0) + l_amount
            ,      bud_tax_accrued = NVL(bud_tax_accrued,0) + l_tax_amount
            WHERE  bud_refno = l_bud_refno;
          ELSIF l_wor_sco_code = 'CLO' AND l_invoice = 'Y' 
          THEN
            l_ret_type := 'E';
            l_amount := NVL(NVL(p1.ljob_invoiced_cost,p1.ljob_estimated_cost),0);
            l_tax_amount := NVL(NVL(p1.ljob_invoiced_tax_amount,p1.ljob_estimated_tax_amount),0);
            UPDATE budgets
            SET    bud_expended = NVL(bud_expended,0) + l_amount
            ,      bud_tax_expended = NVL(bud_tax_expended,0) + l_tax_amount
            WHERE  bud_refno = l_bud_refno;
          ELSIF l_wor_sco_code = 'CLO' AND l_invoice = 'N' 
          THEN
            l_ret_type := 'I';
            l_amount := NVL(NVL(p1.ljob_invoiced_cost,p1.ljob_estimated_cost),0);
            l_tax_amount := NVL(NVL(p1.ljob_invoiced_tax_amount,p1.ljob_estimated_tax_amount),0);
            UPDATE budgets
            SET    bud_invoiced = NVL(bud_invoiced,0) + l_amount
            ,      bud_tax_invoiced = NVL(bud_tax_invoiced,0) + l_tax_amount
            WHERE  bud_refno = l_bud_refno;
          END IF;
          IF l_ret_type IS NOT NULL 
          THEN
            INSERT INTO repairs_transactions
            (RET_REFNO
            ,RET_DATETIME
            ,RET_AMOUNT
            ,RET_TAX_AMOUNT
            ,RET_TYPE
            ,RET_JOB_CMI_REFNO
            ,RET_HDT_CODE
            ,RET_BUD_REFNO
            )
            VALUES
            (ret_refno_seq.nextval
            ,sysdate
            ,l_amount
            ,l_tax_amount
            ,'J'
            ,l_job_refno
            ,l_ret_type
            ,l_bud_refno
            );
          END IF;
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        UPDATE dl_hrm_jobs
        SET    ljob_refno = l_job_refno
        ,      ljob_dl_load_status = 'C'
        WHERE  ljob_dlb_batch_id = p1.ljob_dlb_batch_id
        AND    ljob_dl_seqno = p1.ljob_dl_seqno;
        i := i + 1;
        IF MOD(i,1000) = 0 
        THEN
          COMMIT;
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
    l_an_tab := s_dl_hem_utils.dl_comp_stats('JOBS');
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
    ,p_date IN DATE
    )
  AS
    CURSOR c1
    IS
      SELECT rowid                            rec_rowid
      ,      ljob_dlb_batch_id
      ,      ljob_dl_seqno
      ,      ljob_srq_legacy_refno
      ,      ljob_lwor_legacy_ref
      ,      ljob_order_seqno
      ,      ljob_type
      ,      ljob_hrv_lia_code
      ,      ljob_hrv_trd_code
      ,      ljob_sor_code
      ,      ljob_quantity
      ,      ljob_bhe_code
      ,      ljob_estimated_cost
      ,      ljob_estimated_tax_amount
      ,      ljob_hrv_uom_code
      ,      ljob_pri_code
      ,      ljob_hrv_loc_code
      ,      ljob_description
      ,      ljob_location_notes
      ,      ljob_reported_comp_date
      ,      ljob_system_comp_date
      ,      ljob_target_date
      ,      ljob_vca_code
      ,      ljob_invoiced_cost
      ,      ljob_invoiced_tax_amount
      ,      ljob_invoiced_date
      ,      ljob_hrv_jcl_code
      ,      ljob_comments
      ,      ljob_budget_year
      ,      ljob_liability_type_ind
      ,      ljob_wov_version_no
      ,      ljob_sco_code
      ,      ljob_dts_dty_code
      FROM   dl_hrm_jobs
      WHERE  ljob_dlb_batch_id = p_batch_id
      AND    ljob_dl_load_status IN('L','F','O');
    CURSOR c_srq
      (p_srq_no         VARCHAR2
      ,p_leg_ref        VARCHAR2
      ,p_wov_version_no NUMBER
      )
    IS
      SELECT wov_wor_srq_no
      ,      wov_wor_seqno
      ,      wov_version_no
      ,      wov_raised_datetime
      ,      wor_sco_code
      ,      wov_sco_code
      ,      wov_invoiced_datetime
      FROM   works_orders
      ,      works_order_versions
      ,      service_requests
      WHERE  wov_wor_srq_no = wor_srq_no
      AND    wor_seqno = wov_wor_seqno
      AND    wor_srq_no = srq_no
      AND    srq_legacy_refno = p_srq_no
      AND    wor_legacy_ref = p_leg_ref
      AND    wov_version_no = NVL(p_wov_version_no,1);
    CURSOR c_bud_calendar
      (p_code VARCHAR2
      ,p_date DATE
      )
    IS
      SELECT bud_refno
      FROM   budgets bud
      ,      budget_heads bhe
      ,      budget_calendars bca
      WHERE  bud.bud_bhe_refno = bhe.bhe_refno
      AND    bud.bud_bca_year = bca.bca_year
      AND    p_date >= bca_start_date
      AND    p_date <= bca_end_date
      AND    bhe.bhe_code = p_code;
    CURSOR c_bud_refno
      (p_code VARCHAR2
      ,p_year NUMBER
      )
    IS
      SELECT bud_refno
      FROM   budgets
      ,      budget_heads
      WHERE  bud_bhe_refno = bhe_refno
      AND    bhe_code = p_code
      AND    bud_bca_year = p_year;
    CURSOR c_sor
      (p_sor_code VARCHAR2)
    IS
      SELECT 'x'
      FROM   schedule_of_rates
      WHERE  sor_code = p_sor_code;
    CURSOR c_pri
      (p_pri_code VARCHAR2)
    IS
      SELECT 'x'
      FROM   priorities
      WHERE  pri_code = p_pri_code;
    CURSOR c_vat
      (p_vat_code VARCHAR2)
    IS
      SELECT 'x'
      FROM   vat_categories
      WHERE  vca_code = p_vat_code;
    CURSOR c_dty_code
      (p_sor_code VARCHAR2,p_dty_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   defect_type_sors
      WHERE  dts_sor_code = p_sor_code
      AND    dts_dty_code = p_dty_code;
    CURSOR c_job_exists
      (p_srq_no    NUMBER
      ,p_wov_refno NUMBER
      ,p_version   NUMBER
      ,p_job_no    NUMBER
      )
    IS
      SELECT 'X'
      FROM   jobs
      WHERE  job_wov_wor_srq_no = p_srq_no
      AND    job_wov_wor_seqno = p_wov_refno
      AND    job_wov_version_no = p_version
      AND    job_no = p_job_no;
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'VALIDATE';
    ct VARCHAR2(30) := 'DL_HRM_JOBS';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    l_exists       VARCHAR2(1);
    l_pro_refno    NUMBER(10);
    l_errors       VARCHAR2(10);
    l_error_ind    VARCHAR2(10);
    i              INTEGER := 0;
    l_srq_no       NUMBER(8);
    l_wov_refno    NUMBER(3);
    l_version      NUMBER(3);
    l_wov_raised   DATE;
    l_wov_inv      DATE;
    l_bud_refno    INTEGER;
    l_bud_date     DATE;
    l_job_sco_code VARCHAR2(3);
    l_wor_sco_code VARCHAR2(3);
    l_wov_sco_code VARCHAR2(3);
  BEGIN
    fsc_utils.proc_start('s_dl_hrm_jobs.dataload_validate');
    fsc_utils.debug_message('s_dl_hrm_jobs.dataload_validate',3);
    cb := p_batch_id;
    cd := p_DATE;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.ljob_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors := 'V';
        l_error_ind := 'N';
        l_wor_sco_code := NULL;
        l_wov_sco_code := NULL;
        l_wov_inv := NULL;
        OPEN c_srq(p1.ljob_srq_legacy_refno,p1.ljob_lwor_legacy_ref,p1.ljob_wov_version_no);
        FETCH c_srq INTO l_srq_no,l_wov_refno,l_version,l_wov_raised,l_wor_sco_code,l_wov_sco_code,l_wov_inv;
        IF c_srq%notfound 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',746);
        END IF;
        CLOSE c_srq;
        l_job_Sco_code := NULL;
        l_job_Sco_code := NULL;
        IF p1.ljob_sco_code IS NULL 
        THEN
          IF l_wor_sco_code IN('COM','CLO') 
          THEN
            l_job_sco_code := l_wor_sco_code;
          ELSIF l_wor_sco_code IN('CAN') 
          THEN
            l_job_sco_code := 'UNC';
          END IF;
        ELSE
          l_job_sco_code := p1.ljob_sco_code;
        END IF;
        IF(NOT s_dl_hem_utils.exists_frv('LIABLE',p1.ljob_hrv_lia_code,'Y')) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',747);
        END IF;
        IF(NOT s_dl_hem_utils.exists_frv('TRADE',p1.ljob_hrv_trd_code,'Y')) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',748);
        END IF;
        IF(NOT s_dl_hem_utils.exists_frv('UNITS',p1.ljob_hrv_uom_code,'Y')) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',749);
        END IF;
        IF p1.ljob_hrv_jcl_code IS NOT NULL 
        THEN
          IF(NOT s_dl_hem_utils.exists_frv('JOBCLASS',p1.ljob_hrv_jcl_code,'Y')) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',750);
          END IF;
        END IF;
        OPEN c_pri(p1.ljob_pri_code);
        FETCH c_pri
        INTO l_exists;
        IF c_pri%notfound 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',731);
        END IF;
        CLOSE c_pri;
        IF(NOT s_dl_hem_utils.exists_frv('LOCATION',p1.ljob_hrv_loc_code,'Y')) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',773);
        END IF;
        IF p1.ljob_vca_code IS NOT NULL 
        THEN
          OPEN c_vat(p1.ljob_vca_code);
          FETCH c_vat
          INTO l_exists;
          IF c_vat%notfound 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',705);
          END IF;
          CLOSE c_vat;
        END IF;
        IF upper(NVL(p1.ljob_sor_code,'*!~#')) != 'MANUAL' 
        THEN
          OPEN c_sor(p1.ljob_sor_code);
          FETCH c_sor INTO l_exists;
          IF c_sor%notfound 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',715);
          END IF;
          CLOSE c_sor;
        END IF;
        l_bud_refno := NULL;
        IF p1.ljob_budget_year IS NULL 
        THEN
          l_bud_date := p1.ljob_system_comp_date;
          IF l_bud_date IS NULL 
          THEN
            l_bud_date := l_wov_raised;
          END IF;
          OPEN c_bud_calendar(p1.ljob_bhe_code,l_bud_date);
          FETCH c_bud_calendar INTO l_bud_refno;
          CLOSE c_bud_calendar;
        ELSE
          OPEN c_bud_refno(p1.ljob_bhe_code,p1.ljob_budget_year);
          FETCH c_bud_refno INTO l_bud_refno;
          CLOSE c_bud_refno;
        END IF;
        IF l_bud_refno IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',752);
        END IF;
        IF(l_wov_sco_code = 'COM' AND p1.ljob_system_comp_date IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',753);
        END IF;
        IF p1.ljob_type NOT IN('DEF','DIS','SOR') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',792);
        END IF;
        IF(p1.ljob_location_notes IS NULL AND p1.ljob_hrv_loc_code IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',783);
        END IF;
        IF p1.ljob_quantity IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',793);
        END IF;
        IF p1.ljob_estimated_cost IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',794);
        END IF;
        IF l_wov_inv IS NOT NULL 
        THEN
          IF p1.ljob_invoiced_cost IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',754);
          END IF;
          IF p1.ljob_invoiced_date IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',755);
          END IF;
        END IF;
        IF p1.ljob_invoiced_cost IS NULL 
        THEN
          IF p1.ljob_invoiced_tax_amount IS NOT NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',863);
          END IF;
        END IF;
        IF NVL(p1.ljob_liability_type_ind,'O') NOT IN('O','F','S') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',993);
        END IF;
        IF NVL(l_job_sco_code,'XYZ') NOT IN('CLO','COM','UNC') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',111);
        END IF;
        IF p1.ljob_type = 'DEF' 
        THEN
          OPEN c_dty_code(p1.ljob_sor_code,p1.ljob_dts_dty_code);
          FETCH c_dty_code INTO l_exists;
          IF c_dty_code%notfound 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',525);
          END IF;
          CLOSE c_dty_code;
        END IF;
        OPEN c_job_exists(l_srq_no,l_wov_refno,l_version,p1.ljob_order_seqno);
        FETCH c_job_exists INTO l_exists;
        IF c_job_exists%found 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',526);
        END IF;
        CLOSE c_job_exists;
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
      SELECT rowid                              rec_rowid
      ,      ljob_dlb_batch_id
      ,      ljob_dl_seqno
      ,      ljob_dl_load_status
      ,      ljob_refno
      FROM   dl_hrm_jobs
      WHERE  ljob_dlb_batch_id = p_batch_id
      AND    ljob_dl_load_status = 'C';
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'DELETE';
    ct VARCHAR2(30) := 'DL_HRM_JOBS';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    l_an_tab VARCHAR2(1);
    i        INTEGER := 0;
  BEGIN
    cb := p_batch_id;
    cd := p_date;
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.ljob_dl_seqno;
        l_id := p1.rec_rowid;
        DELETE
        FROM   jobs
        WHERE  job_refno = p1.ljob_refno;
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
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
        set_record_status_flag(l_id,'C');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
    END LOOP;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('JOBS');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_hrm_jobs;
/
