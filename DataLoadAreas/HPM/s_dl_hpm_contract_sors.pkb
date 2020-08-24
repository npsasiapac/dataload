--
CREATE OR REPLACE PACKAGE BODY s_dl_hpm_contract_sors
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.1     5.16.1    PH   22-DEC-2009  Initial Creation.
--  1.2     5.16.1    PH   20-JAN-2010  Added in insert into Contract
--                                      SOR Prices
--  1.3     5.16.1    PH   21-JAN-2010  Corrected error on 688 from HDL
--                                      to HD1 and corrected check on SOR
--  1.4     6.11.0    MJK  24-AUG-2015  Reformatted for 6.11. No logic changes
--
--
--
--  declare package variables AND constants
--
-- ***********************************************************************
  PROCEDURE set_record_status_flag
    (p_rowid  IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_hpm_contract_sors
    SET    lcvs_dl_load_status = p_status
    WHERE  rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_hpm_contract_sors');
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
      ,      lcvs_dlb_batch_id
      ,      lcvs_dl_seqno
      ,      lcvs_dl_load_status
      ,      lcvs_cnt_reference
      ,      lcvs_cve_version_number
      ,      lcvs_sor_code
      ,      NVL(lcvs_current_ind,'Y') lcvs_current_ind
      ,      lcvs_repeat_unit
      ,      lcvs_repeat_period_ind
      ,      lcpc_start_date
      ,      lcpc_price
      ,      lcpc_end_date
      FROM   dl_hpm_contract_sors
      WHERE  lcvs_dlb_batch_id = p_batch_id
      AND    lcvs_dl_load_status = 'V';
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'CREATE';
    ct VARCHAR2(30) := 'DL_HPM_CONTRACT_SORS';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    l_an_tab VARCHAR2(1);
    i        INTEGER := 0;
    l_exists VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_contract_sors.dataload_create');
    fsc_utils.debug_message('s_dl_hpm_contract_sors.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.lcvs_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        INSERT INTO contract_sors
        (cvs_cve_cnt_reference  
        ,cvs_cve_version_number  
        ,cvs_sor_code  
        ,cvs_current_ind  
        ,cvs_created_by  
        ,cvs_created_date  
        ,cvs_repeat_unit  
        ,cvs_repeat_period_ind  
        )  
        VALUES  
        (p1.lcvs_cnt_reference  
        ,p1.lcvs_cve_version_number  
        ,p1.lcvs_sor_code  
        ,p1.lcvs_current_ind  
        ,'DATALOAD'  
        ,sysdate  
        ,p1.lcvs_repeat_unit  
        ,p1.lcvs_repeat_period_ind  
        );  
        IF p1.lcpc_price IS NOT NULL 
        THEN
          INSERT INTO contract_sor_prices
          (cpc_cvs_cve_cnt_reference  
          ,cpc_cvs_cve_version_number  
          ,cpc_cvs_sor_code  
          ,cpc_start_date  
          ,cpc_price  
          ,cpc_created_by  
          ,cpc_created_date  
          ,cpc_end_date  
          )  
          VALUES  
          (p1.lcvs_cnt_reference  
          ,p1.lcvs_cve_version_number  
          ,p1.lcvs_sor_code  
          ,p1.lcpc_start_date  
          ,p1.lcpc_price  
          ,'DATALOAD'  
          ,sysdate  
          ,p1.lcpc_end_date  
          );  
        END IF;
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
    COMMIT;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('CONTRACT_SORS');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('CONTRACT_SOR_PRICES');
    fsc_utils.proc_END;
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
      SELECT rowid rec_rowid
      ,      lcvs_dlb_batch_id
      ,      lcvs_dl_seqno
      ,      lcvs_dl_load_status
      ,      lcvs_cnt_reference
      ,      lcvs_cve_version_number
      ,      lcvs_sor_code
      ,      NVL(lcvs_current_ind,'Y') lcvs_current_ind
      ,      lcvs_repeat_unit
      ,      lcvs_repeat_period_ind
      ,      lcpc_start_date
      ,      lcpc_price
      ,      lcpc_end_date
      FROM   dl_hpm_contract_sors
      WHERE  lcvs_dlb_batch_id = p_batch_id
      AND    lcvs_dl_load_status IN('L','F','O');
    CURSOR c_cve_chk(p_cnt_reference VARCHAR2,p_cve_version NUMBER)
    IS
      SELECT 'X'
      FROM   contract_versions
      WHERE  cve_cnt_reference = p_cnt_reference
      AND    cve_version_number = p_cve_version;
    CURSOR c_cvs_chk(p_cnt_reference VARCHAR2,p_cve_version NUMBER,p_sor_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   contract_sors
      WHERE  cvs_cve_cnt_reference = p_cnt_reference
      AND    cvs_cve_version_number = p_cve_version
      AND    cvs_sor_code = p_sor_code;
    CURSOR c_sor_chk(p_sor_code VARCHAR2)
    IS
      SELECT 'X'
      FROM   schedule_of_rates
      WHERE  sor_code = p_sor_code;
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'VALIDATE';
    ct VARCHAR2(30) := 'DL_HPM_CONTRACT_SORS';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    l_exists    VARCHAR2(1);
    l_pro_refno NUMBER(10);
    l_pro_aun   VARCHAR2(20);
    l_errors    VARCHAR2(10);
    l_error_ind VARCHAR2(10);
    i           INTEGER := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_contract_sors.dataload_validate');
    fsc_utils.debug_message('s_dl_hpm_contract_sors.dataload_validate',3);
    cb := p_batch_id;
    cd := p_DATE;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.lcvs_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors := 'V';
        l_error_ind := 'N';
        OPEN c_cve_chk(p1.lcvs_cnt_reference,p1.lcvs_cve_version_number);
        FETCH c_cve_chk INTO l_exists;
        IF c_cve_chk%NOTFOUND 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',507);
        END IF;
        CLOSE c_cve_chk;
        OPEN c_cvs_chk(p1.lcvs_cnt_reference,p1.lcvs_cve_version_number,p1.lcvs_sor_code);
        FETCH c_cvs_chk INTO l_exists;
        IF c_cvs_chk%FOUND 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',688);
        END IF;
        CLOSE c_cvs_chk;
        IF p1.lcvs_sor_code IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',180);
        ELSE
          OPEN c_sor_chk(p1.lcvs_sor_code);
          FETCH c_sor_chk INTO l_exists;
          IF c_sor_chk%NOTFOUND 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',621);
          END IF;
          CLOSE c_sor_chk;
        END IF;
        IF p1.lcvs_repeat_period_ind IS NOT NULL 
        THEN
          IF p1.lcvs_repeat_period_ind NOT IN('D','W','M','Y') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',399);
          END IF;
        END IF;
        IF p1.lcvs_repeat_period_ind IS NOT NULL AND p1.lcvs_repeat_unit IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',689);
        END IF;
        IF p1.lcvs_repeat_unit IS NOT NULL AND p1.lcvs_repeat_period_ind IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',690);
        END IF;
        IF p1.lcvs_cnt_reference IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',691);
        END IF;
        IF p1.lcvs_cve_version_number IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',297);
        END IF;
        IF p1.lcpc_start_date IS NOT NULL AND p1.lcpc_price IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',692);
        END IF;
        IF p1.lcpc_price IS NOT NULL AND p1.lcpc_start_date IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',693);
        END IF;
        IF p1.lcpc_end_date IS NOT NULL 
        THEN
          IF p1.lcpc_end_date < p1.lcpc_start_date 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',188);
          END IF;
        END IF;
        IF(l_errors = 'F') 
        THEN
          l_error_ind := 'Y';
        ELSE
          l_error_ind := 'N';
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
        set_record_status_flag(l_id,l_errors);
        i := i + 1;
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
    fsc_utils.proc_END;
    COMMIT;
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
    IS
      SELECT rowid rec_rowid
      ,      lcvs_dlb_batch_id
      ,      lcvs_dl_seqno
      ,      lcvs_dl_load_status
      ,      lcvs_cnt_reference
      ,      lcvs_cve_version_number
      ,      lcvs_sor_code
      ,      lcpc_start_date
      ,      lcpc_price
      ,      lcpc_end_date
      FROM   dl_hpm_contract_sors
      WHERE  lcvs_dlb_batch_id = p_batch_id
      AND lcvs_dl_load_status = 'C';
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'DELETE';
    ct VARCHAR2(30) := 'DL_HPM_CONTRACT_SORS';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    l_an_tab VARCHAR2(1);
    l_exists VARCHAR2(1);
    i        INTEGER := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_contract_sors.dataload_delete');
    fsc_utils.debug_message('s_dl_hpm_contract_sors.dataload_delete',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.lcvs_dl_seqno;
        l_id := p1.rec_rowid;
        i := i + 1;
        IF p1.lcpc_price IS NOT NULL 
        THEN
          DELETE
          FROM  contract_sor_prices
          WHERE cpc_cvs_cve_cnt_reference = p1.lcvs_cnt_reference
          AND   cpc_cvs_cve_version_number = p1.lcvs_cve_version_number
          AND   cpc_cvs_sor_code = p1.lcvs_sor_code
          AND   cpc_start_date = p1.lcpc_start_date
          AND   cpc_price = p1.lcpc_price;
        END IF;
        DELETE
        FROM  contract_sors
        WHERE cvs_cve_cnt_reference = p1.lcvs_cnt_reference
        AND   cvs_cve_version_number = p1.lcvs_cve_version_number
        AND   cvs_sor_code = p1.lcvs_sor_code;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        set_record_status_flag(l_id,'V');
        IF mod(i,5000) = 0 
        THEN
          COMMIT;
        END IF;
      EXCEPTION
      WHEN OTHERS 
      THEN
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
        set_record_status_flag(l_id,'C');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
    END LOOP;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('CONTRACT_SORS');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('CONTRACT_SOR_PRICES');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_hpm_contract_sors;
/
