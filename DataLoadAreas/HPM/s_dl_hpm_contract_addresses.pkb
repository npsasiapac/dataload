CREATE OR REPLACE PACKAGE BODY s_dl_hpm_contract_addresses
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VER  DB Ver  WHO   WHEN          WHY
--  1.0  5.12    VRS   12-SEP-2007   Product Dataload
--  1.1  6.11.0  MJK   24-Aug-2015   Reformatted for 6.11. No logic changes
--  1.2  6.13    PJD   06-Jun-2016   Changed delete sql 
--
--  declare package variables AND constants
--
--
-- ***********************************************************************
  PROCEDURE dataload_create
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  AS
    CURSOR c1(p_batch_id VARCHAR2)
    IS
      SELECT rowid rec_rowid
      ,      lcad_dlb_batch_id
      ,      lcad_dl_seqno
      ,      lcad_dl_load_status
      ,      lcad_cad_cnt_reference
      ,      lcad_cad_pro_aun_code
      ,      lcad_cad_type_ind
      ,      lcad_cai_start_date
      ,      lcad_cai_end_date
      ,      lcad_cai_comments
      ,      lcad_cai_cse_section_number
      ,      lcad_cai_hrv_caa_code
      ,      lcad_cai_hrv_cat_code
      FROM   dl_hpm_contract_addresses
      WHERE  lcad_dlb_batch_id = p_batch_id
      AND    lcad_dl_load_status = 'V';
    CURSOR c_get_pro_refno(p_propref VARCHAR2)
    IS
      SELECT pro_refno
      FROM   properties
      WHERE  pro_propref = p_propref;
    cb             VARCHAR2(30);
    cd             DATE;
    cp             VARCHAR2(30) := 'CREATE';
    ct             VARCHAR2(30) := 'DL_HPM_CONTRACT_ADDRESSES';
    cs             INTEGER;
    ce             VARCHAR2(200);
    ci             INTEGER;
    l_pro_refno    NUMBER(10);
    l_pro_aun_code VARCHAR2(20);
    l_scs_refno    NUMBER(8);
    i              INTEGER := 0;
    l_an_tab       VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_contract_addresses.dataload_create');
    fsc_utils.debug_message('s_dl_hpm_contract_addresses.dataload_create',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    ci := s_dl_hem_utils.dl_orig_rows('DL_HPM_CONTRACT_ADDRESSES');
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.lcad_dl_seqno;
        SAVEPOINT SP1;
        IF (p1.lcad_cad_type_ind = 'P') 
        THEN
          l_pro_refno := NULL;
          OPEN c_get_pro_refno(p1.lcad_cad_pro_aun_code);
          FETCH c_get_pro_refno INTO l_pro_refno;
          CLOSE c_get_pro_refno;
          INSERT INTO contract_addresses
          (cad_cnt_reference  
          ,cad_pro_aun_code   
          ,cad_type_ind   
          )  
          VALUES  
          (p1.lcad_cad_cnt_reference  
          ,TO_CHAR(l_pro_refno)  
          ,p1.lcad_cad_type_ind  
          );  
          INSERT INTO contract_address_instances
          (cai_cad_cnt_reference  
          ,cai_cad_pro_aun_code   
          ,cai_cad_type_ind   
          ,cai_start_date   
          ,cai_end_date   
          ,cai_comments   
          ,cai_cse_section_number   
          ,cai_hrv_caa_code   
          ,cai_hrv_cat_code   
          )  
          VALUES  
          (p1.lcad_cad_cnt_reference  
          ,TO_CHAR(l_pro_refno)  
          ,p1.lcad_cad_type_ind  
          ,p1.lcad_cai_start_date  
          ,p1.lcad_cai_end_date  
          ,p1.lcad_cai_comments  
          ,p1.lcad_cai_cse_section_number  
          ,p1.lcad_cai_hrv_caa_code  
          ,p1.lcad_cai_hrv_cat_code  
          );  
        ELSE
          INSERT INTO contract_addresses
          (cad_cnt_reference  
          ,cad_pro_aun_code   
          ,cad_type_ind   
          )  
          VALUES  
          (p1.lcad_cad_cnt_reference  
          ,p1.lcad_cad_pro_aun_code  
          ,p1.lcad_cad_type_ind  
          );  
          INSERT INTO contract_address_instances
          (cai_cad_cnt_reference  
          ,cai_cad_pro_aun_code  
          ,cai_cad_type_ind  
          ,cai_start_date  
          ,cai_end_date  
          ,cai_comments  
          ,cai_cse_section_number  
          ,cai_hrv_caa_code  
          ,cai_hrv_cat_code  
          )  
          VALUES  
          (p1.lcad_cad_cnt_reference  
          ,p1.lcad_cad_pro_aun_code  
          ,p1.lcad_cad_type_ind  
          ,p1.lcad_cai_start_date  
          ,p1.lcad_cai_end_date  
          ,p1.lcad_cai_comments  
          ,p1.lcad_cai_cse_section_number  
          ,p1.lcad_cai_hrv_caa_code  
          ,p1.lcad_cai_hrv_cat_code  
          );  
        END IF;
        i := i + 1;
        IF MOD(i,1000) = 0 
        THEN
          COMMIT;
        END IF;
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
    l_an_tab := s_dl_hem_utils.dl_comp_stats('CONTRACT_ADDRESSES',ci,i);
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_create;
  --
  PROCEDURE dataload_validate
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  AS
    CURSOR c1 (p_batch_id VARCHAR2)
    IS
      SELECT rowid rec_rowid
      ,      lcad_dlb_batch_id
      ,      lcad_dl_seqno
      ,      lcad_dl_load_status
      ,      lcad_cad_cnt_reference
      ,      lcad_cad_pro_aun_code
      ,      lcad_cad_type_ind
      ,      lcad_cai_start_date
      ,      lcad_cai_end_date
      ,      lcad_cai_comments
      ,      lcad_cai_cse_section_number
      ,      lcad_cai_hrv_caa_code
      ,      lcad_cai_hrv_cat_code
      FROM   dl_hpm_contract_addresses
      WHERE  lcad_dlb_batch_id = p_batch_id
      AND    lcad_dl_load_status IN('L','F','O');

    CURSOR chk_contract(p_cnt_reference VARCHAR2)
    IS
      SELECT 'X'
      FROM   contracts
      WHERE  cnt_reference = p_cnt_reference;
    CURSOR chk_contract_section(p_cnt_reference VARCHAR2
                               ,p_cse_section_number NUMBER)
    IS
      SELECT 'X'
      FROM   contract_sections
      WHERE  cse_cnt_reference = p_cnt_reference
      AND    cse_section_number = p_cse_section_number;
    CURSOR chk_cad_exists(p_cad_cnt_reference VARCHAR2
                         ,p_cad_pro_aun_code VARCHAR2
                         ,p_cad_type_ind VARCHAR2)
    IS
      SELECT 'X'
      FROM   contract_addresses
      WHERE  cad_cnt_reference = p_cad_cnt_reference
      AND    cad_pro_aun_code = p_cad_pro_aun_code
      AND    cad_type_ind = p_cad_type_ind;
    CURSOR chk_cai_exists
      (p_cai_cad_cnt_reference VARCHAR2
      ,p_cai_cad_pro_aun_code  VARCHAR2
      ,p_cai_cad_type_ind      VARCHAR2
      ,p_cai_start_date        DATE
      )
    IS
      SELECT 'X'
      FROM   contract_address_instances
      WHERE  cai_cad_cnt_reference = p_cai_cad_cnt_reference
      AND    cai_cad_pro_aun_code = p_cai_cad_pro_aun_code
      AND    cai_cad_type_ind = p_cai_cad_type_ind
      AND    cai_start_date = p_cai_start_date;
    cb           VARCHAR2(30);
    cd           DATE;
    cp           VARCHAR2(30) := 'VALIDATE';
    ct           VARCHAR2(30) := 'DL_HPM_CONTRACT_ADDRESSES';
    cs           INTEGER;
    ce           VARCHAR2(200);
    l_pro_refno  NUMBER(10);
    l_aun_code   VARCHAR2(20);
    l_cnt_exists VARCHAR2(1);
    l_cad_exists VARCHAR2(1);
    l_cai_exists VARCHAR2(1);
    l_cse_exists VARCHAR2(1);
    l_errors     VARCHAR2(1);
    l_error_ind  VARCHAR2(1);
    i            INTEGER := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_contract_addresses.dataload_validate');
    fsc_utils.debug_message('s_dl_hpm_contract_addresses.dataload_validate',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.lcad_dl_seqno;
        l_errors := 'V';
        l_error_ind := 'N';
        IF (p1.lcad_cad_cnt_reference IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',291);
        END IF;
        IF (p1.lcad_cad_pro_aun_code IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',330);
        END IF;
        IF (p1.lcad_cad_type_ind IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',331);
        END IF;
        IF (p1.lcad_cai_start_date IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',332);
        END IF;
        l_cnt_exists := NULL;
        OPEN chk_contract(p1.lcad_cad_cnt_reference);
        FETCH chk_contract INTO l_cnt_exists;
        CLOSE chk_contract;
        IF (l_cnt_exists IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',333);
        END IF;
        IF p1.lcad_cad_type_ind = 'A' 
        THEN
          IF (NOT s_dl_hem_utils.exists_aun_code(p1.lcad_cad_pro_aun_code)) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
          END IF;
        ELSE
          IF (NOT s_dl_hem_utils.exists_propref(p1.lcad_cad_pro_aun_code))
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
          END IF;
        END IF;
        IF (p1.lcad_cad_type_ind NOT IN('A','P')) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',334);
        END IF;
        IF (p1.lcad_cai_hrv_caa_code IS NOT NULL) 
        THEN
          IF (NOT s_dl_hem_utils.exists_frv('CADDADDN'
                                           ,p1.lcad_cai_hrv_caa_code,'Y')) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',335);
          END IF;
        END IF;
        IF (p1.lcad_cai_hrv_cat_code IS NOT NULL) 
        THEN
          IF (NOT s_dl_hem_utils.exists_frv('CADDTERM'
                                           ,p1.lcad_cai_hrv_cat_code,'Y')) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',336);
          END IF;
        END IF;
        IF (p1.lcad_cai_cse_section_number IS NOT NULL) 
        THEN
          l_cse_exists := NULL;
          OPEN chk_contract_section(p1.lcad_cad_cnt_reference
                                   ,p1.lcad_cai_cse_section_number);
          FETCH chk_contract_section INTO l_cse_exists;
          CLOSE chk_contract_section;

          IF (l_cse_exists IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',337);
          END IF;
        END IF;
        IF (p1.lcad_cad_cnt_reference IS NOT NULL 
            AND p1.lcad_cad_pro_aun_code IS NOT NULL 
            AND p1.lcad_cad_type_ind IS NOT NULL) 
        THEN
          l_cad_exists := NULL;
          OPEN chk_cad_exists(p1.lcad_cad_cnt_reference
                             ,p1.lcad_cad_pro_aun_code
                             ,p1.lcad_cad_type_ind);
          FETCH chk_cad_exists INTO l_cad_exists;
          CLOSE chk_cad_exists;
          IF (l_cad_exists IS NOT NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',396);
          END IF;
        END IF;
        IF (p1.lcad_cad_cnt_reference IS NOT NULL 
            AND p1.lcad_cad_pro_aun_code IS NOT NULL 
            AND p1.lcad_cad_type_ind IS NOT NULL 
            AND p1.lcad_cai_start_date IS NOT NULL) 
        THEN
          l_cai_exists := NULL;
          OPEN chk_cai_exists(p1.lcad_cad_cnt_reference
                             ,p1.lcad_cad_pro_aun_code
                             ,p1.lcad_cad_type_ind
                             ,p1.lcad_cai_start_date);
          FETCH chk_cai_exists INTO l_cai_exists;
          CLOSE chk_cai_exists;
          IF (l_cai_exists IS NOT NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',397);
          END IF;
        END IF;
        IF l_errors = 'F' 
        THEN
          l_error_ind := 'Y';
        ELSE
          l_error_ind := 'N';
        END IF;
        i := i + 1;
        IF (MOD(i,1000) = 0) 
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
PROCEDURE dataload_delete
  (p_batch_id IN VARCHAR2
  ,p_date     IN DATE
  )
AS
CURSOR c1(p_batch_id VARCHAR2)
IS
SELECT rowid rec_rowid
,      lcad_dlb_batch_id
,      lcad_dl_seqno
,      lcad_dl_load_status
,      lcad_cad_cnt_reference
,      lcad_cad_pro_aun_code
,      lcad_cad_type_ind
,      lcad_cai_start_date
,      lcad_cai_end_date
,      lcad_cai_comments
,      lcad_cai_cse_section_number
,      lcad_cai_hrv_caa_code
,      lcad_cai_hrv_cat_code
FROM   dl_hpm_contract_addresses
WHERE  lcad_dlb_batch_id = p_batch_id
AND    lcad_dl_load_status IN('C');

cb             VARCHAR2(30);
cd             DATE;
cp             VARCHAR2(30) := 'DELETE';
ct             VARCHAR2(30) := 'DL_HPM_CONTRACT_ADDRESSES';
cs             INTEGER;
ce             VARCHAR2(200);
l_scs_refno    NUMBER(8);
l_pro_refno    NUMBER(10);
l_aun_code     VARCHAR2(20);
l_pro_aun_code VARCHAR2(20);
l_exists       VARCHAR2(1);
l_errors       VARCHAR2(1);
l_error_ind    VARCHAR2(1);
i              INTEGER := 0;
BEGIN
fsc_utils.proc_start('s_dl_hpm_contract_addresses.dataload_delete');
fsc_utils.debug_message('s_dl_hpm_contract_addresses.dataload_delete',3);
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
cb := p_batch_id;
cd := p_date;
FOR p1 IN c1(p_batch_id)
LOOP
  BEGIN
    cs := p1.lcad_dl_seqno;
    i := i + 1;
    SAVEPOINT SP1;
    IF p1.lcad_cad_type_ind  = 'A'
    THEN
      l_pro_aun_code := p1.lcad_cad_pro_aun_code;
    ELSE
      l_pro_refno := NULL;
      l_pro_refno := s_dl_hem_utils.pro_refno_for_propref
                                          (p1.lcad_cad_pro_aun_code);
      l_pro_aun_code := TO_CHAR(l_pro_refno);
    END IF;

    DELETE
    FROM  contract_address_instances
    WHERE cai_cad_cnt_reference = p1.lcad_cad_cnt_reference
    AND   cai_cad_pro_aun_code  = l_pro_aun_code           
    AND   cai_cad_type_ind      = p1.lcad_cad_type_ind
    AND   cai_start_date = p1.lcad_cai_start_date;
    
    -- and now the Contract Addresses
    DELETE
    FROM  contract_addresses
    WHERE cad_cnt_reference = p1.lcad_cad_cnt_reference
    AND   cad_pro_aun_code  = l_pro_aun_code             
    AND   cad_type_ind      = p1.lcad_cad_type_ind;

    IF (mod(i,5000) = 0) 
    THEN
      COMMIT;
    END IF;
    s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
    s_dl_utils.set_record_status_flag(ct,cb,cs,'V');
  EXCEPTION
  WHEN OTHERS 
  THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
    s_dl_utils.set_record_status_flag(ct,cb,cs,ce);
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
END LOOP;
fsc_utils.proc_end;
COMMIT;
EXCEPTION
WHEN OTHERS 
THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
  RAISE;
END dataload_delete;
END s_dl_hpm_contract_addresses;
/

show errors

