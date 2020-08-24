CREATE OR REPLACE PACKAGE BODY s_dl_hem_admin_units
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Ver WHO  WHEN       WHY
--      1.0        MTR  23/11/00   Dataload
--      1.1 5.1.4  PJD  02/02/02   Added Validation on Alt Ref
--                                 Added extra deletes
--      1.2 5.1.6  PJD  21/05/02   Corrected Validation on Alt Ref
--                                 Extra validation on mandatory fields
--                                 Defaulted current indicator to Y
--      1.3 5.2.0  PJD 21/11/02    Check the auy_type is HOU in the validate
--      1.4 5.2.0  SB  27/11/02    Validation on Address added
--                                 Changed check in address insert to look
--                                 if street not null rather than town AND area
--      2.0 5.3.0  PJD  09/12/02   Moved Mandatory Field validation to
--                                 the correct place
--      2.1 5.3.0  PJD  05/02/03   Created by field now defaults to DATALOAD
--      2.2 5.4.0  PJD  18/11/03   Added Commit to Validate Procedure
--                                  Corrected Delete Procedure
--      2.3 5.5.0  PH   24/03/04   Added final commit to create process
--      2.4 5.6.0  PH   13/10/04   Added new validation check on invalid
--                                 characters in aun_code.
--          2.5 5.8.0  MB   05/08/05   Added in extra NULLs in call to dl_hem_utils.
--                                 insert_address
--      3.0 5.10.0 PH   08/05/06   Removed all references to Addresses as these
--                                 should be loaded in the Addresses Dataload.
--      3.1 5.12.0 DH   26/07/07   Amended AUN_CODE validation to allow hyphens
--                                 and exclude underscores.
--      3.2 5.13.0 PH   05/02/08   Amended above validate as there is a system
--                                 parameter ALLOW_UNDERSCORE. Commented out
--                                 HDL969 as allowable within application.
--                                 Now includes its own 
--                                 set_record_status_flag procedure.
--      3.3 6.11   AJ   17/06/15   Multi Language fields added to admin_units and bank_details (function)
--      3.4 6.11   AJ   18/06/15   extra validation on Bank Name and Branch to errors raised rather than
--                                 the insert function stopping is already exists
--      3.5 6.12   MJK  17/08/15   Re-formatted, no logic changes
--
-- ***********************************************************************
--
--
  PROCEDURE set_record_status_flag(
      p_rowid IN ROWID,
      p_status IN VARCHAR2)
  AS
  BEGIN
    UPDATE dl_hem_admin_units
    SET laun_dl_load_status = p_status
    WHERE rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS THEN
    dbms_output.put_line('Error updating status of dl_hem_admin_units') ;
    RAISE;
  END set_record_status_flag;
  --
  PROCEDURE dataload_create
    (p_batch_id IN VARCHAR2
    ,p_date IN DATE
    )
  AS
    CURSOR c1(p_batch_id VARCHAR2)
    IS
      SELECT rowid rec_rowid
      ,      laun_dlb_batch_id
      ,      laun_dl_seqno
      ,      laun_dl_load_status
      ,      laun_code
      ,      laun_name
      ,      laun_auy_code
      ,      laun_current_ind
      ,      laun_tenancy_wk_start
      ,      laun_hb_period
      ,      laun_alt_ref
      ,      laun_comments
      ,      laun_bde_bank_name
      ,      laun_bde_branch_name
      ,      laun_bad_account_no
      ,      laun_bad_account_name
      ,      laun_bad_sort_code
      ,      laun_bad_start_date
      ,      laun_code_mlang
      ,      laun_name_mlang
      ,      laun_bde_bank_name_mlang
      ,      laun_bde_branch_name_mlang
      FROM   dl_hem_admin_units
      WHERE  laun_dlb_batch_id = p_batch_id
      AND    laun_dl_load_status = 'V';
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'CREATE';
    ct VARCHAR2(30) := 'DL_HEM_ADMIN_UNITS';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    i INTEGER := 0;
    l_bad_refno bank_account_details.bad_refno%TYPE;
    l_bde_refno bank_details.bde_refno%TYPE;
    l_an_tab VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hem_admin_units.dataload_create');
    fsc_utils.debug_message('s_dl_hem_admin_units.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1(p_batch_id)
    LOOP
      BEGIN
        cs := p1.laun_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        INSERT INTO admin_units
        (aun_code  
        ,aun_name   
        ,aun_auy_code   
        ,aun_created_by   
        ,aun_creation_date   
        ,aun_current_ind   
        ,aun_reusable_refno   
        ,aun_tenancy_wk_start   
        ,aun_hb_period   
        ,aun_rbrv_precept_type   
        ,aun_alt_ref   
        ,aun_comments   
        ,aun_name_mlang   
        ,aun_code_mlang   
        )  
        VALUES  
        (p1.laun_code
        ,p1.laun_name   
        ,p1.laun_auy_code   
        ,'DATALOAD'   
        ,TRUNC(sysdate)   
        ,NVL(p1.laun_current_ind,'Y')   
        ,NULL   
        ,p1.laun_tenancy_wk_start   
        ,p1.laun_hb_period   
        ,NULL   
        ,p1.laun_alt_ref   
        ,p1.laun_comments   
        ,p1.laun_name_mlang   
        ,p1.laun_code_mlang   
        );
        IF p1.laun_bde_bank_name_mlang IS NOT NULL 
        OR p1.laun_bde_bank_name IS NOT NULL 
        THEN
          IF p1.laun_bde_bank_name_mlang IS NULL 
          AND p1.laun_bde_bank_name IS NOT NULL 
          THEN
            s_dl_hem_utils.insert_bank_details
              (p1.laun_bde_bank_name
              ,p1.laun_bde_branch_name
              ,TO_NUMBER(p1.laun_bad_account_no)
              ,p1.laun_bad_account_name 
              ,p1.laun_bad_sort_code 
              ,p1.laun_bad_start_date 
              ,l_bde_refno 
              ,l_bad_refno 
              );
          END IF;
          IF p1.laun_bde_bank_name_mlang IS NOT NULL 
          AND p1.laun_bde_bank_name IS NOT NULL 
          THEN
            s_dl_hem_utils.insert_bank_details_mlang
              (p1.laun_bde_bank_name
              ,p1.laun_bde_branch_name 
              ,p1.laun_bde_bank_name_mlang 
              ,p1.laun_bde_branch_name_mlang 
              ,TO_NUMBER(p1.laun_bad_account_no) 
              ,p1.laun_bad_account_name 
              ,p1.laun_bad_sort_code 
              ,p1.laun_bad_start_date
              ,l_bde_refno 
              ,l_bad_refno 
              );
          END IF;
          INSERT INTO admin_unit_bank_accounts
          (aub_aun_code
          ,aub_bad_refno 
          ,aub_start_date 
          ,aub_end_date 
          )
          VALUES
          (p1.laun_code
          ,l_bad_refno 
          ,sysdate 
          ,NULL 
          );
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N') ;
        set_record_status_flag(l_id,'C') ;
        i := i + 1;
        IF MOD(i,1000) = 0 
        THEN
          COMMIT;
        END IF;
      EXCEPTION
      WHEN OTHERS 
      THEN
        ROLLBACK TO SP1;
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM) ;
        set_record_status_flag(l_id,'O') ;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y') ;
      END;
    END LOOP;
    COMMIT;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('ADMIN_UNITS') ;
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    set_record_status_flag(l_id,'O') ;
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED') ;
  END dataload_create;
  --
  PROCEDURE dataload_validate
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  AS
    CURSOR c1
    IS
      SELECT rowid rec_rowid
      ,      laun_dlb_batch_id
      ,      laun_dl_seqno
      ,      laun_dl_load_status
      ,      laun_code
      ,      laun_name
      ,      laun_auy_code
      ,      laun_current_ind
      ,      laun_tenancy_wk_start
      ,      laun_hb_period
      ,      laun_alt_ref
      ,      laun_comments
      ,      laun_bde_bank_name
      ,      laun_bde_branch_name
      ,      laun_bad_account_no
      ,      laun_bad_account_name
      ,      laun_bad_sort_code
      ,      laun_bad_start_date
      ,      laun_code_mlang
      ,      laun_name_mlang
      ,      laun_bde_bank_name_mlang
      ,      laun_bde_branch_name_mlang
      FROM   dl_hem_admin_units
      WHERE  laun_dlb_batch_id = p_batch_id
      AND    laun_dl_load_status IN('L','F','O');
    CURSOR c_auy_type(p_admin_unit_type VARCHAR2)
    IS
      SELECT 'x'
      FROM   admin_unit_types
      WHERE  auy_code = p_admin_unit_type
      AND    auy_type = 'HOU';
    CURSOR c_aun_exists(p_aun_code VARCHAR2)
    IS
      SELECT 'x'
      FROM   admin_units
      WHERE  aun_code = p_aun_code;
    CURSOR c_aun_mlang_exists(p_aun_code VARCHAR2)
    IS
      SELECT 'x'
      FROM   admin_units
      WHERE  aun_code_mlang = p_aun_code;
    CURSOR c_aun_exists2(p_aun_alt_ref VARCHAR2)
    IS
      SELECT 'x'
      FROM   admin_units
      WHERE  aun_alt_ref = p_aun_alt_ref;
    CURSOR dup_aun_mlang_exists(p_aun_code_mlang VARCHAR2, p_batch_id VARCHAR2)
    IS
      SELECT COUNT( *)
      FROM   dl_hem_admin_units
      WHERE  laun_code_mlang = p_aun_code_mlang
      AND    laun_dlb_batch_id = p_batch_id ;
    CURSOR c_bank_details(p_bde_bank_name VARCHAR2)
    IS
      SELECT bde_refno
      FROM   bank_details
      WHERE  bde_bank_name = p_bde_bank_name
      AND    bde_branch_name IS NULL;
    CURSOR c_branch_details(p_bde_bank_name VARCHAR2, p_bde_branch_name VARCHAR2)
    IS
      SELECT bde_refno
      FROM   bank_details
      WHERE  bde_bank_name = p_bde_bank_name
      AND    bde_branch_name = p_bde_branch_name;
    CURSOR c_bank_details_m(p_bde_bank_name VARCHAR2)
    IS
      SELECT bde_refno
      FROM   bank_details
      WHERE  bde_bank_name_mlang = p_bde_bank_name
      AND    bde_branch_name_mlang IS NULL;
    CURSOR c_branch_details_m(p_bde_bank_name VARCHAR2, p_bde_branch_name VARCHAR2)
    IS
      SELECT bde_refno
      FROM   bank_details
      WHERE  bde_bank_name_mlang = p_bde_bank_name
      AND    bde_branch_name_mlang = p_bde_branch_name;
    cb VARCHAR2(30) ;
    cd DATE;
    cp VARCHAR2(30) := 'VALIDATE';
    ct VARCHAR2(30) := 'DL_HEM_ADMIN_UNITS';
    cs INTEGER;
    ce VARCHAR2(200) ;
    l_id ROWID;
    l_exists        VARCHAR2(1) ;
    l_mlang_exists  VARCHAR2(1) ;
    l_auy_exists    VARCHAR2(1) ;
    l_altref_exists VARCHAR2(1) ;
    l_pro_refno     NUMBER(10) ;
    l_errors        VARCHAR2(10) ;
    l_error_ind     VARCHAR2(10) ;
    i               INTEGER := 0;
    l_rac_accno revenue_accounts.rac_accno%TYPE;
    l_rac_aun_code admin_units.aun_code%TYPE;
    l_mode      VARCHAR2(1) ;
    l_char      VARCHAR2(1) ;
    l_aun_error VARCHAR2(1) ;
    l_value admin_units.aun_code%TYPE;
    l_allow_underscore VARCHAR2(255) ;
    l_char_m           VARCHAR2(1) ;
    l_aun_error_m      VARCHAR2(1) ;
    l_value_m admin_units.aun_code%TYPE;
    l_allow_underscore_m VARCHAR2(255) ;
    l_mlang_count        INTEGER := 0;
    l_bank_exists        NUMBER(10) ;
    l_branch_exists      NUMBER(10) ;
    l_bank_exists_m      NUMBER(10) ;
    l_branch_exists_m    NUMBER(10) ;
  BEGIN
    fsc_utils.proc_start('s_dl_hem_admin_units.dataload_validate') ;
    fsc_utils.debug_message('s_dl_hem_admin_units.dataload_validate',3) ;
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING') ;
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.laun_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors := 'V';
        l_error_ind := 'N';
        l_exists := NULL;
        l_mlang_exists := NULL;
        l_auy_exists := NULL;
        l_altref_exists := NULL;
        l_mlang_count := 0;
        l_bank_exists := NULL;
        l_branch_exists := NULL;
        l_bank_exists_m := NULL;
        l_branch_exists_m := NULL;
        OPEN c_aun_exists(p1.laun_code) ;
        FETCH c_aun_exists INTO l_exists;
        IF c_aun_exists%found 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',893) ;
        END IF;
        CLOSE c_aun_exists;
        OPEN c_auy_type(p1.laun_auy_code) ;
        FETCH c_auy_type INTO l_auy_exists;
        IF c_auy_type%notfound 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',845) ;
        END IF;
        CLOSE c_auy_type;
        IF p1.laun_current_ind NOT IN('Y','N') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',708) ;
        END IF;
        IF p1.laun_tenancy_wk_start IS NOT NULL 
        THEN
          IF NOT s_dl_utils.check_column_domain('ADMIN_UNITS','AUN_TENANCY_WK_START',p1.laun_tenancy_wk_start) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',846) ;
          END IF;
        END IF;
        IF p1.laun_bad_account_no IS NOT NULL 
        THEN
          IF p1.laun_bde_bank_name IS NOT NULL 
          AND p1.laun_bad_account_name IS NOT NULL 
          AND p1.laun_bad_sort_code IS NOT NULL 
          AND p1.laun_bad_start_date IS NOT NULL 
          THEN
            NULL;
          ELSE
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',853) ;
          END IF;
        END IF;
        IF p1.laun_code IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',970) ;
        END IF;
        IF p1.laun_name IS NULL 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',971) ;
        END IF;
        l_char := NULL;
        l_aun_error := NULL;
        l_value := p1.laun_code;
        l_allow_underscore := fsc_utils.get_sys_param('ALLOW_UNDERSCORE') ;
        WHILE LENGTH(l_value) > 0
        LOOP
          l_char := SUBSTR(l_value,1,1) ;
          l_value := SUBSTR(l_value,2,LENGTH(l_value) - 1) ;
          IF l_char BETWEEN '0' AND '9' 
          THEN
            NULL;
          ELSIF l_char BETWEEN 'A' AND 'Z' 
          THEN
            NULL;
          ELSIF l_char = '-' 
          THEN
            NULL;
          ELSIF l_char = '_' AND l_allow_underscore = 'Y' 
          THEN
            NULL;
          ELSE
            l_aun_error := 'Y';
          END IF;
        END LOOP;
        IF l_aun_error = 'Y' 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',195) ;
        END IF;
        IF(p1.laun_bde_bank_name IS NOT NULL AND p1.laun_bde_branch_name IS NULL) 
        THEN
          OPEN c_bank_details(p1.laun_bde_bank_name) ;
          FETCH c_bank_details
          INTO l_bank_exists;
          CLOSE c_bank_details;
          IF l_bank_exists IS NOT NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',695) ;
          END IF;
        END IF;
        IF(p1.laun_bde_bank_name IS NOT NULL AND p1.laun_bde_branch_name IS NOT NULL) 
        THEN
          OPEN c_branch_details(p1.laun_bde_bank_name,p1.laun_bde_branch_name) ;
          FETCH c_branch_details
          INTO l_branch_exists;
          CLOSE c_branch_details;
          IF l_branch_exists IS NOT NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',695) ;
          END IF;
        END IF;
        IF(p1.laun_bde_bank_name_mlang IS NOT NULL AND p1.laun_bde_branch_name_mlang IS NULL) 
        THEN
          OPEN c_bank_details_m(p1.laun_bde_bank_name_mlang) ;
          FETCH c_bank_details_m
          INTO l_bank_exists_m;
          CLOSE c_bank_details_m;
          IF l_bank_exists_m IS NOT NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',813) ;
          END IF;
        END IF;
        IF(p1.laun_bde_bank_name_mlang IS NOT NULL AND p1.laun_bde_branch_name_mlang IS NOT NULL) 
        THEN
          OPEN c_branch_details_m(p1.laun_bde_bank_name_mlang,p1.laun_bde_branch_name_mlang) ;
          FETCH c_branch_details_m
          INTO l_branch_exists_m;
          CLOSE c_branch_details_m;
          IF l_branch_exists_m IS NOT NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',813) ;
          END IF;
        END IF;
        IF p1.laun_code_mlang IS NOT NULL 
        THEN
          OPEN c_aun_mlang_exists(p1.laun_code_mlang) ;
          FETCH c_aun_mlang_exists
          INTO l_mlang_exists;
          IF c_aun_mlang_exists%found 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',808) ;
          END IF;
          CLOSE c_aun_mlang_exists;
          OPEN dup_aun_mlang_exists(p1.laun_code_mlang,p1.laun_dlb_batch_id) ;
          FETCH dup_aun_mlang_exists
          INTO l_mlang_count;
          CLOSE dup_aun_mlang_exists;
          IF l_mlang_count > 1 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',809) ;
          END IF;
          CLOSE dup_aun_mlang_exists;
          IF p1.laun_name_mlang IS NULL THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',810) ;
          END IF;
          l_char_m := NULL;
          l_aun_error_m := NULL;
          l_value_m := p1.laun_code_mlang;
          l_allow_underscore_m := fsc_utils.get_sys_param('ALLOW_UNDERSCORE') ;
          WHILE LENGTH(l_value_m) > 0
          LOOP
            l_char_m := SUBSTR(l_value_m,1,1) ;
            l_value_m := SUBSTR(l_value_m,2,LENGTH(l_value_m) - 1) ;
            IF l_char_m BETWEEN '0' AND '9' 
            THEN
              NULL;
            ELSIF l_char_m BETWEEN 'A' AND 'Z' 
            THEN
              NULL;
            ELSIF l_char_m = '-' 
            THEN
              NULL;
            ELSIF l_char_m = '_' AND l_allow_underscore = 'Y' 
            THEN
              NULL;
            ELSE
              l_aun_error_m := 'Y';
            END IF;
          END LOOP;
          IF l_aun_error_m = 'Y' 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',812) ;
          END IF;
        END IF;
        IF(p1.laun_name_mlang IS NOT NULL AND p1.laun_code_mlang IS NULL) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',811) ;
        END IF;
        IF l_errors = 'F' 
        THEN
          l_error_ind := 'Y';
        ELSE
          l_error_ind := 'N';
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind) ;
        set_record_status_flag(l_id,l_errors) ;
        i := i + 1;
        IF MOD(i,1000) = 0 
        THEN
          COMMIT;
        END IF;
      EXCEPTION
      WHEN OTHERS 
      THEN
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM) ;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y') ;
        set_record_status_flag(l_id,'O') ;
      END;
    END LOOP;
    COMMIT;
    fsc_utils.proc_end;
  END dataload_validate;
  --
  PROCEDURE dataload_delete
    (p_batch_id IN VARCHAR2
    ,p_date IN DATE
    )
  IS
    CURSOR c1
    IS
      SELECT rowid rec_rowid
      ,      laun_dlb_batch_id
      ,      laun_dl_seqno
      ,      laun_code
      FROM   dl_hem_admin_units
      WHERE  laun_dlb_batch_id = p_batch_id
      AND    laun_dl_load_status = 'C';
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'DELETE';
    ct VARCHAR2(30) := 'DL_HEM_ADMIN_UNITS';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    i        INTEGER := 0;
    l_an_tab VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hem_admin_units.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_admin_units.dataload_delete',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.laun_dl_seqno;
        i := i + 1;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        DELETE FROM address_usages
        WHERE aus_aun_code = p1.laun_code;
        DELETE FROM admin_unit_bank_accounts
        WHERE aub_aun_code = p1.laun_code;
        DELETE FROM admin_units
        WHERE aun_code = p1.laun_code;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N') ;
        set_record_status_flag(l_id,'V') ;
        i := i + 1;
        IF MOD(i,1000) = 0 
        THEN
          COMMIT;
        END IF;
      EXCEPTION
      WHEN OTHERS 
      THEN
        ROLLBACK TO SP1;
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE) ;
        set_record_status_flag(l_id,'C') ;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y') ;
      END;
    END LOOP;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('ADMIN_UNITS') ;
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED') ;
    RAISE;
  END dataload_delete;
END s_dl_hem_admin_units;
/
show errors
