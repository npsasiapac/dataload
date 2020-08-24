CREATE OR REPLACE PACKAGE BODY s_dl_hpm_survey_addresses
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VER  DB Ver  WHO  WHEN        WHY
--  1.0          PJD  19-SEP-2003 Product Dataload
--  1.1          MB   19-MAY-2006 Amended max end of range from x to x+1
--  2.0 5.13.0   PH   06-FEB-2008 Now includes its own 
--                                set_record_status_flag procedure.
--  2.1 6.11.0   MJK  25-AUG-2015 Reformatted for 6.11. No logic changes.
--  2.2 6.13.0   PJD  06-JUN-2016 Corrected delete statement to now use  
--                                the value stored in l_pro_aun_code variable
--
-- ***********************************************************************
--
  PROCEDURE set_record_status_flag
    (p_rowid  IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_hpm_survey_addresses
    SET    lsud_dl_load_status = p_status
    WHERE  rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_hpm_survey_addresses');
    RAISE;
  END set_record_status_flag;
  --
  PROCEDURE dataload_create
    (p_batch_id IN VARCHAR2
    ,p_date     IN DATE
    )
  AS
    CURSOR c_range
      (p_batch_id VARCHAR2)
    IS
      SELECT MIN(lsud_dl_seqno)
      ,      MAX(lsud_dl_seqno + 1)
      FROM   dl_hpm_survey_addresses
      WHERE  lsud_dlb_batch_id = p_batch_id
      AND    lsud_dl_load_status = 'V';
    CURSOR c1
      (p_batch_id VARCHAR2
      ,p_min_seqno NUMBER
      ,p_max_seqno NUMBER
      )
    IS
      SELECT rowid                   rec_rowid
      ,      lsud_dlb_batch_id
      ,      lsud_dl_seqno
      ,      lsud_dl_load_status
      ,      lsud_scs_reference
      ,      lsud_pro_aun_code
      ,      lsud_type
      ,      lsud_sco_code
      ,      lsud_status_date
      ,      lsud_created_date
      ,      lsud_created_by
      ,      lsud_text
      FROM   dl_hpm_survey_addresses
      WHERE  lsud_dlb_batch_id = p_batch_id
      AND    lsud_dl_load_status = 'V'
      AND    lsud_dl_seqno BETWEEN p_min_seqno AND p_max_seqno;
    CURSOR c_scs
      (p_reference VARCHAR2
      )
    IS
      SELECT scs_refno
      FROM   stock_condition_surveys
      WHERE  scs_reference = p_reference;
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'CREATE';
    ct VARCHAR2(30) := 'DL_HPM_SURVEY_ADDRESSES';
    cs INTEGER;
    ce VARCHAR2(200);
    ci INTEGER;
    l_id ROWID;
    l_min_seqno    INTEGER;
    l_max_seqno    INTEGER;
    l_pro_refno    NUMBER(10);
    l_pro_aun_code VARCHAR2(20);
    l_scs_refno    NUMBER(8);
    i              INTEGER := 0;
    l_an_tab       VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_survey_addresses.dataload_create');
    fsc_utils.debug_message('s_dl_hpm_survey_addresses.dataload_create',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_DATE;
    ci := s_dl_hem_utils.dl_orig_rows('DL_HPM_SURVEY_ADDRESSES');
    OPEN c_range(p_batch_id);
    FETCH c_range INTO l_min_seqno,l_max_seqno;
    CLOSE c_range;
    WHILE l_min_seqno < l_max_seqno
    LOOP
      FOR p1 IN c1(p_batch_id,l_min_seqno,l_min_seqno + 29999)
      LOOP
        BEGIN
          cs := p1.lsud_dl_seqno;
          l_id := p1.rec_rowid;
          SAVEPOINT SP1;
          l_scs_refno := NULL;
          OPEN c_scs(p1.lsud_scs_reference);
          FETCH c_scs INTO l_scs_refno;
          CLOSE c_scs;
          IF p1.lsud_type = 'A' 
          THEN
            l_pro_aun_code := p1.lsud_pro_aun_code;
          ELSE
            l_pro_refno := NULL;
            l_pro_refno := s_dl_hem_utils.pro_refno_for_propref(p1.lsud_pro_aun_code);
            l_pro_aun_code := TO_CHAR(l_pro_refno);
          END IF;
          INSERT INTO survey_addresses
          (sud_scs_refno  
          ,sud_pro_aun_code  
          ,sud_type  
          ,sud_sco_code  
          ,sud_status_date  
          ,sud_created_date  
          ,sud_created_by  
          ,sud_ipp_refno  
          ,sud_handheld_download_date  
          ,sud_handheld_upload_date  
          ,sud_usr_download_by  
          ,sud_usr_upload_by  
          ,sud_bru_upload_run_no  
          ,sud_modified_date  
          ,sud_bru_download_run_no  
          ,sud_modified_by  
          ,sud_comments  
          ,sud_eur_run_no  
          )  
          VALUES  
          (l_scs_refno  
          ,l_pro_aun_code  
          ,p1.lsud_type  
          ,p1.lsud_sco_code  
          ,p1.lsud_status_date  
          ,p1.lsud_created_date  
          ,p1.lsud_created_by  
          ,NULL  
          ,NULL  
          ,NULL  
          ,NULL  
          ,NULL  
          ,NULL  
          ,NULL  
          ,NULL  
          ,NULL  
          ,p1.lsud_text  
          ,NULL  
          );  
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
      l_min_seqno := l_min_seqno + 30000;
    END LOOP;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('SURVEY_ADDRESSES',ci,i);
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
    CURSOR c_range
      (p_batch_id VARCHAR2)
    IS
      SELECT MIN(lsud_dl_seqno)
      ,      MAX(lsud_dl_seqno + 1)
      FROM   dl_hpm_survey_addresses
      WHERE  lsud_dlb_batch_id = p_batch_id
      AND    lsud_dl_load_status IN('L','F','O');
    CURSOR c1
      (p_batch_id  VARCHAR2
      ,p_min_seqno NUMBER
      ,p_max_seqno NUMBER
      )
    IS
      SELECT rowid rec_rowid
      ,      lsud_dlb_batch_id
      ,      lsud_dl_seqno
      ,      lsud_dl_load_status
      ,      lsud_scs_reference
      ,      lsud_pro_aun_code
      ,      lsud_type
      ,      lsud_sco_code
      ,      lsud_status_date
      ,      lsud_created_date
      ,      lsud_created_by
      ,      lsud_text
      FROM   dl_hpm_survey_addresses
      WHERE  lsud_dlb_batch_id = p_batch_id
      AND    lsud_dl_load_status IN('L','F','O')
      AND    lsud_dl_seqno BETWEEN p_min_seqno AND p_max_seqno;
    CURSOR c_scs
      (p_reference VARCHAR2)
    IS
      SELECT scs_refno
      FROM   stock_condition_surveys
      WHERE  scs_reference = p_reference;
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'VALIDATE';
    ct VARCHAR2(30) := 'DL_HPM_SURVEY_ADDRESSES';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    l_min_seqno INTEGER;
    l_max_seqno INTEGER;
    l_scs_refno NUMBER(8);
    l_pro_refno NUMBER(10);
    l_aun_code  VARCHAR2(20);
    l_exists    VARCHAR2(1);
    l_errors    VARCHAR2(1);
    l_error_ind VARCHAR2(1);
    i           INTEGER := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_survey_addresses.dataload_validate');
    fsc_utils.debug_message('s_dl_hpm_survey_addresses.dataload_validate',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    OPEN c_range(p_batch_id);
    FETCH c_range
    INTO l_min_seqno
    ,l_max_seqno;
    CLOSE c_range;
    WHILE l_min_seqno < l_max_seqno
    LOOP
      FOR p1 IN c1(p_batch_id,l_min_seqno,l_min_seqno + 29999)
      LOOP
        BEGIN
          cs := p1.lsud_dl_seqno;
          l_id := p1.rec_rowid;
          l_errors := 'V';
          l_error_ind := 'N';
          l_scs_refno := NULL;
          OPEN c_scs(p1.lsud_scs_reference);
          FETCH c_scs INTO l_scs_refno;
          CLOSE c_scs;
          IF l_scs_refno IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',301);
          END IF;
          l_exists := NULL;
          IF p1.lsud_type = 'A' 
          THEN
            IF (NOT s_dl_hem_utils.exists_aun_code(p1.lsud_pro_aun_code)) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
            END IF;
          ELSE
            IF (NOT s_dl_hem_utils.exists_propref(p1.lsud_pro_aun_code)) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
            END IF;
          END IF;
          IF p1.lsud_type NOT IN('A','P') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',618);
          END IF;
          IF p1.lsud_sco_code NOT IN('ASS','DLD','ENT','RAI') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',531);
          END IF;
          IF p1.lsud_status_date IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',777);
          END IF;
          IF p1.lsud_created_date IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',952);
          END IF;
          IF p1.lsud_created_by IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',953);
          END IF;
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
      l_min_seqno := l_min_seqno + 30000;
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
    CURSOR c_range
      (p_batch_id VARCHAR2)
    IS
      SELECT MIN(lsud_dl_seqno)
      ,      MAX(lsud_dl_seqno + 1)
      FROM   dl_hpm_survey_addresses
      WHERE  lsud_dlb_batch_id = p_batch_id
      AND    lsud_dl_load_status IN('C');
    CURSOR c1
      (p_batch_id VARCHAR2
      ,p_min_seqno NUMBER
      ,p_max_seqno NUMBER
      )
    IS
      SELECT rowid rec_rowid
      ,      lsud_dlb_batch_id
      ,      lsud_dl_seqno
      ,      lsud_dl_load_status
      ,      lsud_scs_reference
      ,      lsud_pro_aun_code
      ,      lsud_type
      ,      lsud_sco_code
      ,      lsud_status_date
      ,      lsud_created_date
      ,      lsud_created_by
      ,      lsud_text
      FROM   dl_hpm_survey_addresses
      WHERE  lsud_dlb_batch_id = p_batch_id
      AND    lsud_dl_load_status IN('C')
      AND    lsud_dl_seqno BETWEEN p_min_seqno AND p_max_seqno;
    CURSOR c_scs
      (p_reference VARCHAR2)
    IS
      SELECT scs_refno
      FROM   stock_condition_surveys
      WHERE  scs_reference = p_reference;
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'DELETE';
    ct VARCHAR2(30) := 'DL_HPM_SURVEY_ADDRESSES';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    l_min_seqno    INTEGER;
    l_max_seqno    INTEGER;
    l_scs_refno    NUMBER(8);
    l_pro_refno    NUMBER(10);
    l_aun_code     VARCHAR2(20);
    l_pro_aun_code VARCHAR2(20);
    l_exists       VARCHAR2(1);
    l_errors       VARCHAR2(1);
    l_error_ind    VARCHAR2(1);
    i              INTEGER := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_survey_addresses.dataload_delete');
    fsc_utils.debug_message('s_dl_hpm_survey_addresses.dataload_delete',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    OPEN c_range(p_batch_id);
    FETCH c_range INTO l_min_seqno,l_max_seqno;
    CLOSE c_range;
    WHILE l_min_seqno < l_max_seqno
    LOOP
      FOR p1 IN c1(p_batch_id,l_min_seqno,l_min_seqno + 29999)
      LOOP
        BEGIN
          cs := p1.lsud_dl_seqno;
          l_id := p1.rec_rowid;
          i := i + 1;
          SAVEPOINT SP1;
          l_scs_refno := NULL;
          OPEN c_scs(p1.lsud_scs_reference);
          FETCH c_scs INTO l_scs_refno;
          CLOSE c_scs;
          IF p1.lsud_type = 'A' 
          THEN
            l_pro_aun_code := p1.lsud_pro_aun_code;
          ELSE
            l_pro_refno := NULL;
            l_pro_refno := s_dl_hem_utils.pro_refno_for_propref(p1.lsud_pro_aun_code);
            l_pro_aun_code := TO_CHAR(l_pro_refno);
          END IF;
          DELETE
          FROM   survey_addresses
          WHERE  sud_scs_refno    = l_scs_refno
          AND    sud_type         = p1.lsud_type
          AND    sud_pro_aun_code = l_pro_aun_code;
          IF mod(i,5000) = 0 
          THEN
            COMMIT;
          END IF;
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
        EXCEPTION
        WHEN OTHERS 
        THEN
          ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
          set_record_status_flag(l_id,'C');
          s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
        END;
      END LOOP;
    END LOOP;
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_hpm_survey_addresses;
/
