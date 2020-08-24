CREATE OR REPLACE PACKAGE BODY s_dl_hpm_survey_results                                            
AS                                                                              
-- ***********************************************************************      
--  DESCRIPTION:                                                              
--                                                                            
--  CHANGE CONTROL                                                            
--  VER  DB Ver  WHO  WHEN        WHY                                         
--  1.0          PJD  19-SEP-2003 Product Dataload 
--  1.1  5.5.0   PJD  05-MAY-2004 Correction to wrapped line in create proc
--  1.2  5.5.0   PJD  29-JUN-2004 Correction to use p1.lsrt_sud_type in
--                                Validation section.
--                                Removal of erroneous ! in Delete section. 
--  1.3  5.7.0   PJD  12-APR-2005 added set_record_status_flag proc
--                                changed use of exists_hrv to exits_frv
--  1.4  5.8.0   VRS  04-JAN-2006 Changed remaining exists_hrv to exists_frv
--                                for error HDL704. 
--                                Changed DOMAIN ELELOC to LOCATION
--                                Changed lsrt_hrv_ret_code to lsrt_hrv_pmu_code 
--                                for error 704.
--
--                                Added if null check for lsrt_hrv_ret_code.
--
--  1.5  5.9.0   VRS  02-MAR-2006 Add processing of lsrt_upload_ipp_shortname in CREATE
--                                VALIDATE processes and General Code Tidy up.   
--  1.6  5.10.0  PH   12-MAR-2007 Amended validate on Element type to include SE 
--  2.0 5.13.0   PH   06-FEB-2008 Now includes its own 
--                                set_record_status_flag procedure.  
--  2.1  5.15.0  MB   02-SEP-2009 put +1 against max_seqno into the range cursor 
--  2.2  6.11.0  MJK  25-AUG-2015 Reformatted for 6.11. No logic changes
--
-- ***********************************************************************
  PROCEDURE set_record_status_flag
    (p_rowid IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_hpm_survey_results
    SET    lsrt_dl_load_status = p_status
    WHERE  rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_hpm_survey_results');
    RAISE;
  END set_record_status_flag;
  --
  PROCEDURE dataload_create
    (p_batch_id IN VARCHAR2
    ,p_date IN DATE
    )
  AS
    CURSOR c_range
      (p_batch_id VARCHAR2)
    IS
      SELECT MIN(lsrt_dl_seqno)
      ,      MAX(lsrt_dl_seqno + 1)
      FROM   dl_hpm_survey_results
      WHERE  lsrt_dlb_batch_id = p_batch_id
      AND    lsrt_dl_load_status = 'V';
    CURSOR c1
      (p_batch_id  VARCHAR2
      ,p_min_seqno NUMBER
      ,p_max_seqno NUMBER
      )
    IS
      SELECT lsrt_dlb_batch_id
      ,      lsrt_dl_seqno
      ,      lsrt_dl_load_status
      ,      lsrt_sud_scs_reference
      ,      lsrt_sud_pro_aun_code
      ,      lsrt_sud_type
      ,      lsrt_ele_code
      ,      lsrt_type
      ,      NVL(lsrt_handheld_created_ind,'N') lsrt_handheld_created_ind
      ,      NVL(lsrt_copied_ind,'N')           lsrt_copied_ind
      ,      NVL(lsrt_created_by,'DATALOAD')    lsrt_created_by
      ,      NVL(lsrt_created_date,sysdate)     lsrt_created_date
      ,      lsrt_assessment_date
      ,      lsrt_cmpt_display_seq
      ,      lsrt_sub_cmpt_display_seq
      ,      lsrt_material_display_seq
      ,      lsrt_hrv_loc_code
      ,      lsrt_att_code
      ,      lsrt_fat_code
      ,      lsrt_comments
      ,      lsrt_quantity
      ,      lsrt_date_value
      ,      lsrt_numeric_value
      ,      lsrt_estimated_cost
      ,      lsrt_upload_ipp_shortname
      ,      lsrt_hrv_ret_code
      ,      lsrt_hrv_pmu_code
      ,      lsrt_hrv_sya_code
      ,      lsrt_hrv_rur_code
      ,      lsrt_hrv_rco_code
      ,      rowid rec_rowid
      FROM   dl_hpm_survey_results
      WHERE  lsrt_dlb_batch_id = p_batch_id
      AND    lsrt_dl_load_status = 'V'
      AND    lsrt_dl_seqno BETWEEN p_min_seqno AND p_max_seqno;
    CURSOR c_scs
      (p_reference VARCHAR2)
    IS
      SELECT scs_refno
      FROM   stock_condition_surveys
      WHERE  scs_reference = p_reference;
    CURSOR get_ipp_refno
      (p_ipp_shortname VARCHAR2)
    IS
      SELECT ipp_refno
      FROM   interested_parties
      WHERE  ipp_shortname = p_ipp_shortname;
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'CREATE';
    ct VARCHAR2(30) := 'DL_HPM_SURVEY_RESULTS';
    cs INTEGER;
    ce VARCHAR2(200);
    ci INTEGER;
    l_id ROWID;
    l_min_seqno    INTEGER;
    l_max_seqno    INTEGER;
    l_curr_seqno   PLS_INTEGER := 0;
    l_pro_refno    NUMBER(10);
    l_pro_aun_code VARCHAR2(20);
    l_scs_refno    NUMBER(8);
    i              INTEGER := 0;
    l_an_tab       VARCHAR2(1);
    l_ipp_refno    NUMBER(10);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_survey_results.dataload_create');
    fsc_utils.debug_message('s_dl_hpm_survey_results.dataload_create',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_DATE;
    ci := s_dl_hem_utils.dl_orig_rows('DL_HPM_SURVEY_RESULTS');
    OPEN c_range(p_batch_id);
    FETCH c_range INTO l_min_seqno,l_max_seqno;
    CLOSE c_range;
    l_curr_seqno := l_min_seqno + 30000;
    WHILE l_min_seqno < l_max_seqno
    LOOP
      FOR p1 IN c1(p_batch_id,l_min_seqno,l_curr_seqno)
      LOOP
        BEGIN
          cs := p1.lsrt_dl_seqno;
          l_id := p1.rec_rowid;
          SAVEPOINT SP1;
          l_scs_refno := NULL;
          OPEN c_scs(p1.lsrt_sud_scs_reference);
          FETCH c_scs INTO l_scs_refno;
          CLOSE c_scs;
          IF (p1.lsrt_sud_type = 'A') 
          THEN
            l_pro_aun_code := p1.lsrt_sud_pro_aun_code;
          ELSE
            l_pro_refno := NULL;
            l_pro_refno := s_dl_hem_utils.pro_refno_for_propref(p1.lsrt_sud_pro_aun_code);
            l_pro_aun_code := TO_CHAR(l_pro_refno);
          END IF;
          l_ipp_refno := NULL;
          OPEN get_ipp_refno(p1.lsrt_upload_ipp_shortname);
          FETCH get_ipp_refno INTO l_ipp_refno;
          CLOSE get_ipp_refno;
          INSERT INTO survey_results
          (srt_refno  
          ,srt_sud_scs_refno  
          ,srt_sud_pro_aun_code  
          ,srt_sud_type  
          ,srt_ele_code  
          ,srt_type  
          ,srt_handheld_created_ind  
          ,srt_copied_ind  
          ,srt_created_by  
          ,srt_created_date  
          ,srt_assessment_date  
          ,srt_cmpt_display_seq  
          ,srt_sub_cmpt_display_seq  
          ,srt_material_display_seq  
          ,srt_hrv_loc_code  
          ,srt_att_code  
          ,srt_fat_code  
          ,srt_comments  
          ,srt_quantity  
          ,srt_date_value  
          ,srt_numeric_value  
          ,srt_estimated_cost  
          ,srt_upload_ipp_refno  
          ,srt_hrv_ret_code  
          ,srt_hrv_pmu_code  
          ,srt_hrv_sya_code  
          ,srt_hrv_rur_code  
          ,srt_hrv_rco_code  
          ,srt_modified_by  
          ,srt_modified_date  
          ,srt_reusable_refno  
          )  
          VALUES  
          (srt_refno_seq.nextval  
          ,l_scs_refno  
          ,l_pro_aun_code  
          ,p1.lsrt_sud_type  
          ,p1.lsrt_ele_code  
          ,p1.lsrt_type  
          ,p1.lsrt_handheld_created_ind  
          ,p1.lsrt_copied_ind  
          ,p1.lsrt_created_by  
          ,p1.lsrt_created_date  
          ,p1.lsrt_assessment_date  
          ,p1.lsrt_cmpt_display_seq  
          ,p1.lsrt_sub_cmpt_display_seq  
          ,p1.lsrt_material_display_seq  
          ,p1.lsrt_hrv_loc_code  
          ,p1.lsrt_att_code  
          ,p1.lsrt_fat_code  
          ,p1.lsrt_comments  
          ,p1.lsrt_quantity  
          ,p1.lsrt_date_value  
          ,p1.lsrt_numeric_value  
          ,p1.lsrt_estimated_cost  
          ,l_ipp_refno  
          ,p1.lsrt_hrv_ret_code  
          ,p1.lsrt_hrv_pmu_code  
          ,p1.lsrt_hrv_sya_code  
          ,p1.lsrt_hrv_rur_code  
          ,p1.lsrt_hrv_rco_code  
          ,'DATALOAD'  
          ,sysdate  
          ,reusable_refno_seq.nextval  
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
      l_min_seqno := l_curr_seqno + 1;
      l_curr_seqno := l_curr_seqno + 30000;
    END LOOP;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('SURVEY_RESULTS',ci,i);
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
      SELECT MIN(lsrt_dl_seqno)
      ,      MAX(lsrt_dl_seqno + 1)
      FROM   dl_hpm_survey_results
      WHERE  lsrt_dlb_batch_id = p_batch_id
      AND    lsrt_dl_load_status IN('L','F','O');
    CURSOR c1
      (p_batch_id  VARCHAR2
      ,p_min_seqno NUMBER
      ,p_max_seqno NUMBER
      )
    IS
      SELECT lsrt_dlb_batch_id
      ,      lsrt_dl_seqno
      ,      lsrt_dl_load_status
      ,      lsrt_sud_scs_reference
      ,      lsrt_sud_pro_aun_code
      ,      lsrt_sud_type
      ,      lsrt_ele_code
      ,      lsrt_type
      ,      NVL(lsrt_handheld_created_ind,'N') lsrt_handheld_created_ind
      ,      NVL(lsrt_copied_ind,'N') lsrt_copied_ind
      ,      NVL(lsrt_created_by,'DATALOAD') lsrt_created_by
      ,      NVL(lsrt_created_date,sysdate) lsrt_created_date
      ,      lsrt_assessment_date
      ,      lsrt_cmpt_display_seq
      ,      lsrt_sub_cmpt_display_seq
      ,      lsrt_material_display_seq
      ,      lsrt_hrv_loc_code
      ,      lsrt_att_code
      ,      lsrt_fat_code
      ,      lsrt_comments
      ,      lsrt_quantity
      ,      lsrt_date_value
      ,      lsrt_numeric_value
      ,      lsrt_estimated_cost
      ,      lsrt_upload_ipp_shortname
      ,      lsrt_hrv_ret_code
      ,      lsrt_hrv_pmu_code
      ,      lsrt_hrv_sya_code
      ,      lsrt_hrv_rur_code
      ,      lsrt_hrv_rco_code
      ,      rowid rec_rowid
      FROM   dl_hpm_survey_results
      WHERE  lsrt_dlb_batch_id = p_batch_id
      AND    lsrt_dl_load_status IN('L','F','O')
      AND    lsrt_dl_seqno BETWEEN p_min_seqno AND p_max_seqno;
    CURSOR c_scs
      (p_reference VARCHAR2)
    IS
      SELECT scs_refno
      FROM   stock_condition_surveys
      WHERE  scs_reference = p_reference;
    CURSOR c_ele_code
      (p_ele_code VARCHAR2)
    IS
      SELECT ele_value_type
      ,      ele_type
      FROM   elements
      WHERE  ele_code = p_ele_code;
    CURSOR c_att_code
      (p_ele_code VARCHAR2
      ,p_att_code VARCHAR2
      )
    IS
      SELECT 'X'
      FROM   attributes
      WHERE  att_ele_code = p_ele_code
      AND    att_code = p_att_code;
    CURSOR c_fat_code
      (p_ele_code VARCHAR2
      ,p_att_code VARCHAR2
      ,p_fat_code VARCHAR2
      )
    IS
      SELECT NULL
      FROM   further_attributes
      WHERE  fat_ele_code = p_ele_code
      AND    NVL(fat_att_code,'NUL') = NVL(p_att_code,'NUL')
      AND    fat_code = p_fat_code;
    CURSOR get_ipp_refno
      (p_ipp_shortname VARCHAR2)
    IS
      SELECT ipp_refno
      FROM   interested_parties
      WHERE  ipp_shortname = p_ipp_shortname;
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'VALIDATE';
    ct VARCHAR2(30) := 'DL_HPM_SURVEY_RESULTS';
    cs INTEGER;
    ce VARCHAR2(200);
    ci INTEGER;
    l_id ROWID;
    l_min_seqno      INTEGER;
    l_max_seqno      INTEGER;
    l_scs_refno      NUMBER(8);
    l_pro_refno      NUMBER(10);
    l_aun_code       VARCHAR2(20);
    l_ele_value_type VARCHAR2(1);
    l_ele_type       VARCHAR2(2);
    l_attr_type      VARCHAR2(3);
    l_exists         VARCHAR2(1);
    l_errors         VARCHAR2(1);
    l_error_ind      VARCHAR2(1);
    i                INTEGER := 0;
    l_ipp_refno      NUMBER(10);
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_survey_results.dataload_validate');
    fsc_utils.debug_message('s_dl_hpm_survey_results.dataload_validate',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    OPEN c_range(cb);
    FETCH c_range INTO l_min_seqno,l_max_seqno;
    CLOSE c_range;
    WHILE l_min_seqno < l_max_seqno
    LOOP
      FOR p1 IN c1(p_batch_id,l_min_seqno,l_min_seqno + 29999)
      LOOP
        BEGIN
          cs := p1.lsrt_dl_seqno;
          l_id := p1.rec_rowid;
          l_errors := 'V';
          l_error_ind := 'N';
          l_scs_refno := NULL;
          OPEN c_scs(p1.lsrt_sud_scs_reference);
          FETCH c_scs INTO l_scs_refno;
          CLOSE c_scs;
          IF (l_scs_refno IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',301);
          END IF;
          l_exists := NULL;
          IF p1.lsrt_sud_type = 'A' 
          THEN
            IF (NOT s_dl_hem_utils.exists_aun_code(p1.lsrt_sud_pro_aun_code)) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
            END IF;
          ELSE
            IF (NOT s_dl_hem_utils.exists_propref(p1.lsrt_sud_pro_aun_code)) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
            END IF;
          END IF;
          IF (p1.lsrt_hrv_loc_code IS NOT NULL AND p1.lsrt_hrv_loc_code != 'PRO' AND p1.lsrt_hrv_loc_code != 'AUN')
          THEN
            IF (NOT s_dl_hem_utils.exists_frv('LOCATION',p1.lsrt_hrv_loc_code,'Y')) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',329);
            END IF;
          END IF;
          IF (p1.lsrt_hrv_ret_code IS NOT NULL) 
          THEN
            IF (NOT s_dl_hem_utils.exists_frv('REPRTYPE',p1.lsrt_hrv_ret_code,'Y')) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',305);
            END IF;
          END IF;
          IF (p1.lsrt_hrv_pmu_code IS NOT NULL) 
          THEN
            IF (NOT s_dl_hem_utils.exists_frv('UNITS',p1.lsrt_hrv_pmu_code)) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',704);
            END IF;
          END IF;
          IF (NOT s_dl_hem_utils.exists_frv('SVACTION',p1.lsrt_hrv_sya_code,'Y')) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',302);
          END IF;
          IF (NOT s_dl_hem_utils.exists_frv('REPURGENCY',p1.lsrt_hrv_rur_code,'Y')) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',303);
          END IF;
          IF (NOT s_dl_hem_utils.exists_frv('REPCOND',p1.lsrt_hrv_rco_code,'Y')) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',353);
          END IF;
          l_ele_value_type := NULL;
          l_ele_type := NULL;
          OPEN c_ele_code(p1.lsrt_ele_code);
          FETCH c_ele_code INTO l_ele_value_type,l_ele_type;
          CLOSE c_ele_code;
          IF (l_ele_type NOT IN('PR','SE')) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',981);
          END IF;
          IF (l_ele_value_type IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',031);
          ELSE
            l_attr_type := NULL;
            IF (p1.lsrt_att_code IS NOT NULL) 
            THEN
              l_attr_type := l_attr_type||'A';
            END IF;
            IF (p1.lsrt_date_value IS NOT NULL) 
            THEN
              l_attr_type := l_attr_type||'D';
            END IF;
            IF (p1.lsrt_numeric_value IS NOT NULL) 
            THEN
              l_attr_type := l_attr_type||'V';
            END IF;
            IF (l_attr_type IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',039);
            END IF;
            IF l_attr_type NOT IN('A','D','V') 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',040);
            END IF;
            IF (l_attr_type||l_ele_value_type NOT IN('AM','AC','DD','VN')) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',032);
            END IF;
            IF (p1.lsrt_att_code IS NOT NULL) 
            THEN
              l_exists := NULL;
              OPEN c_att_code(p1.lsrt_ele_code,p1.lsrt_att_code);
              FETCH c_att_code
              INTO l_exists;
              CLOSE c_att_code;
              IF (l_exists IS NULL) 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',041);
              END IF;
            END IF;
          END IF;
          IF (p1.lsrt_fat_code IS NOT NULL AND p1.lsrt_fat_code != 'NUL') 
          THEN
            OPEN c_fat_code(p1.lsrt_ele_code,p1.lsrt_att_code,p1.lsrt_fat_code);
            FETCH c_fat_code
            INTO l_exists;
            IF (c_fat_code%notfound) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',328);
            END IF;
            CLOSE c_fat_code;
          END IF;
          IF (p1.lsrt_created_date IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',952);
          END IF;
          IF (p1.lsrt_created_by IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',953);
          END IF;
          IF (NOT s_dl_hem_utils.yornornull(p1.lsrt_handheld_created_ind)) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',354);
          END IF;
          IF (NOT s_dl_hem_utils.yornornull(p1.lsrt_copied_ind)) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',355);
          END IF;
          IF (p1.lsrt_upload_ipp_shortname IS NOT NULL) 
          THEN
            l_ipp_refno := NULL;
            OPEN get_ipp_refno(p1.lsrt_upload_ipp_shortname);
            FETCH get_ipp_refno INTO l_ipp_refno;
            CLOSE get_ipp_refno;
            IF (l_ipp_refno IS NULL) 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',572);
            END IF;
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
    fsc_utils.proc_end;
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
      SELECT MIN(lsrt_dl_seqno)
      ,      MAX(lsrt_dl_seqno + 1)
      FROM   dl_hpm_survey_results
      WHERE  lsrt_dlb_batch_id = p_batch_id
      AND    lsrt_dl_load_status IN('C');
    CURSOR c1
      (p_batch_id VARCHAR2
      ,p_min_seqno NUMBER
      ,p_max_seqno NUMBER
      )
    IS
      SELECT lsrt_dlb_batch_id
      ,      lsrt_dl_seqno
      ,      lsrt_dl_load_status
      ,      lsrt_sud_scs_reference
      ,      lsrt_sud_pro_aun_code
      ,      lsrt_sud_type
      ,      lsrt_ele_code
      ,      lsrt_type
      ,      NVL(lsrt_handheld_created_ind,'N') lsrt_handheld_created_ind
      ,      NVL(lsrt_copied_ind,'N') lsrt_copied_ind
      ,      NVL(lsrt_created_by,'DATALOAD') lsrt_created_by
      ,      NVL(lsrt_created_date,sysdate) lsrt_created_date
      ,      lsrt_assessment_date
      ,      lsrt_cmpt_display_seq
      ,      lsrt_sub_cmpt_display_seq
      ,      lsrt_material_display_seq
      ,      lsrt_hrv_loc_code
      ,      lsrt_att_code
      ,      lsrt_fat_code
      ,      lsrt_comments
      ,      lsrt_quantity
      ,      lsrt_date_value
      ,      lsrt_numeric_value
      ,      lsrt_estimated_cost
      ,      lsrt_upload_ipp_shortname
      ,      lsrt_hrv_ret_code
      ,      lsrt_hrv_pmu_code
      ,      lsrt_hrv_sya_code
      ,      lsrt_hrv_rur_code
      ,      lsrt_hrv_rco_code
      ,      rowid rec_rowid
      FROM   dl_hpm_survey_results
      WHERE  lsrt_dlb_batch_id = p_batch_id
      AND    lsrt_dl_load_status IN('C')
      AND    lsrt_dl_seqno BETWEEN p_min_seqno AND p_max_seqno;
    CURSOR c_scs
      (p_reference VARCHAR2)
    IS
      SELECT scs_refno
      FROM   stock_condition_surveys
      WHERE  scs_reference = p_reference;
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'DELETE';
    ct VARCHAR2(30) := 'DL_HPM_SURVEY_RESULTS';
    cs INTEGER;
    ce VARCHAR2(200);
    ci INTEGER;
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
    l_an_tab       VARCHAR2(1);
    i              INTEGER := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_hpm_survey_results.dataload_delete');
    fsc_utils.debug_message('s_dl_hpm_survey_results.dataload_delete',3);
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    cb := p_batch_id;
    cd := p_date;
    OPEN  c_range(p_batch_id);
    FETCH c_range INTO l_min_seqno,l_max_seqno;
    CLOSE c_range;
    WHILE l_min_seqno < l_max_seqno
    LOOP
      FOR p1 IN c1(p_batch_id,l_min_seqno,l_min_seqno + 29999)
      LOOP
        BEGIN
          cs := p1.lsrt_dl_seqno;
          l_id := p1.rec_rowid;
          i := i + 1;
          SAVEPOINT SP1;
          l_scs_refno := NULL;
          OPEN c_scs(p1.lsrt_sud_scs_reference);
          FETCH c_scs INTO l_scs_refno;
          CLOSE c_scs;
          IF (p1.lsrt_sud_type = 'A') 
          THEN
            l_pro_aun_code := p1.lsrt_sud_pro_aun_code;
          ELSE
            l_pro_refno := NULL;
            l_pro_refno := s_dl_hem_utils.pro_refno_for_propref(p1.lsrt_sud_pro_aun_code);
            l_pro_aun_code := TO_CHAR(l_pro_refno);
          END IF;
          DELETE
          FROM   survey_results
          WHERE  srt_sud_scs_refno = l_scs_refno
          AND    srt_sud_type = p1.lsrt_sud_type
          AND    srt_sud_pro_aun_code = p1.lsrt_sud_pro_aun_code
          AND    srt_ele_code = p1.lsrt_ele_code
          AND    srt_hrv_loc_code = p1.lsrt_hrv_loc_code
          AND    NVL(srt_att_code,'~#~') = NVL(p1.lsrt_att_code,'~#~');
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
          s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
          set_record_status_flag(l_id,'C');
        END;
      END LOOP;
    END LOOP;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('SURVEY_RESULTS');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_hpm_survey_results;
/
