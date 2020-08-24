CREATE OR REPLACE PACKAGE BODY s_dl_had_adv_case_associations
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     6.14      MJK  14-NOV-2017  Initial Creation.
--  1.1     6.14      AJ   08-DEC-2017  1) Default values for modified date
--                                      and modified by removed so only
--                                      put in when supplied
--                                      2) Validate and copy paste
--                                      issued amended
--                                      3) Missing check added when both the
--                                      linked object references supplied
--                                      4) Check added for created date and
--                                      modified date
--
-- ***********************************************************************
--
PROCEDURE set_record_status_flag
  (p_rowid  IN ROWID
  ,p_status IN VARCHAR2
  )
AS
BEGIN
  UPDATE dl_had_adv_case_associations
  SET    lacan_dl_load_status = p_status
  WHERE  rowid = p_rowid;
EXCEPTION
WHEN OTHERS 
THEN
  dbms_output.put_line('Error updating status of dl_had_adv_case_associations');
  RAISE;
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create
  (p_batch_id          IN VARCHAR2
  ,p_date              IN DATE
  )
AS
CURSOR c1 
IS
  SELECT ROWID  rec_rowid
  ,      lacan_dlb_batch_id      
  ,      lacan_dl_seqno          
  ,      lacan_dl_load_status    
  ,      lacan_acas_alternate_ref
  ,      lacan_app_legacy_ref    
  ,      lacan_tcy_alt_ref       
  ,      lacan_hrv_acar_code     
  ,      NVL(lacan_created_by,'DATALOAD')        lacan_created_by        
  ,      NVL(lacan_created_date,TRUNC(SYSDATE))  lacan_created_date      
  ,      lacan_comments          
  ,      lacan_modified_by        
  ,      lacan_modified_date      
  FROM   dl_had_adv_case_associations
  WHERE  lacan_dlb_batch_id = p_batch_id
  AND    lacan_dl_load_status = 'V';
CURSOR get_acas_reference
  (cp_acas_alt_reference VARCHAR2)
IS
  SELECT acas_reference
  FROM   advice_cases
  WHERE  acas_alternate_reference = TO_CHAR(cp_acas_alt_reference);
CURSOR get_app_refno
  (cp_app_legacy_ref VARCHAR2) 
IS
  SELECT app_refno
  FROM   applications
  WHERE  app_legacy_ref = cp_app_legacy_ref;
CURSOR get_tcy_refno
  (cp_tcy_alt_ref VARCHAR2) 
IS
  SELECT tcy_refno
  FROM   tenancies
  WHERE  tcy_alt_ref = cp_tcy_alt_ref;
cb                VARCHAR2(30);   
cd                DATE;   
cp                VARCHAR2(30) := 'CREATE';   
ct                VARCHAR2(30) := 'DL_HAD_ADV_CASE_ASSOCIATIONS';   
cs                INTEGER;   
ce                VARCHAR2(200);   
l_id              ROWID;   
l_an_tab          VARCHAR2(1);   
i                 INTEGER := 0;
l_exists          VARCHAR2(1);
l_acas_reference  NUMBER(10);
l_app_refno       NUMBER(10);
l_tcy_refno       NUMBER(10);
BEGIN
  fsc_utils.proc_start('s_dl_had_adv_case_associations.dataload_create');
  fsc_utils.debug_message('s_dl_had_adv_case_associations.dataload_create',3);
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  FOR r1 in c1 
  LOOP
    BEGIN  
      cs := r1.lacan_dl_seqno;    
      l_id := r1.rec_rowid;    
      SAVEPOINT SP1;    
      l_acas_reference := NULL;    
      OPEN get_acas_reference(r1.lacan_acas_alternate_ref);    
      FETCH get_acas_reference INTO l_acas_reference;    
      CLOSE get_acas_reference;    
      l_app_refno := NULL;    
      OPEN get_app_refno(r1.lacan_app_legacy_ref);    
      FETCH get_app_refno INTO l_app_refno;    
      CLOSE get_app_refno;    
      l_tcy_refno := NULL;    
      OPEN get_tcy_refno(r1.lacan_tcy_alt_ref);    
      FETCH get_tcy_refno INTO l_tcy_refno;    
      CLOSE get_tcy_refno;    
      --  
      -- Insert into ADVICE_CASE_PEOPLE  
      --  
      INSERT /* +APPEND */ INTO advice_case_associations    
      (acan_refno
      ,acan_acas_reference
      ,acan_app_refno
      ,acan_tcy_refno
      ,acan_hrv_acar_code
      ,acan_created_by
      ,acan_created_date
      ,acan_comments
      ,acan_modified_by
      ,acan_modified_date
      )    
      VALUES     
      (acan_refno_seq.NEXTVAL
      ,l_acas_reference    
      ,l_app_refno    
      ,l_tcy_refno  
      ,r1.lacan_hrv_acar_code  
      ,r1.lacan_created_by  
      ,r1.lacan_created_date   
      ,r1.lacan_comments   
      ,r1.lacan_modified_by    
      ,r1.lacan_modified_date 
      );    
      i := i+1;     
      IF MOD(i,50000) = 0     
      THEN     
        COMMIT;     
      END IF;    
      --  
      -- Now UPDATE the record status and process count  
      --  
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
  fsc_utils.proc_end;
EXCEPTION
WHEN OTHERS 
THEN
  s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
  RAISE;
END dataload_create;
--
-- ***********************************************************************
--
PROCEDURE dataload_validate
  (p_batch_id          IN VARCHAR2
  ,p_date              IN DATE
  )
AS
CURSOR c1 
IS
  SELECT ROWID  rec_rowid
  ,      lacan_dlb_batch_id      
  ,      lacan_dl_seqno          
  ,      lacan_dl_load_status    
  ,      lacan_acas_alternate_ref
  ,      lacan_app_legacy_ref    
  ,      lacan_tcy_alt_ref       
  ,      lacan_hrv_acar_code     
  ,      NVL(lacan_created_by,'DATALOAD')        lacan_created_by        
  ,      NVL(lacan_created_date,TRUNC(SYSDATE))  lacan_created_date      
  ,      lacan_comments          
  ,      lacan_modified_by        
  ,      lacan_modified_date      
  FROM   dl_had_adv_case_associations
  WHERE  lacan_dlb_batch_id = p_batch_id
  AND    lacan_dl_load_status in ('L','F','O');
CURSOR chk_acas_exists
  (cp_acas_alt_reference VARCHAR2)
IS
  SELECT acas_reference
  FROM   advice_cases
  WHERE  acas_alternate_reference = TO_CHAR(cp_acas_alt_reference);
CURSOR chk_app_exists
  (cp_app_legacy_ref VARCHAR2) 
IS
  SELECT app_refno
  FROM   applications
  WHERE  app_legacy_ref = cp_app_legacy_ref;
CURSOR chk_tcy_exists
  (cp_tcy_alt_ref VARCHAR2) 
IS
  SELECT tcy_refno
  FROM   tenancies
  WHERE  tcy_alt_ref = cp_tcy_alt_ref;
CURSOR chk_acar_exists_app
  (cp_acan_hrv_acar_code VARCHAR2)
IS
  SELECT frv_code
  FROM   hrv_adv_case_app_assoc_rsns
  WHERE  frv_code = cp_acan_hrv_acar_code;
CURSOR chk_acar_exists_tcy
  (cp_acan_hrv_acar_code VARCHAR2)
IS
  SELECT frv_code
  FROM   hrv_adv_case_tcy_assoc_rsns
  WHERE  frv_code = cp_acan_hrv_acar_code;  
cb                    VARCHAR2(30);  
cd                    DATE;  
cp                    VARCHAR2(30) := 'VALIDATE';  
ct                    VARCHAR2(30) := 'DL_HAD_ADV_CASE_ASSOCIATIONS';  
cs                    INTEGER;  
ce                    VARCHAR2(200);  
l_id                  ROWID;  
l_exists              VARCHAR2(1);
l_acas_reference      advice_cases.acas_reference%TYPE;       
l_app_refno           applications.app_refno%TYPE; 
l_tcy_refno           tenancies.tcy_refno%TYPE;
l_acar_exists         VARCHAR2(10);  
l_errors              VARCHAR2(10);
l_error_ind           VARCHAR2(10);
i                     INTEGER :=0;
BEGIN
  fsc_utils.proc_start('s_dl_had_adv_case_associations.dataload_validate');  
  fsc_utils.debug_message('s_dl_had_adv_case_associations.dataload_validate',3);  
  cb := p_batch_id;  
  cd := p_DATE;  
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');  
  FOR r1 IN c1   
  LOOP  
    BEGIN
      cs := r1.lacan_dl_seqno;    
      l_id := r1.rec_rowid;    
      l_errors := 'V';    
      l_error_ind := 'N'; 
      IF r1.lacan_acas_alternate_ref IS NULL 
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',113);
      ELSE
        l_acas_reference := NULL;
        OPEN chk_acas_exists(r1.lacan_acas_alternate_ref);
        FETCH chk_acas_exists INTO l_acas_reference;
        CLOSE chk_acas_exists;
        IF l_acas_reference IS NULL 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',144);
        END IF;            
      END IF;
      --
      -- Check that either lacan_app_legacy_ref or lacan_tcy_alt_ref has been supplied (but not both)      
      -- 
      IF r1.lacan_app_legacy_ref IS NULL
      AND  r1.lacan_tcy_alt_ref IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',351);
      ELSE
        IF r1.lacan_app_legacy_ref IS NOT NULL
        THEN
          l_app_refno := NULL;
          OPEN chk_app_exists(r1.lacan_app_legacy_ref);
          FETCH chk_app_exists INTO l_app_refno;
          CLOSE chk_app_exists;
          IF (l_app_refno IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',352);
          END IF;
        ELSE
          l_tcy_refno := NULL;
          OPEN chk_tcy_exists(r1.lacan_tcy_alt_ref);
          FETCH chk_tcy_exists INTO l_tcy_refno;
          CLOSE chk_tcy_exists;
          IF (l_tcy_refno IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',353);
          END IF;
        END IF;
      END IF;
      IF ( r1.lacan_app_legacy_ref IS NOT NULL  AND
           r1.lacan_tcy_alt_ref    IS NOT NULL
          )
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',359);
      END IF;
      --
      -- Check advice case association reason lacan_hrv_acar_code is supplied and valid
      --
      IF r1.lacan_hrv_acar_code IS NULL 
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',354);
      ELSE
        l_acar_exists := NULL;
        IF r1.lacan_app_legacy_ref IS NOT NULL
        THEN
          OPEN chk_acar_exists_app(r1.lacan_hrv_acar_code);
          FETCH chk_acar_exists_app INTO l_acar_exists;
          IF l_acar_exists IS NULL
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',355);
          END IF;
          CLOSE chk_acar_exists_app;
        ELSIF r1.lacan_tcy_alt_ref IS NOT NULL
        THEN
          OPEN chk_acar_exists_tcy(r1.lacan_hrv_acar_code);
          FETCH chk_acar_exists_tcy INTO l_acar_exists;
          IF l_acar_exists IS NULL
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',356);
          END IF;
          CLOSE chk_acar_exists_tcy;
        END IF;
      END IF;
      --
      -- Check for created and modified dates if supplied
      --
      IF ( r1.lacan_created_date > SYSDATE)
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',360);
      END IF;
      --
      IF ( r1.lacan_modified_date IS NOT NULL)
      THEN
        IF (r1.lacan_modified_date < r1.lacan_created_date)
        THEN   
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',361);
        END IF;
      END IF;
      --
      IF (
	      ( r1.lacan_modified_date  IS NOT NULL  AND
            r1.lacan_modified_by    IS NULL
          )
		  OR
          ( r1.lacan_modified_date  IS NULL  AND
            r1.lacan_modified_by    IS NOT NULL
          )
         )
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',362);
      END IF;
      -- 
      -- Now UPDATE the record status and process count 
      -- 
      IF l_errors = 'F' 
      THEN 
        l_error_ind := 'Y'; 
      ELSE 
        l_error_ind := 'N'; 
      END IF; 
      s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind); 
      set_record_status_flag(l_id,l_errors); 
      -- 
      -- keep a count of the rows processed and commit after every 1000 
      -- 
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
-- ***********************************************************************
--
PROCEDURE dataload_delete
  (p_batch_id       IN VARCHAR2
  ,p_date           IN DATE
  ) 
IS
CURSOR c1 
IS
  SELECT ROWID                       rec_rowid
  ,      lacan_dlb_batch_id
  ,      lacan_dl_seqno
  ,      lacan_dl_load_status
  ,      lacan_acas_alternate_ref
  ,      lacan_app_legacy_ref    
  ,      lacan_tcy_alt_ref       
  FROM   dl_had_adv_case_associations
  WHERE  lacan_dlb_batch_id = p_batch_id
  AND    lacan_dl_load_status = 'C';
CURSOR get_acas_reference
  (cp_acas_alt_reference VARCHAR2)
IS
  SELECT acas_reference
  FROM   advice_cases
  WHERE  acas_alternate_reference = TO_CHAR(cp_acas_alt_reference);
CURSOR get_app_refno
  (cp_app_legacy_ref VARCHAR2) 
IS
  SELECT app_refno
  FROM   applications
  WHERE  app_legacy_ref = cp_app_legacy_ref;
CURSOR get_tcy_refno
  (cp_tcy_alt_ref VARCHAR2) 
IS
  SELECT tcy_refno
  FROM   tenancies
  WHERE  tcy_alt_ref = cp_tcy_alt_ref;
cb                    VARCHAR2(30);
cd                    DATE;
cp                    VARCHAR2(30) := 'DELETE';
ct                    VARCHAR2(30) := 'DL_HAD_ADV_CASE_ASSOCIATIONS';
cs                    INTEGER;
ce                    VARCHAR2(200);
l_id                  ROWID;
l_exists              VARCHAR2(1);
l_acas_reference      advice_cases.acas_reference%TYPE;       
l_app_refno           applications.app_refno%TYPE; 
l_tcy_refno           tenancies.tcy_refno%TYPE;
i                     INTEGER :=0;
l_an_tab              VARCHAR2(1);
BEGIN
  fsc_utils.proc_start('s_dl_had_adv_case_associations.dataload_delete');
  fsc_utils.debug_message('s_dl_had_adv_case_associations.dataload_delete',3 );
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  FOR r1 in c1 
  LOOP
    BEGIN
      cs := r1.lacan_dl_seqno;
      l_id := r1.rec_rowid;
      i := i + 1;
      l_acas_reference := NULL;
      OPEN get_acas_reference(r1.lacan_acas_alternate_ref);
      FETCH get_acas_reference INTO l_acas_reference;
      CLOSE get_acas_reference;
      l_app_refno := NULL;
      OPEN get_app_refno(r1.lacan_app_legacy_ref);
      FETCH get_app_refno INTO l_app_refno;
      CLOSE get_app_refno;
      l_tcy_refno := NULL;
      OPEN get_tcy_refno(r1.lacan_tcy_alt_ref);
      FETCH get_tcy_refno INTO l_tcy_refno;
      CLOSE get_tcy_refno;
      DELETE 
      FROM   advice_case_associations
      WHERE  acan_acas_reference = l_acas_reference
      AND    NVL(acan_app_refno,'-1') = NVL(l_app_refno,'-1')
      AND    NVL(acan_tcy_refno,'-1') = NVL(l_tcy_refno,'-1');
      --
      -- Now UPDATE the record status and process count
      --
      s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
      set_record_status_flag(l_id,'V');
      IF MOD(i,5000) = 0 
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
  fsc_utils.proc_end;
  COMMIT;
EXCEPTION
WHEN OTHERS 
THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
  RAISE;
END dataload_delete;
END s_dl_had_adv_case_associations;
/

