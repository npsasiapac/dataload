CREATE OR REPLACE PACKAGE BODY s_dl_hpl_psl_schemes        
AS                 
-- ***********************************************************************
--  DESCRIPTION:                            
--               
--  CHANGE CONTROL                          
--  VERSION DB Ver  WHO  WHEN        WHY         
--  1.0     6.15.0  MJK  02-MAY-2017 Initial version
--  1.1     6.15.0  MJK  08-MAY-2017 Corrected error numbers
--  1.2     6.15    AJ   23-MAY-2017 1)aun_code not mandatory do c4.aun_code
--                                   check amended
--                                   2)Checks added for aun_code, psls_code
--                                   and duplicates in batch load file
--                                   3)Check on created date
--                                   4)Mlang fields added to both create and validate
--  1.3     6.15    AJ   24-MAY-2017 Amended location of l_id in validate section
--
-- ***********************************************************************
  -- ***********************************************************************
  --  Procedure set_record_status_flag                                      
  -- ***********************************************************************
  PROCEDURE set_record_status_flag
   (p_rowid  IN ROWID
   ,p_status IN VARCHAR2
   )     
  AS 
  BEGIN
    UPDATE dl_hpl_psl_schemes
    SET lpsls_dl_load_status = p_status
    WHERE rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_hpl_psl_schemes');
    RAISE;
  END set_record_status_flag;
  -- ***********************************************************************
  --  Procedure dataload_create                                         
  -- ***********************************************************************
  PROCEDURE dataload_create                     
    (p_batch_id          IN VARCHAR2
    ,p_date              IN DATE
    )                 
  AS                                 
  CURSOR c1 
  IS             
    SELECT ROWID rec_rowid
    ,      lpsls_dlb_batch_id
    ,      lpsls_dl_seqno
    ,      lpsls_dl_load_status
    ,      lpsls_code                    
    ,      lpsls_description             
    ,      lpsls_start_date              
    ,      lpsls_sco_code                
    ,      lpsls_status_date             
    ,      lpsls_created_by              
    ,      lpsls_created_date            
    ,      lpsls_par_per_alt_ref         
    ,      lpsls_psty_code               
    ,      lpsls_proposed_end_date       
    ,      lpsls_actual_end_date         
    ,      lpsls_target_no_of_properties 
    ,      lpsls_relet_weeks             
    ,      lpsls_fail_to_nominate_weeks  
    ,      lpsls_fail_to_nominate_charge 
    ,      lpsls_comments                
    ,      lpsls_invoice_cycle           
    ,      lpsls_aun_code
    ,      lpsls_code_mlang
    ,      lpsls_description_mlang	
    FROM   dl_hpl_psl_schemes
    WHERE  lpsls_dlb_batch_id = p_batch_id     
    AND    lpsls_dl_load_status = 'V';
  --
  -- Cursor to derive par_refno
  --
  CURSOR c2 
    (cp_par_per_alt_ref parties.par_per_alt_ref%TYPE)
  IS
    SELECT par_refno
    FROM   parties
    WHERE  par_per_alt_ref = cp_par_per_alt_ref;
  r2 c2%ROWTYPE;
  --
  -- Constants for process_summary  
  --
  cb       VARCHAR2(30);                        
  cd       DATE;     
  cp       VARCHAR2(30) := 'CREATE';            
  ct       VARCHAR2(30) := 'DL_HPL_PSL_SCHEMES';                      
  cs       INTEGER;                             
  ce       VARCHAR2(200);
  l_id     ROWID;
  i integer := 0;
  l_an_tab VARCHAR2(1);
  --  
  BEGIN
    --  
    fsc_utils.proc_start('s_dl_hpl_psl_schemes.dataload_create');
    fsc_utils.debug_message( 's_dl_hpl_psl_schemes.dataload_create',3);                 
    cb := p_batch_id;                             
    cd := p_date;      
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    --	
    FOR r1 in c1 
    LOOP                              
      --
      BEGIN                               
        cs := r1.lpsls_dl_seqno;
        l_id := r1.rec_rowid;
        SAVEPOINT SP1;                      
        OPEN c2(r1.lpsls_par_per_alt_ref);
        FETCH c2 into r2;
        CLOSE c2;
        --
        -- Insert into psl_schemes
        --
        INSERT into psl_schemes 
        (psls_code
        ,psls_description
        ,psls_start_date
        ,psls_sco_code
        ,psls_status_date
        ,psls_created_by
        ,psls_created_date
        ,psls_par_refno
        ,psls_reusable_refno
        ,psls_psty_code
        ,psls_proposed_end_date
        ,psls_actual_end_date
        ,psls_target_no_of_properties
        ,psls_relet_weeks
        ,psls_fail_to_nominate_weeks
        ,psls_fail_to_nominate_charge
        ,psls_comments
        ,psls_invoice_cycle
        ,psls_aun_code
        ,psls_code_mlang
        ,psls_description_mlang		
        )
        VALUES 
        (r1.lpsls_code                    
        ,r1.lpsls_description             
        ,r1.lpsls_start_date              
        ,r1.lpsls_sco_code                
        ,r1.lpsls_status_date             
        ,NVL(r1.lpsls_created_by,'DATALOAD')              
        ,NVL(r1.lpsls_created_date,TRUNC(SYSDATE))            
        ,r2.par_refno 
        ,reusable_refno_seq.nextval
        ,r1.lpsls_psty_code               
        ,r1.lpsls_proposed_end_date       
        ,r1.lpsls_actual_end_date         
        ,TO_NUMBER(r1.lpsls_target_no_of_properties) 
        ,TO_NUMBER(r1.lpsls_relet_weeks)            
        ,TO_NUMBER(r1.lpsls_fail_to_nominate_weeks)
        ,TO_NUMBER(r1.lpsls_fail_to_nominate_charge)
        ,r1.lpsls_comments                
        ,r1.lpsls_invoice_cycle           
        ,r1.lpsls_aun_code
        ,r1.lpsls_code_mlang
        ,r1.lpsls_description_mlang		
        );
        --  
        -- keep a count of the rows processed and commit after every 5000
        -- 
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
    --
    -- Section to analyse the table(s) populated by this data load
    --
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PSL_SCHEMES');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HPL_PSL_SCHEMES');
    --	
    fsc_utils.proc_end;
	COMMIT;	
  EXCEPTION       
  WHEN OTHERS 
  THEN
    set_record_status_flag(l_id,'O');                    
    s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');            
    RAISE;                         
  END dataload_create;                          
  -- ***********************************************************************
  --  Procedure dataload_validate                                          
  -- ***********************************************************************
  PROCEDURE dataload_validate                   
    (p_batch_id          IN VARCHAR2
    ,p_date              IN DATE
    )            
  AS                                 
  CURSOR c1 
  IS             
    SELECT ROWID rec_rowid
    ,      lpsls_dlb_batch_id
    ,      lpsls_dl_seqno
    ,      lpsls_dl_load_status
    ,      lpsls_code                    
    ,      lpsls_description             
    ,      lpsls_start_date              
    ,      lpsls_sco_code                
    ,      lpsls_status_date             
    ,      nvl(lpsls_created_by,'DATALOAD') lpsls_created_by              
    ,      nvl(lpsls_created_date,trunc(SYSDATE)) lpsls_created_date           
    ,      lpsls_par_per_alt_ref         
    ,      lpsls_psty_code               
    ,      lpsls_proposed_end_date       
    ,      lpsls_actual_end_date         
    ,      lpsls_target_no_of_properties 
    ,      lpsls_relet_weeks             
    ,      lpsls_fail_to_nominate_weeks  
    ,      lpsls_fail_to_nominate_charge 
    ,      lpsls_comments                
    ,      lpsls_invoice_cycle           
    ,      lpsls_aun_code
    ,      lpsls_code_mlang
    ,      lpsls_description_mlang
    FROM   dl_hpl_psl_schemes
    WHERE  lpsls_dlb_batch_id = p_batch_id       
    AND    lpsls_dl_load_status in ('L','F','O'); 
  --
  -- Cursor to check party exists
  --
  CURSOR c2 
    (cp_par_per_alt_ref parties.par_per_alt_ref%TYPE)
  IS
    SELECT par_refno
    FROM   parties
    WHERE  par_per_alt_ref = cp_par_per_alt_ref;
  r2 c2%ROWTYPE;
  --
  -- Cursor to check psl_scheme_type exists
  --
  CURSOR c3 
    (cp_psls_psty_code psl_scheme_types.psty_code%TYPE)
  IS
    SELECT psty_code
    FROM   psl_scheme_types
    WHERE  psty_code = cp_psls_psty_code;
  r3 c3%ROWTYPE;
  --
  -- Cursor to check admin_unit exists
  --
  CURSOR c4 
    (cp_aun_code admin_units.aun_code%TYPE)
  IS
    SELECT aun_code
    FROM   admin_units
    WHERE  aun_code = cp_aun_code;
  r4 c4%ROWTYPE;
  --
  -- Cursor to check duplicates in psl_schemes
  --
  CURSOR c5 
    (cp_lpsls_code psl_schemes.psls_code%TYPE)
  IS
    SELECT psls_code
    FROM   psl_schemes
    WHERE  psls_code = cp_lpsls_code;
  r5 c5%ROWTYPE;
  --
  -- Cursor to check duplicate scheme in load file
  --
  CURSOR c6 
    (cp_lpsls_code dl_hpl_psl_schemes.lpsls_code%TYPE
	,cp_batch_id   dl_hpl_psl_schemes.lpsls_dlb_batch_id%TYPE)
  IS
    SELECT count(*)
    FROM   dl_hpl_psl_schemes
    WHERE  lpsls_code = cp_lpsls_code
    AND    lpsls_dlb_batch_id = cp_batch_id;
  --
  -- Cursor to check mlang duplicates in psl_schemes
  --
  CURSOR c7 
    (cp_lpsls_code_mlang psl_schemes.psls_code_mlang%TYPE)
  IS
    SELECT psls_code_mlang
    FROM   psl_schemes
    WHERE  psls_code_mlang = cp_lpsls_code_mlang;
  r7 c7%ROWTYPE;
  --
  -- Cursor to check duplicate mlang scheme in load file
  --
  CURSOR c8 
    (cp_lpsls_code_mlang dl_hpl_psl_schemes.lpsls_code_mlang%TYPE
	,cp_batch_id         dl_hpl_psl_schemes.lpsls_dlb_batch_id%TYPE)
  IS
    SELECT count(*)
    FROM   dl_hpl_psl_schemes
    WHERE  lpsls_code_mlang = cp_lpsls_code_mlang
    AND    lpsls_dlb_batch_id = cp_batch_id;
  --
  cb          VARCHAR2(30);                        
  cd          DATE;     
  cp          VARCHAR2(30) := 'VALIDATE';          
  ct          VARCHAR2(30) := 'DL_HPL_PSL_SCHEMES';                      
  cs          INTEGER;   
  ce          VARCHAR2(200);                
  l_errors    VARCHAR2(10);                
  l_id        ROWID;
  l_error_ind VARCHAR2(10);                
  i           INTEGER := 0;
  l_count_r6  NUMBER(3);
  l_count_r8  NUMBER(3);
  --  
  BEGIN                             
    fsc_utils.proc_start('s_dl_hpl_psl_schemes.dataload_validate');     
    fsc_utils.debug_message( 's_dl_hpl_psl_schemes.dataload_validate',3);                                           
    cb := p_batch_id;                             
    cd := p_DATE;                       
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');                                    
    FOR r1 IN c1 
    LOOP                                              
      --
      BEGIN                               
        cs := r1.lpsls_dl_seqno;
        l_id := r1.rec_rowid;
        l_errors := 'V';   
        l_error_ind := 'N';
        --
        -- Check mandatory fields
        --
        IF r1.lpsls_code IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',204);
        END IF;
        IF r1.lpsls_description IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',205);
        END IF;
        IF r1.lpsls_start_date IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',206);
        END IF;
        IF r1.lpsls_sco_code IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',207);
        END IF;
        IF r1.lpsls_status_date IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',208);
        END IF;
        IF r1.lpsls_par_per_alt_ref IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',209);
        END IF;
        IF r1.lpsls_psty_code IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',210);
        END IF;
        --
        --  Check values
        --
        IF r1.lpsls_sco_code NOT IN ('CLO','CUR','PEN')
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',211);
        END IF;
		--
        IF r1.lpsls_status_date > TRUNC(SYSDATE)
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',212);
        END IF;
		--
        r2.par_refno := NULL;
        OPEN c2(r1.lpsls_par_per_alt_ref);
        FETCH c2 into r2;
        CLOSE c2;
        IF r2.par_refno IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',213);
        END IF;
        --        
        r3.psty_code := NULL;
        OPEN c3(r1.lpsls_psty_code);
        FETCH c3 into r3;
        CLOSE c3;
        IF r3.psty_code IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',214);
        END IF;
        --		
        IF NVL(r1.lpsls_proposed_end_date,r1.lpsls_status_date + 1) <= r1.lpsls_status_date
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',215);
        END IF;        
        IF NVL(r1.lpsls_actual_end_date,r1.lpsls_status_date + 1) <= r1.lpsls_status_date
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',216);
        END IF;
        --
        -- chk only needed when aun_code supplied (AJ)
        --		
        r4.aun_code := NULL;
		--
		IF r1.lpsls_aun_code IS NOT NULL
		 THEN
          OPEN c4(r1.lpsls_aun_code);
          FETCH c4 into r4;
          CLOSE c4;
          IF r4.aun_code IS NULL
           THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',217);
          END IF; 
        END IF; 
        --
        -- Check to make sure scheme does not already exist in psl_schemes
        --        
        r5.psls_code := NULL;
        OPEN c5(r1.lpsls_code);
        FETCH c5 into r5;
        CLOSE c5;
        IF r5.psls_code IS NOT NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',221);
        END IF;
        --
        -- Check to make sure scheme is not a duplicate in load file
        --
        l_count_r6 := NULL;
        OPEN c6(r1.lpsls_code,r1.lpsls_dlb_batch_id);
        FETCH c6 into l_count_r6;
        CLOSE c6;
        IF l_count_r6 > 1
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',222);
        END IF;
        --
        -- Check to make sure mlang scheme does not already exist in psl_schemes
        --        
        r7.psls_code_mlang := NULL;
        OPEN c7(r1.lpsls_code_mlang);
        FETCH c7 into r7;
        CLOSE c7;
        IF r7.psls_code_mlang IS NOT NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',223);
        END IF;
		--
        -- Check to make sure mlang scheme is not a duplicate in load file
        --
        l_count_r8 := NULL;
        OPEN c8(r1.lpsls_code_mlang,r1.lpsls_dlb_batch_id);
        FETCH c8 into l_count_r8;
        CLOSE c8;
        IF l_count_r8 > 1
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',224);
        END IF;
		--
        IF r1.lpsls_created_date > TRUNC(SYSDATE)
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',225);
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
        -- Keep a count of the rows processed and commit after every 2000        
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
    fsc_utils.proc_end;
  EXCEPTION    
  WHEN OTHERS 
  THEN            
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');             
    RAISE;              
  END dataload_validate;                                              
  -- ***********************************************************************
  --  Procedure dataload_delete                                          
  -- ***********************************************************************
  PROCEDURE dataload_delete 
    (p_batch_id        IN VARCHAR2
    ,p_date            IN DATE
    ) 
  IS                 
  CURSOR c1 
  IS       
    SELECT ROWID rec_rowid
    ,      lpsls_dl_seqno
    ,      lpsls_code
    FROM   dl_hpl_psl_schemes
    WHERE  lpsls_dlb_batch_id = p_batch_id
    AND    lpsls_dl_load_status = 'C';
  cb       VARCHAR2(30);                        
  cd       DATE;     
  cp       VARCHAR2(30) := 'DELETE';            
  ct       VARCHAR2(30) := 'DL_HPL_PSL_SCHEMES';                      
  cs       INTEGER;    
  ce       VARCHAR2(200);                               
  l_id     ROWID;
  i integer := 0;
  l_an_tab VARCHAR2(1);
  --  
  BEGIN              
    fsc_utils.proC_START('s_dl_hpl_psl_schemes.dataload_delete');       
    fsc_utils.debug_message( 's_dl_hpl_psl_schemes.dataload_delete',3 );                           
    cb := p_batch_id;                             
    cd := p_date;
    --	
    FOR r1 IN c1 
    LOOP
      --	
      BEGIN              
        cs := r1.lpsls_dl_seqno;
        l_id := r1.rec_rowid;
        SAVEPOINT SP1;                      
        DELETE from psl_schemes
        WHERE  psls_code = r1.lpsls_code;
        --
        -- keep a count of the rows processed and commit after every 2000            
        --      
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
	COMMIT;
    --
    -- Section to analyse the table(s) populated by this data load
    --
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PSL_SCHEMES');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HPL_PSL_SCHEMES');
    --		
    fsc_utils.proc_end;            
    COMMIT; 
  EXCEPTION                   
  WHEN OTHERS 
  THEN         
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');                  
    RAISE;                        
  END dataload_delete;                
END s_dl_hpl_psl_schemes;                
/

show errors



