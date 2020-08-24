create or replace PACKAGE BODY s_dl_hcs_contacts
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--
--     1.0             MOK  18/09/18     Initial Version
--     1.1             MJK  09/10/18     Reformatted
--     1.2             AJ   10/10/18     Added blank lines at bottom and slash
--     1.3             AJ   14/03/19     chk_bro_dets amended check including
--                                       bro_brc_code not required
--     1.4             AJ   26/03/19     1)Added Mandatory Checks and further 
--                                       validation for Subject Business Reason 
--                                       fields and additional checks for IPP and LAS
--     1.5             PL   24/04/19     SAHT BESPOKE remove Loans                                  
--
-- ***********************************************************************   
  PROCEDURE set_record_status_flag
    (p_rowid  IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_hcs_contacts
    SET    lcct_dl_load_status = p_status
    WHERE  ROWID               = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_hcs_contacts');
    RAISE;
  END set_record_status_flag;
  --
  -- ************************************************************************************
  --
  PROCEDURE dataload_create
    (p_batch_id          IN VARCHAR2
    ,p_date              IN DATE
    )
  AS
  CURSOR c1 
  IS  
    SELECT rowid rec_rowid
    ,      lcct_dlb_batch_id
    ,      lcct_dl_seqno
    ,      lcct_dl_load_status
    ,      lcct_obj_legacy_ref
    ,      lcct_secondary_ref
    ,      lcct_secondary_date
    ,      lcct_legacy_type
    ,      lcct_received_date
    ,      lcct_ccm_code
    ,      lcct_cny_code
    ,      lcct_sub_legacy_ref
    ,      lcct_sub_sec_ref
    ,      lcct_sub_sec_date
    ,      lcct_subject_type
    ,      lcct_scn_bro_code
    ,      lcct_scn_brc_code
    ,      lcct_scn_target_date
    ,      lcct_duration
    ,      lcct_aun_code_responsible
    ,      lcct_sco_code
    ,      lcct_status_date
    ,      NVL(lcct_created_by,'DATALOAD') lcct_created_by
    ,      NVL(lcct_created_date, SYSDATE) lcct_created_date
    ,      lcct_comments
    ,      lcct_usr_jro_code
    ,      lcct_usr_username
    ,      lcct_correspond_reference
    ,      lcct_answered_date
    ,      lcct_ano_code
    ,      lcct_outcome_comments
    ,      lcct_ban_alt_ref
    FROM   dl_hcs_contacts
    WHERE  lcct_dlb_batch_id = p_batch_id
    AND    lcct_dl_load_status = 'V';
  CURSOR get_par_refno
    (cp_par_per_alt_ref VARCHAR2) 
  IS
    SELECT par_refno
    FROM   parties
    WHERE  par_per_alt_ref = cp_par_per_alt_ref;
  CURSOR get_pro_refno
    (cp_pro_propref VARCHAR2) 
  IS
    SELECT pro_refno
    FROM   properties
    WHERE  pro_propref = cp_pro_propref;
  CURSOR get_aun_code
    (cp_aun_code VARCHAR2) 
  IS
    SELECT aun_code
    FROM   admin_units
    WHERE  aun_code = cp_aun_code;
  CURSOR get_tcy_refno
    (cp_tcy_refno VARCHAR2) 
  IS
    SELECT tcy_refno
    FROM   tenancies
    WHERE  tcy_refno = cp_tcy_refno;
  CURSOR get_ipp_refno
    (cp_ipp_shortname VARCHAR2
    ,cp_ipp_ipt_code  VARCHAR2
    )   
  IS
    SELECT ipp_refno
    FROM   interested_parties
    WHERE  ipp_shortname = cp_ipp_shortname
    AND    ipp_ipt_code  = cp_ipp_ipt_code;
  CURSOR get_app_refno
    (cp_app_legacy_ref VARCHAR2) 
  IS
    SELECT app_refno
    FROM   applications
    WHERE  app_legacy_ref = cp_app_legacy_ref;
  CURSOR get_peg_code
    (cp_peg_code VARCHAR2) 
  IS
    SELECT peg_code
    FROM   people_groups
    WHERE  peg_code = cp_peg_code;
  CURSOR get_srq_no
    (cp_srq_legacy_refno VARCHAR2) 
  IS
    SELECT srq_no
    FROM   service_requests
    WHERE  srq_legacy_refno = cp_srq_legacy_refno;
  CURSOR get_cos_code
    (cp_cos_code VARCHAR2) 
  IS
    SELECT cos_code
    FROM   contractor_sites
    WHERE  cos_code = cp_cos_code;
  CURSOR get_tcy_refno2
    (cp_tcy_alt_ref VARCHAR2) 
  IS
    SELECT tcy_refno
    FROM   tenancies
    WHERE  tcy_alt_ref = cp_tcy_alt_ref;
  CURSOR get_cct_reference 
  IS
    SELECT cct_reference_seq.nextval
    FROM dual;
  CURSOR get_bro_dets(cp_bro_code VARCHAR2)
  IS 
    SELECT bro_type_ind
    ,      bro_address_required_ind 
    FROM   business_reasons 
    WHERE  bro_code = cp_bro_code 
    AND    bro_current_ind = 'Y';
  CURSOR c_get_reusable 
  IS
    SELECT reusable_refno_seq.nextval
    FROM dual;
  CURSOR cny_addr_code 
    (cp_cny_code VARCHAR2) 
  IS 
    SELECT 'X' 
    FROM   contact_source_types  
    WHERE  cny_object_type = 'PAR' 
    AND    cny_address_req_ind ='N'
    AND    cny_code = cp_cny_code;
  CURSOR get_scn_reference 
  IS
    SELECT scn_refno_seq.nextval
    FROM dual;
  cb                      VARCHAR2(30);
  cd                      DATE;
  cp                      VARCHAR2(30) := 'CREATE';
  ct                      VARCHAR2(30) := 'DL_HCS_BUSINESS_ACTIONS';
  cs                      INTEGER;
  ce                      VARCHAR2(200);
  l_id                    ROWID;
  l_an_tab                VARCHAR2(1);
  i                       INTEGER := 0;
  l_par_refno             NUMBER(8);
  l_pro_refno             NUMBER(10);
  l_las_pro_refno         NUMBER(10);
  l_aun_code              VARCHAR2(20);
  l_tcy_refno             NUMBER(8);
  l_ipp_refno             NUMBER(10);
  l_app_refno             NUMBER(10);
  l_peg_code              VARCHAR2(10);
  l_srq_no                NUMBER(10);
  l_cos_code              VARCHAR2(15);
  l_adr_refno             NUMBER(10);
  l_reusable_refno        NUMBER(20);
  l_ban_ban_reference     NUMBER(10);
  l_cct_reference_seq     NUMBER(10);
  l_sub_par_refno         NUMBER(8);
  l_sub_pro_refno         NUMBER(10);
  l_sub_las_pro_refno     NUMBER(10);
  l_sub_aun_code          VARCHAR2(20);
  l_sub_tcy_refno         NUMBER(8);
  l_sub_ipp_refno         NUMBER(10);
  l_sub_app_refno         NUMBER(10);
  l_sub_peg_code          VARCHAR2(10);
  l_sub_srq_no            NUMBER(10);
  l_sub_cos_code          VARCHAR2(15);
  l_sub_adr_refno         NUMBER(10);
  l_bro_type_ind          VARCHAR2(1);
  l_bro_add_req_ind       VARCHAR2(1);
  l_cny_addr_code         VARCHAR2(10);
  l_scn_reference_seq     NUMBER(10);
  BEGIN
    fsc_utils.proc_start('s_dl_hcs_business_actions.dataload_create');
    fsc_utils.debug_message( 's_dl_hcs_business_actions.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR r1 in c1 
    LOOP
      BEGIN
        cs := r1.lcct_dl_seqno;
        l_id := r1.rec_rowid;
        SAVEPOINT SP1;
        l_par_refno     := NULL;
        l_pro_refno     := NULL;
        l_aun_code      := NULL;
        l_tcy_refno     := NULL;
        l_ipp_refno     := NULL;
        l_app_refno     := NULL;
        l_peg_code      := NULL;
        l_srq_no        := NULL;
        l_cos_code      := NULL;          
        l_las_pro_refno := NULL;
        l_adr_refno     := NULL;
        l_sub_par_refno     := NULL;
        l_sub_pro_refno     := NULL;   
        l_sub_las_pro_refno := NULL;   
        l_sub_aun_code      := NULL;   
        l_sub_tcy_refno     := NULL;        
        l_sub_ipp_refno     := NULL;   
        l_sub_app_refno     := NULL; 
        l_sub_peg_code      := NULL;   
        l_sub_srq_no        := NULL;    
        l_sub_cos_code      := NULL;    
        l_sub_adr_refno     := NULL;    
        l_ban_ban_reference := NULL;
        l_cct_reference_seq := NULL;
        l_reusable_refno    := NULL;
        l_bro_type_ind      := NULL;
        l_bro_add_req_ind   := NULL;
        l_cny_addr_code     :=NULL;
        l_scn_reference_seq := NULL;		
        IF r1.lcct_legacy_type = 'PAR' 
        THEN
          OPEN get_par_refno(r1.lcct_obj_legacy_ref);
          FETCH get_par_refno INTO l_par_refno;
          CLOSE get_par_refno;           
          OPEN cny_addr_code (r1.lcct_cny_code);
          FETCH cny_addr_code INTO l_cny_addr_code;
          CLOSE cny_addr_code; 
          --
          -- If contact is for a party, and a source type with CNY_ADDR_REQ_IND=N, do not save address
          --       
          IF l_cny_addr_code IS NOT NULL
          THEN
            l_adr_refno := NULL;
          ELSE
            l_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_legacy_type,'',l_par_refno); 
          END IF;                     
        ELSIF r1.lcct_legacy_type = 'PRO' 
        THEN
          OPEN get_pro_refno(r1.lcct_obj_legacy_ref);
          FETCH get_pro_refno INTO l_pro_refno;
          CLOSE get_pro_refno;
          l_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_legacy_type,'',l_pro_refno);
        ELSIF r1.lcct_legacy_type = 'AUN' 
        THEN
          OPEN get_aun_code(r1.lcct_obj_legacy_ref);
          FETCH get_aun_code INTO l_aun_code;
          CLOSE get_aun_code;              
          l_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_legacy_type,l_aun_code,NULL);
        ELSIF r1.lcct_legacy_type = 'TAR' 
        THEN
          OPEN get_tcy_refno2(r1.lcct_obj_legacy_ref);
          FETCH get_tcy_refno2 INTO l_tcy_refno;
          CLOSE get_tcy_refno2;              
          l_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_legacy_type,'',l_tcy_refno);
        ELSIF r1.lcct_legacy_type = 'TCY'
        THEN
          OPEN get_tcy_refno(r1.lcct_obj_legacy_ref);
          FETCH get_tcy_refno INTO l_tcy_refno;
          CLOSE get_tcy_refno;              
          l_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_legacy_type,'',l_tcy_refno);
        ELSIF r1.lcct_legacy_type = 'IPP' 
        THEN
          OPEN get_ipp_refno(r1.lcct_obj_legacy_ref,r1.lcct_secondary_ref);
          FETCH get_ipp_refno INTO l_ipp_refno;
          CLOSE get_ipp_refno; 
          l_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_legacy_type,'',l_ipp_refno);
        ELSIF r1.lcct_legacy_type = 'APP' 
        THEN
          OPEN get_app_refno(r1.lcct_obj_legacy_ref);
          FETCH get_app_refno INTO l_app_refno;
          CLOSE get_app_refno;              
          l_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_legacy_type,'',l_app_refno);
        ELSIF r1.lcct_legacy_type = 'PEG'
        THEN
          OPEN get_peg_code(r1.lcct_obj_legacy_ref);
          FETCH get_peg_code INTO l_peg_code;
          CLOSE get_peg_code;
          l_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_legacy_type,l_peg_code,NULL);
        ELSIF r1.lcct_legacy_type = 'SRQ'
        THEN
          OPEN get_srq_no(r1.lcct_obj_legacy_ref);
          FETCH get_srq_no INTO l_srq_no;
          CLOSE get_srq_no;
          l_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_legacy_type,'',l_srq_no);
        ELSIF r1.lcct_legacy_type = 'COS'
        THEN
          OPEN get_cos_code(r1.lcct_obj_legacy_ref);
          FETCH get_cos_code INTO l_cos_code;
          CLOSE get_cos_code;
          l_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_legacy_type,l_cos_code,NULL);
        ELSIF r1.lcct_legacy_type = 'LAS' 
        THEN
          OPEN get_pro_refno(r1.lcct_obj_legacy_ref);
          FETCH get_pro_refno INTO l_las_pro_refno;
          CLOSE get_pro_refno;
          l_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_legacy_type,'',l_las_pro_refno);
        END IF;
        --
        -- Process the SUBJECT           
        --
        IF r1.lcct_subject_type = 'PAR' 
        THEN
          OPEN get_par_refno(r1.lcct_sub_legacy_ref);
          FETCH get_par_refno INTO l_sub_par_refno;
          CLOSE get_par_refno;
          l_sub_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_subject_type,'',l_sub_par_refno); 
        ELSIF r1.lcct_subject_type = 'PRO' 
        THEN
          OPEN get_pro_refno(r1.lcct_sub_legacy_ref);
          FETCH get_pro_refno INTO l_sub_pro_refno;
          CLOSE get_pro_refno;
          l_sub_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_subject_type,'',l_sub_pro_refno); 
        ELSIF r1.lcct_subject_type = 'AUN'
        THEN
          OPEN get_aun_code(r1.lcct_sub_legacy_ref);
          FETCH get_aun_code INTO l_sub_aun_code;
          CLOSE get_aun_code;              
          l_sub_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_subject_type,'l_sub_aun_code',NULL); 
        ELSIF r1.lcct_subject_type = 'TAR' 
        THEN
          OPEN get_tcy_refno2(r1.lcct_sub_legacy_ref);
          FETCH get_tcy_refno2 INTO l_sub_tcy_refno;
          CLOSE get_tcy_refno2;          
          l_sub_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_subject_type,'',l_sub_tcy_refno); 
        ELSIF r1.lcct_subject_type = 'TCY' 
        THEN
          OPEN get_tcy_refno(r1.lcct_sub_legacy_ref);
          FETCH get_tcy_refno INTO l_sub_tcy_refno;
          CLOSE get_tcy_refno;
          l_sub_adr_refno := s_dl_hcs_utils.get_adr_refno(r1.lcct_subject_type,'',l_sub_tcy_refno); 
        ELSIF r1.lcct_subject_type = 'IPP' 
        THEN
          OPEN get_ipp_refno(r1.lcct_sub_legacy_ref,r1.lcct_sub_sec_ref);
          FETCH get_ipp_refno INTO l_sub_ipp_refno;
          CLOSE get_ipp_refno;
          l_sub_adr_refno := s_dl_hcs_utils.get_adr_refno (r1.lcct_subject_type,'',l_sub_ipp_refno); 
        ELSIF r1.lcct_subject_type = 'APP' 
        THEN
          OPEN get_app_refno(r1.lcct_sub_legacy_ref);
          FETCH get_app_refno INTO l_sub_app_refno;
          CLOSE get_app_refno;              
          l_sub_adr_refno := s_dl_hcs_utils.get_adr_refno (r1.lcct_subject_type,'',l_sub_app_refno); 
        ELSIF r1.lcct_subject_type = 'PEG' 
        THEN
          OPEN get_peg_code(r1.lcct_sub_legacy_ref);
          FETCH get_peg_code INTO l_sub_peg_code;
          CLOSE get_peg_code;              
          l_sub_adr_refno := s_dl_hcs_utils.get_adr_refno (r1.lcct_subject_type,l_sub_peg_code,NULL); 
        ELSIF r1.lcct_subject_type = 'SRQ' 
        THEN
          OPEN get_srq_no(r1.lcct_sub_legacy_ref);
          FETCH get_srq_no INTO l_sub_srq_no;
          CLOSE get_srq_no;              
          l_sub_adr_refno := s_dl_hcs_utils.get_adr_refno (r1.lcct_subject_type,'',l_sub_srq_no); 
        ELSIF r1.lcct_subject_type = 'COS'
        THEN
          OPEN get_cos_code(r1.lcct_sub_legacy_ref);
          FETCH get_cos_code INTO l_sub_cos_code;
          CLOSE get_cos_code;              
          l_sub_adr_refno := s_dl_hcs_utils.get_adr_refno (r1.lcct_subject_type,l_sub_cos_code,NULL); 
        ELSIF r1.lcct_subject_type = 'LAS' 
        THEN
          OPEN get_pro_refno(r1.lcct_sub_legacy_ref);
          FETCH get_pro_refno INTO l_sub_las_pro_refno;
          CLOSE get_pro_refno;           
          l_sub_adr_refno := s_dl_hcs_utils.get_adr_refno (r1.lcct_subject_type,'',l_sub_las_pro_refno); 
        END IF; 
        --
        -- check the BRO CODE 
        --
        OPEN get_bro_dets(r1.lcct_scn_bro_code);
        FETCH get_bro_dets INTO l_bro_type_ind, l_bro_add_req_ind ;
        CLOSE get_bro_dets;
        --          
        -- get ccnt reference 
        --
        OPEN get_cct_reference;
        FETCH get_cct_reference INTO l_cct_reference_seq;
        CLOSE get_cct_reference;     
        --     
        --  get reusable refno 
        --
        OPEN c_get_reusable;
        FETCH c_get_reusable INTO l_reusable_refno;
        CLOSE c_get_reusable;     
        INSERT INTO contacts
        (cct_reference
        ,cct_received_date
        ,cct_hrv_ccm_code
        ,cct_cny_code
        ,cct_sco_code
        ,cct_status_date
        ,cct_created_by
        ,cct_created_date
        ,cct_par_refno
        ,cct_tcy_refno
        ,cct_ipp_refno
        ,cct_app_refno
        ,cct_peg_code
        ,cct_cos_code
        ,cct_par_refno_specific_to
        ,cct_adr_refno
        ,cct_jro_code
        ,cct_usr_username
        ,cct_jru_start_date
        ,cct_comments
        ,cct_hrv_ano_code
        ,cct_answered_date
        ,cct_outcome_comments
        ,cct_correspond_reference
        ,cct_las_lea_pro_refno
        ,cct_las_lea_start_date
        ,cct_las_start_date
        ,cct_aun_code
        ,cct_duration
        )
        VALUES
        (l_cct_reference_seq
        ,r1.lcct_received_date
        ,r1.lcct_ccm_code
        ,r1.lcct_cny_code
        ,r1.lcct_sco_code
        ,r1.lcct_status_date
        ,r1.lcct_created_by
        ,r1.lcct_created_date
        ,l_par_refno
        ,l_tcy_refno
        ,l_ipp_refno
        ,l_app_refno
        ,l_peg_code
        ,l_cos_code
        ,NULL
        ,l_adr_refno
        ,r1.lcct_usr_jro_code
        ,r1.lcct_usr_username
        ,NULL         
        ,r1.lcct_comments
        ,r1.lcct_ano_code
        ,r1.lcct_answered_date
        ,r1.lcct_outcome_comments
        ,r1.lcct_correspond_reference
        ,l_las_pro_refno
        ,NULL -- r1.lcct_secondary_ref  Could be a better fix but I'm feeling lazy
        ,NULL -- r1.lcct_secondary_date Could be a better fix but I'm feeling lazy
        ,r1.lcct_aun_code_responsible
        ,r1.lcct_duration
        );
        --        
        -- Update the DL table with the get_cct_reference needed for the delete 
        --
        UPDATE dl_hcs_contacts
        SET lcct_reference = l_cct_reference_seq
        WHERE rowid = r1.rec_rowid;               
        IF l_bro_type_ind = 'N' 
        THEN 
          INSERT INTO non_subj_cont_bus_reasons 
          (nsc_cct_reference
          ,nsc_bro_code
          ,nsc_sco_code
          ,nsc_status_date
          ,nsc_brc_code
          ,nsc_major_subject_ind 
          ,nsc_target_date
          ,nsc_actual_date
          ,nsc_reusable_refno
          ,nsc_comments
          )
          VALUES 
          (l_cct_reference_seq
          ,r1.lcct_scn_bro_code
          ,r1.lcct_sco_code
          ,r1.lcct_status_date
          ,r1.lcct_scn_brc_code
          ,'Y'
          ,r1.lcct_scn_target_date
          ,NULL
          ,l_reusable_refno
          ,r1.lcct_comments
          );            
        ELSIF l_bro_type_ind = 'S'
        THEN
          --          
          -- get_scn_reference 
          --
          OPEN get_scn_reference;
          FETCH get_scn_reference INTO l_scn_reference_seq;
          CLOSE get_scn_reference;     
          --		
          INSERT INTO subj_cont_bus_reasons
          (scn_refno
          ,scn_cct_reference
          ,scn_bro_code
          ,scn_major_subject_ind 
          ,scn_sco_code
          ,scn_status_date
          ,scn_brc_code
          ,scn_reusable_refno
          ,scn_ban_reference
          ,scn_par_refno
          ,scn_pro_refno
          ,scn_aun_code
          ,scn_tcy_refno
          ,scn_ipp_refno
          ,scn_app_refno
          ,scn_peg_code
          ,scn_las_lea_start_date
          ,scn_srq_no
          ,scn_cos_code
          ,scn_adr_refno
          ,scn_target_date
          ,scn_actual_date
          ,scn_comments
          ,scn_las_lea_pro_refno
          ,scn_las_start_date
          )
          VALUES
          (l_scn_reference_seq
          ,l_cct_reference_seq
          ,r1.lcct_scn_bro_code
          ,'Y'
          ,r1.lcct_sco_code
          ,r1.lcct_status_date
          ,r1.lcct_scn_brc_code
          ,l_reusable_refno
          ,NVL(r1.lcct_ban_alt_ref,NULL)
          ,l_sub_par_refno
          ,l_sub_pro_refno
          ,l_sub_aun_code
          ,l_sub_tcy_refno
          ,l_sub_ipp_refno
          ,l_sub_app_refno
          ,l_sub_peg_code
          ,r1.lcct_sub_sec_ref
          ,l_sub_srq_no
          ,l_sub_cos_code
          ,l_sub_adr_refno
          ,r1.lcct_scn_target_date
          ,NULL
          ,r1.lcct_comments 
          ,l_las_pro_refno
          ,r1.lcct_sub_sec_date
          );
          UPDATE dl_hcs_contacts
          SET lscn_reference = l_scn_reference_seq
          WHERE rowid = r1.rec_rowid;  
          --
          -- Update the DL table with the scn_reference needed for the DELETE 
          --
        END IF;     
        --
        -- keep a count of the rows processed and commit after every 5000
        --
        i := i + 1; 
        IF MOD(i,5000) = 0 
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
    fsc_utils.proc_end;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_create;
  --
  -- ************************************************************************************
  --
  PROCEDURE dataload_validate
    (p_batch_id          IN VARCHAR2
    ,p_date              IN DATE
    )
  AS
  CURSOR c1 
  IS
    SELECT rowid rec_rowid
    ,      lcct_dlb_batch_id
    ,      lcct_dl_seqno
    ,      lcct_dl_load_status
    ,      lcct_obj_legacy_ref
    ,      lcct_secondary_ref
    ,      lcct_secondary_date
    ,      lcct_legacy_type
    ,      lcct_received_date
    ,      lcct_ccm_code
    ,      lcct_cny_code
    ,      lcct_sub_legacy_ref
    ,      lcct_sub_sec_ref
    ,      lcct_sub_sec_date
    ,      lcct_subject_type
    ,      lcct_scn_bro_code
    ,      lcct_scn_brc_code
    ,      lcct_scn_target_date
    ,      lcct_duration
    ,      lcct_aun_code_responsible
    ,      lcct_sco_code
    ,      lcct_status_date
    ,      lcct_created_by
    ,      lcct_created_date
    ,      lcct_comments
    ,      lcct_usr_jro_code
    ,      lcct_usr_username
    ,      lcct_correspond_reference
    ,      lcct_answered_date
    ,      lcct_ano_code
    ,      lcct_outcome_comments
    ,      lcct_ban_alt_ref
    FROM   dl_hcs_contacts
    WHERE  lcct_dlb_batch_id   = p_batch_id
    AND    lcct_dl_load_status in ('L','F','O');
  CURSOR get_par_refno
    (cp_par_per_alt_ref VARCHAR2) 
  IS
    SELECT par_refno
    FROM   parties
    WHERE  par_per_alt_ref = cp_par_per_alt_ref;
  CURSOR get_pro_refno
    (cp_pro_propref VARCHAR2) 
  IS
    SELECT pro_refno
    FROM   properties
    WHERE  pro_propref = cp_pro_propref;
  CURSOR get_aun_code
    (cp_aun_code VARCHAR2) 
  IS
    SELECT aun_code
    FROM   admin_units
    WHERE  aun_code = cp_aun_code;
  CURSOR get_tcy_refno
    (cp_tcy_refno VARCHAR2) 
  IS
    SELECT tcy_refno
    FROM   tenancies
    WHERE  tcy_refno = cp_tcy_refno;
  CURSOR get_ipp_refno
    (cp_ipp_shortname VARCHAR2
    ,cp_ipp_ipt_code  VARCHAR2
    )   
  IS
    SELECT ipp_refno
    FROM   interested_parties
    WHERE  ipp_shortname = cp_ipp_shortname
    AND    ipp_ipt_code  = cp_ipp_ipt_code;
  CURSOR get_app_refno
    (cp_app_legacy_ref VARCHAR2) 
  IS
    SELECT app_refno
    FROM   applications
    WHERE  app_legacy_ref = cp_app_legacy_ref;
  CURSOR get_peg_code
    (cp_peg_code VARCHAR2) 
  IS
    SELECT peg_code
    FROM   people_groups
    WHERE  peg_code =  cp_peg_code;
  CURSOR get_srq_no
    (cp_srq_legacy_refno VARCHAR2) 
  IS
    SELECT srq_no
    FROM   service_requests
    WHERE  srq_legacy_refno = cp_srq_legacy_refno;
  CURSOR get_cos_code
    (cp_cos_code VARCHAR2) 
  IS
    SELECT cos_code
    FROM   contractor_sites
    WHERE  cos_code = cp_cos_code;
  CURSOR chk_las_exists
    (cp_las_pro_propref    VARCHAR2
    ,cp_las_lea_start_date DATE
    ,cp_las_start_date     DATE
    ) 
  IS
    SELECT 'X'
    FROM   lease_assignments a
    ,      properties        b
    WHERE  b.pro_propref        = cp_las_pro_propref
    AND    a.las_lea_pro_refno  = b.pro_refno
    AND    a.las_lea_start_date = cp_las_lea_start_date
    AND    a.las_start_date     = cp_las_start_date;
  CURSOR chk_bro_code
    (cp_bro_code VARCHAR2) 
  IS
    SELECT 'X'
    FROM   business_reasons
    WHERE  bro_code = cp_bro_code;
--
-- chk_bro_dets (AJ 14-MAR-2019)
-- Amended check including bro_brc_code not required
--
  CURSOR chk_bro_dets 
    (cp_bro_code VARCHAR2)
  IS 
    SELECT bro_address_required_ind
    ,      bro_addr_to_jrole_req_ind
    ,      bro_contact_required_ind
    ,      bro_addr_to_user_name_req_ind
    ,      bro_addr_to_user_name_req_ind
    FROM   business_reasons 
    WHERE  bro_current_ind = 'Y'
    AND    bro_code = cp_bro_code;
  CURSOR chk_bro_cls_code
    (cp_bro_cls_code VARCHAR2) 
  IS
    SELECT 'X'
    FROM   business_reason_classes
    WHERE  brc_code = cp_bro_cls_code; 
  CURSOR chk_ban_exists
    (cp_ban_reference NUMBER) 
  IS
    SELECT 'X'
    FROM business_actions
    WHERE ban_reference = cp_ban_reference;
  CURSOR chk_sco_code
    (cp_sco_code VARCHAR2) 
  IS
    SELECT 'X'
    FROM   status_codes
    WHERE  sco_code = cp_sco_code;
  CURSOR get_tcy_refno2
    (cp_tcy_alt_ref VARCHAR2) 
  IS
    SELECT tcy_refno
    FROM   tenancies
    WHERE  tcy_alt_ref = cp_tcy_alt_ref;
  CURSOR chk_contype
    (cp_frv_code VARCHAR2) 
  IS
    SELECT 'X'
    FROM   first_ref_values
    WHERE  frv_code = cp_frv_code
    AND    frv_frd_domain = 'CONTTYPE'
    AND    frv_current_ind = 'Y';
  CURSOR chk_contact_type
    (cp_cny_code VARCHAR2) 
  IS
    SELECT 'X' 
    FROM   contact_source_types
    WHERE  cny_code = cp_cny_code
    AND    cny_current_ind = 'Y';
  CURSOR chk_cnansout
    (cp_frv_code VARCHAR2) 
  IS
    SELECT 'X'
    FROM   first_ref_values
    WHERE  frv_code = cp_frv_code
    AND    frv_frd_domain = 'CNANSOUT'
    AND    frv_current_ind = 'Y';
  CURSOR chk_user 
    (cp_cct_user VARCHAR2)
  IS
    SELECT 'x'
    FROM   users usr
    WHERE  usr.usr_current_ind = 'Y'
    AND    usr.usr_username = UPPER(cp_cct_user);
  CURSOR chk_usr_jobrole 
    (cp_cct_user VARCHAR2
    ,cp_job_role VARCHAR2
    )
  IS                       
    SELECT 'X'
    FROM   job_role_users jru, users usr
    WHERE  jru.jru_current_ind = 'Y'
    AND    usr.usr_current_ind = 'Y'
    AND    usr.usr_username = UPPER(cp_cct_user)
    AND    jru.jru_usr_username = usr.usr_username
    AND    jru.jru_jro_code = UPPER(cp_job_role)
    AND    jru.jru_end_date IS NULL;
  CURSOR chk_bra_job 
    (cp_bro_code VARCHAR2 
    ,cp_jro_code VARCHAR2
    )
  IS                   
    SELECT 'X'
    FROM   business_rsn_addr_to_job_roles
    WHERE  brat_bro_bro_code = cp_bro_code
    AND    brat_jro_jro_code = cp_jro_code;
  cb                          VARCHAR2(30);
  cd                          DATE;
  cp                          VARCHAR2(30) := 'VALIDATE';
  ct                          VARCHAR2(30) := 'DL_HCS_CONTACTS';
  cs                          INTEGER;
  ce                          VARCHAR2(200);
  l_id                        ROWID;
  l_bro_exists                VARCHAR2(1);
  l_bro_cls_exists            VARCHAR2(1);
  l_resp_aun_code             VARCHAR2(20);
  l_sco_exists                VARCHAR2(1);
  l_par_refno                 NUMBER(8);
  l_pro_refno                 NUMBER(10);
  l_las_pro_refno             NUMBER(10);
  l_aun_code                  VARCHAR2(20);
  l_tcy_refno                 NUMBER(8);
  l_ipp_refno                 NUMBER(10);
  l_app_refno                 NUMBER(10);
  l_peg_code                  VARCHAR2(10);
  l_srq_no                    NUMBER(10);
  l_cos_code                  VARCHAR2(15);
  l_adr_refno                 NUMBER(10);
  l_sub_par_refno             NUMBER(8);
  l_sub_pro_refno             NUMBER(10);
  l_sub_las_pro_refno         NUMBER(10);
  l_sub_aun_code              VARCHAR2(20);
  l_sub_tcy_refno             NUMBER(8);
  l_sub_ipp_refno             NUMBER(10);
  l_sub_app_refno             NUMBER(10);
  l_sub_peg_code              VARCHAR2(10);
  l_sub_srq_no                NUMBER(10);
  l_sub_cos_code              VARCHAR2(15);
  l_las_fields_supplied       VARCHAR2(1);
  l_las_exists                VARCHAR2(1);
  l_sub_las_fields_supplied   VARCHAR2(1);
  l_sub_las_exists            VARCHAR2(1);
  l_ban_exists                VARCHAR2(1);
  l_ban_ban_exists            VARCHAR2(1);
  l_ccm_code_exists           VARCHAR2(1);
  l_ano_exists                VARCHAR2(1);
  l_cny_exists                VARCHAR2(1);
  l_bro_address_required_ind  VARCHAR2(1);
  l_bro_addr_to_jrole_req_ind VARCHAR2(1);
  l_bro_contact_required_ind  VARCHAR2(1);
  l_bro_addr_username_req     VARCHAR2(1);
  l_chk_user                  VARCHAR2(1);
  l_chk_usr_jobrole           VARCHAR2(1);
  l_chk_bra_job               VARCHAR2(1);
  l_errors                    VARCHAR2(10);
  l_error_ind                 VARCHAR2(10);
  i                           INTEGER :=0;
  l_bro_type_ind              VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_hcs_contacts.dataload_validate');
    fsc_utils.debug_message('s_dl_hcs_contacts.dataload_validate',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR r1 IN c1 
    LOOP
      BEGIN
        cs := r1.lcct_dl_seqno;
        l_id := r1.rec_rowid;
        l_errors    := 'V';
        l_error_ind := 'N';
        l_ban_exists       :=NULL;
        l_ban_ban_exists  :=NULL;        
        l_ccm_code_exists    :=NULL;    
        l_ano_exists     :=NULL;
        l_cny_exists       :=NULL;
        l_bro_address_required_ind  :=NULL;
        l_bro_addr_to_jrole_req_ind  :=NULL;
        l_bro_contact_required_ind  :=NULL;
        l_bro_addr_username_req  :=NULL;
        l_chk_user  := NULL;
        l_chk_usr_jobrole  :=NULL;
        l_chk_bra_job := NULL;
        l_bro_type_ind := NULL;
        --
        -- dates must be in the past.
        --
        IF r1.lcct_received_date > TRUNC(sysdate)
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',837); 
        END IF;      
        IF r1.lcct_scn_target_date > TRUNC(sysdate)
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',538);
        END IF;
        IF r1.lcct_status_date > TRUNC(sysdate)
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',841); 
        END IF; 
        IF r1.lcct_created_date > TRUNC(sysdate)
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',749);
        END IF;       
        l_par_refno     := NULL;
        l_pro_refno     := NULL;
        l_aun_code      := NULL;
        l_tcy_refno     := NULL;
        l_ipp_refno     := NULL;
        l_app_refno     := NULL;
        l_peg_code      := NULL;
        l_srq_no        := NULL;
        l_cos_code      := NULL;
        l_las_pro_refno := NULL;
        l_las_fields_supplied := 'Y';
        IF r1.lcct_legacy_type = 'PAR' 
        THEN
          OPEN get_par_refno(r1.lcct_obj_legacy_ref);
          FETCH get_par_refno INTO l_par_refno;
          CLOSE get_par_refno;
          IF l_par_refno IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',590);
          END IF;
        ELSIF r1.lcct_legacy_type = 'PRO' 
        THEN
          OPEN get_pro_refno(r1.lcct_obj_legacy_ref);
          FETCH get_pro_refno INTO l_pro_refno;
          CLOSE get_pro_refno;
          IF l_pro_refno IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',591);
          END IF;
        ELSIF r1.lcct_legacy_type = 'AUN' 
        THEN
          OPEN get_aun_code(r1.lcct_obj_legacy_ref);
          FETCH get_aun_code INTO l_aun_code;
          CLOSE get_aun_code;
          IF l_aun_code IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',592);
          END IF;
        ELSIF r1.lcct_legacy_type = 'TAR' 
        THEN
          OPEN get_tcy_refno2(r1.lcct_obj_legacy_ref);
          FETCH get_tcy_refno2 INTO l_tcy_refno;
          CLOSE get_tcy_refno2;
          IF l_tcy_refno IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',599); --593
          END IF;
        ELSIF r1.lcct_legacy_type = 'TCY' 
        THEN
          OPEN get_tcy_refno(r1.lcct_obj_legacy_ref);
          FETCH get_tcy_refno INTO l_tcy_refno;
          CLOSE get_tcy_refno;
          IF l_tcy_refno IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',593);
          END IF;
        ELSIF r1.lcct_legacy_type = 'IPP' 
        THEN
          OPEN get_ipp_refno(r1.lcct_obj_legacy_ref,r1.lcct_secondary_ref);
          FETCH get_ipp_refno INTO l_ipp_refno;
          CLOSE get_ipp_refno;
          IF l_ipp_refno IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',594);
          END IF;
        ELSIF r1.lcct_legacy_type = 'APP' 
        THEN
          OPEN get_app_refno(r1.lcct_obj_legacy_ref);
          FETCH get_app_refno INTO l_app_refno;
          CLOSE get_app_refno;
          IF l_app_refno IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',595);
          END IF;
        ELSIF r1.lcct_legacy_type = 'PEG' 
        THEN
          OPEN get_peg_code(r1.lcct_obj_legacy_ref);
          FETCH get_peg_code INTO l_peg_code;
          CLOSE get_peg_code;
          IF l_peg_code IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',596);
          END IF;
        ELSIF r1.lcct_legacy_type = 'SRQ' 
        THEN
          OPEN get_srq_no(r1.lcct_obj_legacy_ref);
          FETCH get_srq_no INTO l_srq_no;
          CLOSE get_srq_no;
          IF l_srq_no IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',597);
          END IF;
        ELSIF r1.lcct_legacy_type = 'COS' 
        THEN
          OPEN get_cos_code(r1.lcct_obj_legacy_ref);
          FETCH get_cos_code INTO l_cos_code;
          CLOSE get_cos_code;
          IF l_cos_code IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',598);
          END IF;
        ELSIF r1.lcct_legacy_type = 'LAS' 
        THEN
          OPEN get_pro_refno(r1.lcct_obj_legacy_ref);
          FETCH get_pro_refno INTO l_las_pro_refno;
          CLOSE get_pro_refno;
          IF l_las_pro_refno IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',599);
          END IF;
          IF r1.lcct_secondary_ref IS NULL
          OR r1.lcct_secondary_date IS NULL 
          THEN
            l_las_fields_supplied := 'N';
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',600);
          END IF;
          IF l_las_pro_refno IS NOT NULL
          AND l_las_fields_supplied = 'Y' 
          THEN
            OPEN chk_las_exists(r1.lcct_obj_legacy_ref, r1.lcct_secondary_ref, r1.  lcct_secondary_date);
            FETCH chk_las_exists INTO l_las_exists;
            CLOSE chk_las_exists;
            IF l_las_exists IS NULL 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',601);
            END IF;
          END IF;
        END IF;
        IF r1.lcct_legacy_type IS NOT NULL 
        THEN
          IF r1.lcct_legacy_type NOT IN ('PAR','PRO','AUN','SRQ','IPP','APP','PEG','COS','LAS','TCY','TAR') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',602); 
          END IF;
        END IF;
        l_bro_exists := NULL;
        IF r1.lcct_scn_bro_code IS NOT NULL 
        THEN
          OPEN chk_bro_code(r1.lcct_scn_bro_code);
          FETCH chk_bro_code INTO l_bro_exists;
          CLOSE chk_bro_code;
          IF l_bro_exists IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',823); 
          END IF;
        END IF;
        l_bro_cls_exists := NULL;
        IF r1.lcct_scn_brc_code IS NOT NULL 
        THEN
          OPEN chk_bro_cls_code(r1.lcct_scn_brc_code);
          FETCH chk_bro_cls_code INTO l_bro_cls_exists;
          CLOSE chk_bro_cls_code;
          IF l_bro_cls_exists IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',822); 
          END IF;
        END IF;
        l_resp_aun_code := NULL;
        IF r1.lcct_aun_code_responsible IS NOT NULL 
        THEN
          OPEN get_aun_code(r1.lcct_aun_code_responsible);
          FETCH get_aun_code INTO l_resp_aun_code;
          CLOSE get_aun_code;
          IF l_resp_aun_code IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',605);  
          END IF;
        END IF;
        l_sco_exists := NULL;
        IF r1.lcct_sco_code IS NOT NULL 
        THEN
          OPEN chk_sco_code(r1.lcct_sco_code);
          FETCH chk_sco_code INTO l_sco_exists;
          CLOSE chk_sco_code;
          IF l_sco_exists IS NULL
          OR r1.lcct_sco_code NOT IN ('LOG','CAN','COM') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',900); -- okay 
          END IF;
        END IF;
        IF r1.lcct_ban_alt_ref IS NOT NULL 
        THEN
          l_ban_ban_exists := NULL;
          OPEN chk_ban_exists(r1.lcct_ban_alt_ref);
          FETCH chk_ban_exists INTO l_ban_ban_exists;
          CLOSE chk_ban_exists;
          IF l_ban_ban_exists IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',627);  
          END IF;
        END IF;
        IF r1.lcct_ccm_code IS NOT NULL 
        THEN
          l_ccm_code_exists := NULL;
          OPEN chk_contype(r1.lcct_ccm_code);
          FETCH chk_contype INTO l_ccm_code_exists;
          CLOSE chk_contype;           
          IF l_ccm_code_exists IS NULL
          OR r1.lcct_ccm_code IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',838);  
          END IF;
        END IF;
        IF r1.lcct_cny_code IS NOT NULL 
        THEN
          l_cny_exists := NULL;
          OPEN chk_contact_type(r1.lcct_cny_code);
          FETCH chk_contact_type INTO l_cny_exists;
          CLOSE chk_contact_type;
          IF l_cny_exists IS NULL
          OR r1.lcct_cny_code IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',839);  
          END IF;
        END IF;
        IF r1.lcct_ano_code IS NOT NULL 
        THEN
          l_ano_exists := NULL;
          OPEN chk_cnansout(r1.lcct_ano_code);
          FETCH chk_cnansout INTO l_ano_exists;
          CLOSE chk_cnansout;
          IF l_ano_exists IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',851);  --needs new 
          END IF;
        END IF;
        l_sub_par_refno     := NULL;
        l_sub_pro_refno     := NULL;
        l_sub_aun_code      := NULL;
        l_sub_tcy_refno     := NULL;
        l_sub_ipp_refno     := NULL;
        l_sub_app_refno     := NULL;
        l_sub_peg_code      := NULL;
        l_sub_srq_no        := NULL;
        l_sub_cos_code      := NULL;
        l_sub_las_pro_refno := NULL;
        l_sub_las_fields_supplied := 'Y';
        IF (r1.lcct_sub_legacy_ref IS NOT NULL AND r1.lcct_subject_type IS NULL)
        THEN
        --'The Subject Type must be supplied if the Subject Legacy Ref is supplied'
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',689); 
        END IF;
        IF (r1.lcct_sub_legacy_ref IS NULL AND r1.lcct_subject_type IS NOT NULL)
        THEN
        --'The Subject Legacy Ref must be supplied if the Subject Type is supplied'
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',690); 
        END IF;
        IF r1.lcct_subject_type = 'PAR' 
        THEN
          OPEN get_par_refno(r1.lcct_sub_legacy_ref);
          FETCH get_par_refno INTO l_sub_par_refno;
          CLOSE get_par_refno;
          IF l_sub_par_refno IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',590); 
          END IF;
        ELSIF r1.lcct_subject_type = 'PRO' 
        THEN
          OPEN get_pro_refno(r1.lcct_sub_legacy_ref);
          FETCH get_pro_refno INTO l_sub_pro_refno;
          CLOSE get_pro_refno;
          IF l_sub_pro_refno IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',591); 
          END IF;
        ELSIF r1.lcct_subject_type = 'AUN' 
        THEN
          OPEN get_aun_code(r1.lcct_sub_legacy_ref);
          FETCH get_aun_code INTO l_sub_aun_code;
          CLOSE get_aun_code;
          IF l_sub_aun_code IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',592);  
          END IF;
        ELSIF r1.lcct_subject_type = 'TAR' 
        THEN
          OPEN get_tcy_refno2(r1.lcct_sub_legacy_ref);
          FETCH get_tcy_refno2 INTO l_sub_tcy_refno;
          CLOSE get_tcy_refno2;
          IF l_sub_tcy_refno IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',599);  --593
          END IF;
        ELSIF r1.lcct_subject_type = 'TCY' 
        THEN
          OPEN get_tcy_refno(r1.lcct_sub_legacy_ref);
          FETCH get_tcy_refno INTO l_sub_tcy_refno;
          CLOSE get_tcy_refno;
          IF l_sub_tcy_refno IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',593);  
          END IF;
        ELSIF r1.lcct_subject_type = 'IPP' 
        THEN
          IF r1.lcct_sub_sec_ref IS NULL
          THEN
            --'If the Subject Type is IPP then the Subject Secondary Reference must be supplied'
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',691); 
          END IF;
          IF r1.lcct_sub_sec_ref IS NOT NULL
          THEN
            OPEN get_ipp_refno(r1.lcct_sub_legacy_ref,r1.lcct_sub_sec_ref);
            FETCH get_ipp_refno INTO l_sub_ipp_refno;
            CLOSE get_ipp_refno;
            IF l_sub_ipp_refno IS NULL 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',594);  
            END IF;
          END IF;
        ELSIF r1.lcct_subject_type = 'APP' 
        THEN
          OPEN get_app_refno(r1.lcct_sub_legacy_ref);
          FETCH get_app_refno INTO l_sub_app_refno;
          CLOSE get_app_refno;
          IF l_sub_app_refno IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',595);  
          END IF;
        ELSIF r1.lcct_subject_type = 'PEG' 
        THEN
          OPEN get_peg_code(r1.lcct_sub_legacy_ref);
          FETCH get_peg_code INTO l_sub_peg_code;
          CLOSE get_peg_code;
          IF l_sub_peg_code IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',596);
          END IF;
        ELSIF r1.lcct_subject_type = 'SRQ' 
        THEN
          OPEN get_srq_no(r1.lcct_sub_legacy_ref);
          FETCH get_srq_no INTO l_sub_srq_no;
          CLOSE get_srq_no;
          IF l_sub_srq_no IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',597);
          END IF;
        ELSIF r1.lcct_subject_type = 'COS' 
        THEN
          OPEN get_cos_code(r1.lcct_sub_legacy_ref);
          FETCH get_cos_code INTO l_sub_cos_code;
          CLOSE get_cos_code;
          IF l_sub_cos_code IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',598);  
          END IF;
        ELSIF r1.lcct_subject_type = 'LAS' 
        THEN
          OPEN get_pro_refno(r1.lcct_sub_legacy_ref);
          FETCH get_pro_refno INTO l_sub_las_pro_refno;
          CLOSE get_pro_refno;
          IF l_sub_las_pro_refno IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',599);  
          END IF;
          IF r1.lcct_sub_sec_ref IS NULL
          OR r1.lcct_sub_sec_date IS NULL 
          THEN
            l_sub_las_fields_supplied := 'N';
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',600);  
          END IF;
          IF l_sub_las_pro_refno IS NOT NULL
          AND l_sub_las_fields_supplied = 'Y' 
          THEN
            OPEN chk_las_exists(r1.lcct_sub_legacy_ref, r1.lcct_sub_sec_ref, r1.lcct_sub_sec_date);
            FETCH chk_las_exists INTO l_sub_las_exists;
            CLOSE chk_las_exists;
            IF l_sub_las_exists IS NULL 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',601); 
            END IF;
          END IF;
        END IF;
        IF r1.lcct_subject_type IS NOT NULL 
        THEN
          IF r1.lcct_subject_type NOT IN ('PAR','PRO','AUN','SRQ','IPP','APP','PEG','COS','LAS','TCY','TAR') 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',602); 
          END IF;
        END IF;
        --
        -- chk_bro_dets (AJ 14-MAR-2019)
        -- Amended check including bro_brc_code not required
        --
        OPEN chk_bro_dets(r1.lcct_scn_bro_code);
        FETCH chk_bro_dets INTO l_bro_address_required_ind
                               ,l_bro_addr_to_jrole_req_ind
                               ,l_bro_contact_required_ind
                               ,l_bro_addr_username_req
                               ,l_bro_type_ind;
        IF chk_bro_dets%NOTFOUND 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',604);
        END IF;
        CLOSE  chk_bro_dets;
        IF l_bro_addr_username_req = 'Y'
        THEN
          OPEN chk_user(r1.lcct_usr_username);
          FETCH chk_user INTO l_chk_user;
          IF chk_user%NOTFOUND 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',432); 
          END IF;
          CLOSE chk_user;
        END IF;
        IF l_bro_addr_to_jrole_req_ind IN ('Y','D')
        THEN
          OPEN chk_usr_jobrole(r1.lcct_usr_username,r1.lcct_created_date);
          FETCH chk_usr_jobrole INTO l_chk_usr_jobrole;
          IF chk_usr_jobrole%NOTFOUND 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',849);  
          END IF;
          CLOSE chk_usr_jobrole;
          OPEN chk_bra_job(r1.lcct_scn_bro_code,r1.lcct_usr_jro_code);
          FETCH chk_bra_job INTO l_chk_bra_job;
          IF chk_bra_job%NOTFOUND 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',932);  
          END IF;
          CLOSE chk_bra_job;            
        END IF;
        IF l_bro_address_required_ind = 'Y'
        THEN
          IF r1.lcct_usr_username IS NULL 
          AND r1.lcct_created_date IS NULL
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',860);  
          END IF;
        END IF;
        IF (    r1.lcct_answered_date IS NULL 
            AND r1.lcct_ano_code IS NULL 
            AND r1.lcct_outcome_comments IS NULL
           ) 
        OR (    r1.lcct_answered_date IS NOT NULL 
            AND r1.lcct_ano_code IS NOT NULL 
            AND r1.lcct_outcome_comments is not NULL
           ) 
        THEN
          NULL;
        ELSE 
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',701);  
        END IF;
        IF l_bro_type_ind = 'S'
        THEN
          IF r1.lcct_sub_legacy_ref IS NULL
          THEN
          --'This is a Subject Business Reason so the Subject Legacy Ref must be supplied'
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',692); 
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
    COMMIT;
    fsc_utils.proc_end;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
  END dataload_validate;
  --
  -- ************************************************************************************
  --
  --
  PROCEDURE dataload_delete 
    (p_batch_id       IN VARCHAR2
    ,p_date           IN DATE
    ) 
  IS
  CURSOR c1 
  IS
    SELECT ROWID rec_rowid
    ,      lcct_dlb_batch_id
    ,      lcct_dl_seqno
    ,      lcct_dl_load_status
    ,      lcct_reference
    ,      lscn_reference
    FROM   dl_hcs_contacts
    WHERE  lcct_dlb_batch_id   = p_batch_id
    AND    lcct_dl_load_status = 'C';
  cb              VARCHAR2(30);
  cd              DATE;
  cp              VARCHAR2(30) := 'DELETE';
  ct              VARCHAR2(30) := 'DL_HCS_CONTACTS';
  cs              INTEGER;
  ce              VARCHAR2(200);
  l_id            ROWID;
  l_an_tab        VARCHAR2(1);
  i               INTEGER := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_hcs_contacts.dataload_delete');
    fsc_utils.debug_message( 's_dl_hcs_contacts.dataload_delete',3 );
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR r1 IN c1 
    LOOP
      BEGIN
        cs := r1.lcct_dl_seqno;
        l_id := r1.rec_rowid;
        SAVEPOINT SP1;
        DELETE 
        FROM contacts
        WHERE cct_reference = r1.lcct_reference;
        DELETE 
        FROM   subj_cont_bus_reasons
        WHERE  scn_cct_reference = r1.lcct_reference;
        IF r1.lscn_reference IS NOT NULL
        THEN
          DELETE 
          FROM   non_subj_cont_bus_reasons
          WHERE  nsc_cct_reference = r1.lcct_reference
          AND    nsc_cct_reference = r1.lscn_reference;
        ELSE
          DELETE 
          FROM   non_subj_cont_bus_reasons
          WHERE  nsc_cct_reference = r1.lcct_reference;
        END IF;
        --
        -- keep a count of the rows processed and commit after every 1000
        --
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        set_record_status_flag(l_id,'V');
        i := i + 1; 
        IF MOD(i,1000) = 0 
        THEN 
          COMMIT; 
        END IF;
      EXCEPTION
      WHEN OTHERS 
      THEN
        ROLLBACK TO SP1;
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
        set_record_status_flag(l_id,'C');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
    END LOOP;
    COMMIT;
    fsc_utils.proc_end;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
--
END s_dl_hcs_contacts;
/

show errors
--commit;

