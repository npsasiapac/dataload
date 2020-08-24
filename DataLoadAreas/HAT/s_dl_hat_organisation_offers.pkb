CREATE OR REPLACE PACKAGE BODY s_dl_hat_organisation_offers
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO    WHEN         WHY
--  1.0     6.16      MJK    26-MAR-2018  Initial Creation.
--  1.1     6.16      MJK    08-MAY-2018  Incorporated fixes from D Bessell
--  1.2     6.16      AJ     24-MAY-2018  Further fixes from issues raised by D Bessell
--  1.3     6.16      AJ  11/17-JUN-2018  Major changes to Validation and delete added
--                                        fields for void instances and removed HPC_Code
--                                        TABLE UPDATES
--                                        1)LOOF_CASH_INCENTIVE CHANGED FROM VARCHAR(10) TO NUMBER(8,2)
--                                        2)LOOF_REFNO CHANGED FROM NUMBER(8) TO (10)
--                                        PACKAGE UPDATED -
--                                        1)Added separators before doing any changes so can check more easily any changes
--                                        2)r2 - r6 set to null added before each cursor in create
--                                        3)r7 variable sync text amended where setting to null in validate
--                                        4)loof_accepted_date and loof_accepted_by amended in create as will not insert "default" if left blank
--                                        5)Added section to analyse tables after create
--                                        6)Bespoke Table create created and also added void instance fields and vin_refno field(AJ 11-JUN-2018)
--                                        7)CLT file updated with additional fields (AJ 11-JUN-2018)
--                                        8)Further updates to package made (AJ 11-JUN-2018)
--                                        9)Void Instance Fields added with validation (AJ 15/06/2018)
--                                       10)Further mandatory checks for tty, ttyp, accepted date
--  1.4     6.16      AJ     18-JUN-2018  1)Checked and Updated Create
--                                        2)Added further validation to check numbers of records in the batch do not exceed system maximums
--  1.5     6.16      AJ     03-JUL-2018  1)Further updates to validation so void instance check has Status Start DATE
--                                          and one other field mandatory to fin vin_refno
--  1.6     6.16      AJ     05-JUL-2018  Further changes during testing
--                                        1) Error numbers corrected and duplicates removed - ora-20002 found
--                                        2) off_refno amended to be based on off_nof_seqno not oof_refno_seq as originally used
--
-- ***********************************************************************
--
  PROCEDURE set_record_status_flag
    (p_rowid  IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_hat_organisation_offers
    SET    loof_dl_load_status = p_status
    WHERE  rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_hat_organisation_offers');
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
    SELECT ROWID                            rec_rowid
    ,      loof_dlb_batch_id            
    ,      loof_dl_seqno
    ,      loof_dl_load_status
    ,      loof_offer_date
    ,      loof_respond_by_date
    ,      loof_pro_propref
    ,      loof_hps_start_date
    ,      loof_vin_dec_allowance
    ,      loof_vin_man_created
    ,      loof_vin_vst_code
    ,      loof_vin_tgt_date
    ,      loof_vin_apt_code
    ,      loof_vin_vgr_code
    ,      loof_vin_vpa_curr_code
    ,      loof_vin_sco_code
    ,      loof_vin_hrv_rfv_code
    ,      loof_vin_hrv_vcl_code
    ,      loof_vin_effective_date
    ,      loof_vin_text
    ,      loof_vin_created_date
    ,      loof_ale_app_legacy_ref
    ,      loof_ale_rli_code
    ,      loof_ttyp_hrv_code
    ,      loof_tty_code
    ,      loof_osg_ost_code
    ,      loof_expected_tcy_start_date
    ,      loof_cash_incentive
    ,      loof_comments
    ,      loof_type
    ,      NVL(loof_sco_code,'CUR')         loof_sco_code
    ,      NVL(loof_created_by,'DATALOAD')  loof_created_by
    ,      NVL(loof_created_date,SYSDATE)   loof_created_date
    ,      NVL(loof_accepted_by,'DATALOAD') loof_accepted_by
    ,      loof_accepted_date
    ,      loof_refno
    ,      loof_vin_refno
    FROM   dl_hat_organisation_offers
    WHERE  loof_dlb_batch_id = p_batch_id
    AND    loof_dl_load_status = 'V';
  --*********
  CURSOR c2 
    (cp_pro_propref       dl_hat_organisation_offers.loof_pro_propref%TYPE)
  IS
    SELECT pro_refno
    FROM   properties
    WHERE  pro_propref = cp_pro_propref;
  r2 c2%ROWTYPE;
  --*********
  CURSOR c3
    (cp_app_legacy_ref    dl_hat_organisation_offers.loof_ale_app_legacy_ref%TYPE
    ,cp_ale_rli_code      dl_hat_organisation_offers.loof_ale_rli_code%TYPE
    )
  IS
    SELECT ale_app_refno
    ,      ale_lst_code
    ,      ale_registered_date
    ,      ale_rli_code
    ,      ale_defined_hty_code
    ,      ale_category_start_date
    ,      ale_ala_hrv_apc_code
    FROM   applic_list_entries
    WHERE  ale_app_refno = (SELECT app_refno FROM applications WHERE app_legacy_ref = cp_app_legacy_ref)
    AND    ale_rli_code = cp_ale_rli_code;
  r3  c3%ROWTYPE;
  --*********
  CURSOR c4
  IS
    SELECT osa_code
    FROM   offer_statuses 
    WHERE  osa_current_ind = 'Y'
    AND    osa_status_type = 'CURR';
  r4  c4%ROWTYPE;
  --*********
  CURSOR c5
    (cp_vin_refno       dl_hat_organisation_offers.loof_vin_refno%TYPE
    )
  IS
    SELECT vin_apt_code
    ,      vin_refno
    FROM   void_instances
    WHERE  vin_refno = cp_vin_refno;
  r5  c5%ROWTYPE;
  --*********
  cb  VARCHAR2(30);   
  cd  DATE;   
  cp  VARCHAR2(30) := 'CREATE';   
  ct  VARCHAR2(30) := 'DL_HAT_ORGANISATION_OFFERS';   
  cs  INTEGER;   
  ce  VARCHAR2(200);   
  i   INTEGER := 0;
  l_oof_refno NUMBER;
  l_id        ROWID;
  l_an_tab    VARCHAR2(1);
  --*********
  BEGIN
    fsc_utils.proc_start('s_dl_hat_organisation_offers.dataload_create');
    fsc_utils.debug_message('s_dl_hat_organisation_offers.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --*********
    FOR r1 in c1 
    LOOP
    --
      BEGIN  
        cs := r1.loof_dl_seqno;
        l_id := r1.rec_rowid; 		
        SAVEPOINT SP1;
		--
        -- get property reference
        --
        r2 := NULL;		
        OPEN c2(r1.loof_pro_propref);
        FETCH c2 INTO r2;
        CLOSE c2;
		--
        -- get application list entries details
        --
        r3 := NULL;			
        OPEN c3(r1.loof_ale_app_legacy_ref,r1.loof_ale_rli_code);
        FETCH c3 INTO r3;
        CLOSE c3;
		--
        -- get offer statuses code CHECK AJ 
        --
        r4 := NULL;			
        OPEN c4;
        FETCH c4 INTO r4;
        CLOSE c4;
		--
        -- get void instance references
        --
        r5 := NULL;			
        OPEN c5(r1.loof_vin_refno);
        FETCH c5 INTO r5;
        CLOSE c5;
		--
        -- get next organisation offers reference from sequence
        -- this should use oof_nof_seqno for organisation_offers (found in a_organisation_offers package)
		-- not the obvious off_refno_seq which is was originally used by Martin (AJ 05-JUL-2018)
		--
        l_oof_refno := NULL;					
        SELECT oof_nof_seqno.NEXTVAL
        INTO   l_oof_refno
        FROM   dual;
        --	
        INSERT INTO organisation_offers   
        (oof_refno
        ,oof_offer_date
        ,oof_status_effective_date
        ,oof_status_system_date
        ,oof_created_by
        ,oof_created_date
        ,oof_ale_registered_date
        ,oof_respond_by_date
        ,oof_pro_refno
        ,oof_type
        ,oof_osa_code
        ,oof_lst_code
        ,oof_ttyp_hrv_code
        ,oof_expected_tcy_start_date
        ,oof_comments
        ,oof_cash_incentive
        ,oof_vin_refno 
        ,oof_tty_code
        ,oof_ale_app_refno
        ,oof_ale_rli_code
        ,oof_apt_code
        ,oof_ala_rli_code
        ,oof_ala_hrv_apc_code
        ,oof_hty_code
        ,oof_osr_hrv_code
        ,oof_ofr_hrv_code
        ,oof_sco_code
        ,oof_reusable_refno
        ,oof_accepted_by
        ,oof_accepted_date
        ,oof_specified_applicant_ind
        ,oof_category_start_date
        )    
        VALUES     
        (l_oof_refno
        ,r1.loof_offer_date
        ,r1.loof_offer_date
        ,SYSDATE 
        ,r1.loof_created_by
        ,r1.loof_created_date
        ,r3.ale_registered_date
        ,r1.loof_respond_by_date
        ,r2.pro_refno
        ,r1.loof_type
        ,r4.osa_code
        ,r3.ale_lst_code
        ,r1.loof_ttyp_hrv_code
        ,r1.loof_expected_tcy_start_date
        ,r1.loof_comments
        ,r1.loof_cash_incentive
        ,r1.loof_vin_refno
        ,r1.loof_tty_code
        ,r3.ale_app_refno
        ,r1.loof_ale_rli_code
        ,r5.vin_apt_code
        ,r1.loof_ale_rli_code
        ,r3.ale_ala_hrv_apc_code
        ,r3.ale_defined_hty_code
        ,r3.ale_ala_hrv_apc_code
        ,r3.ale_defined_hty_code
        ,r1.loof_sco_code
        ,reusable_refno_seq.NEXTVAL
        ,r1.loof_accepted_by
        ,r1.loof_accepted_date
        ,'N'
        ,r3.ale_category_start_date
        );
        --	
        INSERT INTO offer_stages
        (osg_ost_code
        ,osg_oof_refno
        ,osg_created_by
        ,osg_created_date
        )
        VALUES
        (r1.loof_osg_ost_code
        ,l_oof_refno
        ,r1.loof_created_by
        ,r1.loof_created_date
        );
        --	
        UPDATE dl_hat_organisation_offers
        SET    loof_refno = l_oof_refno
        WHERE  ROWID = r1.rec_rowid;
        --  
        -- Now UPDATE the record status and process count  
        --
        i := i + 1;     
        IF MOD(i,50000) = 0     
         THEN     
          COMMIT;     
        END IF;
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
           --	
      END;
      --	
    END LOOP;
    --	
    COMMIT;
    --
    -- Section to analyse the table(s) populated by this data load
    --
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ORGANISATION_OFFERS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('OFFER_STAGES');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HAT_ORGANISATION_OFFERS');
    --
    fsc_utils.proc_end;
    COMMIT;
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
    SELECT ROWID                            rec_rowid
    ,      loof_dlb_batch_id            
    ,      loof_dl_seqno                
    ,      loof_dl_load_status          
    ,      loof_offer_date              
    ,      loof_respond_by_date         
    ,      loof_pro_propref               
    ,      loof_hps_start_date
    ,      loof_vin_dec_allowance 
    ,      loof_vin_man_created
    ,      loof_vin_vst_code
    ,      loof_vin_tgt_date
    ,      loof_vin_apt_code
    ,      loof_vin_vgr_code
    ,      loof_vin_vpa_curr_code
    ,      loof_vin_sco_code
    ,      loof_vin_hrv_rfv_code
    ,      loof_vin_hrv_vcl_code
    ,      loof_vin_effective_date
    ,      loof_vin_text
    ,      loof_vin_created_date	
    ,      loof_ale_app_legacy_ref           
    ,      loof_ale_rli_code            
    ,      loof_ttyp_hrv_code           
    ,      loof_tty_code                
    ,      loof_osg_ost_code            
    ,      loof_expected_tcy_start_date 
    ,      loof_cash_incentive
    ,      loof_comments
    ,      loof_type                    
    ,      NVL(loof_sco_code,'CUR')         loof_sco_code
    ,      NVL(loof_created_by,'DATALOAD')  loof_created_by
    ,      NVL(loof_created_date,SYSDATE)   loof_created_date
    ,      NVL(loof_accepted_by,'DATALOAD') loof_accepted_by
    ,      loof_accepted_date
    ,      loof_refno
    ,      loof_vin_refno
    FROM   dl_hat_organisation_offers
    WHERE  loof_dlb_batch_id = p_batch_id
    AND    loof_dl_load_status in ('L','F','O');
  --*********
  CURSOR c2
    (cp_pro_propref    dl_hat_organisation_offers.loof_pro_propref%TYPE)
  IS
    SELECT pro_refno
    FROM   properties
    WHERE  pro_propref = cp_pro_propref;
  r2  c2%ROWTYPE;
  --*********
  -- NO LONGER USED (AJ 17-JUN-2018)
  --
  CURSOR c3
    (cp_pro_propref       dl_hat_organisation_offers.loof_pro_propref%TYPE
    ,cp_hps_start_date    dl_hat_organisation_offers.loof_hps_start_date%TYPE
    )
  IS
    SELECT COUNT(*) + 1   no_of_offers
    FROM   organisation_offers
    WHERE  oof_vin_refno =
             (SELECT vin_refno
              FROM   void_instances
              WHERE  vin_pro_refno = (SELECT pro_refno FROM properties WHERE pro_propref = cp_pro_propref)
              AND    vin_status_start = cp_hps_start_date
             );
  r3  c3%ROWTYPE;
  --*********
  CURSOR c4
    (cp_app_legacy_ref    dl_hat_organisation_offers.loof_ale_app_legacy_ref%TYPE
    ,cp_ale_rli_code      dl_hat_organisation_offers.loof_ale_rli_code%TYPE
    )
  IS
    SELECT ale_rli_code
    ,      ale_current_offer_count
    ,      fro_maximum_offer
    FROM   applic_list_entries
    ,      rehousing_list_groups
    WHERE  ale_app_refno = (SELECT app_refno FROM applications WHERE app_legacy_ref = cp_app_legacy_ref)
    AND    ale_rli_code = cp_ale_rli_code
    AND    fro_code = 'NORMAL';
  r4  c4%ROWTYPE;
  --*********
  CURSOR c5
    (cp_ttyp_hrv_code     dl_hat_organisation_offers.loof_ttyp_hrv_code%TYPE)
  IS
    SELECT frv_code
    FROM   hrv_tenure_types 
    WHERE  frv_code = cp_ttyp_hrv_code;
  r5  c5%ROWTYPE;
  --*********
  CURSOR c6
    (cp_tty_code          dl_hat_organisation_offers.loof_tty_code%TYPE)
  IS
    SELECT tty_code
    FROM   tenancy_types 
    WHERE  tty_code = cp_tty_code;
  r6  c6%ROWTYPE;
  --*********
  CURSOR c7
    (cp_ost_code          dl_hat_organisation_offers.loof_osg_ost_code%TYPE)
  IS
    SELECT ost_code
    FROM   offer_stage_types 
    WHERE  ost_code = cp_ost_code;
  r7  c7%ROWTYPE;
  --*********
  CURSOR c8
    (cp_pro_refno     organisation_offers.oof_pro_refno%TYPE
    ,cp_vin_refno     organisation_offers.oof_vin_refno%TYPE
    )
  IS
    SELECT COUNT(*) + 1   no_of_offers
    FROM   organisation_offers
    WHERE  oof_vin_refno = cp_vin_refno
    AND    oof_pro_refno = cp_pro_refno;
  r8  c8%ROWTYPE;
  --*********
  -- VCL
  CURSOR c_vcl_code (p_frv VARCHAR2) IS
  SELECT frv_code
  FROM   first_ref_values
  WHERE  frv_frd_domain = 'VOID_CLASS'
  AND    frv_code = p_frv;
  --*********
  --VST
  CURSOR c_vst (p_vst VARCHAR2)IS
  SELECT vst_code
  FROM   void_statuses
  WHERE  vst_code = p_vst;
  --*********
  --APT
  CURSOR c_apt_code (p_apt VARCHAR2)IS
  SELECT 'X'
  FROM   alloc_prop_types
  WHERE  apt_code = p_apt
  AND apt_current = 'Y';
  --*********
  --VOID_GROUPS
  CURSOR c_vgr_code (p_vgr VARCHAR2)IS
  SELECT 'X' 
  FROM   void_groups
  WHERE  vgr_code = p_vgr 
  AND    vgr_current = 'Y';
  --*********
  --VPA
  CURSOR c_vpa_code (p_vpa VARCHAR2)IS
  SELECT 'X' 
  FROM   void_paths
  WHERE  vpa_code = p_vpa 
  AND    vpa_current = 'Y';
  --*********
  -- RFV
  CURSOR c_rfv_code (p_rfv VARCHAR2) IS
  SELECT 'X' 
  FROM   first_ref_values
  WHERE  frv_frd_domain = 'VREASON'
  AND    frv_current_ind = 'Y'
  AND    frv_code = p_rfv;
  --*********
  -- GET VIN_REFNO
  CURSOR c_get_vin_refno (p_pro_refno     NUMBER
                     ,p_start_date    DATE
                     ,p_dec_allowance NUMBER
                     ,p_man_created   VARCHAR
                     ,p_vst_code      VARCHAR
                     ,p_tgt_date      DATE
                     ,p_apt_code      VARCHAR
                     ,p_vgr_code      VARCHAR
                     ,p_vpa_curr_code VARCHAR
                     ,p_sco_code      VARCHAR
                     ,p_rfv_code      VARCHAR
                     ,p_vcl_code      VARCHAR
                     ,p_eff_date      DATE
                     ,p_vin_text      VARCHAR
                     ,p_created_date  DATE    ) IS
  SELECT vin_refno
  FROM   void_instances
  WHERE  vin_pro_refno = p_pro_refno           
    AND  trunc(vin_status_start) = p_start_date
    AND  nvl(vin_dec_allowance,1) = nvl(p_dec_allowance,nvl(vin_dec_allowance,1))
    AND  nvl(vin_man_created,'N') = nvl(p_man_created,nvl(vin_man_created,'N')) 
    AND  vin_vst_code = nvl(p_vst_code,vin_vst_code) 
    AND  nvl(trunc(vin_tgt_date),trunc(sysdate)) = nvl(p_tgt_date,nvl(trunc(vin_tgt_date),trunc(sysdate)))
    AND  vin_apt_code = nvl(p_apt_code,vin_apt_code)
    AND  vin_vgr_code = nvl(p_vgr_code,vin_vgr_code)
    AND  nvl(vin_vpa_curr_code,'XXXX') = nvl(p_vpa_curr_code,nvl(vin_vpa_curr_code,'XXXX'))
    AND  vin_sco_code = nvl(p_sco_code,vin_sco_code)
    AND  vin_hrv_rfv_code = nvl(p_rfv_code,vin_hrv_rfv_code)
    AND  vin_hrv_vcl_code = nvl(p_vcl_code,vin_hrv_vcl_code)
    AND  trunc(vin_effective_date) = nvl(p_eff_date,trunc(vin_effective_date))
    AND  nvl(vin_text,'XXX') = nvl(p_vin_text,nvl(vin_text,'XXX'))
    AND  trunc(vin_created_date) = nvl(p_created_date,trunc(vin_created_date));
  --*********	
  -- COUNT VIN_REFNO 
  CURSOR c_count_vin_refno (p_pro_refno     NUMBER
                     ,p_start_date    DATE
                     ,p_dec_allowance NUMBER
                     ,p_man_created   VARCHAR
                     ,p_vst_code      VARCHAR
                     ,p_tgt_date      DATE
                     ,p_apt_code      VARCHAR
                     ,p_vgr_code      VARCHAR
                     ,p_vpa_curr_code VARCHAR
                     ,p_sco_code      VARCHAR
                     ,p_rfv_code      VARCHAR
                     ,p_vcl_code      VARCHAR
                     ,p_eff_date      DATE
                     ,p_vin_text      VARCHAR
                     ,p_created_date  DATE    ) IS
  SELECT count(*)
  FROM   void_instances
  WHERE  vin_pro_refno = p_pro_refno           
    AND  trunc(vin_status_start) = p_start_date
    AND  nvl(vin_dec_allowance,1) = nvl(p_dec_allowance,nvl(vin_dec_allowance,1))
    AND  nvl(vin_man_created,'N') = nvl(p_man_created,nvl(vin_man_created,'N')) 
    AND  vin_vst_code = nvl(p_vst_code,vin_vst_code) 
    AND  nvl(trunc(vin_tgt_date),trunc(sysdate)) = nvl(p_tgt_date,nvl(trunc(vin_tgt_date),trunc(sysdate)))
    AND  vin_apt_code = nvl(p_apt_code,vin_apt_code)
    AND  vin_vgr_code = nvl(p_vgr_code,vin_vgr_code)
    AND  nvl(vin_vpa_curr_code,'XXXX') = nvl(p_vpa_curr_code,nvl(vin_vpa_curr_code,'XXXX'))
    AND  vin_sco_code = nvl(p_sco_code,vin_sco_code)
    AND  vin_hrv_rfv_code = nvl(p_rfv_code,vin_hrv_rfv_code)
    AND  vin_hrv_vcl_code = nvl(p_vcl_code,vin_hrv_vcl_code)
    AND  trunc(vin_effective_date) = nvl(p_eff_date,trunc(vin_effective_date))
    AND  nvl(vin_text,'XXX') = nvl(p_vin_text,nvl(vin_text,'XXX'))
    AND  trunc(vin_created_date) = nvl(p_created_date,trunc(vin_created_date));
  --*********
  CURSOR c9
    (cp_app_legacy_ref    dl_hat_organisation_offers.loof_ale_app_legacy_ref%TYPE
    ,cp_batch_id          dl_hat_organisation_offers.loof_dlb_batch_id%TYPE
    )
  IS
    SELECT count(*)
    FROM   dl_hat_organisation_offers
    WHERE  loof_ale_app_legacy_ref = cp_app_legacy_ref
    AND    loof_dlb_batch_id = cp_batch_id;
  r9  c9%ROWTYPE;
  --*********
  CURSOR c10
    (cp_pro_propref    dl_hat_organisation_offers.loof_pro_propref%TYPE
    ,cp_hps_start_date dl_hat_organisation_offers.loof_hps_start_date%TYPE
    ,cp_batch_id       dl_hat_organisation_offers.loof_dlb_batch_id%TYPE
    )
  IS
    SELECT count(*)
    FROM   dl_hat_organisation_offers
    WHERE  loof_pro_propref = cp_pro_propref
    AND    loof_hps_start_date = cp_hps_start_date
    AND    loof_dlb_batch_id = cp_batch_id;
  r10  c10%ROWTYPE;
  --*********
  --
  -- Constants for process_summary
  --
  cb           VARCHAR2(30);  
  cd           DATE;  
  cp           VARCHAR2(30) := 'VALIDATE';  
  ct           VARCHAR2(30) := 'DL_HAT_ORGANISATION_OFFERS';  
  cs           INTEGER;  
  ce           VARCHAR2(200);  
  l_errors     VARCHAR2(10);
  l_error_ind  VARCHAR2(10);
  i            INTEGER :=0;
  l_id         ROWID;
  l_an_tab     VARCHAR2(1);
  --
  -- Other variables
  --
  l_vcl_code         VARCHAR2(10);
  l_vst              VARCHAR2(4);
  l_apt_code         VARCHAR2(1);
  l_vgr_code         VARCHAR2(1);
  l_vpa_code         VARCHAR2(1);
  l_frv_code         VARCHAR2(1);
  l_loof_vin_refno   NUMBER(8);
  l_count_vin_refno  INTEGER;
  l_count_app_legacy_ref  INTEGER;
  l_count_prop_offers     INTEGER;
  l_vin_check        VARCHAR2(1);
  --*********
  BEGIN
  --
    fsc_utils.proc_start('s_dl_hat_organisation_offers.dataload_validate');  
    fsc_utils.debug_message('s_dl_hat_organisation_offers.dataload_validate',3);  
    cb := p_batch_id;  
    cd := p_DATE;  
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    --	
    FOR r1 IN c1   
    LOOP
    --*********
      BEGIN
        --
        cs := r1.loof_dl_seqno;
        l_id := r1.rec_rowid; 				
        r1.rec_rowid := r1.rec_rowid;    
        l_errors := 'V';    
        l_error_ind := 'N';
        r2 := NULL;
        r3 := NULL;
        r4 := NULL;
        r5 := NULL;
        r6 := NULL;
        r7 := NULL;
        r8 := NULL;
        r9 := NULL;
        r10 := NULL;
        l_vcl_code  := NULL;
        l_vst       := NULL;
        l_apt_code  := NULL;
        l_vgr_code  := NULL;
        l_vpa_code  := NULL;
        l_frv_code  := NULL;
        l_loof_vin_refno  := NULL;
        l_count_vin_refno  := 0;
        l_count_app_legacy_ref  := 0;
        l_count_prop_offers   := 0;
        l_vin_check  := NULL;
        --
        -- Mandatory field checks added (AJ)
        --
        -- 'The Offer Date must be supplied'
        IF (r1.loof_offer_date IS NULL)
         THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',597);
        END IF;
        --
        -- 'The Respond by Date must be supplied'
        IF (r1.loof_respond_by_date IS NULL)
         THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',598);
        END IF;
        --
        -- 'The Property reference must be supplied'
        IF (r1.loof_pro_propref IS NULL)
         THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',613);
        END IF;
        --
        -- 'The Offer Status Code must be supplied'
        IF (r1.loof_sco_code IS NULL)
         THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',607);
        END IF;
        --
        -- 'The void instance status start date must be supplied'
        IF (r1.loof_hps_start_date IS NULL)
         THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',614);
        END IF;
        --
        -- 'The Application Legacy Reference must be supplied'
        IF (r1.loof_ale_app_legacy_ref IS NULL)
         THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',451);
        END IF;
        --
        -- 'The Rehousing List Code must be supplied'
        IF (r1.loof_ale_rli_code IS NULL)
         THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',320);
        END IF;
        --
        -- 'The Tenure Type Code must be supplied'
        IF (r1.loof_ttyp_hrv_code IS NULL)
         THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',603);
        END IF;
        --
        -- 'The Tenancy Type Code must be supplied'
        IF (r1.loof_tty_code IS NULL)
         THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',604);
        END IF;
        --
        -- 'The Offer Stage Types Code must be supplied'
        IF (r1.loof_osg_ost_code IS NULL)
         THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',605);
        END IF;
        --
        -- 'The Expected Tenancy Start Date must be supplied'
        IF (r1.loof_expected_tcy_start_date IS NULL)
         THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',606);
        END IF;	
        --
        -- Check that loof_respond_by_date is greater than loof_offer_date  
        --
        IF (r1.loof_respond_by_date IS NOT NULL AND r1.loof_offer_date IS NOT NULL )
         THEN        
          IF r1.loof_respond_by_date <= r1.loof_offer_date 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',556);
			-- The Respond By Date must be greater than the Offer Date
          END IF;
        END IF;
        --
        -- Check that loof_pro_propref exists on the properties table  
        --
        IF (r1.loof_pro_propref IS NOT NULL)
         THEN        
          OPEN c2(r1.loof_pro_propref);
          FETCH c2 INTO r2;
          IF c2%NOTFOUND 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',557);
			-- The supplied Property Reference is not an existing property reference
          END IF;
          CLOSE c2;
        END IF;
        --
		-- **************************************************	
        -- Check that the number of offers for the void instance will not exceed the system maximum	
        -- NOT USED DONE AT BOTTOM AFTER FINDING VIN_REFNO by c8 after vin_refno found (AJ 17Jun2018)
		--
        -- IF (r1.loof_pro_propref IS NOT NULL AND r1.loof_hps_start_date IS NOT NULL)
        -- THEN 
        --  OPEN c3(r1.loof_pro_propref,r1.loof_hps_start_date);
        --  FETCH c3 INTO r3;
        --  CLOSE c3;
        --  IF r3.no_of_offers > fsc_utils.get_sys_param('OFFCON')
        --   THEN
        --    l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',558);
		--	-- The number of offers for the void will exceed the system maximum
        --  END IF;
        --END IF;
		-- **************************************************	
        --
        -- Check that the loof_ale_rli_code exists on an applic_list_entries row for the loof_ale_app_legacy_ref 
        --
		IF (r1.loof_ale_app_legacy_ref IS NOT NULL AND r1.loof_ale_rli_code IS NOT NULL)
		 THEN
          OPEN c4(r1.loof_ale_app_legacy_ref,r1.loof_ale_rli_code);
          FETCH c4 INTO r4;
          CLOSE c4;
          --
          IF (r4.ale_current_offer_count IS NULL)
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',559);
			-- No applic_list_entries record was found for the application with the supplied Rehousing List Code
          ELSE  
          --
          -- Check that the number of offers for the application reference will not exceed the system maximum  
          --
           IF (r4.ale_current_offer_count +1) > nvl(r4.fro_maximum_offer,1)
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',560);
			 -- The number of offers for the application reference will exceed the system maximum
           END IF;
          END IF;
		  --
          -- Also check other records in the data load batch to make sure these when loaded
		  -- will no exceed the maximum
		  --
		  IF (r4.ale_current_offer_count IS NOT NULL)
		   THEN
            OPEN c9(r1.loof_ale_app_legacy_ref,r1.loof_dlb_batch_id);
            FETCH c9 INTO l_count_app_legacy_ref;
            CLOSE c9;
            --
            IF (r4.ale_current_offer_count + l_count_app_legacy_ref) > nvl(r4.fro_maximum_offer,1)
             THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',609);
			 -- The number of offers for the application will exceed the system max in this batch
            END IF;
          END IF;			
        END IF;
        --
        -- Check that the loof_ttyp_hrv_code matches a tenure type on hrv_tenure_types  
        --
		IF (r1.loof_ttyp_hrv_code IS NOT NULL)
		 THEN
          OPEN c5(r1.loof_ttyp_hrv_code);
          FETCH c5 INTO r5;
          IF c5%NOTFOUND
           THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',561);   -- Tenure Type does not exist
          END IF;        
          CLOSE c5;
        END IF;
        --
        -- Check that the loof_tty_code matches a tenancy type on tenancy_types  
        --
		IF(r1.loof_tty_code IS NOT NULL)
		 THEN
          OPEN c6(r1.loof_tty_code);
          FETCH c6 INTO r6;
          IF c6%NOTFOUND
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',562);   -- Tenancy Type does not exist
          END IF;        
          CLOSE c6;
        END IF;
        --
        -- Check that the loof_ost_code matches a offer stage code on offer_stage_types  
        --
		IF (r1.loof_osg_ost_code IS NOT NULL)
		 THEN
          OPEN c7(r1.loof_osg_ost_code);
          FETCH c7 INTO r7;
          IF c7%NOTFOUND
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',563);   -- Stage Code does not exist
          END IF;        
          CLOSE c7;
        END IF;
        --
        -- Expected tenancy start date must be greater than or equal to offer date
        --
		IF (r1.loof_expected_tcy_start_date IS NOT NULL
            AND r1.loof_offer_date IS NOT NULL )
         THEN
          IF r1.loof_expected_tcy_start_date < r1.loof_offer_date
           THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',564);
		   -- Expected tenancy start date must be greater than or equal to offer date                   
          END IF;
        END IF;
        --
        -- Offer type must be 'MOF' i.e. Manual Offer
        --
        IF NVL(r1.loof_type,'XXX') != 'MOF'
         THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',565);
		  -- Offer type must be MOF i.e. Manual Offer                   
        END IF;
        --
        -- Status code must be CUR – Current ONLY the others 
		-- CON – Confirmed, WIT - Withdraw, REF - Refused, ACC - Accepted
        -- are NOT ALLOWED
		--		
		IF (r1.loof_sco_code IS NOT NULL)
		 THEN
          IF (r1.loof_sco_code !='CUR')
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',608);
		    -- If the status NOT Current the Accepted Date must be supplied
          END IF;		  
        END IF;
        --
        --  Check the created date isn't greater than today
        --
        IF NVL(r1.loof_created_date,SYSDATE) > SYSDATE
         THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',567);
		  -- If you supply a created date, the date cannot be greater than today's date                   
        END IF;
        --
		-- Void Instances field mandatory checks and validation
		-- A minimum of One field must be supplied to use to find void instance
		-- along with the loof_hps_start_date
		-- 
		IF ( r1.loof_vin_effective_date is NULL
		 AND r1.loof_vin_created_date is NULL
		 AND r1.loof_vin_hrv_vcl_code IS NULL
         AND r1.loof_vin_vst_code IS NULL
         AND r1.loof_vin_apt_code IS NULL  
         AND r1.loof_vin_sco_code IS NULL
         AND r1.loof_vin_vgr_code IS NULL
         AND r1.loof_vin_vpa_curr_code IS NULL
		 AND r1.loof_vin_hrv_rfv_code IS NULL
         AND r1.loof_vin_text IS NULL
         AND r1.loof_vin_tgt_date IS NULL
         AND r1.loof_vin_man_created IS NULL
         AND r1.loof_vin_dec_allowance IS NULL )
		  THEN
		   l_vin_check  := 'N';
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',611);
		   -- 'At Least one Other Void Instance field must be supplied as well as the status start date'
        END IF;		
		--
        -- VIN VCL
		--
        IF r1.loof_vin_hrv_vcl_code IS NOT NULL 
         THEN
          OPEN  c_vcl_code (r1.loof_vin_hrv_vcl_code);
          FETCH c_vcl_code into l_vcl_code;
          CLOSE c_vcl_code;
          --
          IF l_vcl_code IS NULL
           THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',360);
          END IF;
        END IF;
		--
        -- VIN VST CODE
		--
        IF r1.loof_vin_vst_code IS NOT NULL 
         THEN
          OPEN  c_vst (r1.loof_vin_vst_code);
          FETCH c_vst into l_vst;
          CLOSE c_vst;
		  --
          IF l_vst IS NULL
           THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',359);
          END IF;
        END IF;
		--
        -- VIN APT
		--
        IF r1.loof_vin_apt_code IS NOT NULL 
         THEN
          OPEN  c_apt_code (r1.loof_vin_apt_code);
          FETCH c_apt_code into l_apt_code;
          CLOSE c_apt_code;
          --
          IF l_apt_code IS NULL
           THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',897);
          END IF;
        END IF;
        --
        -- VIN SCO
		--
		IF r1.loof_vin_sco_code IS NOT NULL 
         THEN		
          IF r1.loof_vin_sco_code NOT IN ('FIN','COM','CUR','CAN','PRO','NEW') 
           THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',898);
          END IF;
        END IF;
		--
        --VOID GROUP
		--
        IF r1.loof_vin_vgr_code IS NOT NULL 
         THEN
          OPEN  c_vgr_code (r1.loof_vin_vgr_code);
          FETCH c_vgr_code into l_vgr_code;
          CLOSE c_vgr_code;
          --
          IF l_vgr_code IS NULL
           THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',358);
          END IF;
        END IF;
		--
        --VOID PATH
		--
        IF r1.loof_vin_vpa_curr_code IS NOT NULL
         THEN
          OPEN  c_vpa_code (r1.loof_vin_vpa_curr_code);
          FETCH c_vpa_code into l_vpa_code;
          CLOSE c_vpa_code;
          --    
          IF l_vpa_code IS NULL
           THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',899);
          END IF;
        END IF;
		--
        --VOID REASON
		--
        IF r1.loof_vin_hrv_rfv_code IS NOT NULL 
         THEN
          OPEN  c_rfv_code (r1.loof_vin_hrv_rfv_code);
          FETCH c_rfv_code into l_frv_code;
          CLOSE c_rfv_code;
          --
          IF l_frv_code IS NULL
           THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',903);
          END IF;
        END IF;
        --
        -- Find Associated void instance for offer
        -- use all fields and match current table value if field not supplied
		--
        IF ( NVL(l_vin_check,'Y') = 'Y' AND r2.pro_refno IS NOT NULL )
          THEN
          --
          OPEN  c_count_vin_refno 
                     (r2.pro_refno
                     ,r1.loof_hps_start_date
                     ,r1.loof_vin_dec_allowance
                     ,r1.loof_vin_man_created
                     ,r1.loof_vin_vst_code
                     ,r1.loof_vin_tgt_date
                     ,r1.loof_vin_apt_code
                     ,r1.loof_vin_vgr_code
                     ,r1.loof_vin_vpa_curr_code
                     ,r1.loof_vin_sco_code
                     ,r1.loof_vin_hrv_rfv_code
                     ,r1.loof_vin_hrv_vcl_code
                     ,r1.loof_vin_effective_date
                     ,r1.loof_vin_text
                     ,r1.loof_vin_created_date);
          FETCH c_count_vin_refno into l_count_vin_refno;
          CLOSE c_count_vin_refno;
          --
          IF (l_count_vin_refno > 1)
           THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',601);
          END IF;		 
          --
          IF (l_count_vin_refno < 1)
           THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',602);
          END IF;		 
          --
		  IF (l_count_vin_refno = 1)
           THEN
            OPEN  c_get_vin_refno
                     (r2.pro_refno
                     ,r1.loof_hps_start_date
                     ,r1.loof_vin_dec_allowance
                     ,r1.loof_vin_man_created
                     ,r1.loof_vin_vst_code
                     ,r1.loof_vin_tgt_date
                     ,r1.loof_vin_apt_code
                     ,r1.loof_vin_vgr_code
                     ,r1.loof_vin_vpa_curr_code
                     ,r1.loof_vin_sco_code
                     ,r1.loof_vin_hrv_rfv_code
                     ,r1.loof_vin_hrv_vcl_code
                     ,r1.loof_vin_effective_date
                     ,r1.loof_vin_text
                     ,r1.loof_vin_created_date);
            FETCH c_get_vin_refno into l_loof_vin_refno;
            CLOSE c_get_vin_refno;
            --
            IF (l_loof_vin_refno IS NULL)
             THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',612);
            END IF;
            --
          END IF;
          --
        END IF;
        --
        -- Check that the number of offers for the void instance will not exceed the system maximum  
        --
        IF  ( r2.pro_refno            IS NOT NULL
		  AND l_loof_vin_refno        IS NOT NULL )
         THEN 
          OPEN c8(r2.pro_refno,l_loof_vin_refno);
          FETCH c8 INTO r8;
          CLOSE c8;
          IF r8.no_of_offers > fsc_utils.get_sys_param('OFFCON')
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',558);
			-- The number of offers for the void will exceed the system maximum
          END IF;
          --
		  -- now check the total number for the void and property in the batch
		  --
		  IF (r1.loof_hps_start_date IS NOT NULL)
		   THEN
            OPEN c10(r1.loof_pro_propref
                    ,r1.loof_hps_start_date
                    ,r1.loof_dlb_batch_id);
            FETCH c10 INTO l_count_prop_offers;
            CLOSE c10;
            IF l_count_prop_offers > fsc_utils.get_sys_param('OFFCON')
             THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',610);
			  -- The number of offers for the property void will exceed the system max in this batch
            END IF;
          END IF;
        END IF;
        --
        -- Update DL table loof_vin_refno if found
        --
		IF (l_loof_vin_refno IS NOT NULL AND l_count_vin_refno = 1)
		 THEN
          UPDATE dl_hat_organisation_offers
          SET    loof_vin_refno = l_loof_vin_refno
          WHERE  ROWID = r1.rec_rowid;	
		END IF;
        --
        -- **********************************************  
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
        --
      END;
    --
    END LOOP;
    --
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HAT_ORGANISATION_OFFERS');
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
    SELECT ROWID                             rec_rowid
    ,      loof_dlb_batch_id         
    ,      loof_dl_seqno             
    ,      loof_dl_load_status       
    ,      loof_refno
    ,      loof_vin_refno	
    FROM   dl_hat_organisation_offers
    WHERE  loof_dlb_batch_id = p_batch_id
    AND    loof_dl_load_status = 'C';
  --
  cb  VARCHAR2(30);
  cd  DATE;
  cp  VARCHAR2(30) := 'DELETE';
  ct  VARCHAR2(30) := 'DL_HAT_ORGANISATION_OFFERS';
  cs  INTEGER;
  ce  VARCHAR2(200);
  i   INTEGER :=0;
  l_id        ROWID;
  l_an_tab    VARCHAR2(1);
  --
  BEGIN
  --
    fsc_utils.proc_start('s_dl_hat_organisation_offers.dataload_delete');
    fsc_utils.debug_message('s_dl_hat_organisation_offers.dataload_delete',3 );
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    --
    FOR r1 in c1 
    LOOP
    --
      BEGIN
        --
        cs := r1.loof_dl_seqno;
        l_id := r1.rec_rowid; 		
        i := i + 1;
        SAVEPOINT SP1;
        --
        DELETE 
        FROM   offer_stages
        WHERE  osg_oof_refno = r1.loof_refno;
        --
        DELETE 
        FROM   organisation_offers
        WHERE  oof_refno = r1.loof_refno;
        --
        UPDATE dl_hat_organisation_offers
        SET    loof_refno = null
        WHERE  ROWID = r1.rec_rowid;
        --		
        -- Now UPDATE the record status and process count
        --
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        set_record_status_flag(l_id,'V');
        IF MOD(i,5000) = 0 
         THEN 
          COMMIT; 
        END IF;
		--
        EXCEPTION
         WHEN OTHERS 
          THEN
           ROLLBACK TO SP1;
           ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
           set_record_status_flag(l_id,'C');
           s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
        --
      END;
      --
    END LOOP;
	COMMIT;
    --
	l_an_tab:=s_dl_hem_utils.dl_comp_stats('ORGANISATION_OFFERS');
	l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HAT_ORGANISATION_OFFERS');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_hat_organisation_offers;
/

show errors

