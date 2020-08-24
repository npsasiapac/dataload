CREATE OR REPLACE PACKAGE BODY s_dl_hem_properties
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER  WHO  WHEN        WHY
--      1.0          PJD  05/09/2000  Dataload
--      1.1          PJD  31/10/2000  Include latest revisions to dataload process
--      1.2          PJD  20/12/2000  Minor corrections to exception handling
--      1.3          PJD  14/03/2001  Now Creates Prop Status batch header
--                                    in dl_batches table.
--      1.4          SPAU 09/11/2001  Now creates Contact details.
--      1.5  5.1.4   PJD  02/02/2002  Allow for Batch question on 'Allow Property
--                                    Update'
--                                    Added NVL Clauses to ptv_refno cursor     
--                                    Added Savepoints
--                                    Added validation on Alt Ref            
--      1.6  5.1.4   PJD  15/02/2002  Further changes for 'Allow Property Update'
--      1.7  5.1.4   PJD  21/02/2002  Change to c_add to use pro_refno           
--      1.8  5.1.5   PH   08/04/2002  Added validation on local_ind and
--                                    ael_street
--      1.9  5.1.5   PJD  23/04/2002  Added validation of Street No /Flat No/  
--                                    Building
--      1.10 5.1.5   MH   25/04/2002  Added delete address cleansing code
--      1.11 5.1.5   PJD  01/05/2002  Changed insert and delete processes to 
--                                    take account of the existance of 
--                                    dl_hem_property_statuses
--      1.12 5.1.6   PJD  15/05/2002  Changed length of batch id to 30 on 
--                                    insert to dl_hem_prop_statuses
--      1.13 5.1.6   SB   16/05/2002  Added check on Flat/ Building combination 
--                                    error 962
--      1.14 5.1.6   PJD  17/05/2002  Only insert address if supplied
--      1.15 5.1.6   PJD  20/05/2002  Close cursors before Insert/Update rather 
--                                    than afterwards
--      1.16 5.1.6.  PH   22/05/2002  Added delete from summary_pro_accounts 
--                                    and prop_debit_statuses
--      1.17 5.1.6   PJD  23/05/2002  Added periodic analyze table within the 
--                                    create process
--      1.18 5.1.6   PJD  28/05/2002  Corrected cursor c_add - now returns 'X' 
--                                    rather than NULL
--                                    Insert into contact details now uses 
--                                    acquired_date 
--      1.19 5.1.6   PJD  05/06/2002  Added extra tables into analyze section
--                                    Added some mandatory FRB fields into the
--                                    properties insert statement
--      2.0  5.2.0   PH   11/07/2002  Software Release 520. Have amended delete
--                                    so we delete from property_movement_audits
--      2.1  5.2.0   PJD  29/07/2002  Error code 889 changed to 389
--      2.2  5.2.0   PJD  30/07/2002  Removed the validation on Freeholder address. 
--      2.3  5.2.0   PJD  31/07/2002  Changed delete proc to now delete from
--                                    dl_hem_property_statuses (rather than just update)
--      2.4  5.2.0   PH   01/10/2002  Improved handling of parent_propref
--      3.0  5.3.0   PJD  05/02/2003  Allow for changes to Prop_Status_codes
--                                    if the property is being updated
--      3.1  5.3.0   PJD  08/05/2003  Changed Periodic commit to 5* 
--      3.2  5.3.0   PJD  05/06/2003  Added a commit to end of validate
--      3.3  5.3.0   PH   01/09/2003  Amended validate and create to cater for Leasholders 
--                                    by checking that a party exists and linking 
--                                    to that par_refno. Parties must be loaded first.
--      3.4  5.4.0   PJD  21/10/2003  Changed logic of previous amendment as it was
--                                    rejecting all owned properties.
--      3.5  5.4.0   PJD  20/11/2003  Moved Update of Record Status and Process Count
--      3.6  5.5.0   PJD  12/05/2004  Put NVL clauses around update statements 
--      3.7  5.5.0   PJD  19/05/2004  minor correction to above regarding lpro_sale_date
--      3.8  5.8.0   PH   19/07/2005  Added new fields for addresses, 
--                                    Eastings and Northings
--
--      4.0  5.9.0   VRS  16/01/2006  Removed INSERT of CDE_TCY_REFNO from Contact_Details
--      4.1  5.9.0   PJD  14/03/2006  Remove Address Tidy up section as it can cause the 
--                                    process to fail - especially if address child records 
--                                    exist in customer services.
--      5.0  5.10.0  PH   08-MAY-2006 Removed all references to Addresses as these
--                                    should be loaded in the Addresses Dataload.
--      5.1  5.13.0  PH   06-FEB-2008 Now includes its own 
--                                    set_record_status_flag procedure.
--      5.2  5.15.1  PH   20-NOV-2009 Corrected c_pro_refno cursor in validate
--                                    previously select null, now select pro_refno
--      5.3  6.9.0   MJK  05-MAR-2015 Changed hps_hpc_code to hps_hpc_typein Create section
--      5.4  6.9.0   AJ   05-MAR-2015 Changed hps_hpc_code to hps_hpc_type in Validate section
--                                    Cursor c_chk_dup_hps line 594
--      5.3  6.12    MJK  17-AUG-2015 Reformatted for 6.12. No changes to logic
--      5.4  6.13    AJ   02-MAR-2016 Added validate check on telephone number (lpro_phone) to
--                                    allow for options of spaces allowed, min max length and
--                                    digits only against contact method
--      5.5  6.13    PAH  25-APR-2016 Amended error number from HDL 734 to HD2 890 as wrong
--                                    message for check p1.lpro_sco_code NOT IN('OCC','VOI','CLO')
--                                    in validate (AJ 27Apr2016)
--
--      5.6  6.13    PJD  16-OCT-2017 Create Pre Alloc Payrefs
--      5.7  6.14    AJ   10-NOV-2017 batch question (l_answer2) put around Create of Pre Alloc Payrefs
--                                    because if done at this point before revenue account data load the
--                                    new rac_accnos will be lower than the current ones
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
    UPDATE dl_hem_properties
    SET    lpro_dl_load_status = p_status
    WHERE  rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_hem_properties') ;
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
      SELECT rowid rec_rowid
      ,      lpro_dlb_batch_id
      ,      lpro_dl_seqno
      ,      lpro_dl_load_status
      ,      lpro_propref
      ,      lpro_hou_frb
      ,      lpro_sco_code
      ,      lpro_organisation_ind
      ,      lpro_hou_hrv_hot_code
      ,      lpro_hou_hrv_hrs_code
      ,      lpro_hou_hrv_hbu_code
      ,      lpro_hou_hrv_hlt_code
      ,      lpro_parent_propref
      ,      lpro_hou_sale_date
      ,      NVL(lpro_hou_service_prop_ind,'N')                               lpro_hou_service_prop_ind
      ,      NVL(lpro_hou_acquired_date,TO_DATE('01-JAN-1900','DD-MON-YYYY')) lpro_hou_acquired_date
      ,      NVL(lpro_hou_defects_ind,'N')                                    lpro_hou_defects_ind
      ,      NVL(lpro_hou_allow_placement_ind,'N')                            lpro_hou_allow_placement_ind
      ,      NVL(lpro_hou_residential_ind,'N')                                lpro_hou_residential_ind
      ,      lpro_hou_alt_ref
      ,      lpro_hou_debit_to_date
      ,      lpro_hou_lease_start_date
      ,      lpro_hou_lease_review_date
      ,      lpro_hou_construction_date
      ,      lpro_hou_ptv_code
      ,      lpro_hou_hrv_pst_code
      ,      lpro_hou_hrv_hmt_code
      ,      lpro_hou_management_end_date
      ,      lpro_free_refno
      ,      lpro_free_name
      ,      lpro_prop_status
      ,      lpro_status_start
      ,      lpro_phone
      ,      lpro_refno
      FROM   dl_hem_properties
      WHERE  lpro_dlb_batch_id = p_batch_id
      AND    lpro_dl_load_status = 'V';

    CURSOR c_ptv_refno(p_ptv_code VARCHAR2,p_pst_code VARCHAR2)
    IS
      SELECT ptv_refno
      ,      reusable_refno_seq.nextval
      FROM   prop_type_values
      WHERE  ptv_pty_code = p_ptv_code
      AND    NVL(ptv_pst_frv_code,'NO SUBTYPE') 
                                 = NVL(p_pst_code,'NO SUBTYPE') ;

    CURSOR c_pro(p_propref VARCHAR2)
    IS
      SELECT pro_refno
      FROM   properties
      WHERE  pro_propref = p_propref;

    CURSOR c_accno 
    IS
      SELECT rac_accno_seq.nextval
      FROM   dual;
    --

    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'CREATE';
    ct VARCHAR2(30) := 'DL_HEM_PROPERTIES';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    l_pro_refno         NUMBER;
    l_ptv_refno         INTEGER;
    l_reusable_refno    INTEGER;
    l_adr_refno         INTEGER;
    l_pro_parent        NUMBER;
    l_street_index_code VARCHAR2(12);
    ai                  INTEGER := 100;
    i                   INTEGER := 0;
    l_dummy             VARCHAR2(1);
    l_an_tab            VARCHAR2(1);
    l_par_refno         NUMBER;
    l_prealloc_payrefs  VARCHAR2(10);
    l_payref revenue_accounts.rac_pay_ref%type;
    l_accno  revenue_accounts.rac_accno%type;
    l_cd     VARCHAR2(1);
    l_answer2           VARCHAR2(1);
--
  BEGIN
    fsc_utils.proc_start('s_dl_hem_properties.dataload_create');
    fsc_utils.debug_message('s_dl_hem_properties.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;

    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    l_answer2 := s_dl_batches.get_answer(p_batch_id,2);

    INSERT INTO dl_batches
    (dlb_batch_id  
    ,dlb_dla_product_area  
    ,dlb_dla_dataload_area  
    ,dlb_created_by  
    ,dlb_created_date  
    )  
    SELECT RTRIM(SUBSTR(p_batch_id,1,29)) ||'S'
    ,      'HEM'
    ,      'PROPERTY_STATUSES'
    ,      USER
    ,      SYSDATE
    FROM   dual
    WHERE  NOT EXISTS
             (SELECT NULL
              FROM   dl_batches
              WHERE  dlb_batch_id = 
                        RTRIM(SUBSTR(p_batch_id,1,29)) ||'S'
             );

    l_prealloc_payrefs := fsc_utils.get_sys_param('PREPAYRF');

    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.lpro_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        l_ptv_refno := NULL;
        l_reusable_refno := NULL;

        OPEN c_ptv_refno(p1.lpro_hou_ptv_code,p1.lpro_hou_hrv_pst_code) ;
        FETCH c_ptv_refno INTO l_ptv_refno,l_reusable_refno;
        CLOSE c_ptv_refno;

        l_pro_refno := NULL;
        l_pro_parent := NULL;
        l_par_refno := NULL;

        OPEN c_pro(p1.lpro_propref) ;
        FETCH c_pro INTO l_pro_refno;
        CLOSE c_pro;

        OPEN c_pro(p1.lpro_parent_propref);
        FETCH c_pro INTO l_pro_parent;
        CLOSE c_pro;

        IF l_pro_refno IS NOT NULL 
        THEN
          UPDATE properties
          SET    pro_type = DECODE(pro_type,'FRB','BOTH',pro_type)
          ,      pro_organisation_ind = NVL(p1.lpro_organisation_ind,pro_organisation_ind)
          ,      pro_propref = p1.lpro_propref
          ,      pro_sco_code = NVL(p1.lpro_sco_code,pro_sco_code)
          ,      pro_modified_date = SYSDATE
          ,      pro_modified_by = USER
          ,      pro_hou_hrv_hot_code = NVL(p1.lpro_hou_hrv_hot_code,pro_hou_hrv_hot_code)
          ,      pro_hou_hrv_hrs_code = NVL(p1.lpro_hou_hrv_hrs_code,pro_hou_hrv_hrs_code)
          ,      pro_hou_hrv_hbu_code = NVL(p1.lpro_hou_hrv_hbu_code,pro_hou_hrv_hbu_code)
          ,      pro_hou_hrv_hlt_code = NVL(p1.lpro_hou_hrv_hlt_code,pro_hou_hrv_hlt_code)
          ,      pro_hou_parent_pro_refno = NVL(l_pro_parent,pro_hou_parent_pro_refno)
          ,      pro_hou_sale_date = NVL(p1.lpro_hou_sale_date,pro_hou_sale_date)
          ,      pro_hou_service_prop_ind = NVL(p1.lpro_hou_service_prop_ind,pro_hou_service_prop_ind)
          ,      pro_hou_acquired_date = NVL(p1.lpro_hou_acquired_date,pro_hou_acquired_date)
          ,      pro_hou_defects_ind = NVL(p1.lpro_hou_defects_ind,pro_hou_defects_ind)
          ,      pro_hou_allow_placement_ind = NVL(p1.lpro_hou_allow_placement_ind, pro_hou_allow_placement_ind)
          ,      pro_hou_residential_ind = NVL(p1.lpro_hou_residential_ind,pro_hou_residential_ind)
          ,      pro_hou_alt_ref = NVL(p1.lpro_hou_alt_ref,pro_hou_alt_ref)
          ,      pro_hou_debit_to_date = NVL(p1.lpro_hou_debit_to_date,pro_hou_debit_to_date)
          ,      pro_hou_lease_start_date = NVL(p1.lpro_hou_lease_start_date,pro_hou_lease_start_date)
          ,      pro_hou_lease_review_date = NVL(p1.lpro_hou_lease_review_date, pro_hou_lease_review_date)
          ,      pro_hou_construction_date = NVL(p1.lpro_hou_construction_date, pro_hou_construction_date)
          ,      pro_hou_ptv_refno = NVL(l_ptv_refno,pro_hou_ptv_refno)
          ,      pro_hou_hrv_hmt_code = NVL(p1.lpro_hou_hrv_hmt_code,pro_hou_hrv_hmt_code)
          ,      pro_hou_management_end_date = NVL(p1.lpro_hou_management_end_date, pro_hou_management_end_date)
          WHERE  pro_refno = l_pro_refno;

          UPDATE hou_prop_statuses
          SET    hps_end_date = NVL(p1.lpro_status_start - 1,TRUNC(sysdate) - 1)
          WHERE  hps_pro_refno = l_pro_refno
          AND    hps_hpc_type = 'C'
          AND    NVL(hps_end_date,p1.lpro_status_start + 1) > NVL(p1.lpro_status_start,TRUNC(sysdate)) ;

          DELETE FROM hou_prop_statuses
          WHERE  hps_pro_refno = l_pro_refno
          AND    hps_hpc_type = 'C'
          AND    NVL(hps_start_date,p1.lpro_status_start + 1) > NVL(p1.lpro_status_start,TRUNC(sysdate)) ;

        ELSE

          l_pro_refno := p1.lpro_refno;

          INSERT INTO properties
          (pro_refno  
          ,pro_type  
          ,pro_reusable_refno  
          ,pro_organisation_ind  
          ,pro_creation_date  
          ,pro_created_by  
          ,pro_propref  
          ,pro_sco_code  
          ,pro_modified_date  
          ,pro_modified_by  
          ,pro_description  
          ,pro_hou_hrv_hot_code  
          ,pro_hou_hrv_hrs_code  
          ,pro_hou_hrv_hbu_code  
          ,pro_hou_hrv_hlt_code  
          ,pro_hou_parent_pro_refno  
          ,pro_hou_sale_date  
          ,pro_hou_service_prop_ind  
          ,pro_hou_acquired_date  
          ,pro_hou_defects_ind  
          ,pro_hou_allow_placement_ind  
          ,pro_hou_residential_ind  
          ,pro_hou_alt_ref  
          ,pro_hou_debit_to_date  
          ,pro_hou_lease_start_date  
          ,pro_hou_lease_review_date  
          ,pro_hou_construction_date  
          ,pro_hou_ptv_refno  
          ,pro_hou_hrv_hmt_code  
          ,pro_hou_management_end_date  
          ,pro_frb_inhibit_canvass_ind  
          ,pro_frb_inhibit_inspection_ind  
          ,pro_frb_start_date  
          )  
          VALUES  
          (l_pro_refno  
          ,NVL(p1.lpro_hou_frb,'HOU')  
          ,l_reusable_refno  
          ,p1.lpro_organisation_ind  
          ,TRUNC(sysdate)  
          ,'DATALOAD'  
          ,p1.lpro_propref  
          ,p1.lpro_sco_code  
          ,NULL  
          ,NULL  
          ,NULL  
          ,p1.lpro_hou_hrv_hot_code  
          ,p1.lpro_hou_hrv_hrs_code  
          ,p1.lpro_hou_hrv_hbu_code  
          ,p1.lpro_hou_hrv_hlt_code  
          ,l_pro_parent  
          ,p1.lpro_hou_sale_date  
          ,p1.lpro_hou_service_prop_ind  
          ,p1.lpro_hou_acquired_date  
          ,p1.lpro_hou_defects_ind  
          ,p1.lpro_hou_allow_placement_ind  
          ,p1.lpro_hou_residential_ind  
          ,p1.lpro_hou_alt_ref  
          ,p1.lpro_hou_debit_to_date  
          ,p1.lpro_hou_lease_start_date  
          ,p1.lpro_hou_lease_review_date  
          ,p1.lpro_hou_construction_date  
          ,l_ptv_refno  
          ,p1.lpro_hou_hrv_hmt_code  
          ,p1.lpro_hou_management_end_date  
          ,DECODE(p1.lpro_hou_frb,'BOTH','N',NULL)  
          ,DECODE(p1.lpro_hou_frb,'BOTH','N',NULL)  
          ,DECODE(p1.lpro_hou_frb,'BOTH',p1.lpro_hou_acquired_date,NULL)  
          );  

        END IF;

        IF p1.lpro_prop_status IS NOT NULL 
        THEN
          INSERT INTO hou_prop_statuses
          (hps_pro_refno  
          ,hps_hpc_code  
          ,hps_hpc_type  
          ,hps_start_date  
          ,hps_end_date  
          )  
          SELECT l_pro_refno
          ,      p1.lpro_prop_status
          ,      'C'
          ,      p1.lpro_status_start
          ,      NULL
          FROM   DUAL
          WHERE NOT EXISTS
                  (SELECT NULL
                   FROM   hou_prop_statuses
                   WHERE  hps_pro_refno = l_pro_refno
                   AND    NVL(hps_end_date,p1.lpro_status_start + 1) > p1.lpro_status_start
                  );
        END IF;

        INSERT INTO dl_hem_property_statuses
        (lhps_dlb_batch_id  
        ,lhps_dl_seqno  
        ,lhps_dl_load_status  
        ,lhps_pro_propref  
        ,lhps_start_date  
        )  
        SELECT rtrim(SUBSTR(p_batch_id,1,29)) ||'S'
        ,      p1.lpro_dl_seqno
        ,      'L'
        ,      p1.lpro_propref
        ,      p1.lpro_hou_acquired_date
        FROM   dual
        WHERE  NOT EXISTS
                 (SELECT NULL
                  FROM   dl_hem_property_statuses
                  WHERE  lhps_dlb_batch_id = rtrim(SUBSTR(p_batch_id,1,29)) ||'S'
                  AND    lhps_dl_seqno = p1.lpro_dl_seqno
                 );

        IF p1.lpro_phone IS NOT NULL 
        THEN
          INSERT INTO contact_details
          (cde_refno  
          ,cde_start_date  
          ,cde_created_date  
          ,cde_created_by  
          ,cde_contact_value  
          ,cde_frv_cme_code  
          ,cde_contact_name  
          ,cde_end_date  
          ,cde_pro_refno  
          ,cde_aun_code  
          ,cde_par_refno  
          ,cde_bde_refno  
          ,cde_cos_code  
          ,cde_cse_contact  
          ,cde_srq_no  
          )  
          SELECT cde_refno.nextval
          ,      p1.lpro_hou_acquired_date
          ,      TRUNC(SYSDATE)
          ,      'DATALOAD'
          ,      p1.lpro_phone
          ,      'TELEPHONE'
          ,      NULL
          ,      NULL
          ,      l_pro_refno
          ,      NULL
          ,      NULL
          ,      NULL
          ,      NULL
          ,      NULL
          ,      NULL
          FROM   DUAL
          WHERE NOT EXISTS
                  (SELECT NULL
                   FROM   CONTACT_DETAILS
                   WHERE  cde_pro_refno = l_pro_refno
                   AND    cde_frv_cme_code = 'TELEPHONE'
                  );

        END IF;
--
        IF NVL(l_answer2,'N') = 'Y' THEN
--
        IF l_prealloc_payrefs = 'Y'
         THEN
          l_accno := NULL;
 
          OPEN  c_accno;
          FETCH c_accno INTO l_accno;
          CLOSE c_accno;
          --
          l_payref := TO_CHAR(l_accno);
          --
          s_hra_check_digit.p_gen_check_digit 
                                  (p_reference    => l_payref
                                  ,p_accno        => l_accno
                                  ,p_check_digit  => l_cd
                                  ,p_hrv_ate_code => 'REN'
                                  ,p_class_code   => 'REN'
                                  ,p_pro_refno    => l_pro_refno
                                  );
  
         INSERT INTO pre_alloc_payrefs 
                                 (pap_pro_refno
                                 ,pap_next_rac_payref
                                 ,pap_next_rac_accno
                                 ,pap_next_check_digit)
         VALUES (l_pro_refno
                ,l_payref
                ,l_accno
                ,l_cd);
          --
        END IF;
        END IF;  -- l_answer2

        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        set_record_status_flag(l_id,'C');
        i := i + 1;
        IF MOD(i,1000) = 0 
        THEN
          COMMIT;
          IF i >=(ai * 5)
          THEN
            ai := i;
            l_an_tab := s_dl_hem_utils.dl_comp_stats('PROPERTIES') ;
            l_an_tab := s_dl_hem_utils.dl_comp_stats('HOU_PROP_STATUSES') ;
            l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HEM_PROPERTY_STATUSES') ;
            l_an_tab := s_dl_hem_utils.dl_comp_stats('CONTACT_DETAILS') ;
          END IF;
        END IF;
      EXCEPTION
      WHEN OTHERS 
      THEN
        ROLLBACK TO SP1;
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM) ;
        set_record_status_flag(l_id,'O') ;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y') ;
      END;
      COMMIT;
    END LOOP;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('PROPERTIES') ;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('HOU_PROP_STATUSES') ;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('CONTACT_DETAILS') ;
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED') ;
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
      ,      lpro_dlb_batch_id
      ,      lpro_dl_seqno
      ,      lpro_propref
      ,      lpro_hou_frb
      ,      lpro_sco_code
      ,      lpro_organisation_ind
      ,      lpro_hou_hrv_hot_code
      ,      lpro_hou_hrv_hrs_code
      ,      lpro_hou_hrv_hbu_code
      ,      lpro_hou_hrv_hlt_code
      ,      lpro_parent_propref
      ,      lpro_hou_sale_date
      ,      lpro_hou_service_prop_ind
      ,      lpro_hou_acquired_date
      ,      lpro_hou_defects_ind
      ,      lpro_hou_allow_placement_ind
      ,      lpro_hou_residential_ind
      ,      lpro_hou_alt_ref
      ,      lpro_hou_debit_to_date
      ,      lpro_hou_lease_start_date
      ,      lpro_hou_lease_review_date
      ,      lpro_hou_construction_date
      ,      lpro_hou_ptv_code
      ,      lpro_hou_hrv_pst_code
      ,      lpro_hou_hrv_hmt_code
      ,      lpro_hou_management_end_date
      ,      lpro_free_refno
      ,      lpro_free_name
      ,      lpro_prop_status
      ,      lpro_status_start
      ,      lpro_phone
      ,      lpro_refno
      FROM   dl_hem_properties
      WHERE  lpro_dlb_batch_id = p_batch_id
      AND    lpro_dl_load_status IN('L','F','O');
    CURSOR c_aun_code(p_aun_code VARCHAR2)
    IS
      SELECT NULL
      FROM   admin_units
      WHERE  aun_code = p_aun_code;
    CURSOR c_pro(p_propref VARCHAR2)
    IS
      SELECT pro_refno
      FROM   properties
      WHERE  pro_propref = p_propref;
    CURSOR c_alt_ref(p_alt_ref VARCHAR2,p_propref VARCHAR2)
    IS
      SELECT NULL
      FROM   properties
      WHERE  pro_hou_alt_ref = p_alt_ref
      AND    pro_propref != p_propref;
    CURSOR c_ptv(p_ptv_pty_code VARCHAR2,p_ptv_pst_frv_code VARCHAR2)
    IS
      SELECT NULL
      FROM   prop_type_values
      WHERE  ptv_pty_code = p_ptv_pty_code
      AND    NVL(ptv_pst_frv_code,'NO SUBTYPE') = NVL(p_ptv_pst_frv_code,'NO SUBTYPE') ;
    CURSOR c_chk_dup_hps(p_pro_refno NUMBER,p_date DATE)
    IS
      SELECT 'X'
      FROM   hou_prop_statuses
      WHERE  hps_pro_refno = p_pro_refno
      AND    NVL(hps_end_date,p_date + 1) > p_date
      AND    hps_hpc_type != 'C';
    CURSOR c_hpc(p_hpc_code VARCHAR2)
    IS
      SELECT hpc_type
      FROM   hou_prop_status_codes
      WHERE  hpc_code = p_hpc_code;
    CURSOR c_conm(p_conm_code VARCHAR2)
    IS
      SELECT conm_current_ind 
            ,conm_code
            ,conm_digits_only_ind
            ,conm_value_min_length
            ,conm_value_max_length
            ,conm_spaces_allow_ind
      FROM   contact_methods
      WHERE  conm_code = p_conm_code;
    CURSOR c_conm_spaces(p_dl_conm_value     VARCHAR2
                        ,p_lpro_dlb_batch_id VARCHAR2
                        ,p_lpro_dl_seqno     NUMBER
                        ,p_lpro_phone        VARCHAR2
                        ,p_lpro_propref      VARCHAR2)
    IS
      SELECT 'X'
      FROM   dl_hem_properties
      WHERE  lpro_dlb_batch_id = p_lpro_dlb_batch_id
        AND  lpro_dl_seqno = p_lpro_dl_seqno
        AND  lpro_phone = p_lpro_phone
        AND  lpro_propref = p_lpro_propref
        AND  NVL(p_dl_conm_value,'') NOT LIKE '% %';
    cb VARCHAR2(30);
    cd DATE;
    cp VARCHAR2(30) := 'VALIDATE';
    ct VARCHAR2(30) := 'DL_HEM_PROPERTIES';
    cs INTEGER;
    ce VARCHAR2(200);
    l_id ROWID;
    l_answer            VARCHAR2(1);
    l_pro_refno         NUMBER(10);
    l_errors            VARCHAR2(10);
    l_error_ind         VARCHAR2(10);
    l_dummy             VARCHAR2(10);
    l_exists            VARCHAR2(10);
    l_mode              VARCHAR2(10);
    l_street_index_code VARCHAR2(12);
    l_adr_refno         INTEGER;
    i                   INTEGER := 0;
    li                  INTEGER := 0;
    l_conm_code_in      VARCHAR2(10);
    l_conm_code_out     VARCHAR2(10);
    l_conm_cur          VARCHAR2(1);
    l_conm_dig          VARCHAR2(1);
    l_conm_min_len      NUMBER(3,0);
    l_conm_max_len      NUMBER(3,0);
    l_conm_spaces       VARCHAR2(1);
    l_chk_conm_spaces   VARCHAR2(1);
    l_char contact_details.cde_contact_value%TYPE;
  BEGIN
    fsc_utils.proc_start('s_dl_hem_properties.dataload_validate');
    fsc_utils.debug_message('s_dl_hem_properties.dataload_validate',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    l_answer := s_dl_batches.get_answer(p_batch_id,1);
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.lpro_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors := 'V';
        l_error_ind := 'N';
        IF p1.lpro_sco_code NOT IN('OCC','VOI','CLO') 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',890);
        END IF;
        l_pro_refno := NULL;
        OPEN c_pro(p1.lpro_propref) ;
        FETCH c_pro INTO l_pro_refno;
        CLOSE c_pro;
        IF l_pro_refno IS NOT NULL 
        THEN
          IF NVL(l_answer,'N') = 'N' 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',001);
          ELSE
            OPEN c_chk_dup_hps(l_pro_refno,NVL(p1.lpro_status_start,TRUNC(sysdate)));
            FETCH c_chk_dup_hps INTO l_exists;
            CLOSE c_chk_dup_hps;
            IF l_exists IS NOT NULL 
            THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',417);
            END IF;
          END IF;
        END IF;
        IF p1.lpro_parent_propref IS NOT NULL 
        THEN
          OPEN c_pro(p1.lpro_parent_propref);
          FETCH c_pro INTO l_dummy;
          IF c_pro%NOTFOUND 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',351);
          END IF;
          CLOSE c_pro;
        END IF;
        OPEN c_alt_ref(p1.lpro_hou_alt_ref,p1.lpro_propref) ;
        FETCH c_alt_ref
        INTO l_dummy;
        IF c_alt_ref%found 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',389);
        END IF;
        CLOSE c_alt_ref;
        IF(NOT s_dl_hem_utils.yorn(p1.lpro_organisation_ind)) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',002);
        END IF;
        IF(NOT s_dl_hem_utils.yornornull(p1.lpro_hou_residential_ind)) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',003);
        END IF;
        IF(NOT s_dl_hem_utils.yornornull(p1.lpro_hou_service_prop_ind)) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',006);
        END IF;
        IF(NOT s_dl_hem_utils.yornornull(p1.lpro_hou_defects_ind)) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',007);
        END IF;
        IF(NOT s_dl_hem_utils.exists_frv('OWN_TYPE',p1.lpro_hou_hrv_hot_code)) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',008);
        END IF;
        fsc_utils.debug_message('frv '||p1.lpro_hou_hrv_hmt_code,3);
        IF(NOT s_dl_hem_utils.exists_frv('MAINT_TYPE',p1.lpro_hou_hrv_hmt_code,'Y')) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',010);
        END IF;
        IF(NOT s_dl_hem_utils.exists_frv('PRO_SOURCE',p1.lpro_hou_hrv_hrs_code,'Y')) 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',011);
        END IF;
        OPEN c_ptv(p1.lpro_hou_ptv_code,p1.lpro_hou_hrv_pst_code);
        FETCH c_ptv INTO l_dummy;
        IF c_ptv%notfound 
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',012);
        END IF;
        CLOSE c_ptv;
        IF(p1.lpro_prop_status IS NOT NULL) 
        THEN
          l_dummy := NULL;
          OPEN c_hpc(p1.lpro_prop_status);
          FETCH c_hpc INTO l_dummy;
          CLOSE c_hpc;
          IF l_dummy != 'C' 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',014);
          END IF;
          IF l_dummy IS NULL 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',028);
          END IF;
          IF(p1.lpro_status_start IS NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',015);
          END IF;
        END IF;
        IF(p1.lpro_organisation_ind = 'Y')
        THEN
          IF(p1.lpro_free_name IS NOT NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',018);
          END IF;
          IF(p1.lpro_hou_lease_start_date IS NOT NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',024);
          END IF;
          IF(p1.lpro_hou_lease_review_date IS NOT NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',025);
          END IF;
          IF(p1.lpro_hou_hrv_hmt_code IS NOT NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',026);
          END IF;
          IF(p1.lpro_hou_hrv_hlt_code IS NOT NULL) 
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',027);
          END IF;
        END IF;
--
-- add check on telephone number check if supplied AJ 09Feb2016
--
        l_conm_cur        := NULL;
        l_conm_code_in    := 'TELEPHONE';
        l_conm_code_out   := NULL;
        l_conm_dig        := NULL;
        l_conm_min_len    := NULL;
        l_conm_max_len    := NULL;
        l_conm_spaces     := NULL;
        l_chk_conm_spaces := NULL;
        l_char            := NULL;
--
        IF p1.lpro_phone IS NOT NULL
         THEN
--		
          OPEN c_conm(l_conm_code_in);
          FETCH c_conm INTO l_conm_cur
                           ,l_conm_code_out		  
                           ,l_conm_dig
                           ,l_conm_min_len
                           ,l_conm_max_len
                           ,l_conm_spaces;
          CLOSE c_conm;		
--
-- check that contact method exists (l_conm_code_out)
--
          IF l_conm_code_out IS NULL 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',829);
          END IF;
--
-- further checks only if contact method found
--
          IF l_conm_code_out IS NOT NULL 
           THEN
            li := LENGTH(p1.lpro_phone);
--
-- check that only contains digits if set to Y (l_conm_dig)
--
            IF l_conm_dig = 'Y' 
             THEN
              l_char := SUBSTR(p1.lpro_phone,li,1);
               IF l_char NOT IN ('0','1','2','3','4','5','6','7','8','9')
                THEN
                 l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',830);
               END IF;
            END IF;
--
-- check the contact value length conforms to min and max lengths specified
-- l_conm_min_len and l_conm_max_len
--
            IF NVL(l_conm_min_len,li) > li 
             THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',831);
            END IF;
--
            IF NVL(l_conm_max_len,li) < li 
             THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',832);
            END IF;
--
-- check that contact values does not contain spaces if set (l_conm_spaces)
--
            IF l_conm_spaces = 'N'
             THEN
--
              OPEN c_conm_spaces(p1.lpro_phone
                                ,p1.lpro_dlb_batch_id
                                ,p1.lpro_dl_seqno
                                ,p1.lpro_phone
                                ,p1.lpro_propref);
              FETCH c_conm_spaces INTO l_chk_conm_spaces;
               IF c_conm_spaces%NOTFOUND 
                THEN
                 l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',833);
               END IF;
              CLOSE c_conm_spaces;
--
            END IF;
--			 
          END IF;
--
        END IF;		-- end of telephone number check
--    
        IF l_errors = 'F' 
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
    ,p_date IN DATE
    )
  IS
    CURSOR c1
    IS
      SELECT rowid rec_rowid
      ,      lpro_dlb_batch_id
      ,      lpro_dl_seqno
      ,      lpro_propref
      FROM   dl_hem_properties
      WHERE  lpro_dlb_batch_id = p_batch_id
      AND    lpro_dl_load_status = 'C';
    CURSOR c_pro_refno(p_propref VARCHAR2)
    IS
      SELECT pro_refno
      FROM   properties
      WHERE  pro_propref = p_propref;
    i           INTEGER := 0;
    l_pro_refno INTEGER;
    l_an_tab    VARCHAR2(1);
    cb          VARCHAR2(30);
    cd          DATE;
    cp          VARCHAR2(30) := 'DELETE';
    ct          VARCHAR2(30) := 'DL_HEM_PROPERTIES';
    cs          INTEGER;
    ce          VARCHAR2(200);
    l_id ROWID;
  BEGIN
    fsc_utils.proc_start('s_dl_hem_properties.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_properties.dataload_delete',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1
    LOOP
      BEGIN
        cs := p1.lpro_dl_seqno;
        i := i + 1;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        l_pro_refno := NULL;
        OPEN c_pro_refno(p1.lpro_propref);
        FETCH c_pro_refno INTO l_pro_refno;
        CLOSE c_pro_refno;

        DELETE FROM address_usages
        WHERE  aus_pro_refno = l_pro_refno;

        DELETE FROM HOU_PROP_STATUSES
        WHERE  hps_pro_refno = l_pro_refno;

        DELETE FROM contact_details
        WHERE  cde_pro_refno = l_pro_refno;

        DELETE FROM summary_pro_accounts
        WHERE  spa_pro_refno = l_pro_refno;

        DELETE FROM prop_debit_statuses
        WHERE  pds_pro_refno = l_pro_refno;

        DELETE FROM property_movement_audits
        WHERE  pma_pro_refno = l_pro_refno;

        DELETE FROM pre_alloc_payrefs
        WHERE  pap_pro_refno = l_pro_refno;

        DELETE FROM properties
        WHERE  pro_propref = p1.lpro_propref;

        DELETE FROM dl_hem_property_statuses
        WHERE  lhps_dlb_batch_id = 
                        rtrim(SUBSTR(p_batch_id,1,29)) ||'S'
        AND    lhps_dl_seqno = p1.lpro_dl_seqno;

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
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
        set_record_status_flag(l_id,'C') ;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
    END LOOP;
    l_an_tab := s_dl_hem_utils.dl_comp_stats('PROPERTIES');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('HOU_PROP_STATUSES');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('SUMMARY_PRO_ACCOUNTS');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('PROP_DEBIT_STATUSES');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('ADDRESS_USAGES');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('CONTACT_DETAILS');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_hem_properties;
/
show errors
