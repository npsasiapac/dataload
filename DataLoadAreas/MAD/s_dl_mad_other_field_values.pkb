CREATE OR REPLACE  PACKAGE BODY s_dl_mad_other_field_values
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VER  DB VER    WHO  WHEN         WHY
--  1.0  5.1.4     PJD  24-MAR-2002  Bespoke Dataload for NCCW
--  1.1  5.1.6     SB   23-MAY-2002  Amendment to c_pdf cursor
--  1.2  5.3.0     MH   08-DEC-2003  Added table name object checks
--  1.3  5.5.0     PH   30-JUN-2004  Bespoke Dataload for Anchor Trust
--  1.4  5.5.0     PH   14-JUL-2004  Amended code so that tcy_refno is
--                                   supplied as the legacy_ref
--  1.5  5.5.0     PH   20-JUL-2004  Amended Delete process so table
--                                   name is in upper case
--  1.6  5.12.0    PH   14-NOV-2007  Included Contract Versions
--  1.7  5.12.0    PH   06-DEC-2007  Included Task Versions
--  1.8  5.12.0    PH   10-APR-2008  Amended TASK_VERSIONS to
--                                   CONTRACT_TASKS
--  1.9  5.15.0    PH   26-MAR-2009  Amended create for Arrears Actions
--  1.10 5.16.1    PH   28-AUG-2009  Included Deliverables
--  1.11 5.16.1    VS   21-SEP-2009  Amended and tidied code
--  1.12 5.16.1    VS   07-NOV-2009  Defect id 2549, to allow bae_seqno to
--                                   identify correct otherfields records.
--
--  1.13 5.16.1    VS   24-NOV-2009  Defect id 2659, Oracle error detected
--
--  1.14 5.16.1    VS   05-DEC-2009  Get ADVICE CASE HOUSING OPTIONS other
--                                   fields values to work and insert into
--                                   Lettings benchmarks tables
--
--  1.15 5.16.1    VS   29-JAN-2010  HNSW Defect id 3125, to allow loading of
--                                   otherfield against Prevention Payments.
--
--  1.16 5.16.1    VS   05-FEB-2010  HNSW Defect id 3245 fix. disable trigger
--                                   FSC.PVA_CREATED_BY_DEFAULT
--
--  1.17 5.16.1    VS   07-APR-2010  HNSW Defect id 4011 fix. disable/enable
--                                   trigger FSC.PVA_CREATED_BY_DEFAULT
--                                   to be performed as part of POST MIG DBA
--                                   Task.
--
--  1.18 5.16.1    VS   09-APR-2010  HNSW Defect id 4011 fix. disable/enable
--                                   trigger FSC.PVA_CREATED_BY_DEFAULT
--                                   can be performed as part of CREATE process
--                                   due to permission change
--
--  1.19 5.16.1    VS   02-JUN-2010  HNSW Defect 4720 - PRS address letting
--                                   other fields  not displaying on migrated
--                                   data
--
--  1.20 5.16.1    VS   01-SEP-2010  HNSW Defect 4720 - PRS address letting not
--                                   being assigned right pgp_refno.
--                                   PGP_REFNO should come from
--                                   address_registers based on the adre_code
--
--  1.21 5.16.1    VS   21-SEP-2010  HNSW Defect 4720 - PRS address letting not
--                                   being assigned right pgp_refno. Reverting
--                                   DL version back to v1.19 using the
--                                   HOOP_CODE to derive the PGP_REFNO.
--
--  1.22 6.3.0     MM   27-APR-2011  Removed PVA_CREATED_BY_DEFAULT
--                                   disable/enable, as resulting in a
--                                   permissions issue.
--  1.23 6.3.0     PH   28-APR-2011  Added in update clause so the created
--                                   by/date gets set correctly (in relation
--                                   to above change)
--  1.24 6.3.0     MM   06-MAY-2011  Amended Tenancy reference to use
--                                   Tenancy Alt Ref
--  2.0  6.9.0     AJ   20-OCT-2013  Updated for 6.9.0 and WA loading of Other
--                                   Fields against property elements and
--                                   admin unit elements
--                                   Removed disable trigger statements as no
--                                   triggers or constraints are to be disabled
--                                   in std dataloads as this introduces
--                                   restrictions on who can run dataload
--                                   processes.
--  2.1  6.9.0     PJD  15-NOV-2013  Added in clause "AND pva_pdu_display_seqno
--                                                    = l_display_seqno
--                                   in update of Parameter Values Created_Date
--                                   Reinstated comments from Apr/May 2011 above
--  2.2  6.9.0     PJD  12-DEC-2013  Added in additonal validation for
--                                   duplicate records - hdl673
--
--  2.3  6.9.0     PJD  07-JAN-2014  Added in TENANCIES2 choice for
--                                   tcy_alt_ref
--  2.4  6.9.0     AJ   22-JAN-2014  Amended Create/Validate/Delete took
--                                   TENANCIES2 additions and put them in their
--                                   own section and  added new variable l_obj2
--                                   so the checks are done
--                                   against the TENANCIES table.
--
--  2.5  6.9.0     AJ   12-FEB-2014  Amended c_get_MUL_pro_ele_refno in
--                                   validation where location
--                                   and attribute wrong way around
--
--  2.6  6.9.0     PJD  18-FEB-2014  Amended Validation and Create Processes
--                                   to improve way that ARREARS ACTION
--                                   related Other Fields are processed.
--                                   Also update Delete process for ARREARS
--                                   ACTIONS to ensure it deleted correctly.
--                                   Improved logic in acho loops to prevent
--                                   them running unnecessarily
--
--  2.7  6.9.0     AJ   24-FEB-2014  Amended check c_chk_ele_exists line 2666 amended
--                                   from %NOTFOUND TO is null
--
--  2.8  6.9.0     AJ   25-FEB-2014  Correct IS NULL check to check against correct variable
--                                   l_ele_code_exists line 2666 previously incorrectly against
--                                   the cursor name
--
--  2.9  6.9.0.1   AJ   12-MAR-2014  Added Other Fields values data load for ORGANISATIONS available from
--                                   6.9.0.1 (v6 only) LEGACY_REF for LPVA_ )Other Field Values) Dataload 
--                                   tables amended from VARCHAR2(30) TO VARCHAR2(60) to accommodate the 
--                                   PAR_ORG_NAME in the parties table.
--
--  2.9  6.9.0     AJ   31-MAR-2014  Added Other Fields values data load for REVENUE_ACCOUNTS available from
--                                   6.9.0  (v6 only).
--  3.0  6.9.0.1   AJ   15-DEC-2014  Amended get reusable ref for contractors and contractor site was referencing 
--                                   cursor to get for parties incorrectly lines 878/885 2391/403 and 3811/18
--  4.0  6.10/6.11 AJ   05-MAR-2015  Changed dataload Area from FSC to MAD(Multi Area Dataload)
--  4.1  6.10/6.11 AJ   01-JUN-2015  Amended insert/validate for ADVICE_CASE_HOUSING_OPTIONS not nullable fields
--                                   of LEBE_GENERATED_IND and LEBE_MANUAL_CREATED_IND added to the table
--                                   LETTINGS_BENCHMARKS (v6.10) which this dataload inserts a record into.
--  4.2  6.13      MJK  09-NOV-2015  Allow loading of other field values related to land titles
--  4.3  6.13      MJK  24-NOV-2015  c_chk_ele_code was not being closed.  Is now.
--  4.4  6.13      AJ   26-APR-2016  DELIVERABLES section updated does not get reusable refno if a property (P)
--  4.5  6.13      AJ   22-JUN-2016  1) Checked create and validate and appears to allow CODED and non YN fields (TEXT)
--                                   so no changes made to allow these
--                                   2) Added data type check against char_value the date_value and numeric_value fields
--                                   for those that do not have individual sections and link via reusable refno pdf
--                                   name and object table name
--                                   3)Some reformatting of code done for ease of reading
--  4.5  6.13      AJ   23-JUN-2016  1) Further updated data type check for specific ones that use PGP reference as
--                                   link rather than object table name and reusable reference
--                                   2) TENANCIES2 validate updated against data type and duplicates
--  4.6  6.13      AJ   10-AUG-2016  Added Other Fields for Organisation Contacts
--  4.7  6.13      AJ   05-OCT-2016  Added Other Fields for Organisation Contacts UPDATED Validate added Created
--                                   and delete
--  4.8  6.13      MJK  19-APR-2017  Change to cursor which finds task version to use unique key
--  4.9  6.15      AJ   21-JUN-2017  Changes to allow for ORGTYPE option against Organisations parameter HEM_ORG_OTHERFLDS
--                                   if set to "TABLE" then as per 6.10 not used if set to ORGTYPE then used
--  5.0  6.15      AJ   25-JUN-2017  Amended legacy ref for ORGANISATION_CONTACTS around par_refno option
--                                                                        
-- ***********************************************************************
--
--  declare package variables AND constants
--
--
  PROCEDURE set_record_status_flag
    (p_rowid  IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_mad_other_field_values
    SET    lpva_dl_load_status = p_status
    WHERE  rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS
  THEN
    dbms_output.put_line('Error updating status of dl_mad_other_field_values');
    RAISE;
  END set_record_status_flag;
  --
  -- ****************************************************************************
  --
  PROCEDURE dataload_create
    (p_batch_id          IN VARCHAR2
    ,p_date              IN DATE
    )
  AS
  --*******************
  CURSOR c_count_acho
  IS
    SELECT COUNT(*)
    FROM   dl_mad_other_field_values
    WHERE  lpva_dlb_batch_id = p_batch_id
    AND    lpva_dl_load_status = 'V'
    AND    lpva_pdu_pob_table_name = 'ADVICE_CASE_HOUSING_OPTIONS';
  --*******************  
  CURSOR pre_process_acho
  IS
    SELECT DISTINCT
           lpva_legacy_ref
    ,      lpva_pdu_pob_table_name
    ,      lpva_bm_grp_seq
    FROM   dl_mad_other_field_values
    WHERE  lpva_dlb_batch_id = p_batch_id
    AND    lpva_dl_load_status = 'V'
    AND    lpva_pdu_pob_table_name = 'ADVICE_CASE_HOUSING_OPTIONS'
    AND    lpva_lebe_refno IS NULL
    AND    lpva_lebe_reusable_refno IS NULL;
  --******************* 
  CURSOR post_process_acho
  IS
    SELECT DISTINCT
          lpva_legacy_ref
    ,      lpva_pdu_pob_table_name
    ,      lpva_desc
    ,      lpva_created_by
    ,      lpva_created_date
    ,      lpva_bm_grp_seq
    ,      lpva_lebe_refno
    ,      lpva_lebe_reusable_refno
    ,      lpva_further_ref
    ,      lpva_further_ref2
    FROM   dl_mad_other_field_values
    WHERE  lpva_dlb_batch_id = p_batch_id
    AND    lpva_dl_load_status = 'C'
    AND    lpva_pdu_pob_table_name = 'ADVICE_CASE_HOUSING_OPTIONS';
  --*******************  
  CURSOR c1
  IS
    SELECT rowid rec_rowid
    ,      lpva_dlb_batch_id
    ,      lpva_dl_seqno
    ,      lpva_dl_load_status
    ,      lpva_legacy_ref
    ,      lpva_pdf_name
    ,      lpva_created_by
    ,      lpva_created_date
    ,      lpva_date_value
    ,      lpva_number_value
    ,      lpva_char_value
    ,      lpva_secondary_ref
    ,      lpva_secondary_date
    ,      lpva_pdu_pob_table_name
    ,      lpva_further_ref
    ,      lpva_hrv_loc_code
    ,      lpva_further_ref2
    ,      lpva_further_ref3
    ,      lpva_desc
    ,      lpva_bm_grp_seq
    ,      lpva_lebe_refno
    ,      lpva_lebe_reusable_refno
    ,      lpva_pdu_pgp_refno
    ,      lpva_pdu_table_name
    ,      lpva_pdu_display_seqno
    FROM   dl_mad_other_field_values
    WHERE  lpva_dlb_batch_id   = p_batch_id
    AND    lpva_dl_load_status = 'V';
  --*******************
  CURSOR c_get_aun_refno
    (p_aun_ref VARCHAR2)
  IS
    SELECT aun_reusable_refno
    FROM   admin_units
    WHERE  p_aun_ref = aun_code;
  --*******************
  CURSOR c_get_aca_refno
    (p_pay_ref  VARCHAR2
    ,p_ara_code VARCHAR2
    ,p_date     DATE
    )
  IS
    SELECT aca_reusable_refno
    FROM   account_arrears_actions a
    ,      revenue_accounts        r
    WHERE  p_pay_ref = r.rac_pay_ref
    AND    a.aca_rac_accno = r.rac_accno
    AND    p_ara_code = a.aca_ara_code
    AND    p_date = trunc(a.aca_created_date);
  --*******************
  CURSOR c_get_con_refno
    (p_con_ref VARCHAR2)
  IS
    SELECT con_reusable_refno
    FROM   contractors
    WHERE  con_code = p_con_ref;
  --*******************
  CURSOR c_get_cos_refno
    (p_cos_ref VARCHAR2)
  IS
    SELECT cos_reusable_refno
    FROM   contractor_sites
    WHERE  cos_code = p_cos_ref;
  --*******************
  CURSOR c_get_par_refno
    (p_per_alt_ref VARCHAR2)
  IS
    SELECT par_reusable_refno
    FROM   parties
    WHERE  par_per_alt_ref = p_per_alt_ref;
  --*******************
  CURSOR c_get_ltl_refno
    (p_reference VARCHAR2)
  IS
    SELECT ltl_reusable_refno
    FROM   land_titles
    WHERE  ltl_reference = p_reference;
  --*******************
  CURSOR c_get_pro_refno
    (p_pro_propref VARCHAR2)
  IS
    SELECT pro_reusable_refno
    FROM   properties
    WHERE  pro_propref = p_pro_propref;
  --*******************
  CURSOR c_get_srq_refno
    (p_srq_ref VARCHAR2) 
  IS
    SELECT srq_reusable_refno
    FROM   service_requests
    WHERE  srq_legacy_refno = p_srq_ref;
  --*******************
  CURSOR c_get_tcy_refno
   (p_tcy_ref VARCHAR2)
  IS
    SELECT tcy_reusable_refno
    FROM   tenancies
    WHERE  tcy_refno = TO_NUMBER(p_tcy_ref);
  --*******************
  CURSOR c_get_tcy_refno2
    (p_tcy_alt_ref VARCHAR2)
  IS
    SELECT tcy_reusable_refno
    FROM   tenancies
    WHERE  tcy_alt_ref = p_tcy_alt_ref;
  --*******************
  CURSOR c_get_wor_refno
    (p_wor_ref VARCHAR2)
  IS
    SELECT wor_reusable_refno
    FROM   works_orders
    WHERE  wor_legacy_ref = p_wor_ref;
  --*******************
  CURSOR c_get_srt_refno
    (p_srt_sud_pro_aun_code VARCHAR2
    ,p_srt_ele_code         VARCHAR2
    ,p_scs_reference        VARCHAR2
    )
  IS
    SELECT srt_reusable_refno
    FROM   survey_results
    ,      stock_condition_surveys
    WHERE  srt_sud_pro_aun_code = p_srt_sud_pro_aun_code
    AND    srt_ele_code = p_srt_ele_code
    AND    scs_reference = p_scs_reference
    AND    scs_refno = srt_sud_scs_refno;
  --*******************
  CURSOR c_obj
    (p_obj        VARCHAR2
    ,p_table_name VARCHAR2
    )
  IS
    SELECT pdu_pob_table_name
    ,      pdu_pgp_refno
    ,      pdu_display_seqno
    ,      pdu_pdf_param_type
    FROM   parameter_definition_usages
    WHERE  pdu_pdf_name = p_obj
    AND    pdu_pob_table_name = p_table_name;
  --*******************
  CURSOR c_pro_refno
    (p_propref VARCHAR2)
  IS
    SELECT pro_refno
    FROM   properties
    WHERE  pro_propref = p_propref;
  --*******************
  CURSOR c_get_cve_refno
    (p_cnt_reference VARCHAR2
    ,p_version_no    VARCHAR2
    )
  IS
    SELECT cve_reusable_refno
    FROM   contract_versions
    WHERE  cve_cnt_reference = p_cnt_reference
    AND    cve_version_number = p_version_no;
  --*******************
  CURSOR c_get_tve_refno 
    (cp_tkg_src_reference  VARCHAR2
    ,cp_tkg_code           VARCHAR2
    ,cp_tkg_src_type       VARCHAR2
    ,cp_tsk_alt_reference  VARCHAR2
    ,cp_version_number     NUMBER
    )
  IS
    SELECT tve_reusable_refno
    FROM   task_versions
    WHERE  tve_tsk_tkg_src_reference = cp_tkg_src_reference
    AND    tve_tsk_tkg_code = cp_tkg_code
    AND    tve_tsk_tkg_src_type = cp_tkg_src_type
    AND    tve_tsk_id = 
             (SELECT tsk_id 
              FROM   tasks 
              WHERE  tsk_alt_reference = cp_tsk_alt_reference
             )
    AND    tve_version_number = cp_version_number;
  --*******************
  CURSOR c_get_ara
    (p_obj      VARCHAR2
    ,p_ara_code VARCHAR2
    )
  IS
    SELECT pdu_pgp_refno
    ,      pdu_pdf_param_type
    ,      pdu_display_Seqno
    FROM   parameter_definition_usages
    ,      arrears_actions
    WHERE  pdu_pdf_name = p_obj
    AND    pdu_pgp_refno = ara_pgp_refno
    AND    ara_code = p_ara_code;
  --*******************
  CURSOR c_get_dve_refno
    (p_cnt_reference VARCHAR2
    ,p_pro_aun_code  VARCHAR2
    ,p_pro_aun_ind   VARCHAR2
    ,p_display_seqno VARCHAR2
    )
  IS
    SELECT dve_reusable_refno
    FROM   deliverables
    ,      deliverable_versions
    WHERE  dlv_refno = dve_dlv_refno
    AND    dlv_cnt_reference = p_cnt_reference
    AND    dlv_cad_pro_aun_code = p_pro_aun_code
    AND    dlv_cad_type_ind = p_pro_aun_ind
    AND    dve_display_sequence = p_display_seqno
    AND    dve_current_ind = 'Y';
  --*******************
  CURSOR c_get_ipp_refno
    (p_ipp_shortname VARCHAR2
    ,p_ipp_ipt_code  VARCHAR2
    )
  IS
    SELECT ipp_reusable_refno
    FROM   interested_parties
    WHERE  ipp_shortname = p_ipp_shortname
    AND    ipp_ipt_code  = p_ipp_ipt_code;
  --*******************
  CURSOR c_get_ipt_pgp_refno
    (p_ipt_code  VARCHAR2)
  IS
    SELECT ipt_pgp_refno
    FROM   interested_party_types
    WHERE  ipt_code = p_ipt_code;
  --*******************
  CURSOR c_get_hoop_pgp_refno(p_hoop_code  VARCHAR2)
  IS
    SELECT hoop_pgp_refno
    FROM   housing_options
    WHERE  hoop_code = p_hoop_code;
  --*******************
  CURSOR c_get_ppyt_pgp_refno
    (p_ppyt_alternative_reference  VARCHAR2)
  IS
    SELECT b.pptp_pgp_refno
    FROM   prevention_payments      a
    ,      prevention_payment_types b
    WHERE  b.pptp_code = a.ppyt_pptp_code
    AND    a.ppyt_alternative_reference = p_ppyt_alternative_reference;
  --*******************
  CURSOR c_get_aet_pgp_refno
    (p_aet_code  VARCHAR2)
  IS
    SELECT aet_pgp_refno
    FROM   action_event_types
    WHERE  aet_code = p_aet_code;
  --*******************
  CURSOR c_get_bro_pgp_refno
    (p_bro_code  VARCHAR2)
  IS
    SELECT bro_pgp_action_refno
    FROM   business_reasons
    WHERE  bro_code = p_bro_code;
  --*******************
  CURSOR c_get_ban_bro_code
    (p_ban_reference VARCHAR2)
  IS
    SELECT ban_bro_code
    FROM   business_actions
    WHERE  ban_reference = p_ban_reference;
  --*******************
  CURSOR c_get_pdu_details
    (p_pdf_name  VARCHAR2
    ,p_pgp_refno NUMBER
    )
  IS
    SELECT pdu_display_seqno
    ,      pdu_pdf_param_type
    FROM   parameter_definition_usages
    WHERE  pdu_pdf_name = p_pdf_name
    AND    pdu_pgp_refno = p_pgp_refno;
  --*******************
  CURSOR c_get_ban_refno
    (p_ban_reference VARCHAR2)
  IS
    SELECT ban_reusable_refno
    FROM   business_actions
    WHERE  ban_reference = p_ban_reference;
  --*******************
  CURSOR c_get_real_refno
    (p_real_reference VARCHAR2)
  IS
    SELECT real_reuseable_refno
    FROM   registered_address_lettings
    WHERE  real_reference = p_real_reference;
  --*******************
  CURSOR c_get_bae_refno
    (p_bae_ban_reference    VARCHAR2
    ,p_bae_aet_code         VARCHAR2
    ,p_bae_status_date      DATE
    ,p_bae_seqno            NUMBER
    )
  IS
    SELECT bae_reusable_refno
    FROM   business_action_events
    WHERE  bae_ban_reference = p_bae_ban_reference
    AND    bae_aet_code = p_bae_aet_code
    AND    bae_status_date = p_bae_status_date
    AND    bae_sequence = p_bae_seqno;
  --*******************
  CURSOR c_get_acho_reference(p_acho_reference VARCHAR2)
  IS
    SELECT acho_reference
    FROM   advice_case_housing_options
    WHERE  acho_alternative_reference = p_acho_reference;
  --*******************
  CURSOR c_get_ppyt_refno
    (p_ppyt_alternative_reference VARCHAR2)
  IS
    SELECT ppyt_reusable_refno
    FROM prevention_payments
    WHERE ppyt_alternative_reference = p_ppyt_alternative_reference;
  --*******************
  CURSOR c_get_reusable_refno
  IS
    SELECT reusable_refno_seq.nextval
    FROM   dual;
  --*******************
  CURSOR c_get_lebe_refno
  IS
    SELECT lebe_refno_seq.nextval
    FROM   dual;
  --*******************
  CURSOR c_get_real_pgp_refno(p_real_reference  VARCHAR2)
  IS
    SELECT hoop_pgp_refno
    FROM housing_options
    ,    advice_case_housing_options
    ,    registered_address_lettings
    WHERE hoop_code = acho_hoop_code
    AND acho_reference = real_acho_reference
    AND real_reference = p_real_reference;
  --*******************
  CURSOR c_get_ele_pgp_refno(p_ele_code VARCHAR2)
  IS
    SELECT ele_pgp_refno
    ,      ele_value_type
    FROM   elements
    WHERE  ele_code = p_ele_code
    AND    ele_type = 'PR';
  --*******************
  CURSOR c_get_ele_pdu_details
    (p_pdf_name  VARCHAR2
    ,p_pgp_refno NUMBER
    )
  IS
    SELECT pdu_display_seqno
    ,      pdu_pdf_param_type
    FROM   parameter_definition_usages
    WHERE  pdu_pdf_name = p_pdf_name
    AND    pdu_pgp_refno = p_pgp_refno
    AND    pdu_pob_table_name = 'NULL'
    AND    pdu_pdf_param_type = 'OTHER FIELDS';
  --
  --*******************
  --
  -- Multi-Value Elements Included 12Feb2014
  --
  CURSOR c_get_M_pro_ele_refno
    (p_pro_refno          NUMBER
    ,p_ele_code           VARCHAR2
    ,p_start_date         DATE
    ,p_att_code           VARCHAR2
    ,p_hrv_loc_code       VARCHAR2
    ,p_fat_code           VARCHAR2
    ,p_ele_value_type     VARCHAR2
    )
  IS
    SELECT pel_reusable_refno
    FROM   property_elements
    WHERE  pel_pro_refno = p_pro_refno
    AND    pel_ele_code = p_ele_code
    AND    pel_start_date = p_start_date
    AND    pel_att_code = p_att_code
    AND    NVL(pel_hrv_elo_code,'NUL') = NVL(p_hrv_loc_code,'NUL')
    AND    pel_fat_code = NVL(p_fat_code,'NUL')
    AND    pel_type_ind = p_ele_value_type;
  --
  --*******************
  -- Multi-Value Elements Included 12Feb2014
  --
  CURSOR c_get_M_aue_ele_refno 
    (p_aun_code           VARCHAR2                           
    ,p_ele_code           VARCHAR2                           
    ,p_start_date         DATE                           
    ,p_att_code           VARCHAR2                           
    ,p_hrv_loc_code       VARCHAR2                           
    ,p_fat_code           VARCHAR2                           
    ,p_ele_value_type     VARCHAR2
    )
  IS
   SELECT aue_reusable_refno
   FROM   admin_unit_elements
   WHERE  aue_aun_code = p_aun_code
   AND    aue_ele_code = p_ele_code
   AND    aue_start_date = p_start_date
   AND    aue_att_code = p_att_code
   AND    NVL(aue_hrv_elo_code,'NUL') = NVL(p_hrv_loc_code,'NUL')
   AND    aue_fat_code = NVL(p_fat_code,'NUL')
   AND    aue_type_ind = p_ele_value_type;
  --
  --*******************
  -- Non Multi-Value Elements
  --
  CURSOR c_get_CND_aue_ele_refno
    (p_aun_code           VARCHAR2
    ,p_ele_code           VARCHAR2
    ,p_start_date         DATE
    ,p_ele_value_type     VARCHAR2
    )
  IS
    SELECT aue_reusable_refno
    FROM   admin_unit_elements
    WHERE  aue_aun_code = p_aun_code
    AND    aue_ele_code = p_ele_code
    AND    aue_start_date = p_start_date
    AND    aue_type_ind = p_ele_value_type;
  --
  --*******************
  -- Non Multi-Value Elements
  --
  CURSOR c_get_CND_pro_ele_refno
    (p_pro_refno          NUMBER
    ,p_ele_code           VARCHAR2
    ,p_start_date         DATE
    ,p_ele_value_type     VARCHAR2)
  IS
    SELECT pel_reusable_refno
    FROM   property_elements
    WHERE  pel_pro_refno = p_pro_refno
    AND    pel_ele_code = p_ele_code
    AND    pel_start_date = p_start_date
    AND    pel_type_ind = p_ele_value_type;
  --*******************
  CURSOR c_get_prop_pro_refno
    (p_pro_propref VARCHAR2)
  IS
    SELECT pro_refno
    FROM   properties
    WHERE  pro_propref = p_pro_propref;
  --*******************
  CURSOR c_chk_org_refno
    (p_org_name       VARCHAR2
    ,p_org_short_name VARCHAR2
    ,p_org_type       VARCHAR2
    )
  IS
    SELECT count(distinct par_refno)
    FROM   parties
    WHERE  par_org_name = p_org_name
    AND    par_org_short_name = p_org_short_name
    AND    NVL(par_org_frv_oty_code,'NUL') = NVL(p_org_type,'NUL');
  --*******************
  CURSOR c_get_par2_refno
    (p_org_name       VARCHAR2
    ,p_org_short_name VARCHAR2
    ,p_org_type       VARCHAR2
    )
  IS
    SELECT par_reusable_refno
    FROM   parties
    WHERE  par_org_name = p_org_name
    AND    par_org_short_name = p_org_short_name
    AND    NVL(par_org_frv_oty_code,'NUL') = NVL(p_org_type,'NUL');
  --*******************
  CURSOR c_get_ofvat_pgp_refno
    (p_ofat_code VARCHAR2)
  IS
    SELECT ofat_pgp_refno
    FROM   other_fields_account_types
    WHERE  ofat_code = p_ofat_code;
  --*******************
  CURSOR c_get_rac_pdu_details
    (p_pdf_name  VARCHAR2
    ,p_pgp_refno NUMBER
    )
  IS
    SELECT pdu_display_seqno
    ,      pdu_pdf_param_type
    FROM   parameter_definition_usages
    WHERE  pdu_pdf_name = p_pdf_name
    AND    pdu_pgp_refno = p_pgp_refno
    AND    pdu_pob_table_name = 'NULL'
    AND    pdu_pdf_param_type = 'OTHER FIELDS';
  --*******************
  CURSOR c_get_rac_refno
    (p_pay_ref      VARCHAR2
    ,p_hrv_ate_code VARCHAR2
    )
  IS
    SELECT rac_reusable_refno
    FROM   revenue_accounts
    WHERE  rac_pay_ref = p_pay_ref
    AND    rac_hrv_ate_code = p_hrv_ate_code;
  --
  --******************* 
  CURSOR c_get_org_refno
    (p_par_refno VARCHAR2)
  IS
    SELECT par_refno
    FROM   parties
    WHERE  par_refno = to_number(p_par_refno)
        AND    par_type  = 'ORG';
  --*******************  
  CURSOR c_get_org_alt_refno
    (p_par_alt_refno VARCHAR2)
  IS
    SELECT par_refno
    FROM   parties
    WHERE  par_per_alt_ref = p_par_alt_refno
        AND    par_type  = 'ORG';
  --*******************  
  CURSOR c_get_oco_refno
    (p_par_refno NUMBER
    ,p_forename  VARCHAR2
    ,p_surname   VARCHAR2
    ,p_date      DATE)
  IS
    SELECT oco_reusable_refno
    FROM   organisation_contacts
    WHERE  oco_par_refno = p_par_refno
        AND    oco_forename = p_forename
        AND    oco_surname = p_surname
    AND    oco_start_date = p_date;     
  --*******************
  CURSOR c_get_orgtype_param 
    (p_pdf_name        VARCHAR2) 
  IS
    SELECT pva_char_value
    FROM   parameter_values
    ,      parameter_definition_usages
    WHERE  pdu_pdf_param_type = 'SYSTEM'
    AND    pva_pdu_pdf_name = pdu_pdf_name
    AND    pva_pdu_pdf_param_type = pdu_pdf_param_type
    AND    pva_pdu_pob_table_name = 'SYSTEM'
    AND    pva_pdu_pgp_refno = pdu_pgp_refno
    AND    pva_pdu_display_seqno  = pdu_display_seqno
    AND    pdu_pdf_name = p_pdf_name;
  --******************* 
  CURSOR c_get_orgtype_refno
    (p_par_refno VARCHAR2)
  IS
    SELECT par_reusable_refno, par_org_frv_oty_code
    FROM   parties
    WHERE  par_refno = to_number(p_par_refno)
    AND    par_type  = 'ORG';
  --******************* 
  CURSOR c_get_orgtype_alt_refno
    (p_par_alt_refno VARCHAR2)
  IS
    SELECT par_reusable_refno, par_org_frv_oty_code
    FROM   parties
    WHERE  par_per_alt_ref = p_par_alt_refno
    AND    par_type  = 'ORG';
  --*******************
  CURSOR c_obj_orgtype
    (p_obj        VARCHAR2
    ,p_orgtype    VARCHAR2
    )
  IS
    SELECT pdu_pob_table_name
    ,      pdu_pgp_refno
    ,      pdu_display_seqno
    ,      pdu_pdf_param_type
    FROM   parameter_definition_usages
	,      organisation_type_param_groups
    WHERE  pdu_pdf_name = p_obj
    AND    pdu_pgp_refno = otpg_pgp_refno
	AND    otpg_frv_oty_code = p_orgtype;
  --*******************  
  -- Constants for process_summary
  --
  cb              VARCHAR2(30);
  cd              DATE;
  cp              VARCHAR2(30) := 'CREATE';
  ct              VARCHAR2(30) := 'DL_MAD_OTHER_FIELD_VALUES';
  cs              PLS_INTEGER;
  ce              VARCHAR2(200);
  l_id            ROWID;
  l_an_tab        VARCHAR2(1);
  --
  --*******************
  -- Other variables
  --
  l_reusable_refno        PLS_INTEGER;
  i                       PLS_INTEGER := 0;
  l_obj                   VARCHAR2(30);
  l_obj2                  VARCHAR2(30);
  l_pgp_refno             PLS_INTEGER;
  l_ara_pgp_refno         PLS_INTEGER;
  l_display_seqno         NUMBER;
  l_param_type            VARCHAR2(20);
  l_pro_aun_code          VARCHAR2(20);
  l_count_acho            PLS_INTEGER;
  l_ipt_pgp_refno         PLS_INTEGER;
  l_hoop_pgp_refno        PLS_INTEGER;
  l_ppyt_pgp_refno        PLS_INTEGER;
  l_real_pgp_refno        PLS_INTEGER;
  l_aet_pgp_refno         PLS_INTEGER;
  l_bro_pgp_refno         PLS_INTEGER;
  l_ofvat_pgp_refno       PLS_INTEGER;
  l_ban_bro_code          VARCHAR2(10);
  l_lebe_refno            NUMBER(10);
  l_lebe_reusable_refno   NUMBER(10);
  l_acho_reference        NUMBER(10);
  l_pel_pgp_refno         PLS_INTEGER;
  l_auel_pgp_refno        PLS_INTEGER;
  l_ele_pro_refno         NUMBER(10);
  l_ele_value_type        VARCHAR2(1);
  l_chk_par_refno         NUMBER(8);
  l_chk_par_alt_refno     NUMBER(8);
  l_lebe_generated_ind       VARCHAR2(1);
  l_lebe_manual_created_ind  VARCHAR2(1);
  l_orgtype_param         VARCHAR2(10);
  l_orgtype               VARCHAR2(10);
  --
  --*******************
  --
  BEGIN
    fsc_utils.proc_start('s_dl_mad_other_field_values.dataload_create');
    fsc_utils.debug_message('s_dl_mad_other_field_values.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;
    --
    -- Get HEM_ORG_OTHERFLDS parameter
    l_orgtype_param   := NULL;
    OPEN  c_get_orgtype_param('HEM_ORG_OTHERFLDS');
    FETCH c_get_orgtype_param INTO l_orgtype_param;
    CLOSE c_get_orgtype_param;
    --	
    OPEN  c_count_acho;
    FETCH c_count_acho INTO l_count_acho;
    CLOSE c_count_acho;
    --
    --*******************
    -- Pre process the advice case housing options records before they are processed
    -- by the main loop. Assign the lebe_refno and lebe_reusable_refno which will
    -- be used to assign to the other field parameter values records.
    --
    IF NVL(l_count_acho,0) != 0
    THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'ACHO PRE');
      FOR pre in pre_process_acho 
      LOOP
        BEGIN
          l_lebe_refno            := NULL;
          l_lebe_reusable_refno   := NULL;
                  --
          OPEN  c_get_lebe_refno;
          FETCH c_get_lebe_refno INTO l_lebe_refno;
          CLOSE c_get_lebe_refno;
                  --
          OPEN  c_get_reusable_refno;
          FETCH c_get_reusable_refno INTO l_lebe_reusable_refno;
          CLOSE c_get_reusable_refno;
                  --
          UPDATE dl_mad_other_field_values
          SET    lpva_lebe_refno = l_lebe_refno
          ,      lpva_lebe_reusable_refno = l_lebe_reusable_refno
          WHERE  lpva_legacy_ref = pre.lpva_legacy_ref
          AND    lpva_pdu_pob_table_name = pre.lpva_pdu_pob_table_name
          AND    lpva_bm_grp_seq = pre.lpva_bm_grp_seq
          AND    lpva_lebe_refno IS NULL
          AND    lpva_lebe_reusable_refno IS NULL;
                  --
        END;
      END LOOP; -- pre_process_acho
    END IF;
    --
    --*******************
    --
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 in c1 
    LOOP
        --
      BEGIN
          --
        cs := p1.lpva_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        --
        -- get the object and param def information
        --
        l_obj := NULL;
        l_obj2 := NULL;
        l_pgp_refno := NULL;
        l_ara_pgp_refno := NULL;
        l_display_seqno := NULL;
        l_param_type := NULL;
        l_pro_aun_code := NULL;
        l_ipt_pgp_refno := NULL;
        l_hoop_pgp_refno := NULL;
        l_ppyt_pgp_refno := NULL;
        l_aet_pgp_refno := NULL;
        l_bro_pgp_refno := NULL;
        l_ofvat_pgp_refno := NULL;
        l_ban_bro_code := NULL;
        l_real_pgp_refno := NULL;
        l_pel_pgp_refno := NULL;
        l_auel_pgp_refno := NULL;
        l_ele_pro_refno := NULL;
        l_ele_value_type := NULL;
        l_chk_par_refno := NULL;
        l_chk_par_alt_refno := NULL;
        l_lebe_generated_ind := NULL;
        l_lebe_manual_created_ind := NULL;
        l_reusable_refno := NULL;
        l_orgtype := NULL;
                --
        IF p1.lpva_pdu_pob_table_name NOT IN 
            ('INTERESTED_PARTIES'
            ,'BUSINESS_ACTIONS'
            ,'BUSINESS_ACTION_EVENTS'
            ,'ADVICE_CASE_HOUSING_OPTIONS'
            ,'PREVENTION_PAYMENTS'
            ,'REGISTERED_ADDRESS_LETTINGS'
            ,'ARREARS_ACTIONS'
            ,'PROPERTY_ELEMENTS'
            ,'ADMIN_UNIT_ELEMENTS'
            ,'TENANCIES2'
            ,'ORGANISATIONS'
            ,'REVENUE_ACCOUNTS'
            ) 
        THEN
                --
          OPEN c_obj(p1.lpva_pdf_name,p1.lpva_pdu_pob_table_name);
          FETCH c_obj INTO l_obj,l_pgp_refno,l_display_seqno,l_param_type;
          CLOSE c_obj;
                  --
        ELSIF p1.lpva_pdu_pob_table_name = 'ARREARS_ACTIONS'
        THEN
                --
          l_obj := 'ARREARS_ACTIONS';
        END IF;
        --
        --*******************
        -- get the reusable_refno
        --
        l_reusable_refno := NULL;
                --
        IF  l_obj = 'ADMIN_UNITS'
        THEN
                --
          OPEN c_get_aun_refno(p1.lpva_legacy_ref);
          FETCH c_get_aun_refno into l_reusable_refno;
          CLOSE c_get_aun_refno;
                --
        ELSIF l_obj = 'CONTRACTORS'
        THEN
                --
          OPEN  c_get_con_refno(p1.lpva_legacy_ref);
          FETCH c_get_con_refno into l_reusable_refno;
          CLOSE c_get_con_refno;
                --
        ELSIF l_obj = 'CONTRACTOR_SITES'
        THEN
                --
          OPEN  c_get_cos_refno(p1.lpva_legacy_ref);
          FETCH c_get_cos_refno into l_reusable_refno;
          CLOSE c_get_cos_refno;
                --
        ELSIF l_obj = 'PARTIES'
        THEN
                --
          OPEN  c_get_par_refno(p1.lpva_legacy_ref);
          FETCH c_get_par_refno into l_reusable_refno;
          CLOSE c_get_par_refno;
                --
        ELSIF l_obj = 'LAND_TITLES'
        THEN
                --
          OPEN  c_get_ltl_refno(p1.lpva_legacy_ref);
          FETCH c_get_ltl_refno into l_reusable_refno;
          CLOSE c_get_ltl_refno;
                --
        ELSIF l_obj = 'PROPERTIES'
        THEN
                --
          OPEN  c_get_pro_refno(p1.lpva_legacy_ref);
          FETCH c_get_pro_refno into l_reusable_refno;
          CLOSE c_get_pro_refno;
                --
        ELSIF l_obj = 'SERVICE_REQUESTS'
        THEN
                --
          OPEN  c_get_srq_refno(p1.lpva_legacy_ref);
          FETCH c_get_srq_refno into l_reusable_refno;
          CLOSE c_get_srq_refno;
                --
        ELSIF l_obj = 'TENANCIES'
        THEN
                --
          OPEN  c_get_tcy_refno(p1.lpva_legacy_ref);
          FETCH c_get_tcy_refno into l_reusable_refno;
          CLOSE c_get_tcy_refno;
                --
        ELSIF l_obj = 'WORKS_ORDERS'
        THEN
                --
          OPEN  c_get_wor_refno(p1.lpva_legacy_ref);
          FETCH c_get_wor_refno into l_reusable_refno;
          CLOSE c_get_wor_refno;
                --
        ELSIF l_obj = 'SURVEY_RESULTS'
        THEN
                --
          OPEN  c_get_srt_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref,p1.lpva_further_ref);
          FETCH c_get_srt_refno into l_reusable_refno;
          CLOSE c_get_srt_refno;
                --
        ELSIF l_obj = 'CONTRACT_VERSIONS'
        THEN
                --
          OPEN  c_get_cve_refno(p1.lpva_legacy_ref, p1.lpva_secondary_ref);
          FETCH c_get_cve_refno into l_reusable_refno;
          CLOSE c_get_cve_refno;
                --
        ELSIF l_obj = 'CONTRACT_TASKS'
        THEN
        --
          OPEN  c_get_tve_refno(p1.lpva_legacy_ref
                               ,p1.lpva_secondary_ref
                               ,p1.lpva_further_ref
                               ,p1.lpva_further_ref2
                               ,p1.lpva_further_ref3
                               );
          FETCH c_get_tve_refno into l_reusable_refno;
          CLOSE c_get_tve_refno;
                --
        ELSIF l_obj = 'DELIVERABLES'
        THEN
        --
        -- Get the pro_refno or use Admin Unit Code supplied
        -- Updated end if missing off initial check (AJ)
        -- 
          IF p1.lpva_further_ref = 'P'
          THEN
            l_pro_aun_code := s_properties.get_refno_for_propref(p1.lpva_secondary_ref);
          ELSE
            l_pro_aun_code := p1.lpva_secondary_ref;
          END IF;
        --
          OPEN  c_get_dve_refno(p1.lpva_legacy_ref
                               ,l_pro_aun_code
                               ,p1.lpva_further_ref
                               ,p1.lpva_further_ref2
                               );
          FETCH c_get_dve_refno into l_reusable_refno;
          CLOSE c_get_dve_refno;
        --
        ELSIF l_obj = 'ORGANISATION_CONTACTS' 
        THEN
        --
        -- Get the Org reference using legacy ref then link to Org Contact
        --
          IF UPPER(NVL(p1.lpva_secondary_ref,'BLANK')) = 'REFNO'
           THEN
            OPEN  c_get_org_refno(p1.lpva_legacy_ref);
            FETCH c_get_org_refno into l_chk_par_refno;
            CLOSE c_get_org_refno;
          ELSE
            OPEN c_get_org_alt_refno(p1.lpva_legacy_ref);
            FETCH c_get_org_alt_refno into l_chk_par_alt_refno;
            CLOSE c_get_org_alt_refno;
          END IF;
        --
        --
          OPEN c_get_oco_refno(nvl(l_chk_par_refno,l_chk_par_alt_refno)
                              ,p1.lpva_further_ref2
                              ,p1.lpva_further_ref3
                              ,p1.lpva_secondary_date);
          FETCH c_get_oco_refno into l_reusable_refno;
          CLOSE c_get_oco_refno;
        --
        -- PUT REUSABLE REF INTO DL TABLE lpva_lebe_reusable_refno TO BE USED TO DELETE
        --
          l_lebe_refno := NULL;
          l_lebe_reusable_refno := NULL;
          l_lebe_reusable_refno := l_reusable_refno;
                --
          UPDATE dl_mad_other_field_values
          SET    lpva_lebe_reusable_refno = l_lebe_reusable_refno
          WHERE  lpva_dlb_batch_id = p1.lpva_dlb_batch_id
          AND    lpva_dl_seqno = p1.lpva_dl_seqno
          AND    lpva_legacy_ref = p1.lpva_legacy_ref
          AND    lpva_pdu_pob_table_name = p1.lpva_pdu_pob_table_name;            
        --
        END IF;
        --              
                --*****************************************
                --
        -- This has been added for Other Fields against Revenue Accounts(v6.9.0)
        -- and is for the APEX v6 front end only
        --
        -- For REVENUE_ACCOUNTS
        -- 1) we have to get the pgp_refno assigned to the OFAT_CODE(Account Type Code)
        --    in OTHER_FIELDS_ACCOUNT_TYPES table populated at v6.9.0 install
        -- 2) The other fields will be held at account type level
        -- 3) put reusable ref into dl_mad_other_field_values (l_lebe_reusable_refno)
        --    so it can be used for DELETE
        --
        IF p1.lpva_pdu_pob_table_name = 'REVENUE_ACCOUNTS'
        THEN
                --
          OPEN c_get_ofvat_pgp_refno(p1.lpva_secondary_ref);
          FETCH c_get_ofvat_pgp_refno INTO l_ofvat_pgp_refno;
           IF (c_get_ofvat_pgp_refno%FOUND)
            THEN
             l_pgp_refno := l_ofvat_pgp_refno;
             l_obj := 'NULL';
           END IF;
          CLOSE c_get_ofvat_pgp_refno;
                --
          OPEN c_get_rac_pdu_details(p1.lpva_pdf_name,l_ofvat_pgp_refno);
          FETCH c_get_rac_pdu_details INTO l_display_seqno,l_param_type;
          CLOSE c_get_rac_pdu_details;
                --
          OPEN c_get_rac_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref);
          FETCH c_get_rac_refno INTO l_reusable_refno;
          CLOSE c_get_rac_refno;
        --
        -- PUT REUSABLE REF INTO DL TABLE lpva_lebe_reusable_refno TO BE USED TO DELETE
        --
          l_lebe_refno := NULL;
          l_lebe_reusable_refno := NULL;
          l_lebe_reusable_refno := l_reusable_refno;
                --
          UPDATE dl_mad_other_field_values
          SET    lpva_lebe_reusable_refno = l_lebe_reusable_refno
          WHERE  lpva_dlb_batch_id = cb
          AND    lpva_dl_seqno = cs;
        END IF;
        --
        --*****************************************
        -- This has been added for Other Fields against Organisations (v6.9.0.1)
        -- and is for the APEX v6 front end only
        --
        -- For ORGANISATIONS
        -- 1) The Name, Short Name and Organisation Type (optional) to find the organisation 
        --    as the system accepts duplicates so for WA these are for info only and not used
        --    in any checks creates or deletes (see 2 below).
        -- 2) For WA the par_per_alt_ref has been included as the driver and must be supplied
        --    use this only.
        -- 3) put reusable ref into dl_mad_other_field_values (l_lebe_reusable_refno)
        --    so it can be used for DELETE
        -- 4) We now need to check what level these are held against on Organisations as they
		--    need different information HEM_ORG_OTHERFLDS = TABLE (no Change) = ORGTYPE (pgp)
		--    pgp initially from organisation_type_param_groups then pramter_definition_usages
		-- 5) Also allow the use of the par_refno ala ORGANISATION_CONTACTS
		--
        IF p1.lpva_pdu_pob_table_name = 'ORGANISATIONS'
        THEN
		--
        -- Use the par_per_alt_ref or par_refno only as unique (c_get_par_refno 1107 old)
        --
		IF UPPER(NVL(p1.lpva_further_ref,'BLANK')) = 'REFNO'
		 THEN
          OPEN c_get_orgtype_refno(p1.lpva_legacy_ref);
          FETCH c_get_orgtype_refno into l_reusable_refno, l_orgtype;
          CLOSE c_get_orgtype_refno;
        ELSE
          OPEN c_get_orgtype_alt_refno(p1.lpva_legacy_ref);
          FETCH c_get_orgtype_alt_refno into l_reusable_refno, l_orgtype;
          CLOSE c_get_orgtype_alt_refno;
        END IF;
        --
		 IF nvl(l_orgtype_param,'TABLE') = 'TABLE'
		 THEN
          OPEN c_obj(p1.lpva_pdf_name, p1.lpva_pdu_pob_table_name);
          FETCH c_obj INTO l_obj,l_pgp_refno,l_display_seqno,l_param_type;
          CLOSE c_obj;
         END IF;
        --
		 IF nvl(l_orgtype_param,'TABLE') = 'ORGTYPE' AND l_orgtype IS NOT NULL
		 THEN
          OPEN c_obj_orgtype(p1.lpva_pdf_name, l_orgtype);
          FETCH c_obj_orgtype INTO l_obj,l_pgp_refno,l_display_seqno,l_param_type;
          CLOSE c_obj_orgtype;
         END IF;
        --
        -- PUT REUSABLE REF INTO DL TABLE lpva_lebe_reusable_refno TO BE USED TO DELETE
        --
          l_lebe_refno := NULL;
          l_lebe_reusable_refno := NULL;
          l_lebe_reusable_refno := l_reusable_refno;
                --
          UPDATE dl_mad_other_field_values
          SET    lpva_lebe_reusable_refno = l_lebe_reusable_refno
          WHERE  lpva_dlb_batch_id = cb
          AND    lpva_dl_seqno = cs		
          AND    ROWID = p1.rec_rowid;
        END IF;
        --
        --*****************************************
        -- added as Tenancies using the tcy_alt_ref the table name supplied is TENANCIES2
        -- and need to allow for this still checking and loading against TENANCIES table
        --
        IF p1.lpva_pdu_pob_table_name = 'TENANCIES2'
        THEN
                --
          l_obj2 := 'TENANCIES';
                --
          OPEN c_obj(p1.lpva_pdf_name,l_obj2);
          FETCH c_obj INTO l_obj,l_pgp_refno,l_display_seqno,l_param_type;
          CLOSE c_obj;
        --
        -- get the reusable_refno
        --
          l_reusable_refno := NULL;
          OPEN  c_get_tcy_refno2(p1.lpva_legacy_ref);
          FETCH c_get_tcy_refno2 into l_reusable_refno;
          CLOSE c_get_tcy_refno2;
        END IF;
        --
                --*****************************************
        -- The following are new additions to the other fields load.
        -- INTERESTED_PARTIES are held against the individual ipp_shortname therefore
        -- we need to get the ipp_pgp_refno held against the ipt_code
        --
        IF p1.lpva_pdu_pob_table_name = 'INTERESTED_PARTIES'
        THEN
                --
          OPEN c_get_ipt_pgp_refno(p1.lpva_secondary_ref);
          FETCH c_get_ipt_pgp_refno INTO l_ipt_pgp_refno;
           IF c_get_ipt_pgp_refno%FOUND
            THEN
             l_pgp_refno := l_ipt_pgp_refno;
             l_obj := 'NULL';
           END IF;
          CLOSE c_get_ipt_pgp_refno;
        --
        -- Now get parameter definition details for the other field name
        --
          OPEN c_get_pdu_details(p1.lpva_pdf_name, l_ipt_pgp_refno);
          FETCH c_get_pdu_details INTO l_display_seqno,l_param_type;
          CLOSE c_get_pdu_details;
                --
          OPEN c_get_ipp_refno(p1.lpva_legacy_ref, p1.lpva_secondary_ref);
          FETCH c_get_ipp_refno into l_reusable_refno;
          CLOSE c_get_ipp_refno;
        END IF;
                --
                --*****************************************
        IF p1.lpva_pdu_pob_table_name = 'BUSINESS_ACTIONS'
        THEN
                --
          OPEN c_get_ban_bro_code(p1.lpva_legacy_ref);
          FETCH c_get_ban_bro_code into l_ban_bro_code;
          CLOSE c_get_ban_bro_code;
                --
          OPEN c_get_bro_pgp_refno(l_ban_bro_code);
          FETCH c_get_bro_pgp_refno INTO l_bro_pgp_refno;
           IF c_get_bro_pgp_refno%FOUND
            THEN
             l_pgp_refno := l_bro_pgp_refno;
             l_obj := 'NULL';
           END IF;
          CLOSE c_get_bro_pgp_refno;
        --
        -- Now get parameter definition details for the other field name
        --
          OPEN c_get_pdu_details(p1.lpva_pdf_name, l_bro_pgp_refno);
          FETCH c_get_pdu_details INTO l_display_seqno,l_param_type;
          CLOSE c_get_pdu_details;
                --
          OPEN c_get_ban_refno(p1.lpva_legacy_ref);
          FETCH c_get_ban_refno into l_reusable_refno;
          CLOSE c_get_ban_refno;
        END IF;
                --
                --*****************************************
        IF p1.lpva_pdu_pob_table_name = 'BUSINESS_ACTION_EVENTS'
        THEN
                --
          OPEN c_get_aet_pgp_refno(p1.lpva_secondary_ref);
          FETCH c_get_aet_pgp_refno INTO l_aet_pgp_refno;
           IF c_get_aet_pgp_refno%FOUND
            THEN
             l_pgp_refno := l_aet_pgp_refno;
             l_obj := 'NULL';
           END IF;
          CLOSE c_get_aet_pgp_refno;
                --
          OPEN c_get_pdu_details(p1.lpva_pdf_name, l_aet_pgp_refno);
          FETCH c_get_pdu_details INTO l_display_seqno,l_param_type;
          CLOSE c_get_pdu_details;
                --
          OPEN c_get_bae_refno(p1.lpva_legacy_ref
                              ,p1.lpva_secondary_ref
                              ,p1.lpva_secondary_date
                              ,p1.lpva_further_ref
                              );
          FETCH c_get_bae_refno into l_reusable_refno;
          CLOSE c_get_bae_refno;
        END IF;
        --
                --*****************************************
        -- For Advice Case Housing Options
        -- 1) we have to get the pgp_refno assigned to the hoop_code
        -- 2) The other fields will be held against the lettings_benchmark
        --    reusable_refno against the acho_reference, created_date. These will be
        --    dealt within another loop in the create process.
        --
        IF p1.lpva_pdu_pob_table_name = 'ADVICE_CASE_HOUSING_OPTIONS'
        THEN
                --
          OPEN c_get_hoop_pgp_refno(p1.lpva_secondary_ref);
          FETCH c_get_hoop_pgp_refno INTO l_hoop_pgp_refno;
           IF c_get_hoop_pgp_refno%FOUND
            THEN
             l_pgp_refno := l_hoop_pgp_refno;
             l_obj       := 'NULL';
           END IF;
          CLOSE c_get_hoop_pgp_refno;
                --
          OPEN c_get_pdu_details(p1.lpva_pdf_name, l_hoop_pgp_refno);
          FETCH c_get_pdu_details INTO l_display_seqno,l_param_type;
          CLOSE c_get_pdu_details;
                --
          l_reusable_refno := p1.lpva_lebe_reusable_refno;
        END IF;
        --
                --*****************************************
        -- For Prevention Payments
        -- 1) we have to get the pgp_refno assigned to the PPYT_PPTP_CODE (Prevention Payment Type)
        -- 2) The other fields will be held against the prevention payments reusable_refno
        --
        IF p1.lpva_pdu_pob_table_name = 'PREVENTION_PAYMENTS'      
        THEN
        --              
          OPEN c_get_ppyt_pgp_refno(p1.lpva_legacy_ref);     
          FETCH c_get_ppyt_pgp_refno INTO l_ppyt_pgp_refno;     
           IF c_get_ppyt_pgp_refno%FOUND     
            THEN     
             l_pgp_refno := l_ppyt_pgp_refno;     
             l_obj := 'NULL';           
           END IF;     
          CLOSE c_get_ppyt_pgp_refno;
        --                
          OPEN c_get_pdu_details(p1.lpva_pdf_name, l_ppyt_pgp_refno);     
          FETCH c_get_pdu_details INTO l_display_seqno,l_param_type;     
          CLOSE c_get_pdu_details;
        --                
          OPEN c_get_ppyt_refno(p1.lpva_legacy_ref);     
          FETCH c_get_ppyt_refno into l_reusable_refno;     
          CLOSE c_get_ppyt_refno;     
        END IF;     
        --
                --*****************************************
        -- For Registered Address Lettings
        -- 1) we have to get the pgp_refno assigned to the HOOP_CODE (Housing Options Code)
        -- 2) The other fields will be held against the registered Address Lettings reusable_refno
        --
        IF p1.lpva_pdu_pob_table_name = 'REGISTERED_ADDRESS_LETTINGS'      
        THEN
        --              
          OPEN c_get_real_pgp_refno(p1.lpva_legacy_ref);     
          FETCH c_get_real_pgp_refno INTO l_real_pgp_refno;     
           IF c_get_real_pgp_refno%FOUND     
            THEN     
             l_pgp_refno := l_real_pgp_refno;     
             l_obj := 'NULL';     
           END IF;     
          CLOSE c_get_real_pgp_refno;
        --                
          OPEN c_get_pdu_details(p1.lpva_pdf_name,l_real_pgp_refno);     
          FETCH c_get_pdu_details INTO l_display_seqno,l_param_type;     
          CLOSE c_get_pdu_details;
        --                
          OPEN c_get_real_refno(p1.lpva_legacy_ref);     
          FETCH c_get_real_refno into l_reusable_refno;     
          CLOSE c_get_real_refno;     
        END IF;     
        --
                --*****************************************
        -- Amended 26-MAR-2009. The other field can be held against the individual
        -- arrears action and if this is the case we need to get the ara_pgp_refno
        -- and set the table name to be 'NULL' not ARREARS_ACTIONS. This is because you
        -- can set an other field to be against ALL actions in parameter setup or
        -- specific ones (eg court details) in arrears actions setup.
        -- If we don't do this the other fields will not display.
        --
        IF l_obj = 'ARREARS_ACTIONS'
        THEN
                --
          fsc_utils.debug_message('l_obj = ARREARS_ACTIONS',3);
                --
          OPEN c_get_ara(p1.lpva_pdf_name,p1.lpva_secondary_ref);
          FETCH c_get_ara INTO l_ara_pgp_refno,l_param_type,l_display_seqno;
          fsc_utils.debug_message('l_ara_pgp_refno = '||l_ara_pgp_refno,3);
           IF c_get_ara%FOUND
            THEN
             l_pgp_refno := l_ara_pgp_refno;
             l_obj := 'NULL';
           END IF;
          CLOSE c_get_ara;
                --
          OPEN c_get_aca_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref,p1.lpva_secondary_date);
          FETCH c_get_aca_refno into l_reusable_refno;
          CLOSE c_get_aca_refno;
          fsc_utils.debug_message('l_reusable_refno = '||l_reusable_refno,3);
        END IF;
        --
                --*****************************************
        -- This has been added for Other Fields against Property Elements (v6.9.0)
        -- and is for the APEX v6 front end only
        --
        -- For PROPERTY_ELEMENTS
        -- 1) we have to get the pgp_refno assigned to the ELE_CODE(Element Unique Code)
        --    where the ELE_TYPE = 'PR' and ele_value_type (could be one of either D, N
        --    , M, C) The M (Multi-Value) Included 12Feb2014
        -- 2) The other fields will be held against the property element itself
        -- 3) put reusable ref into dl_mad_other_field_values (l_lebe_reusable_refno)
        --    so it can be used for DELETE
        --
        IF p1.lpva_pdu_pob_table_name = 'PROPERTY_ELEMENTS'
        THEN
                --
          OPEN  c_get_ele_pgp_refno(p1.lpva_secondary_ref);
          FETCH c_get_ele_pgp_refno INTO l_pel_pgp_refno,l_ele_value_type;
           IF c_get_ele_pgp_refno%FOUND
            THEN
             l_pgp_refno := l_pel_pgp_refno;
             l_obj := 'NULL';
           END IF;
          CLOSE c_get_ele_pgp_refno;
                --
          OPEN c_get_ele_pdu_details(p1.lpva_pdf_name,l_pel_pgp_refno);
          FETCH c_get_ele_pdu_details INTO l_display_seqno,l_param_type;
          CLOSE c_get_ele_pdu_details;
                --
          l_ele_pro_refno := NULL;
                --
          OPEN c_get_prop_pro_refno(p1.lpva_legacy_ref);
          FETCH c_get_prop_pro_refno INTO l_ele_pro_refno;
          CLOSE c_get_prop_pro_refno;
                --
        --  MULTI-VALUE BIT INCLUDED 12Feb2014
        --
          IF l_ele_value_type = 'M' 
          THEN
            OPEN c_get_M_pro_ele_refno
                   (l_ele_pro_refno 
                   ,p1.lpva_secondary_ref
                   ,p1.lpva_secondary_date                    
                   ,p1.lpva_further_ref                    
                   ,p1.lpva_hrv_loc_code                    
                   ,p1.lpva_further_ref2                    
                   ,l_ele_value_type                    
                   );                    
            FETCH c_get_M_pro_ele_refno INTO l_reusable_refno;
            CLOSE c_get_M_pro_ele_refno;
          ELSE
            OPEN c_get_CND_pro_ele_refno
                   (l_ele_pro_refno
                   ,p1.lpva_secondary_ref
                   ,p1.lpva_secondary_date                      
                   ,l_ele_value_type                      
                   );                      
            FETCH c_get_CND_pro_ele_refno INTO l_reusable_refno;
            CLOSE c_get_CND_pro_ele_refno;
          END IF;
        --
        -- PUT REUSABLE REF INTO DL TABLE lpva_lebe_reusable_refno TO  BE USE TO DELETE
        --
          l_lebe_refno            := NULL;
          l_lebe_reusable_refno   := NULL;
          l_lebe_reusable_refno:= l_reusable_refno;
                --
          UPDATE dl_mad_other_field_values
          SET    lpva_lebe_reusable_refno = l_lebe_reusable_refno
          WHERE  lpva_dlb_batch_id = cb
          AND    lpva_dl_seqno = cs;
        END IF;
        --
                --*****************************************
        -- This has been added for Other Fields against Admin Unit Elements (v6.9.0)
        --  APEX v6 front end only
        --
        -- For ADMIN_UNIT_ELEMENTS
        -- 1) we have to get the pgp_refno assigned to the ELE_CODE(Element Unique Code)
        --    where the ELE_TYPE = 'PR' and ele_value_type (could be one of either D, N
        --    , M, C) The M (Multi-Value) Included 12Feb2014
        -- 2) The other fields will be held against the admin_unit_elements table itself
        -- 3) put reusable ref into dl_mad_other_field_values (l_lebe_reusable_refno)
        --    so it can be used for DELETE (Not Done Yet)
        --
        IF p1.lpva_pdu_pob_table_name = 'ADMIN_UNIT_ELEMENTS'
        THEN
                --
          OPEN  c_get_ele_pgp_refno(p1.lpva_secondary_ref);
          FETCH c_get_ele_pgp_refno INTO l_auel_pgp_refno,l_ele_value_type;
           IF c_get_ele_pgp_refno%FOUND
            THEN
             l_pgp_refno := l_auel_pgp_refno;
             l_obj       := 'NULL';
           END IF;
          CLOSE c_get_ele_pgp_refno;
                --
          OPEN c_get_ele_pdu_details(p1.lpva_pdf_name,l_auel_pgp_refno);
          FETCH c_get_ele_pdu_details INTO l_display_seqno,l_param_type;
          CLOSE c_get_ele_pdu_details;
        --
        --  MULTI-VALUE INCLUDED 12Feb2014
        --
          IF l_ele_value_type = 'M' 
          THEN
            OPEN c_get_M_aue_ele_refno
                   (p1.lpva_legacy_ref                    
                   ,p1.lpva_secondary_ref                    
                   ,p1.lpva_secondary_date                    
                   ,p1.lpva_further_ref                    
                   ,p1.lpva_hrv_loc_code                    
                   ,p1.lpva_further_ref2                    
                   ,l_ele_value_type                    
                   );
            FETCH c_get_M_aue_ele_refno INTO l_reusable_refno;
            CLOSE c_get_M_aue_ele_refno;
          ELSE
            OPEN c_get_CND_aue_ele_refno
                   (p1.lpva_legacy_ref                      
                   ,p1.lpva_secondary_ref                      
                   ,p1.lpva_secondary_date                      
                   ,l_ele_value_type                      
                   );
            FETCH c_get_CND_aue_ele_refno INTO l_reusable_refno;
            CLOSE c_get_CND_aue_ele_refno;
          END IF;
        --
        -- PUT REUSABLE REF INTO DL TABLE lpva_lebe_reusable_refno for del process
        --
          l_lebe_refno            := NULL;
          l_lebe_reusable_refno   := NULL;
          l_lebe_reusable_refno:= l_reusable_refno;
          --
          UPDATE dl_mad_other_field_values
          SET    lpva_lebe_reusable_refno = l_lebe_reusable_refno
          WHERE  lpva_dlb_batch_id = cb
          AND    lpva_dl_seqno = cs;
        END IF;
                --
        fsc_utils.debug_message('l_obj            = '||l_obj,3);
        fsc_utils.debug_message('l_reusable_refno = '||l_reusable_refno,3);
                --
                --*****************************************
                --
        INSERT INTO parameter_values
        (pva_reusable_refno
        ,pva_pdu_pdf_name                       
        ,pva_pdu_pdf_param_type                       
        ,pva_pdu_pob_table_name                       
        ,pva_pdu_pgp_refno                       
        ,pva_pdu_display_seqno                       
        ,pva_created_by                       
        ,pva_created_date                       
        ,pva_date_value                       
        ,pva_number_value                       
        ,pva_char_value                       
        ) 
        VALUES                       
        (l_reusable_refno                       
        ,p1.lpva_pdf_name                       
        ,l_param_type                       
        ,l_obj                       
        ,l_pgp_refno                        
        ,l_display_seqno                       
        ,NVL(p1.lpva_created_by,USER)                       
        ,NVL(p1.lpva_created_date,SYSDATE)                       
        ,p1.lpva_date_value                       
        ,p1.lpva_number_value                       
        ,p1.lpva_char_value                       
        );
        --              
        IF p1.lpva_created_by IS NOT NULL
        OR p1.lpva_created_date IS NOT NULL
        THEN
                --
          UPDATE parameter_values
          SET    pva_created_by = NVL(p1.lpva_created_by,pva_created_by)
          ,      pva_created_date = NVL(p1.lpva_created_date,pva_created_date)
          WHERE  pva_reusable_refno = l_reusable_refno
          AND    pva_pdu_pdf_name = p1.lpva_pdf_name
          AND    pva_pdu_pdf_param_type = l_param_type
          AND    pva_pdu_pob_table_name = l_obj
          AND    pva_pdu_pgp_refno = l_pgp_refno
          AND    pva_pdu_display_seqno = l_display_seqno;
        END IF;
        --
        -- Update DL table with 
        --		
        UPDATE dl_mad_other_field_values
        SET    lpva_pdu_pgp_refno = l_pgp_refno
              ,lpva_pdu_table_name = l_obj  
              ,lpva_pdu_display_seqno = l_display_seqno
        WHERE  lpva_dlb_batch_id = cb
        AND    lpva_dl_seqno = cs		
        AND    ROWID = p1.rec_rowid;
        --
                --*****************************************
        -- keep a count of the rows processed and commit after every 5000
        --
        i := i+1;
        IF MOD(i,5000)=0
        THEN
          COMMIT;
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        set_record_status_flag(l_id,'C');
        --
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
    --*****************************************
    -- Now INSERT into LETTINGS_BENCHMARKS the ADVICE CASE HOUSING OPTIONS a record
    --  for other field values created.
    --
    IF NVL(l_count_acho,0) > 0
    THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'ACHO POST');
      FOR post in post_process_acho 
      LOOP
          --
        BEGIN
        --
          l_acho_reference := NULL;
                --
          OPEN c_get_acho_reference(post.lpva_legacy_ref);
          FETCH c_get_acho_reference INTO l_acho_reference;
          CLOSE c_get_acho_reference;
                --
          l_lebe_manual_created_ind := SUBSTR(post.lpva_further_ref2,1,1);
          l_lebe_generated_ind := SUBSTR(post.lpva_further_ref,1,1);
                --
          INSERT INTO lettings_benchmarks
          (lebe_refno
          ,lebe_reusable_refno                           
          ,lebe_acho_reference                           
          ,lebe_description                           
          ,lebe_approved_ind                           
          ,lebe_created_by                           
          ,lebe_created_date                           
          ,lebe_comments                           
          ,lebe_approved_by                           
          ,lebe_modified_date                           
          ,lebe_modified_by                           
          ,lebe_manual_created_ind                           
          ,lebe_generated_ind                           
          ) 
          VALUES
          (post.lpva_lebe_refno
          ,post.lpva_lebe_reusable_refno                           
          ,l_acho_reference                           
          ,post.lpva_desc                           
          ,'Y'                           
          ,post.lpva_created_by                           
          ,post.lpva_created_date                           
          ,NULL                           
          ,NULL                           
          ,NULL                           
          ,NULL                           
          ,l_lebe_manual_created_ind                           
          ,l_lebe_generated_ind                           
          );
        END;
      END LOOP; -- post_process_acho
    END IF;
    --
    --*****************************************
    -- Section to analyse the table(s) populated by this data load
    --
    l_an_tab := s_dl_hem_utils.dl_comp_stats('PARAMETER_VALUES');
    fsc_utils.proc_end;
  EXCEPTION
  WHEN OTHERS
  THEN
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_create;
  --
  -- *****************************************************************************
  --
  PROCEDURE dataload_validate
    (p_batch_id          IN VARCHAR2
    ,p_date              IN DATE
    )
  AS
  --*******************
  CURSOR c1
  IS
    SELECT rowid rec_rowid
    ,      lpva_dlb_batch_id
    ,      lpva_dl_seqno
    ,      lpva_dl_load_status
    ,      lpva_legacy_ref
    ,      lpva_pdf_name
    ,      lpva_created_by
    ,      lpva_created_date
    ,      lpva_date_value
    ,      lpva_number_value
    ,      lpva_char_value
    ,      lpva_secondary_ref
    ,      lpva_secondary_date
    ,      lpva_pdu_pob_table_name
    ,      lpva_further_ref
    ,      lpva_hrv_loc_code
    ,      lpva_further_ref2
    ,      lpva_further_ref3
    ,      lpva_desc
    FROM   dl_mad_other_field_values
    WHERE  lpva_dlb_batch_id    = p_batch_id
    AND    lpva_dl_load_status in ('L','F','O');
  --*******************
  CURSOR c_pdf 
    (p_pdf_name   VARCHAR2
    ,p_table_name VARCHAR2
    )
  IS
    SELECT pdu_pob_table_name
    FROM   parameter_definition_usages
    WHERE  pdu_pdf_name = p_pdf_name
    AND    pdu_pob_table_name = p_table_name;
  --*******************
  CURSOR c_pdu 
    (p_pdf_name   VARCHAR2
    ,p_table_name VARCHAR2
    )
  IS
    SELECT 'X'
    FROM   parameter_definition_usages
    WHERE  pdu_pdf_name = p_pdf_name
    AND    pdu_pdf_param_type = 'OTHER FIELDS'
    AND    pdu_pob_table_name = p_table_name;
  --*******************
  CURSOR c_get_aun_refno
    (p_aun_ref VARCHAR2)
  IS
    SELECT aun_reusable_refno
    FROM   admin_units
    WHERE  aun_code = p_aun_ref;
  --*******************
  CURSOR c_get_aca_refno
    (p_pay_ref  VARCHAR2
    ,p_ara_code VARCHAR2
    ,p_date     DATE
    )
  IS
    SELECT aca_reusable_refno
    FROM   account_arrears_actions a
    ,      revenue_accounts        r
    WHERE  p_pay_ref = r.rac_pay_ref
    AND    a.aca_rac_accno = r.rac_accno
    AND    p_ara_code = a.aca_ara_code
    AND    p_date = trunc(a.aca_created_date);
  --*******************
  CURSOR c_get_con_refno
    (p_con_ref VARCHAR2)
  IS
    SELECT con_reusable_refno
    FROM   contractors
    WHERE  con_code = p_con_ref;
  --*******************
  CURSOR c_get_cos_refno
   (p_cos_ref VARCHAR2)
  IS
    SELECT cos_reusable_refno
    FROM   contractor_sites
    WHERE  cos_code = p_cos_ref;
  --*******************
  CURSOR c_get_par_refno
    (p_per_alt_ref VARCHAR2)
  IS
    SELECT par_reusable_refno
    FROM   parties
    WHERE  par_per_alt_ref = p_per_alt_ref;
  --*******************
  CURSOR c_get_ltl_refno
    (p_reference VARCHAR2)
  IS
    SELECT ltl_reusable_refno
    FROM   land_titles
    WHERE  ltl_reference = p_reference;
  --*******************
  CURSOR c_get_par_type(p_per_alt_ref VARCHAR2)
  IS
    SELECT par_type
    FROM parties
    WHERE par_per_alt_ref = p_per_alt_ref;
  --*******************
  CURSOR c_get_par_type2(p_par_refno VARCHAR2)
  IS
    SELECT par_type
    FROM parties
    WHERE par_refno = to_number(p_par_refno);
  --*******************
  CURSOR c_pdf_datatype 
    (p_pdf_name   VARCHAR2
    ,p_table_name VARCHAR2
    )
  IS
    SELECT pdf.pdf_datatype
    FROM   parameter_definition_usages pdu
    ,      parameter_definitions pdf
    WHERE  pdu.pdu_pob_table_name = p_table_name
    AND    pdu.pdu_pdf_param_type = 'OTHER FIELDS'
    AND    pdu.pdu_pdf_name = pdf.pdf_name
    AND    pdu.pdu_pdf_param_type = pdf.pdf_param_type
    AND    pdf.pdf_name = p_pdf_name;
  --*******************
  CURSOR c_chk_org_dup 
    (p_reusable_refno  NUMBER
    ,p_pdf_name        VARCHAR2
    ) 
  IS
    SELECT 'X'
    FROM   parameter_values
    ,      parameter_definition_usages
    WHERE  pva_reusable_refno = p_reusable_refno
    AND    pdu_pob_table_name = 'ORGANISATIONS'
    AND    pdu_pdf_param_type = 'OTHER FIELDS'
    AND    pva_pdu_pdf_name = pdu_pdf_name
    AND    pva_pdu_pdf_param_type = pdu_pdf_param_type
    AND    pva_pdu_pob_table_name = pdu_pob_table_name
    AND    pva_pdu_pgp_refno = pdu_pgp_refno
    AND    pva_pdu_display_seqno = pdu_display_seqno
    AND    pdu_pdf_name = p_pdf_name;
  --*******************
  CURSOR c_get_pro_refno
    (p_pro_propref VARCHAR2)
  IS
    SELECT pro_reusable_refno
    FROM   properties
    WHERE  pro_propref = p_pro_propref;
  --*******************
  CURSOR c_get_srq_refno
    (p_srq_ref VARCHAR2)
  IS
    SELECT srq_reusable_refno
    FROM   service_requests
    WHERE  srq_legacy_refno = p_srq_ref;
  --*******************
  CURSOR c_get_tcy_refno
    (p_tcy_alt_ref VARCHAR2)
  IS
    SELECT tcy_reusable_refno
    FROM   tenancies
    WHERE  tcy_refno = p_tcy_alt_ref;
  --*******************
  CURSOR c_get_tcy_refno2
    (p_tcy_alt_ref VARCHAR2)
  IS
    SELECT tcy_reusable_refno
    FROM   tenancies
    WHERE  tcy_alt_ref = p_tcy_alt_ref;
  --*******************
  CURSOR c_get_wor_refno
    (p_wor_ref VARCHAR2)
  IS
    SELECT wor_reusable_refno
    FROM   works_orders
    WHERE  wor_legacy_ref = p_wor_ref;
  --*******************
  CURSOR c_get_srt_refno
    (p_srt_sud_pro_aun_code VARCHAR2
    ,p_srt_ele_code         VARCHAR2
    ,p_scs_reference        VARCHAR2
    )
  IS
    SELECT srt_reusable_refno
    FROM   survey_results
    ,      stock_condition_surveys
    WHERE  srt_sud_pro_aun_code = p_srt_sud_pro_aun_code
    AND    srt_ele_code = p_srt_ele_code
    AND    scs_reference = p_scs_reference
    AND    scs_refno = srt_sud_scs_refno;
  --*******************
  CURSOR c_get_cve_refno
    (p_cnt_reference VARCHAR2
    ,p_version_no    VARCHAR2
    )
  IS
    SELECT cve_reusable_refno
    FROM   contract_versions
    WHERE  cve_cnt_reference = p_cnt_reference
    AND    cve_version_number = p_version_no;
  --*******************
  CURSOR c_get_tve_refno 
    (cp_tkg_src_reference  VARCHAR2
    ,cp_tkg_code           VARCHAR2
    ,cp_tkg_src_type       VARCHAR2
    ,cp_tsk_alt_reference  VARCHAR2
    ,cp_version_number     NUMBER
    )
  IS
    SELECT tve_reusable_refno
    FROM   task_versions
    WHERE  tve_tsk_tkg_src_reference = cp_tkg_src_reference
    AND    tve_tsk_tkg_code = cp_tkg_code
    AND    tve_tsk_tkg_src_type = cp_tkg_src_type
    AND    tve_tsk_id = 
             (SELECT tsk_id 
              FROM   tasks 
              WHERE  tsk_alt_reference = cp_tsk_alt_reference
             )
    AND    tve_version_number = cp_version_number;
  --*******************
  CURSOR c_get_dve_refno
    (p_cnt_reference VARCHAR2
    ,p_pro_aun_code  VARCHAR2
    ,p_pro_aun_ind   VARCHAR2
    ,p_display_seqno VARCHAR2
    )
  IS
    SELECT dve_reusable_refno
    FROM   deliverables
    ,      deliverable_versions
    WHERE  dlv_refno = dve_dlv_refno
    AND    dlv_cnt_reference = p_cnt_reference
    AND    dlv_cad_pro_aun_code = p_pro_aun_code
    AND    dlv_cad_type_ind = p_pro_aun_ind
    AND    dve_display_sequence = p_display_seqno
    AND    dve_current_ind = 'Y';
  --*******************
  CURSOR c_get_ipp_refno
    (p_ipp_shortname VARCHAR2
    ,p_ipp_ipt_code  VARCHAR2
    )
  IS
    SELECT ipp_reusable_refno
    FROM   interested_parties
    WHERE  ipp_shortname = p_ipp_shortname
    AND    ipp_ipt_code = p_ipp_ipt_code;
  --*******************
  CURSOR c_get_ipt_pgp_refno
    (p_ipt_code  VARCHAR2)
  IS
    SELECT ipt_pgp_refno
    FROM   interested_party_types
    WHERE  ipt_code = p_ipt_code;
  --*******************
  CURSOR c_get_hoop_pgp_refno
    (p_hoop_code  VARCHAR2)
  IS
    SELECT hoop_pgp_refno
    FROM   housing_options
    WHERE  hoop_code = p_hoop_code;
  --*******************
  CURSOR c_get_aet_pgp_refno
    (p_aet_code  VARCHAR2)
  IS
    SELECT aet_pgp_refno
    FROM   action_event_types
    WHERE  aet_code = p_aet_code;
  --*******************
  CURSOR c_get_bro_pgp_refno
    (p_bro_code  VARCHAR2)
  IS
    SELECT bro_pgp_action_refno
    FROM   business_reasons
    WHERE  bro_code = p_bro_code;
  --*******************
  CURSOR c_get_ban_bro_code
    (p_ban_reference VARCHAR2)
  IS
    SELECT ban_bro_code
    FROM   business_actions
    WHERE  ban_reference = p_ban_reference;
  --*******************
  CURSOR c_get_pdu_details
    (p_pdf_name  VARCHAR2
    ,p_pgp_refno NUMBER
    )
  IS
    SELECT 'X'
    FROM   parameter_definition_usages
    WHERE  pdu_pdf_name = p_pdf_name
    AND    pdu_pgp_refno = p_pgp_refno;
  --*******************
  CURSOR chk_real_exists
    (p_real_reference VARCHAR2)
  IS
    SELECT real_refno
    FROM   registered_address_lettings
    WHERE  real_reference = p_real_reference;
  --*******************
  CURSOR c_get_bae_refno
    (p_bae_ban_reference    VARCHAR2
    ,p_bae_aet_code         VARCHAR2
    ,p_bae_status_date      DATE
    ,p_bae_seqno            NUMBER
    )
  IS
    SELECT bae_reusable_refno
    FROM   business_action_events
    WHERE  bae_ban_reference = p_bae_ban_reference
    AND    bae_aet_code = p_bae_aet_code
    AND    bae_status_date = p_bae_status_date
    AND    bae_sequence = p_bae_seqno;
  --*******************
  CURSOR chk_acho_exists
    (p_acho_reference VARCHAR2)
  IS
    SELECT acho_reference
    FROM   advice_case_housing_options
    WHERE  acho_alternative_reference = p_acho_reference;
  --*******************
  CURSOR chk_ppyt_exists
    (p_ppyt_alternative_reference VARCHAR2)
  IS
    SELECT ppyt_reusable_refno
    FROM   prevention_payments
    WHERE  ppyt_alternative_reference = p_ppyt_alternative_reference;
  --*******************
  CURSOR c_get_ppyt_pgp_refno
    (p_ppyt_alternative_reference  VARCHAR2)
  IS
    SELECT b.pptp_pgp_refno
    FROM   prevention_payments      a
    ,      prevention_payment_types b
    WHERE  b.pptp_code = a.ppyt_pptp_code
    AND    a.ppyt_alternative_reference = p_ppyt_alternative_reference;
  --*******************
  CURSOR c_get_real_pgp_refno
    (p_real_reference  VARCHAR2)
  IS
    SELECT hoop_pgp_refno
    FROM   housing_options
    ,      advice_case_housing_options
    ,      registered_address_lettings
    WHERE  hoop_code      = acho_hoop_code
    AND    acho_reference = real_acho_reference
    AND    real_reference = p_real_reference;
  --*******************
  CURSOR c_chk_ele_exists
    (p_ele_code VARCHAR2)
  IS
    SELECT 'X'
    FROM   elements
    WHERE  ele_code = p_ele_code;
  --*******************
  CURSOR c_chk_ele_code
    (p_ele_code VARCHAR2)
  IS
    SELECT ele_pgp_refno
    ,      ele_value_type
    ,      ele_type
    FROM   elements
    WHERE  ele_code = p_ele_code;
  --*******************
  CURSOR c_propref_chk 
    (p_propref VARCHAR2) 
  IS
    SELECT 'X'
    FROM   properties
    WHERE  pro_propref = p_propref
    AND    pro_type IN ('HOU','BOTH');
  --*******************
  CURSOR c_get_ele_pdu_details
    (p_pdf_name  VARCHAR2
    ,p_pgp_refno NUMBER
    )
  IS
    SELECT pdu_display_seqno
    ,      pdu_pdf_param_type
    FROM   parameter_definition_usages
    WHERE  pdu_pdf_name  = p_pdf_name
    AND    pdu_pgp_refno = p_pgp_refno
    AND    pdu_pob_table_name = 'NULL'
    AND    pdu_pdf_param_type ='OTHER FIELDS';
  --
  --*******************
  -- Multi-Value Elements property element
  --
  CURSOR c_get_MUL_pro_ele_refno
    (p_pro_refno        NUMBER
    ,p_ele_code         VARCHAR2
    ,p_start_date       DATE
    ,p_att_code         VARCHAR2
    ,p_hrv_loc_code     VARCHAR2
    ,p_fat_code         VARCHAR2
    )
  IS
    SELECT pel_reusable_refno
    FROM   property_elements
    WHERE  pel_pro_refno = p_pro_refno
    AND    pel_ele_code = p_ele_code
    AND    pel_start_date = p_start_date
    AND    pel_att_code = p_att_code
    AND    NVL(pel_hrv_elo_code,'NUL')  = NVL(p_hrv_loc_code,'NUL')
    AND    pel_fat_code = NVL(p_fat_code,'NUL');
  --
  --*******************
  -- Multi-Value Elements admin unit element
  --
  CURSOR c_get_MUL_aue_ele_refno
    (p_aun_code           VARCHAR2
    ,p_ele_code           VARCHAR2
    ,p_start_date         DATE
    ,p_att_code           VARCHAR2
    ,p_hrv_loc_code       VARCHAR2
    ,p_fat_code           VARCHAR2
    )
  IS
    SELECT aue_reusable_refno
    FROM   admin_unit_elements
    WHERE  aue_aun_code = p_aun_code
    AND    aue_ele_code = p_ele_code
    AND    aue_start_date = p_start_date
    AND    aue_att_code = p_att_code
    AND    NVL(aue_hrv_elo_code,'NUL')= NVL(p_hrv_loc_code,'NUL')
    AND    aue_fat_code = NVL(p_fat_code,'NUL');
  --
  --*******************
  -- None Multi-Value Elements admin unit element
  --
  CURSOR c_get_CND_aue_ele_refno
    (p_aun_code           VARCHAR2
    ,p_ele_code           VARCHAR2
    ,p_start_date         DATE
    )
  IS
    SELECT aue_reusable_refno
    FROM   admin_unit_elements
    WHERE  aue_aun_code = p_aun_code
    AND    aue_ele_code = p_ele_code
    AND    aue_start_date = p_start_date;
  --
  --*******************
  -- None Multi-Value Elements property element
  --
  CURSOR c_get_CND_pro_ele_refno
    (p_pro_refno          NUMBER
    ,p_ele_code           VARCHAR2
    ,p_start_date         DATE
    )
  IS
    SELECT pel_reusable_refno
    FROM   property_elements
    WHERE  pel_pro_refno = p_pro_refno
    AND    pel_ele_code = p_ele_code
    AND    pel_start_date = p_start_date;
  --*******************
  CURSOR c_get_prop_pro_refno
    (p_pro_propref VARCHAR2)
  IS
    SELECT pro_refno
    FROM   properties
    WHERE  pro_propref = p_pro_propref;
  --*******************
  CURSOR c_chk_for_dup 
    (p_reusable_refno  NUMBER
    ,p_pdf_name        VARCHAR2
    ) 
  IS
    SELECT 'X'
    FROM   parameter_values
    ,      parameter_definition_usages
    WHERE  pva_reusable_refno = p_reusable_refno
    AND    pdu_pdf_param_type = 'OTHER FIELDS'
    AND    pva_pdu_pdf_name = pdu_pdf_name
    AND    pva_pdu_pdf_param_type = pdu_pdf_param_type
    AND    pva_pdu_pob_table_name = pdu_pob_table_name
    AND    pva_pdu_pgp_refno = pdu_pgp_refno
    AND    pva_pdu_display_seqno  = pdu_display_seqno
    AND    pdu_pdf_name = p_pdf_name;
  --*******************
  CURSOR c_get_ofvat_pgp_refno
   (p_ofat_code VARCHAR2)
  IS
    SELECT ofat_pgp_refno
    FROM   other_fields_account_types
    WHERE  ofat_code = p_ofat_code;
  --*******************
  CURSOR c_chk_rac_refno
    (p_pay_ref  VARCHAR2)
  IS
    SELECT 'X'
    FROM   revenue_accounts
    WHERE  rac_pay_ref = p_pay_ref;
  --*******************
  CURSOR c_get_rac_refno
    (p_pay_ref      VARCHAR2
    ,p_hrv_ate_code VARCHAR2
    )
  IS
    SELECT rac_reusable_refno
    FROM   revenue_accounts
    WHERE  rac_pay_ref = p_pay_ref
    AND    rac_hrv_ate_code = p_hrv_ate_code;
  --*******************
  CURSOR c_chk_rac_pdu_exist
    (p_pdf_name  VARCHAR2
    ,p_pgp_refno NUMBER
    )
  IS
    SELECT 'X'
    FROM   parameter_definition_usages
    WHERE  pdu_pdf_name = p_pdf_name
    AND    pdu_pgp_refno = p_pgp_refno
    AND    pdu_pob_table_name = 'NULL'
    AND    pdu_pdf_param_type ='OTHER FIELDS';
  --*******************
  CURSOR c_rac_pdf_datatype 
    (p_pdf_name   VARCHAR2
    ,p_pgp_refno  NUMBER
    )
  IS
    SELECT pdf.pdf_datatype
    FROM   parameter_definition_usages pdu
    ,      parameter_definitions       pdf
    WHERE  pdu.pdu_pob_table_name = 'NULL'
    AND    pdu.pdu_pdf_param_type = 'OTHER FIELDS'
    AND    pdu.pdu_pgp_refno = p_pgp_refno
    AND    pdu.pdu_pdf_name = pdf.pdf_name
    AND    pdu.pdu_pdf_param_type = pdf.pdf_param_type
    AND    pdf.pdf_name = p_pdf_name;
  --*******************
  CURSOR c_chk_rac_dup
    (p_reusable_refno  NUMBER
    ,p_pdf_name        VARCHAR2
    ,p_pgp_refno       NUMBER
    )
  IS
    SELECT 'X'
    FROM   parameter_values
    ,      parameter_definition_usages
    WHERE  pva_reusable_refno = p_reusable_refno
    AND    pdu_pob_table_name = 'NULL'
    AND    pdu_pdf_param_type = 'OTHER FIELDS'
    AND    pva_pdu_pdf_name = pdu_pdf_name
    AND    pva_pdu_pdf_param_type = pdu_pdf_param_type
    AND    pva_pdu_pob_table_name = pdu_pob_table_name
    AND    pva_pdu_pgp_refno = pdu_pgp_refno
    AND    pva_pdu_display_seqno = pdu_display_seqno
    AND    pdu_pdf_name = p_pdf_name
    AND    pdu_pgp_refno = p_pgp_refno;
  --******************* 
  CURSOR c_get_org_refno
    (p_par_refno VARCHAR2)
  IS
    SELECT par_refno
    FROM   parties
    WHERE  par_refno = to_number(p_par_refno)
    AND    par_type  = 'ORG';
  --*******************  
  CURSOR c_get_org_alt_refno
    (p_par_alt_refno VARCHAR2)
  IS
    SELECT par_refno
    FROM   parties
    WHERE  par_per_alt_ref = p_par_alt_refno
    AND    par_type  = 'ORG';
  --*******************  
  CURSOR c_get_oco_refno
    (p_par_refno NUMBER
    ,p_forename  VARCHAR2
    ,p_surname   VARCHAR2
    ,p_date      DATE)
  IS
    SELECT oco_reusable_refno
    FROM   organisation_contacts
    WHERE  oco_par_refno = p_par_refno
        AND    oco_forename = p_forename
        AND    oco_surname = p_surname
    AND    oco_start_date = p_date;     
  --*******************
  CURSOR c_get_orgtype_param 
    (p_pdf_name        VARCHAR2) 
  IS
    SELECT pva_char_value
    FROM   parameter_values
    ,      parameter_definition_usages
    WHERE  pdu_pdf_param_type = 'SYSTEM'
    AND    pva_pdu_pdf_name = pdu_pdf_name
    AND    pva_pdu_pdf_param_type = pdu_pdf_param_type
    AND    pva_pdu_pob_table_name = 'SYSTEM'
    AND    pva_pdu_pgp_refno = pdu_pgp_refno
    AND    pva_pdu_display_seqno  = pdu_display_seqno
    AND    pdu_pdf_name = p_pdf_name;
  --*******************
  CURSOR c_chk_orgtype_refno
    (p_par_refno VARCHAR2)
  IS
    SELECT par_reusable_refno
    ,      par_org_frv_oty_code
    ,      par_type
    FROM   parties
    WHERE  par_refno = to_number(p_par_refno);
  --******************* 
  CURSOR c_chk_orgtype_alt_refno
    (p_par_alt_refno VARCHAR2)
  IS
    SELECT par_reusable_refno
    ,      par_org_frv_oty_code
    ,      par_type
    FROM   parties
    WHERE  par_per_alt_ref = p_par_alt_refno;
  --*******************
  CURSOR c_obj_orgtype
    (p_obj        VARCHAR2
    ,p_orgtype    VARCHAR2
    )
  IS
    SELECT pdu_pob_table_name
    ,      pdu_pgp_refno
    ,      pdu_display_seqno
    ,      pdu_pdf_param_type
    FROM   parameter_definition_usages
	,      organisation_type_param_groups
    WHERE  pdu_pdf_name = p_obj
    AND    pdu_pgp_refno = otpg_pgp_refno
	AND    otpg_frv_oty_code = p_orgtype;
  --*******************
  CURSOR c_pdf_datatype2 
    (p_pdf_name   VARCHAR2
    )
  IS
    SELECT pdf_datatype, pdf_vru_id
    FROM   parameter_definitions
    WHERE  pdf_name = p_pdf_name
    AND    pdf_param_type = 'OTHER FIELDS';
  --*******************
  CURSOR c_obj
    (p_obj        VARCHAR2
    ,p_table_name VARCHAR2
    )
  IS
    SELECT pdu_pob_table_name
    ,      pdu_pgp_refno
    ,      pdu_display_seqno
    ,      pdu_pdf_param_type
    FROM   parameter_definition_usages
    WHERE  pdu_pdf_name = p_obj
    AND    pdu_pob_table_name = p_table_name;
  --*******************
  CURSOR c_chk_dup_orgtype
    (p_obj             VARCHAR2
    ,p_orgtype         VARCHAR2
    ,p_reusable_refno  NUMBER
    )
  IS
    SELECT 'X'
    FROM   parameter_values   
    ,      parameter_definition_usages
    ,      organisation_type_param_groups
    WHERE  pva_reusable_refno = p_reusable_refno
    AND    pdu_pdf_param_type = 'OTHER FIELDS'
    AND    pva_pdu_pdf_name = pdu_pdf_name
    AND    pva_pdu_pdf_param_type = pdu_pdf_param_type
    AND    pva_pdu_pob_table_name = pdu_pob_table_name
    AND    pva_pdu_display_seqno  = pdu_display_seqno
    AND    pdu_pdf_name = p_obj
    AND    pdu_pgp_refno = otpg_pgp_refno
    AND    otpg_frv_oty_code = p_orgtype;
  --*******************
  CURSOR c_chk_dup_batch
    (p_legacy_ref  VARCHAR2
    ,p_pdf_name    VARCHAR2
    ,p_batch       VARCHAR2
    )
  IS
    SELECT COUNT(*)
    FROM   dl_mad_other_field_values
    WHERE  lpva_legacy_ref = p_legacy_ref
    AND    lpva_pdf_name = p_pdf_name
    AND    lpva_dlb_batch_id = p_batch;
  --*******************
  CURSOR c_chk_org_info
    (p_orgtype        VARCHAR2
    ,p_name           VARCHAR2
    ,p_short_name     VARCHAR2
    ,p_reusable_refno NUMBER
    )
  IS
    SELECT 'X'
    FROM   parties
    WHERE  par_org_frv_oty_code = p_orgtype
    AND    par_org_name = p_name 
    AND    par_org_short_name = p_short_name
    AND    par_reusable_refno = p_reusable_refno;
  --*******************
  -- Constants for process_summary
  --
  cb             VARCHAR2(30);
  cd             DATE;
  cp             VARCHAR2(30) := 'VALIDATE';
  ct             VARCHAR2(30) := 'DL_MAD_OTHER_FIELD_VALUES';
  cs             PLS_INTEGER;
  ce             VARCHAR2(200);
  l_id           ROWID;
  --
  --*******************
  -- Other Constants
  --
  l_reusable_refno           PLS_INTEGER; 
  l_pdu                      VARCHAR2(30); 
  l_obj                      VARCHAR2(30); 
  l_obj2                     VARCHAR2(30);
  l_obj3                     VARCHAR2(30);  
  l_exists                   VARCHAR2(1);
  l_dup_exists               VARCHAR2(1); 
  l_pro_refno                NUMBER  (10); 
  l_errors                   VARCHAR2(10); 
  l_error_ind                VARCHAR2(10); 
  i                          PLS_INTEGER :=0; 
  l_pro_aun_code             VARCHAR2(20); 
  l_ipt_pgp_refno            PLS_INTEGER; 
  l_ipt_pdu_exists           VARCHAR2(1); 
  l_hoop_pgp_refno           PLS_INTEGER;
  l_hoop_pdu_exists          VARCHAR2(1);
  l_aet_pgp_refno            PLS_INTEGER;
  l_aet_pdu_exists           VARCHAR2(1);
  l_bro_pgp_refno            PLS_INTEGER;
  l_bro_pdu_exists           VARCHAR2(1);
  l_ban_bro_code             VARCHAR2(10);
  l_ppyt_pgp_refno           PLS_INTEGER;
  l_ppyt_pdu_exists          VARCHAR2(1);
  l_acho_reference           NUMBER(10);
  l_ppyt_reference           NUMBER(10);
  l_real_pgp_refno           PLS_INTEGER;
  l_real_refno               PLS_INTEGER;
  l_real_pdu_exists          VARCHAR2(1);
  l_pel_pgp_refno            PLS_INTEGER;
  l_pel_pdu_exists           VARCHAR2(1);
  l_pel_reusable_refno       PLS_INTEGER;
  l_aun_reusable_refno       PLS_INTEGER;
  l_auel_pgp_refno           PLS_INTEGER;
  l_ele_pro_refno            NUMBER(10);
  l_ele_value_type           VARCHAR2(1);
  l_ele_type                 VARCHAR2(2);
  l_propref_exists           VARCHAR2(1);
  l_ele_code_exists          VARCHAR2(1);
  l_org_pdu_exists           VARCHAR2(1);
  l_par_type                 VARCHAR2(5);
  l_pdf_datatype             VARCHAR2(10);
  l_pdf_datatype2            VARCHAR2(10);
  l_pdf_datatype3            VARCHAR2(10);
  l_org_pva_exists           VARCHAR2(1);
  l_rac_pdu_exists           VARCHAR2(1);
  l_ofvat_pgp_refno          PLS_INTEGER;
  l_rac_exists               VARCHAR2(1);
  l_rac_pva_exists           VARCHAR2(1);
  l_lebe_manual_created_ind  VARCHAR2(1);
  l_lebe_generated_ind       VARCHAR2(1);
  l_chk_datatype             VARCHAR2(30);
  l_chk_par_refno            NUMBER(8);
  l_chk_par_alt_refno        NUMBER(8);
  l_orgtype_param            VARCHAR2(10);
  l_orgtype                  VARCHAR2(10);
  l_orgtype_dt               VARCHAR2(10);
  l_pdf_vru_id               NUMBER(10);
  l_pgp_refno                PLS_INTEGER;
  l_display_seqno            NUMBER;
  l_param_type               VARCHAR2(20);
  l_org_pva_exists2          VARCHAR2(1);
  l_count_batch              PLS_INTEGER;
  l_org_info                 VARCHAR2(1);

  --
  --*****************************************
  --
  BEGIN
  --
    fsc_utils.proc_start('s_dl_mad_other_field_values.dataload_validate');
    fsc_utils.debug_message('s_dl_mad_other_field_values.dataload_validate',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    --
    -- Get HEM_ORG_OTHERFLDS parameter
    --
    l_orgtype_param   := NULL;
    OPEN  c_get_orgtype_param('HEM_ORG_OTHERFLDS');
    FETCH c_get_orgtype_param INTO l_orgtype_param;   --'TABLE' or 'ORGTYPE'
    CLOSE c_get_orgtype_param;
    --	
    FOR p1 IN c1 
    LOOP
        --
      BEGIN
          --
        cs := p1.lpva_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors    := 'V';
        l_error_ind := 'N';
        l_reusable_refno  := NULL;
        l_pro_aun_code    := NULL;
        l_ipt_pgp_refno   := NULL;
        l_hoop_pgp_refno  := NULL;
        l_ppyt_pgp_refno  := NULL;
        l_aet_pgp_refno   := NULL;
        l_bro_pgp_refno   := NULL;
        l_ofvat_pgp_refno := NULL;
        l_ipt_pdu_exists  := NULL;
        l_hoop_pdu_exists := NULL;
        l_ppyt_pdu_exists := NULL;
        l_aet_pdu_exists  := NULL;
        l_bro_pdu_exists  := NULL;
        l_pel_pdu_exists  := NULL;
        l_org_pdu_exists  := NULL;
        l_org_pva_exists  := NULL;
        l_rac_pdu_exists  := NULL;
        l_pdf_datatype    := NULL;
        l_pdf_datatype2   := NULL;
        l_pdf_datatype3   := NULL;
        l_par_type        := NULL;
        l_ban_bro_code    := NULL;
        l_pel_pgp_refno   := NULL;
        l_auel_pgp_refno  := NULL;
        l_ele_pro_refno   := NULL;
        l_ele_value_type  := NULL;
        l_ele_type        := NULL;
        l_ele_code_exists := NULL;
        l_propref_exists  := NULL;
        l_rac_exists      := NULL;
        l_rac_pva_exists  := NULL;
        l_chk_datatype    := NULL;
        l_chk_par_refno   := NULL;
        l_chk_par_alt_refno       := NULL;
        l_lebe_manual_created_ind := NULL;
        l_lebe_generated_ind      := NULL;
        l_orgtype         := NULL;
        l_orgtype_dt      := NULL;
        l_pdf_vru_id      := NULL;
        l_pgp_refno       := NULL;
        l_display_seqno   := NULL;
        l_param_type      := NULL;
        l_org_pva_exists2 := NULL;
        l_count_batch     := 0;
        l_org_info        := NULL;
        --
        --*********************
        -- Check the Links to Other Tables
        --
        -- Check the Definition Name is valid
        --
        l_pdu    := NULL;
        l_obj    := NULL;
        l_obj2   := NULL;
        l_obj3   := NULL;
        l_exists := NULL;
        l_dup_exists := NULL;
        --
        IF nvl(p1.lpva_pdu_pob_table_name,'BLANK')
                                      NOT IN('ADMIN_UNITS'
                                             ,'CONTRACTORS'
                                             ,'CONTRACTOR_SITES'
                                             ,'PARTIES'
                                             ,'LAND_TITLES'
                                             ,'PROPERTIES'
                                             ,'SERVICE_REQUESTS'
                                             ,'TENANCIES'
                                             ,'WORKS_ORDERS'
                                             ,'SURVEY_RESULTS'
                                             ,'CONTRACT_VERSIONS'
                                             ,'CONTRACT_TASKS'
                                             ,'DELIVERABLES'
                                             ,'ORGANISATION_CONTACTS'
                                             ,'INTERESTED_PARTIES'
                                             ,'BUSINESS_ACTIONS'
                                             ,'BUSINESS_ACTION_EVENTS'
                                             ,'ADVICE_CASE_HOUSING_OPTIONS'
                                             ,'PREVENTION_PAYMENTS'
                                             ,'REGISTERED_ADDRESS_LETTINGS'
                                             ,'ARREARS_ACTIONS'
                                             ,'PROPERTY_ELEMENTS'
                                             ,'ADMIN_UNIT_ELEMENTS'
                                             ,'TENANCIES2'
                                             ,'ORGANISATIONS'
                                             ,'REVENUE_ACCOUNTS'
                                             )
          THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',247);
         END IF;
        --
		--
        IF p1.lpva_pdu_pob_table_name NOT IN ('INTERESTED_PARTIES'
                                             ,'BUSINESS_ACTIONS'
                                             ,'BUSINESS_ACTION_EVENTS'
                                             ,'ADVICE_CASE_HOUSING_OPTIONS'
                                             ,'PREVENTION_PAYMENTS'
                                             ,'REGISTERED_ADDRESS_LETTINGS'
                                             ,'ARREARS_ACTIONS'
                                             ,'PROPERTY_ELEMENTS'
                                             ,'ADMIN_UNIT_ELEMENTS'
                                             ,'TENANCIES2'
                                             ,'ORGANISATIONS'
                                             ,'REVENUE_ACCOUNTS'
                                             )
         THEN
        --
        -- Check that the legacy ref has been supplied
        --
          IF p1.lpva_legacy_ref IS NULL
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',732);
          END IF;
        --
        -- Check that the Other Field Name has been supplied
        --
          IF p1.lpva_pdf_name IS NULL
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',733);
          END IF;
        --
        -- Check When The Object and Other Field Name have been supplied
        --  
          IF p1.lpva_pdf_name IS NOT NULL 
          AND p1.lpva_pdu_pob_table_name IS NOT NULL
          THEN
        --
        -- Check that the Other Field and Object Name combination exists
        -- in parameter definition usages
        --
            OPEN c_pdu(p1.lpva_pdf_name,p1.lpva_pdu_pob_table_name);
            FETCH c_pdu into l_exists;
             IF c_pdu%NOTFOUND
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',736);
             END IF;
            CLOSE c_pdu;
        --
        -- Get the parameter type so we can check the data type is correct
                -- l_obj is actually getting p1 lpva_pdu_pob_table_name but will be
                -- blank if not found
        --
            OPEN c_pdf(p1.lpva_pdf_name, p1.lpva_pdu_pob_table_name);
            FETCH c_pdf into l_obj;
            CLOSE c_pdf;
             IF l_obj IS NULL
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',735);
             END IF;
          END IF;
        END IF;
                --
        --*********************
                -- Check that they exist by getting the appropriate reusable refno
                -- using the legacy ref
                --
        IF l_obj = 'ADMIN_UNITS'
        THEN
          OPEN c_get_aun_refno(p1.lpva_legacy_ref);
          FETCH c_get_aun_refno into l_reusable_refno;
          CLOSE c_get_aun_refno;
           IF l_reusable_refno IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
           END IF;
                --
        ELSIF l_obj = 'ARREARS_ACTIONS'
        THEN
          OPEN c_get_aca_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref,p1.lpva_secondary_date);
          FETCH c_get_aca_refno into l_reusable_refno;
          CLOSE c_get_aca_refno;
           IF l_reusable_refno IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',941);
           END IF;
                --
        ELSIF l_obj = 'CONTRACTORS'
        THEN
          OPEN c_get_con_refno(p1.lpva_legacy_ref);
          FETCH c_get_con_refno into l_reusable_refno;
          CLOSE c_get_con_refno;
           IF l_reusable_refno IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',939);
           END IF;
                --
        ELSIF l_obj = 'CONTRACTOR_SITES'
        THEN
          OPEN c_get_cos_refno(p1.lpva_legacy_ref);
          FETCH c_get_cos_refno into l_reusable_refno;
          CLOSE c_get_cos_refno;
           IF l_reusable_refno IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',718);
           END IF;
                --
        ELSIF l_obj = 'PARTIES'
        THEN
          OPEN c_get_par_refno(p1.lpva_legacy_ref);
          FETCH c_get_par_refno into l_reusable_refno;
          CLOSE c_get_par_refno;
           IF l_reusable_refno IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',510);
           END IF;
                --
        ELSIF l_obj = 'LAND_TITLES'
        THEN
          OPEN c_get_ltl_refno(p1.lpva_legacy_ref);
          FETCH c_get_ltl_refno into l_reusable_refno;
          CLOSE c_get_ltl_refno;
           IF l_reusable_refno IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',48);
           END IF;
                --
        ELSIF l_obj = 'PROPERTIES'
        THEN
          OPEN c_get_pro_refno(p1.lpva_legacy_ref);
          FETCH c_get_pro_refno into l_reusable_refno;
          CLOSE c_get_pro_refno;
           IF l_reusable_refno IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
           END IF;
                --
        ELSIF l_obj = 'SERVICE_REQUESTS'
        THEN
          OPEN c_get_srq_refno(p1.lpva_legacy_ref);
          FETCH c_get_srq_refno into l_reusable_refno;
          CLOSE c_get_srq_refno;
           IF l_reusable_refno IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',693);
           END IF;
                --
        ELSIF l_obj = 'TENANCIES'
        THEN
          OPEN c_get_tcy_refno(p1.lpva_legacy_ref);
          FETCH c_get_tcy_refno into l_reusable_refno;
          CLOSE c_get_tcy_refno;
           IF l_reusable_refno IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',080);
           END IF;
                --
        ELSIF l_obj = 'WORKS_ORDERS'
        THEN
          OPEN c_get_wor_refno(p1.lpva_legacy_ref);
          FETCH c_get_wor_refno into l_reusable_refno;
          CLOSE c_get_wor_refno;
           IF l_reusable_refno IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',938);
           END IF;
                --
        ELSIF l_obj = 'SURVEY_RESULTS'
        THEN
          OPEN c_get_srt_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref,p1.lpva_further_ref);
          FETCH c_get_srt_refno into l_reusable_refno;
          CLOSE c_get_srt_refno;
           IF l_reusable_refno IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',047);
           END IF;
                --
        ELSIF l_obj = 'CONTRACT_VERSIONS'
        THEN
          OPEN c_get_cve_refno(p1.lpva_legacy_ref, p1.lpva_secondary_ref );      
          FETCH c_get_cve_refno into l_reusable_refno;      
          CLOSE c_get_cve_refno;      
           IF l_reusable_refno IS NULL       
            THEN      
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',507);      
           END IF;
        --                 
        ELSIF l_obj = 'CONTRACT_TASKS' 
        THEN
          OPEN  c_get_tve_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref,p1.lpva_further_ref,p1.lpva_further_ref2,p1.lpva_further_ref3);
          FETCH c_get_tve_refno into l_reusable_refno;
          CLOSE c_get_tve_refno;
           IF l_reusable_refno IS NULL       
            THEN      
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',507);      
           END IF;
        --                 
        ELSIF l_obj = 'DELIVERABLES' 
        THEN
        --
        -- Get the pro_refno or use Admin Unit Code supplied
        --
          IF p1.lpva_further_ref = 'P' 
          THEN
            l_pro_aun_code := s_properties.get_refno_for_propref(p1.lpva_secondary_ref);
          ELSE
            l_pro_aun_code := p1.lpva_secondary_ref;
          END IF;
        --
          OPEN c_get_dve_refno(p1.lpva_legacy_ref,l_pro_aun_code,p1.lpva_further_ref,p1.lpva_further_ref2);
          FETCH c_get_dve_refno into l_reusable_refno;
          CLOSE c_get_dve_refno;
        --
          IF l_reusable_refno IS NULL 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',569);
          END IF;
        --                 
        ELSIF l_obj = 'ORGANISATION_CONTACTS' 
        THEN
        --
        -- check specific mandatory fields for these records
        --
          IF (p1.lpva_secondary_date IS NULL) 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',12);
          END IF;
                --
          IF (p1.lpva_further_ref2 IS NULL) 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',13);
          END IF;
                --
          IF (p1.lpva_further_ref3 IS NULL) 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',14);
          END IF;
        --
        -- Check the Organisation Exists using legacy ref and getting par_refno needed to check
        -- that Contact is linked to organisation
        --
          IF (p1.lpva_legacy_ref IS NOT NULL)
           THEN
		    IF UPPER(NVL(p1.lpva_secondary_ref,'BLANK')) = 'REFNO'
		     THEN
              OPEN  c_get_org_refno(p1.lpva_legacy_ref);
              FETCH c_get_org_refno into l_chk_par_refno;
              CLOSE c_get_org_refno;
            ELSE
              OPEN c_get_org_alt_refno(p1.lpva_legacy_ref);
              FETCH c_get_org_alt_refno into l_chk_par_alt_refno;
              CLOSE c_get_org_alt_refno;
            END IF;
        --
            IF (l_chk_par_refno IS NULL AND l_chk_par_alt_refno IS NULL) 
             THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',938);
            END IF;
        --
            IF (l_chk_par_refno IS NOT NULL OR l_chk_par_alt_refno IS NOT NULL)
             THEN
              IF  (p1.lpva_secondary_date IS NOT NULL
               AND p1.lpva_further_ref2   IS NOT NULL
               AND p1.lpva_further_ref3   IS NOT NULL)                  
               THEN               
                OPEN c_get_oco_refno(nvl(l_chk_par_refno,l_chk_par_alt_refno)
                                        ,p1.lpva_further_ref2
                                        ,p1.lpva_further_ref3
                                        ,p1.lpva_secondary_date);
                FETCH c_get_oco_refno into l_reusable_refno;
                CLOSE c_get_oco_refno;
        --                
                IF l_reusable_refno IS NULL 
                 THEN
                 l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',937);
                END IF;
              END IF;
            END IF;
          END IF;
        --
        END IF;
        --              
        --*****************************************************
        -- Get the data type (pdf_datatype) of the Other Field so we 
        -- can check that the right value has been provided
        -- THIS DATA CHECK FOR PGP ONES AT BOTTOM OF VALIDATE
        --
        IF l_obj IN ('DELIVERABLES'
                    ,'CONTRACT_TASKS'
                    ,'CONTRACT_VERSIONS'
                    ,'SURVEY_RESULTS'
                    ,'WORKS_ORDERS'
                    ,'TENANCIES'
                    ,'SERVICE_REQUESTS'
                    ,'PROPERTIES'
                    ,'LAND_TITLES'
                    ,'PARTIES'
                    ,'CONTRACTOR_SITES'
                    ,'CONTRACTORS'
                    ,'ARREARS_ACTIONS'
                    ,'ADMIN_UNITS'
                    ,'ORGANISATION_CONTACTS' 
                    )
         THEN
        --
        -- Firstly Check that correct field for data type has been provided
        --              
          l_pdf_datatype2    := NULL;
                --
          OPEN c_pdf_datatype(p1.lpva_pdf_name,p1.lpva_pdu_pob_table_name);
          FETCH c_pdf_datatype into l_pdf_datatype2;
          CLOSE c_pdf_datatype;
        --
        -- Check the pdf_datatype matches the values provided
        --
          IF l_pdf_datatype2 IN ('YN','TEXT','CODED')
          AND p1.lpva_char_value IS NULL 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',740);
          END IF;
                --
          IF l_pdf_datatype2 IN ('NUMERIC')
          AND p1.lpva_number_value IS NULL 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',741);
          END IF;
                --
          IF l_pdf_datatype2 IN ('DATE')
          AND p1.lpva_date_value IS NULL 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',742);
          END IF;
                --
          IF l_pdf_datatype2 IN ('YN','TEXT','CODED','DATE')
          AND p1.lpva_number_value IS NOT NULL 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',743);
          END IF;
                --
          IF l_pdf_datatype2 IN ('YN','TEXT','CODED','NUMERIC')
          AND p1.lpva_date_value IS NOT NULL 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',743);
          END IF;
                --
          IF l_pdf_datatype2 IN ('DATE','NUMERIC')
          AND p1.lpva_char_value IS NOT NULL 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',743);
          END IF;
                --
          IF l_pdf_datatype2 = 'YN'
          AND p1.lpva_char_value IS NOT NULL
          AND p1.lpva_char_value NOT IN ('Y','N') 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',925);
          END IF;
                --
        END IF; -- end of data type check
        --*****************************************************************
        -- This section checks the 2 mandatory fields that are required for
        -- all BUT this is for Non PGPs that have separate sections below
        --*****************************************************************
        IF p1.lpva_pdu_pob_table_name IN ('INTERESTED_PARTIES'
                                          ,'BUSINESS_ACTIONS'
                                          ,'BUSINESS_ACTION_EVENTS'
                                          ,'ADVICE_CASE_HOUSING_OPTIONS'
                                          ,'PREVENTION_PAYMENTS'
                                          ,'REGISTERED_ADDRESS_LETTINGS'
                                          ,'ARREARS_ACTIONS'
                                          ,'PROPERTY_ELEMENTS'
                                          ,'ADMIN_UNIT_ELEMENTS'
                                          ,'TENANCIES2'
                                          ,'ORGANISATIONS'
                                          ,'REVENUE_ACCOUNTS'
                                          )
         THEN
        --
        -- Check that the legacy ref has been supplied
        --
          IF p1.lpva_legacy_ref IS NULL
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',248);
          END IF;
        --
        -- Check that the Other Field Name has been supplied
        --
          IF p1.lpva_pdf_name IS NULL
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',250);
          END IF;
        --
        END IF;
        --*********************
        --*********************
        -- This has been added for Other Fields against Revenue Accounts (v6.9.0)
        -- and is for the APEX v6 front end only
        --
        -- For REVENUE_ACCOUNTS
        -- 1) we have to get the pgp_refno assigned to the OFAT_CODE(Account Type Code)
        --    in OTHER_FIELDS_ACCOUNT_TYPES table populated at v6.9.0 install
        -- 2) The other fields will be held at account type level
        -- 3) put reusable ref into dl_mad_other_field_values (l_lebe_reusable_refno)
        --    so it can be used for DELETE
        --
        IF p1.lpva_pdu_pob_table_name = 'REVENUE_ACCOUNTS'
        THEN
        --
        -- Check that the Date Number or text value has been supplied
        --
          IF p1.lpva_date_value IS NULL
          AND p1.lpva_number_value IS NULL
          AND p1.lpva_char_value IS NULL
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',739);
          END IF;
        --
        -- Check that the Secondary Ref (RAC_HRV_ATE_CODE) has been supplied
        --
          IF p1.lpva_secondary_ref IS NULL
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',745);
          END IF;
        --
        -- Check that the Account Type (RAC_HRV_ATE_CODE) is valid
        --
          IF p1.lpva_secondary_ref IS NOT NULL
           THEN
            OPEN c_get_ofvat_pgp_refno(p1.lpva_secondary_ref);
            FETCH c_get_ofvat_pgp_refno INTO l_ofvat_pgp_refno;
             IF c_get_ofvat_pgp_refno%NOTFOUND
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',746);
             END IF;  
            CLOSE c_get_ofvat_pgp_refno;
        --
        -- Check that the rac_pay_ref (legacy_ref) exists
        --
            l_rac_exists := NULL;
                --
            IF p1.lpva_legacy_ref IS NOT NULL
            THEN
              OPEN c_chk_rac_refno(p1.lpva_legacy_ref);
              FETCH c_chk_rac_refno INTO l_rac_exists;
               IF c_chk_rac_refno%NOTFOUND
                THEN
                 l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',747);
               END IF;  
              CLOSE c_chk_rac_refno;
            END IF;
        --
        -- Check that the rac_pay_ref and account type combination exists
        --
            l_reusable_refno := NULL;
                --
            IF p1.lpva_legacy_ref IS NOT NULL
            AND p1.lpva_secondary_ref IS NOT NULL
             THEN
              OPEN c_get_rac_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref);
              FETCH c_get_rac_refno INTO l_reusable_refno;
               IF c_get_rac_refno%NOTFOUND
                THEN
                 l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',748);
               END IF;  
              CLOSE c_get_rac_refno;
            END IF;
        --
        -- Check parameter definition usages to make sure the Other Field Name
        -- and Object Combination is correct
        --
            IF p1.lpva_pdf_name IS NOT NULL
            AND l_ofvat_pgp_refno IS NOT NULL
             THEN
                --
              l_rac_pdu_exists := NULL;
                --
              OPEN c_chk_rac_pdu_exist(p1.lpva_pdf_name,l_ofvat_pgp_refno);
              FETCH c_chk_rac_pdu_exist INTO l_rac_pdu_exists;
               IF c_chk_rac_pdu_exist%NOTFOUND
                THEN
                 l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',736);
               END IF;
              CLOSE c_chk_rac_pdu_exist; 
        --
        -- Get the data type (pdf_datatype) of the Other Field 
        --
              IF l_rac_pdu_exists IS NOT NULL
               THEN      
                l_pdf_datatype := NULL;
                --
                OPEN c_rac_pdf_datatype(p1.lpva_pdf_name,l_ofvat_pgp_refno);
                FETCH c_rac_pdf_datatype INTO l_pdf_datatype;
                CLOSE c_rac_pdf_datatype;
        --
        -- Check the pdf_datatype matches the values provided
        --
                IF l_pdf_datatype IN ('YN','TEXT','CODED')
              AND p1.lpva_char_value IS NULL 
                 THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',740);
                END IF;
                --
                IF l_pdf_datatype IN ('NUMERIC')
              AND p1.lpva_number_value IS NULL
                 THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',741);
                END IF;
                --
                IF l_pdf_datatype IN ('DATE')
              AND p1.lpva_date_value IS NULL 
                 THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',742);
                END IF;
                --
                IF l_pdf_datatype IN ('YN','TEXT','CODED','DATE')
              AND p1.lpva_number_value IS NOT NULL 
                 THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',743);
                END IF;
                --
                IF l_pdf_datatype IN ('YN','TEXT','CODED','NUMERIC')
              AND p1.lpva_date_value IS NOT NULL 
                 THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',743);
                END IF;
                --
                IF l_pdf_datatype IN ('DATE','NUMERIC')
              AND p1.lpva_char_value IS NOT NULL 
                 THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',743);
                END IF;
                --
                IF l_pdf_datatype = 'YN'
              AND p1.lpva_char_value IS NOT NULL
              AND p1.lpva_char_value NOT IN ('Y','N') 
                 THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',925);
                END IF;
                --
              END IF; -- end of data type check
            END IF; -- end if pdf name supplied and pgp refno found
        --
        -- Check to see if the Other Field record already exists in parameter_values
        --
            l_rac_pva_exists := NULL;
            OPEN c_chk_rac_dup(l_reusable_refno,p1.lpva_pdf_name,l_ofvat_pgp_refno);
            FETCH c_chk_rac_dup into l_rac_pva_exists;
             IF c_chk_rac_dup%FOUND
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',749);
             END IF;
            CLOSE c_chk_rac_dup;
          END IF; -- end of checks if reusable has been found   
        END IF; -- end of revenue accounts Section
        --
        --************************************
        -- This has been added for Other Fields against Organisations (v6.9.0.1)
        -- and is for the APEX v6 front end only
        --
        -- For ORGANISATIONS
        -- 1) The Name, Short Name and Organisation Type (optional) to find the organisation 
        --    as the system accepts duplicates so for WA these are for info only and not used
        --    in any checks creates or deletes (see 2 below).
        -- 2) For WA the par_per_alt_ref has been included as the driver and must be supplied
        --    use this only.
        -- 3) check exist and datatype with value fields
        -- 4) check if Other Field already exists in Parameter values error if found
        -- 5) We now need to check what level these are held against on Organisations as they
		--    need different information HEM_ORG_OTHERFLDS = TABLE (no Change) = ORGTYPE (pgp)
		--    pgp initially from organisation_type_param_groups then pramter_definition_usages
		-- 6) Also allow the use of the par_refno ala ORGANISATION_CONTACTS
		--
        --
        IF p1.lpva_pdu_pob_table_name = 'ORGANISATIONS'
        THEN
        --
        -- Check that the Date Number or text value has been supplied
        --
          IF p1.lpva_date_value IS NULL
          AND p1.lpva_number_value IS NULL
          AND p1.lpva_char_value IS NULL
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',739);
          END IF;
        -- 
        -- Check parameter definition usages to make sure the Other Field Name
        -- and Object Combination is correct
        --
          IF p1.lpva_pdf_name IS NOT NULL
           THEN
            l_org_pdu_exists := NULL;
        --
            IF nvl(l_orgtype_param,'TABLE') = 'TABLE'
		     THEN
              OPEN c_pdu(p1.lpva_pdf_name, p1.lpva_pdu_pob_table_name);
              FETCH c_pdu into l_org_pdu_exists;
              IF c_pdu%NOTFOUND
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',736);
              END IF;
              CLOSE c_pdu; 
        --
        -- Get the data type (pdf_datatype) of the Other Field 
        --
              IF l_org_pdu_exists IS NOT NULL
               THEN        
                l_pdf_datatype    := NULL;
                OPEN c_pdf_datatype(p1.lpva_pdf_name,p1.lpva_pdu_pob_table_name);
                FETCH c_pdf_datatype into l_pdf_datatype;
                CLOSE c_pdf_datatype;
            END IF;
        --
            IF nvl(l_orgtype_param,'TABLE') = 'ORGTYPE'
		     THEN
              OPEN c_pdf_datatype2(p1.lpva_pdf_name);
              FETCH c_pdf_datatype2 into l_orgtype_dt,l_pdf_vru_id;
              CLOSE c_pdf_datatype2;			  
              IF (l_orgtype_dt IS NULL)
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',241);
              END IF;
              l_pdf_datatype:= NULL;
              l_pdf_datatype:= l_orgtype_dt;			  
            END IF;			 
        --
        -- Check the pdf_datatype matches the values provided
        --
              IF l_pdf_datatype IN ('YN','TEXT','CODED')
            AND p1.lpva_char_value IS NULL 
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',740);
              END IF;
                --
              IF l_pdf_datatype IN ('NUMERIC')
            AND p1.lpva_number_value IS NULL 
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',741);
              END IF;
                --
              IF l_pdf_datatype IN ('DATE')
            AND p1.lpva_date_value IS NULL 
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',742);
              END IF;
                --
              IF l_pdf_datatype IN ('YN','TEXT','CODED','DATE')
            AND p1.lpva_number_value IS NOT NULL 
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',743);
              END IF;
                --
              IF l_pdf_datatype IN ('YN','TEXT','CODED','NUMERIC')
            AND p1.lpva_date_value IS NOT NULL 
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',743);
              END IF;
                --
              IF l_pdf_datatype IN ('DATE','NUMERIC')
            AND p1.lpva_char_value IS NOT NULL 
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',743);
              END IF;
                --
                IF l_pdf_datatype = 'YN'
              AND p1.lpva_char_value IS NOT NULL
              AND p1.lpva_char_value NOT IN ('Y','N') 
                 THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',925);
                END IF;
                --
            END IF; -- end of data type check
        --
        -- Check that there are duplicates in data load batch
        --
            OPEN c_chk_dup_batch(p1.lpva_legacy_ref,p1.lpva_pdf_name,p1.lpva_dlb_batch_id);
            FETCH c_chk_dup_batch into l_count_batch;
            CLOSE c_chk_dup_batch;
            IF l_count_batch > 1
             THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',249);
            END IF;
          END IF; -- end if pdf name has been supplied
        --
        -- Check that the par_per_alt_ref (legacy_ref) exists
        --
          l_reusable_refno := NULL;
          l_orgtype        := NULL;
          l_par_type       := NULL;
          l_obj3           := NULL;
          l_pgp_refno      := NULL;
          l_display_seqno  := NULL;
          l_param_type     := NULL;
          l_org_pva_exists := NULL;
        --
          IF p1.lpva_legacy_ref IS NOT NULL
           THEN
        --
		   IF UPPER(NVL(p1.lpva_further_ref,'BLANK')) = 'REFNO'
		    THEN
             OPEN c_chk_orgtype_refno(p1.lpva_legacy_ref);
             FETCH c_chk_orgtype_refno into l_reusable_refno, l_orgtype, l_par_type;
             CLOSE c_chk_orgtype_refno;
           ELSE
             OPEN c_chk_orgtype_alt_refno(p1.lpva_legacy_ref);
             FETCH c_chk_orgtype_alt_refno into l_reusable_refno, l_orgtype, l_par_type;
             CLOSE c_chk_orgtype_alt_refno;
           END IF;
        --
            IF l_reusable_refno IS NULL
             THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',737);
            END IF;
        --
            IF l_reusable_refno IS NOT NULL
             THEN
              IF l_par_type NOT IN ('ORG')
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',738);
              END IF;
            END IF;        		
        --		
            IF nvl(l_orgtype_param,'TABLE') = 'TABLE'
		     THEN
              IF (p1.lpva_pdf_name IS NOT NULL  AND p1.lpva_pdu_pob_table_name IS NOT NULL)
               THEN	
                OPEN c_obj(p1.lpva_pdf_name, p1.lpva_pdu_pob_table_name);
                FETCH c_obj INTO l_obj3,l_pgp_refno,l_display_seqno,l_param_type;
                CLOSE c_obj;
                IF (l_obj3 IS NULL)
                  THEN
                   l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',243);
                END IF;
              END IF;
        --
              IF l_reusable_refno IS NOT NULL
               THEN
        --
        -- Check to see if the Other Field record already exists in parameter_values
        --
               IF p1.lpva_pdf_name IS NOT NULL
                THEN
                 l_org_pva_exists := NULL;
                 OPEN c_chk_org_dup(l_reusable_refno,p1.lpva_pdf_name);
                 FETCH c_chk_org_dup into l_org_pva_exists;
                 IF c_chk_org_dup%FOUND
                  THEN
                   l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',246);
                 END IF;
                 CLOSE c_chk_org_dup;			   
               END IF;
              END IF; -- end of checks if reusable has been found
        --
            END IF;  -- end of TABLE check
        --
		    IF nvl(l_orgtype_param,'TABLE') = 'ORGTYPE'
		     THEN
              IF (l_reusable_refno IS NOT NULL AND l_orgtype IS NULL)
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',242);
              END IF;
        --			 
              IF (p1.lpva_pdf_name IS NOT NULL  AND l_orgtype IS NOT NULL)
               THEN			 
                OPEN c_obj_orgtype(p1.lpva_pdf_name, l_orgtype);
                FETCH c_obj_orgtype INTO l_obj3,l_pgp_refno,l_display_seqno,l_param_type;
                CLOSE c_obj_orgtype;
        --
                IF (l_obj3 IS NULL)
                  THEN
                   l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',244);
                END IF;
              END IF;
              --
              IF l_reusable_refno IS NOT NULL
               THEN
        --
        -- Check to see if the Other Field record already exists in parameter_values
        --
               IF p1.lpva_pdf_name IS NOT NULL AND l_orgtype IS NOT NULL
                THEN
                 l_org_pva_exists2 := NULL;
                 OPEN c_chk_dup_orgtype(p1.lpva_pdf_name,l_orgtype,l_reusable_refno);
                 FETCH c_chk_dup_orgtype into l_org_pva_exists2;
                 IF c_chk_dup_orgtype%FOUND
                  THEN
                   l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',245);
                 END IF;
                 CLOSE c_chk_dup_orgtype;			   
               END IF;
              END IF;
            END IF;  -- end of ORGTYPE check
          END IF;  -- end of lpva_legacy_ref
        END IF; -- end of Organisations Section
        --
        --*****************************************
        -- added as Tenancies using the tcy_alt_ref the table name supplied is TENANCIES2
        -- and need to allow for this still checking and loading against TENANCIES table
        -- added further checks Aj 23Jun2016
                -- 1) mandatory fields check if object name provided
        -- 2) check if exists and datatype and value fields provided
        -- 3) check if Other Field already exists in Parameter values error if found
        --
        IF p1.lpva_pdu_pob_table_name ='TENANCIES2'
         THEN
          l_obj2   := 'TENANCIES';
          l_exists := NULL;
        --
        -- Check that the Date Number or text value has been supplied
        --
          IF p1.lpva_date_value IS NULL
          AND p1.lpva_number_value IS NULL
          AND p1.lpva_char_value IS NULL
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',739);
          END IF;
        --
        -- Get and check the parameter combination exits in parameter definitions
        --
          IF p1.lpva_pdf_name IS NOT NULL
           THEN           
            OPEN c_pdu(p1.lpva_pdf_name,l_obj2);
            FETCH c_pdu into l_exists;
             IF c_pdu%NOTFOUND
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',940);
             END IF;
            CLOSE c_pdu;
          END IF;         
        --
        -- Get the parameter type so we can check the data type is correct
        --
          IF l_exists IS NOT NULL
           THEN
            OPEN  c_pdf(p1.lpva_pdf_name, l_obj2);
            FETCH c_pdf into l_obj;
            CLOSE c_pdf;
             IF l_obj IS NULL
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',933);
             END IF;
        --
            IF l_obj IN ('TENANCIES')   
             THEN
        --
        -- Firstly Check that correct field for data type has been provided
        --              
              l_pdf_datatype3    := NULL;
                --
              OPEN c_pdf_datatype(p1.lpva_pdf_name,l_obj);
              FETCH c_pdf_datatype into l_pdf_datatype3;
              CLOSE c_pdf_datatype;
        --
        -- Check the pdf_datatype matches the values provided
        --
              IF l_pdf_datatype3 IN ('YN','TEXT','CODED')
              AND p1.lpva_char_value IS NULL 
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',740);
              END IF;
                --
              IF l_pdf_datatype3 IN ('NUMERIC')
              AND p1.lpva_number_value IS NULL 
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',741);
              END IF;
                --
              IF l_pdf_datatype3 IN ('DATE')
              AND p1.lpva_date_value IS NULL 
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',742);
              END IF;
                --
              IF l_pdf_datatype3 IN ('YN','TEXT','CODED','DATE')
              AND p1.lpva_number_value IS NOT NULL 
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',743);
              END IF;
                --
              IF l_pdf_datatype3 IN ('YN','TEXT','CODED','NUMERIC')
              AND p1.lpva_date_value IS NOT NULL 
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',743);
              END IF;
                --
              IF l_pdf_datatype3 IN ('DATE','NUMERIC')
              AND p1.lpva_char_value IS NOT NULL 
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',743);
              END IF;
                --
              IF l_pdf_datatype3 = 'YN'
              AND p1.lpva_char_value IS NOT NULL
              AND p1.lpva_char_value NOT IN ('Y','N') 
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',925);
              END IF;
                --
            END IF; -- end of data type check
          END IF; -- l_exists
                --
          IF p1.lpva_legacy_ref IS NOT NULL
           THEN
            OPEN c_get_tcy_refno2(p1.lpva_legacy_ref);
            FETCH c_get_tcy_refno2 into l_reusable_refno;
            CLOSE c_get_tcy_refno2;
             IF l_reusable_refno IS NULL
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',080);
             END IF;
          END IF;
        --                
          IF l_reusable_refno IS NOT NULL
           THEN            
            OPEN c_chk_for_dup(l_pel_reusable_refno,p1.lpva_pdf_name);
            FETCH c_chk_for_dup INTO l_dup_exists;
            CLOSE  c_chk_for_dup;
             IF l_dup_exists IS NOT NULL
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',744);
             END IF;
          END IF;  -- duplicate check
        END IF;  -- end of TENANCIES2
        --
                --*******************************************
        -- For interested parties
        -- 1) mandatory fields check if object name provided
        -- 2) we have to get the pgp_refno assigned to the ipt_code
        -- 3) Check the parameter definition usages record exists for parameter name
        --    pgp_refno
        -- 4) get reusable_refno for ip based on shortname and ipt_code
        -- 5) check if Other Field already exists in Parameter values error if found
        --
        IF p1.lpva_pdu_pob_table_name = 'INTERESTED_PARTIES'
         THEN
                  l_chk_datatype :='INTERESTED_PARTIES';
        --
        -- Check that the Date Number or text value has been supplied
        --
          IF p1.lpva_date_value IS NULL
          AND p1.lpva_number_value IS NULL
          AND p1.lpva_char_value IS NULL
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',739);
          END IF;
        --
        -- Check that the Secondary Ref has been supplied
        --
          IF p1.lpva_secondary_ref IS NULL
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',926);
          END IF;
        --
          OPEN c_get_ipt_pgp_refno(p1.lpva_secondary_ref);
          FETCH c_get_ipt_pgp_refno INTO l_ipt_pgp_refno;   
          CLOSE c_get_ipt_pgp_refno;
        --                
          IF l_ipt_pgp_refno IS NULL
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',635);
          ELSE
            OPEN c_get_pdu_details(p1.lpva_pdf_name,l_ipt_pgp_refno);
            FETCH c_get_pdu_details INTO l_ipt_pdu_exists;
            CLOSE c_get_pdu_details;
             IF l_ipt_pdu_exists IS NULL    
             THEN    
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',636);    
             END IF;    
          END IF;
                --
          IF (p1.lpva_secondary_ref IS NOT NULL AND p1.lpva_legacy_ref IS NOT NULL)
           THEN
            OPEN c_get_ipp_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref);
            FETCH c_get_ipp_refno into l_reusable_refno;
            CLOSE c_get_ipp_refno;
             IF l_reusable_refno IS NULL    
              THEN   
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',570);   
             END IF;   
          END IF;                 
        END IF;  -- end of interested parties check
        --
                --*******************************************
        -- For Advice Case Housing Options
        -- 1) Check that advice case housing option record exists for legacy ref supplied
        -- 2) we have to get the pgp_refno assigned to the hoop_code
        -- 3) Check the parameter definition usages record exists for parameter name
        --    pgp_refno
        -- 4) Check that lpva_further_ref2(l_lebe_manual_created_ind) and lpva_further_ref
        --    (l_lebe_generated_ind) have been supplied as mandatory and the combination must
        --    be either (Y and N) OR (N and Y)
        --
        IF p1.lpva_pdu_pob_table_name = 'ADVICE_CASE_HOUSING_OPTIONS'
         THEN
          l_acho_reference := NULL;
        --
          IF p1.lpva_legacy_ref IS NOT NULL  THEN                  
           OPEN chk_acho_exists(p1.lpva_legacy_ref);
           FETCH chk_acho_exists INTO l_acho_reference;
           CLOSE chk_acho_exists;
            IF l_acho_reference IS NULL
             THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',185);
            END IF;
          END IF;
        --
        -- Check that the Secondary Ref has been supplied and validate
        --
          IF p1.lpva_secondary_ref IS NULL
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',926);
          END IF;
        --
                  IF p1.lpva_secondary_ref IS NOT NULL  THEN
           OPEN c_get_hoop_pgp_refno(p1.lpva_secondary_ref);
           FETCH c_get_hoop_pgp_refno INTO l_hoop_pgp_refno;
           CLOSE c_get_hoop_pgp_refno;
            IF l_hoop_pgp_refno IS NULL 
             THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',638);
            ELSE
             OPEN c_get_pdu_details(p1.lpva_pdf_name,l_hoop_pgp_refno);
             FETCH c_get_pdu_details INTO l_hoop_pdu_exists;
             CLOSE c_get_pdu_details;
              IF l_hoop_pdu_exists IS NULL
               THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',639);
              END IF;
            END IF;
          END IF;
                --
          IF p1.lpva_further_ref2 IS NULL
           THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',804);
           END IF;
                --
          IF p1.lpva_further_ref IS NULL
           THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',805);
           END IF;
                --
          IF p1.lpva_further_ref IS NOT NULL  
          AND p1.lpva_further_ref2 IS NOT NULL     
           THEN
            l_lebe_manual_created_ind := SUBSTR(p1.lpva_further_ref2,1,1);
            l_lebe_generated_ind := SUBSTR(p1.lpva_further_ref,1,1);
                --
            IF l_lebe_manual_created_ind = 'Y'  
            AND l_lebe_generated_ind     != 'N'
             THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',806);
            END IF;
            IF l_lebe_manual_created_ind = 'N'  
            AND l_lebe_generated_ind     != 'Y' 
             THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',806);
            END IF;
          END IF;
        END IF;
        --
                --*******************************************
        -- For business actions
        -- 1) we have to get the action pgp_refno assigned to the bro_code
        -- 2) Check the parameter definition usages record exists for parameter name
        --    pgp_refno
        -- 3) get reusable_refno for ban based on ban_reference
        --
        IF p1.lpva_pdu_pob_table_name = 'BUSINESS_ACTIONS' 
         THEN
          OPEN c_get_ban_bro_code(p1.lpva_legacy_ref);
          FETCH c_get_ban_bro_code into l_ban_bro_code;
          CLOSE c_get_ban_bro_code;
           IF l_ban_bro_code IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',571);
           ELSE
            OPEN c_get_bro_pgp_refno(l_ban_bro_code);    
            FETCH c_get_bro_pgp_refno INTO l_bro_pgp_refno;    
            CLOSE c_get_bro_pgp_refno;    
             IF l_bro_pgp_refno IS NULL
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',640);
             ELSE
              OPEN c_get_pdu_details(p1.lpva_pdf_name,l_bro_pgp_refno);
              FETCH c_get_pdu_details INTO l_bro_pdu_exists;
              CLOSE c_get_pdu_details;
               IF l_bro_pdu_exists IS NULL
                THEN
                 l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',641);
               END IF;
             END IF;
           END IF;
        END IF;
        --
                --*******************************************
        -- For business action events
        -- 1) we have to get the pgp_refno assigned to the aet_code
        -- 2) Check the parameter definition usages record exists for parameter name
        --    pgp_refno
        -- 3) get reusable_refno for bae based on ban_reference, aet_code, status_date
        --
        IF p1.lpva_pdu_pob_table_name = 'BUSINESS_ACTION_EVENTS'
        THEN
          OPEN c_get_aet_pgp_refno(p1.lpva_secondary_ref);
          FETCH c_get_aet_pgp_refno INTO l_aet_pgp_refno;
          CLOSE c_get_aet_pgp_refno;
                --
          IF l_aet_pgp_refno IS NULL
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',642);
          ELSE
            OPEN c_get_pdu_details(p1.lpva_pdf_name,l_aet_pgp_refno);
            FETCH c_get_pdu_details INTO l_aet_pdu_exists;
            CLOSE c_get_pdu_details;
             IF l_aet_pdu_exists IS NULL
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',643);
             END IF;
          END IF;
                --
          OPEN c_get_bae_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref,p1.lpva_secondary_date,p1.lpva_further_ref);
          FETCH c_get_bae_refno into l_reusable_refno;
          CLOSE c_get_bae_refno;
           IF l_reusable_refno IS NULL 
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',573);
           END IF;
        END IF;
        --
                --*******************************************
        -- For Prevention Payments
        -- 1) Check that Prevention Payment record exists for legacy ref supplied
        -- 2) we have to get the pgp_refno assigned to the ppyt_pptp_code
        -- 3) Check the parameter definition usages record exists for parameter name
        --    pgp_refno
        --
        IF p1.lpva_pdu_pob_table_name = 'PREVENTION_PAYMENTS'
         THEN
          l_ppyt_reference  := NULL;
          l_ppyt_pgp_refno  := NULL;
          l_ppyt_pdu_exists := NULL;
                --
          OPEN chk_ppyt_exists(p1.lpva_legacy_ref);
          FETCH chk_ppyt_exists INTO l_ppyt_reference;
          CLOSE chk_ppyt_exists;
           IF l_ppyt_reference IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',660);
           END IF;
                --
          IF l_ppyt_reference IS NOT NULL 
           THEN
            OPEN c_get_ppyt_pgp_refno(p1.lpva_legacy_ref);
            FETCH c_get_ppyt_pgp_refno INTO l_ppyt_pgp_refno;
            CLOSE c_get_ppyt_pgp_refno;
                --
             IF l_ppyt_pgp_refno IS NULL 
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',661);
             ELSE
              OPEN c_get_pdu_details(p1.lpva_pdf_name,l_ppyt_pgp_refno);   
              FETCH c_get_pdu_details INTO l_ppyt_pdu_exists;   
              CLOSE c_get_pdu_details;   
               IF l_ppyt_pdu_exists IS NULL 
                THEN
                 l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',662);
               END IF;
             END IF; -- l_ppyt_pgp_refno IS NOT NULL
          END IF; -- l_ppyt_reference IS NOT NULL
        END IF;
        --
                --*******************************************
        -- For Registered Address Lettings
        -- 1) Check Registered Address Lettings record exists
        -- 2) we have to get the pgp_refno assigned to the HOOP_CODE (Housing Options Code)
        -- 3) Check the parameter definition usages record exists for parameter name
        --    pgp_refno
        --
        IF p1.lpva_pdu_pob_table_name = 'REGISTERED_ADDRESS_LETTINGS'
         THEN
          l_real_refno      := NULL;
          l_real_pgp_refno  := NULL;
          l_real_pdu_exists := NULL;
                --
          OPEN chk_real_exists(p1.lpva_legacy_ref);
          FETCH chk_real_exists INTO l_real_refno;
          CLOSE chk_real_exists;    
           IF l_real_refno IS NULL 
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',572);
           ELSE
            OPEN c_get_real_pgp_refno(p1.lpva_legacy_ref);
            FETCH c_get_real_pgp_refno INTO l_real_pgp_refno;
            CLOSE c_get_real_pgp_refno;
             IF l_real_pgp_refno IS NULL
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',663);
             ELSE
              OPEN c_get_pdu_details(p1.lpva_pdf_name,l_real_pgp_refno);
              FETCH c_get_pdu_details INTO l_real_pdu_exists;
              CLOSE c_get_pdu_details;
               IF l_real_pdu_exists IS NULL
                THEN
                 l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',664);
               END IF;
             END IF; -- l_real_pgp_refno IS NOT NULL
           END IF; -- l_real_refno IS NULL
        END IF;
                --
        --*******************************************
        -- First Check that the Element Type is a suitable one for an other Field to
        -- be linked to
        -- So ele_code must be supplied. must be of type of 'PR' and ele_value_type
        -- of D C N (Multi-Value element ele_value_type M allowed from 12Feb2014)
        IF p1.lpva_pdu_pob_table_name = 'PROPERTY_ELEMENTS'
        THEN            
        --
          l_ele_code_exists:= NULL;
                --
          IF p1.lpva_secondary_ref IS NULL
           THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',662);
          ELSE
            l_ele_code_exists := NULL;
                -- 
            OPEN c_chk_ele_exists(p1.lpva_secondary_ref);
            FETCH c_chk_ele_exists INTO l_ele_code_exists;
            CLOSE c_chk_ele_exists;
             IF l_ele_code_exists IS NULL
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',662);
             ELSE
              OPEN  c_chk_ele_code(p1.lpva_secondary_ref);
              FETCH c_chk_ele_code INTO l_pel_pgp_refno,l_ele_value_type,l_ele_type;
              CLOSE c_chk_ele_code;
               IF l_ele_type != 'PR'
                THEN
                 l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',981);
               END IF;
             END IF;
        --
        -- OK, now check if this Other Field links to the Element Type
        --
            OPEN c_get_pdu_details(p1.lpva_pdf_name,l_pel_pgp_refno);
            FETCH c_get_pdu_details INTO l_pel_pdu_exists;
            CLOSE c_get_pdu_details;
             IF l_pel_pdu_exists IS NULL
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',651);
             END IF;
          END IF;
        --
        --  Now check the Property is suitable
        --  Check pro_propref(lpva_legancy_refno)is supplied and must be a property
        -- type of 'HOU' or 'BOTH'
        --
          l_pro_refno := NULL;
          l_pro_refno := s_dl_hem_utils.pro_refno_for_propref(p1.lpva_legacy_ref);
                --
          IF l_pro_refno IS NULL
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',615);
          ELSE
            IF p1.lpva_legacy_ref IS NOT NULL
             THEN
              OPEN  c_propref_chk(p1.lpva_legacy_ref);
              FETCH c_propref_chk into l_propref_exists;
               IF c_propref_chk%NOTFOUND
                THEN
                 l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',643);
              END IF;
              CLOSE c_propref_chk;
            END IF;  -- lpva_legacy_ref IS NOT NULL
          END IF; -- exists_propref
        --
        -- and finally do checks on whether a suitable Property Element
        -- exists for us to tag this other field on to
        --
          l_pel_reusable_refno := NULL;
                --
          IF l_ele_value_type IN ('C','D','N')
           THEN
            OPEN c_get_CND_pro_ele_refno(l_pro_refno,p1.lpva_secondary_ref,p1.lpva_secondary_date);
            FETCH c_get_CND_pro_ele_refno INTO l_pel_reusable_refno;
            CLOSE c_get_CND_pro_ele_refno;
          ELSE -- multivalue
            OPEN c_get_MUL_pro_ele_refno(l_pro_refno,p1.lpva_secondary_ref,p1.lpva_secondary_date,p1.lpva_further_ref,p1.lpva_hrv_loc_code,p1.lpva_further_ref2);
            FETCH c_get_MUL_pro_ele_refno INTO l_pel_reusable_refno;
            CLOSE c_get_MUL_pro_ele_refno;
          END IF;
           IF l_pel_reusable_refno IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',126);
           ELSE
        --
        -- Else check whether it is a duplicate
        --
            l_exists := NULL;
                --
            OPEN c_chk_for_dup(l_pel_reusable_refno,p1.lpva_pdf_name);
            FETCH c_chk_for_dup INTO l_exists;
            CLOSE  c_chk_for_dup;
             IF l_exists IS NOT NULL
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',673);
             END IF;
          END IF;
        END IF; -- Property Element
        --
        --*******************************************
        -- First Check that the Admin Unit Element Type is a suitable one for an other Field to
        -- be linked to
        -- So ele_code must be supplied. must be of type of 'PR' and ele_value_type
        -- of D C N (Multi-Value allowed from 12Feb2014) and now M
        --
        --  Check the Admin Unit
        --  a cursor already exists to do this so use it again even though it
        -- retrieves an aun_reusable_refno that we don't need.
        --
        IF p1.lpva_pdu_pob_table_name = 'ADMIN_UNIT_ELEMENTS'
         THEN
        --
          l_aun_reusable_refno := NULL;
                --
          OPEN c_get_aun_refno(p1.lpva_legacy_ref);
          FETCH c_get_aun_refno into l_aun_reusable_refno;
          CLOSE c_get_aun_refno;
           IF l_aun_reusable_refno IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
           END IF;
                --
          l_ele_code_exists:= NULL;
                --
          IF (p1.lpva_secondary_ref IS NULL)
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',662);
          ELSE
            OPEN c_chk_ele_exists(p1.lpva_secondary_ref);
            FETCH c_chk_ele_exists INTO l_ele_code_exists;
            CLOSE c_chk_ele_exists;
             IF l_ele_code_exists IS NULL
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',662);
             ELSE
              OPEN c_chk_ele_code(p1.lpva_secondary_ref);
              FETCH c_chk_ele_code INTO l_pel_pgp_refno, l_ele_value_type, l_ele_type;
              CLOSE c_chk_ele_code;
               IF (l_ele_type != 'PR')
                THEN
                 l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',981);
               END IF;
             END IF;
        --
        -- OK, now check if this Other Field links to the Element Value
        --
            OPEN c_get_pdu_details(p1.lpva_pdf_name, l_pel_pgp_refno);
            FETCH c_get_pdu_details INTO l_pel_pdu_exists;
            CLOSE c_get_pdu_details;
             IF (l_pel_pdu_exists IS NULL)
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',664);
             END IF;
          END IF;
                --
          l_pel_reusable_refno := NULL;
                --
          IF l_ele_value_type IN ('C','D','N')
           THEN
            OPEN c_get_CND_aue_ele_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref,p1.lpva_secondary_date);
            FETCH c_get_CND_aue_ele_refno INTO l_pel_reusable_refno;
            CLOSE c_get_CND_aue_ele_refno;
          ELSE -- multivalue
            OPEN c_get_MUL_aue_ele_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref,p1.lpva_secondary_date,p1.lpva_further_ref,p1.lpva_hrv_loc_code,p1.lpva_further_ref2);
            FETCH c_get_MUL_aue_ele_refno INTO l_pel_reusable_refno;
            CLOSE c_get_MUL_aue_ele_refno;
          END IF;
           IF l_pel_reusable_refno IS NULL
            THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',126);
           ELSE
        --
        -- Else check whether it is a duplicate
        --
            l_exists := NULL;
        --
            OPEN c_chk_for_dup(l_pel_reusable_refno,p1.lpva_pdf_name);
            FETCH c_chk_for_dup INTO l_exists;
            CLOSE  c_chk_for_dup;
             IF l_exists IS NOT NULL
              THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',673);
             END IF;
           END IF;
        END IF; -- Admin Unit Element
        --
        --*****************************************************
        -- Get the data type (pdf_datatype) of the Other Field so we 
        -- can check that the right value has been provided this needs to 
        -- at the BOTTOM as using l_chk_datatype variable
        -- INTERESTED_PARTIES (PGP)
        --
        IF l_chk_datatype IN ('INTERESTED_PARTIES'
                             )
         THEN
        --
        -- Firstly Check that correct field for data type has been provided
        --              
          l_pdf_datatype3    := NULL;
                --
          OPEN c_pdf_datatype(p1.lpva_pdf_name,p1.lpva_pdu_pob_table_name);
          FETCH c_pdf_datatype into l_pdf_datatype3;
          CLOSE c_pdf_datatype;
        --
        -- Check the pdf_datatype matches the values provided
        --
          IF l_pdf_datatype3 IN ('YN','TEXT','CODED')
          AND p1.lpva_char_value IS NULL 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',740);
          END IF;
                --
          IF l_pdf_datatype3 IN ('NUMERIC')
          AND p1.lpva_number_value IS NULL 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',741);
          END IF;
                --
          IF l_pdf_datatype3 IN ('DATE')
          AND p1.lpva_date_value IS NULL 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',742);
          END IF;
                --
          IF l_pdf_datatype3 IN ('YN','TEXT','CODED','DATE')
          AND p1.lpva_number_value IS NOT NULL 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',743);
          END IF;
                --
          IF l_pdf_datatype3 IN ('YN','TEXT','CODED','NUMERIC')
          AND p1.lpva_date_value IS NOT NULL 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',743);
          END IF;
                --
          IF l_pdf_datatype3 IN ('DATE','NUMERIC')
          AND p1.lpva_char_value IS NOT NULL 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',743);
          END IF;
                --
          IF l_pdf_datatype3 = 'YN'
          AND p1.lpva_char_value IS NOT NULL
          AND p1.lpva_char_value NOT IN ('Y','N') 
           THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',925);
          END IF;
                --
        END IF; -- end of data type check
        --
        --*******************************************
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
        --*******************************************
        -- keep a count of the rows processed and commit after every 1000
        --
        i := i + 1;
        IF MOD(i,1000)=0
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
  -- *****************************************************************************
  --
  PROCEDURE dataload_delete 
    (p_batch_id        IN VARCHAR2
    ,p_date            IN DATE
    ) 
  AS
  --*******************
  CURSOR c1 
  IS
    SELECT rowid rec_rowid 
    ,      lpva_dlb_batch_id 
    ,      lpva_dl_seqno 
    ,      lpva_dl_load_status 
    ,      lpva_legacy_ref 
    ,      lpva_pdf_name 
    ,      lpva_created_by 
    ,      lpva_created_date 
    ,      lpva_date_value 
    ,      lpva_number_value 
    ,      lpva_char_value 
    ,      lpva_secondary_ref 
    ,      lpva_secondary_date 
    ,      lpva_pdu_pob_table_name 
    ,      lpva_further_ref 
    ,      lpva_hrv_loc_code 
    ,      lpva_further_ref2 
    ,      lpva_further_ref3 
    ,      lpva_desc 
    ,      lpva_bm_grp_seq 
    ,      lpva_lebe_refno 
    ,      lpva_lebe_reusable_refno
    ,      lpva_pdu_pgp_refno
    ,      lpva_pdu_table_name
    ,      lpva_pdu_display_seqno
    FROM   dl_mad_other_field_values
    WHERE  lpva_dlb_batch_id = p_batch_id
    AND    lpva_dl_load_status = 'C';
  --*******************
  CURSOR c_obj
    (p_obj        VARCHAR2
    ,p_table_name VARCHAR2
    )
  IS
    SELECT pdu_pob_table_name
    ,      pdu_pgp_refno
    ,      pdu_display_seqno
    ,      pdu_pdf_param_type
    FROM   parameter_definition_usages
    WHERE  pdu_pdf_name = p_obj
    AND    pdu_pob_table_name = p_table_name;
  --*******************
  CURSOR c_get_aun_refno
    (p_aun_ref VARCHAR2)
  IS
    SELECT aun_reusable_refno
    FROM   admin_units
    WHERE  aun_code = p_aun_ref;
  --*******************
  CURSOR c_get_aca_refno
    (p_pay_ref  VARCHAR2
    ,p_ara_code VARCHAR2
    ,p_date     DATE
    )
  IS
    SELECT aca_reusable_refno
    FROM   account_arrears_actions a
    ,      revenue_accounts        r
    WHERE  p_pay_ref = r.rac_pay_ref
    AND    a.aca_rac_accno = r.rac_accno
    AND    p_ara_code = a.aca_ara_code
    AND    p_date = trunc(a.aca_created_date);
  --*******************
  CURSOR c_get_con_refno(p_con_ref VARCHAR2)
  IS
    SELECT con_reusable_refno
    FROM   contractors
    WHERE  con_code = p_con_ref;
  --*******************
  CURSOR c_get_cos_refno
    (p_cos_ref VARCHAR2)
  IS
    SELECT cos_reusable_refno
    FROM   contractor_sites
    WHERE  cos_code = p_cos_ref;
  --*******************
  CURSOR c_get_par_refno
    (p_per_alt_ref VARCHAR2)
  IS
    SELECT par_reusable_refno
    FROM   parties
    WHERE  par_per_alt_ref = p_per_alt_ref;
  --*******************
  CURSOR c_get_ltl_refno
    (p_reference VARCHAR2)
  IS
    SELECT ltl_reusable_refno
    FROM   land_titles
    WHERE  ltl_reference = p_reference;
  --*******************
  CURSOR c_get_pro_refno
    (p_pro_propref VARCHAR2)
  IS
    SELECT pro_reusable_refno
    FROM   properties
    WHERE  p_pro_propref = pro_propref;
  --*******************
  CURSOR c_get_srq_refno
    (p_srq_ref VARCHAR2)
  IS
    SELECT srq_reusable_refno
    FROM   service_requests
    WHERE  srq_legacy_refno = p_srq_ref;
  --*******************
  CURSOR c_get_tcy_refno
    (p_tcy_alt_ref VARCHAR2)
  IS
    SELECT tcy_reusable_refno
    FROM   tenancies
    WHERE  tcy_refno = p_tcy_alt_ref;
  --*******************
  CURSOR c_get_tcy_refno2
    (p_tcy_alt_ref VARCHAR2)
  IS
    SELECT tcy_reusable_refno
    FROM   tenancies
    WHERE  tcy_alt_ref = p_tcy_alt_ref;
  --*******************
  CURSOR c_get_wor_refno
    (p_wor_ref VARCHAR2)
  IS
    SELECT wor_reusable_refno
    FROM   works_orders
    WHERE  wor_legacy_ref = p_wor_ref;
  --*******************
  CURSOR c_get_srt_refno
    (p_srt_sud_pro_aun_code VARCHAR2
    ,p_srt_ele_code         VARCHAR2
    ,p_scs_reference        VARCHAR2
    )
  IS
    SELECT srt_reusable_refno
    FROM   survey_results
    ,      stock_condition_surveys
    WHERE  srt_sud_pro_aun_code = p_srt_sud_pro_aun_code
    AND    srt_ele_code = p_srt_ele_code
    AND    scs_reference = p_scs_reference
    AND    scs_refno = srt_sud_scs_refno;
  --*******************
  CURSOR c_get_cve_refno
    (p_cnt_reference VARCHAR2
    ,p_version_no    VARCHAR2
    )
  IS
    SELECT cve_reusable_refno
    FROM   contract_versions
    WHERE  cve_cnt_reference = p_cnt_reference
    AND    cve_version_number = p_version_no;
  --*******************
  CURSOR c_get_tve_refno 
    (cp_tkg_src_reference  VARCHAR2
    ,cp_tkg_code           VARCHAR2
    ,cp_tkg_src_type       VARCHAR2
    ,cp_tsk_alt_reference  VARCHAR2
    ,cp_version_number     NUMBER
    )
  IS
    SELECT tve_reusable_refno
    FROM   task_versions
    WHERE  tve_tsk_tkg_src_reference = cp_tkg_src_reference
    AND    tve_tsk_tkg_code = cp_tkg_code
    AND    tve_tsk_tkg_src_type = cp_tkg_src_type
    AND    tve_tsk_id = 
             (SELECT tsk_id 
              FROM   tasks 
              WHERE  tsk_alt_reference = cp_tsk_alt_reference
             )
    AND    tve_version_number = cp_version_number;
  --*******************
  CURSOR c_get_dve_refno
    (p_cnt_reference VARCHAR2
    ,p_pro_aun_code  VARCHAR2
    ,p_pro_aun_ind   VARCHAR2
    ,p_display_seqno VARCHAR2
    )
  IS
    SELECT dve_reusable_refno
    FROM   deliverables
    ,      deliverable_versions
    WHERE dlv_refno = dve_dlv_refno
    AND dlv_cnt_reference = p_cnt_reference 
    AND dlv_cad_pro_aun_code = p_pro_aun_code 
    AND dlv_cad_type_ind = p_pro_aun_ind 
    AND dve_display_sequence = p_display_seqno 
    AND dve_current_ind = 'Y';
  --******************* 
  CURSOR c_get_ipp_refno
    (p_ipp_shortname VARCHAR2
    ,p_ipp_ipt_code  VARCHAR2
    )
  IS
    SELECT ipp_reusable_refno
    FROM   interested_parties
    WHERE  ipp_shortname = p_ipp_shortname
    AND    ipp_ipt_code = p_ipp_ipt_code;
  --*******************
  CURSOR c_get_ban_refno
    (p_ban_reference VARCHAR2)
  IS
    SELECT ban_reusable_refno
    FROM   business_actions
    WHERE  ban_reference = p_ban_reference;
  --*******************
  CURSOR c_get_real_refno
    (p_real_reference VARCHAR2)
  IS
    SELECT real_reuseable_refno
    FROM   registered_address_lettings
    WHERE  real_reference = p_real_reference;
  --*******************
  CURSOR c_get_bae_refno
    (p_bae_ban_reference VARCHAR2
    ,p_bae_aet_code      VARCHAR2
    ,p_bae_status_date   DATE
    ,p_bae_seqno         NUMBER
    )
  IS
    SELECT bae_reusable_refno
    FROM   business_action_events
    WHERE  bae_ban_reference = p_bae_ban_reference
    AND    bae_aet_code = p_bae_aet_code
    AND    bae_status_date = p_bae_status_date
    AND    bae_sequence = p_bae_seqno;
  --*******************
  CURSOR c_get_ppyt_refno
    (p_ppyt_alternative_reference VARCHAR2)
  IS
    SELECT ppyt_reusable_refno
    FROM   prevention_payments
    WHERE  ppyt_alternative_reference = p_ppyt_alternative_reference;
  --
  --*******************
  CURSOR c_get_orgtype_param 
    (p_pdf_name        VARCHAR2) 
  IS
    SELECT pva_char_value
    FROM   parameter_values
    ,      parameter_definition_usages
    WHERE  pdu_pdf_param_type = 'SYSTEM'
    AND    pva_pdu_pdf_name = pdu_pdf_name
    AND    pva_pdu_pdf_param_type = pdu_pdf_param_type
    AND    pva_pdu_pob_table_name = 'SYSTEM'
    AND    pva_pdu_pgp_refno = pdu_pgp_refno
    AND    pva_pdu_display_seqno  = pdu_display_seqno
    AND    pdu_pdf_name = p_pdf_name;
--*******************
  -- Constants for process_summary
  --
  cb              VARCHAR2(30);
  cd              DATE;
  cp              VARCHAR2(30) := 'DELETE';
  ct              VARCHAR2(30) := 'DL_MAD_OTHER_FIELD_VALUES';
  cs              PLS_INTEGER;
  ce              VARCHAR2(200);
  l_id            ROWID;
  --
  --*******************
  -- Other Variables
  --
  l_reusable_refno    NUMBER(10);
  l_an_tab            VARCHAR2(1);
  l_obj               VARCHAR2(30);
  l_pgp_refno         PLS_INTEGER;
  l_display_seqno     NUMBER;
  l_param_type        VARCHAR2(20);
  i                   PLS_INTEGER := 0;
  l_pro_aun_code      VARCHAR2(20);
  l_lebe_refno        NUMBER(10);
  l_orgtype_param     VARCHAR2(10);
  --
  --*******************
  --
  BEGIN
    fsc_utils.proc_start('s_dl_mad_other_field_values.dataload_delete');
    fsc_utils.debug_message( 's_dl_mad_other_field_values.dataload_delete',3 );
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    --
    -- Get HEM_ORG_OTHERFLDS parameter
    --
    l_orgtype_param   := NULL;
    OPEN  c_get_orgtype_param('HEM_ORG_OTHERFLDS');
    FETCH c_get_orgtype_param INTO l_orgtype_param;
    CLOSE c_get_orgtype_param;
    --	
    FOR p1 IN c1 
    LOOP
      BEGIN
        cs := p1.lpva_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        --
        -- get the object and param def information
        --
        l_obj            := NULL;
        l_pgp_refno      := NULL;
        l_display_seqno  := NULL;
        l_param_type     := NULL;
        l_pro_aun_code   := NULL;
        IF p1.lpva_pdu_pob_table_name NOT IN ('INTERESTED_PARTIES'
                                             ,'BUSINESS_ACTIONS'
                                             ,'BUSINESS_ACTION_EVENTS'
                                             ,'ADVICE_CASE_HOUSING_OPTIONS'
                                             ,'PREVENTION_PAYMENTS'
                                             ,'REGISTERED_ADDRESS_LETTINGS'
                                             ,'ARREARS_ACTIONS'
                                             ,'PROPERTY_ELEMENTS'
                                             ,'ADMIN_UNIT_ELEMENTS'
                                             ,'TENANCIES2'
                                             ,'ORGANISATIONS'
                                             ,'REVENUE_ACCOUNTS'
                                             ,'ORGANISATION_CONTACTS'
                                             )
        THEN
          OPEN c_obj(p1.lpva_pdf_name, p1.lpva_pdu_pob_table_name);
          FETCH c_obj INTO l_obj,l_pgp_refno,l_display_seqno,l_param_type;
          CLOSE c_obj;
        ELSIF p1.lpva_pdu_pob_table_name = 'ARREARS_ACTIONS'
        THEN
          l_obj := 'ARREARS_ACTIONS';
        END IF;
        --
        -- get the reusable_refno
        --
        l_reusable_refno := NULL;
        IF  l_obj = 'ADMIN_UNITS'
        THEN
          OPEN c_get_aun_refno(p1.lpva_legacy_ref);
          FETCH c_get_aun_refno into l_reusable_refno;
          CLOSE c_get_aun_refno;
        ELSIF l_obj = 'ARREARS_ACTIONS'
        THEN
          OPEN c_get_aca_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref,p1.lpva_secondary_date);
          FETCH c_get_aca_refno into l_reusable_refno;
          CLOSE c_get_aca_refno;
        ELSIF l_obj = 'CONTRACTORS'
        THEN
          OPEN c_get_con_refno(p1.lpva_legacy_ref);
          FETCH c_get_con_refno into l_reusable_refno;
          CLOSE c_get_con_refno;
        ELSIF l_obj = 'CONTRACTOR_SITES'
        THEN
          OPEN c_get_cos_refno(p1.lpva_legacy_ref);
          FETCH c_get_cos_refno into l_reusable_refno;
          CLOSE c_get_cos_refno;
        ELSIF l_obj = 'PARTIES'
        THEN
          OPEN c_get_par_refno(p1.lpva_legacy_ref);
          FETCH c_get_par_refno into l_reusable_refno;
          CLOSE c_get_par_refno;
        ELSIF l_obj = 'LAND_TITLES'
        THEN
          OPEN c_get_ltl_refno(p1.lpva_legacy_ref);
          FETCH c_get_ltl_refno into l_reusable_refno;
          CLOSE c_get_ltl_refno;
        ELSIF l_obj = 'PROPERTIES'
        THEN
          OPEN c_get_pro_refno(p1.lpva_legacy_ref);
          FETCH c_get_pro_refno into l_reusable_refno;
          CLOSE c_get_pro_refno;
        ELSIF l_obj = 'SERVICE_REQUESTS'
        THEN
          OPEN c_get_srq_refno(p1.lpva_legacy_ref);
          FETCH c_get_srq_refno into l_reusable_refno;
          CLOSE c_get_srq_refno;
        ELSIF l_obj = 'TENANCIES'
        THEN
          OPEN c_get_tcy_refno(p1.lpva_legacy_ref);
          FETCH c_get_tcy_refno into l_reusable_refno;
          CLOSE c_get_tcy_refno;
        ELSIF l_obj = 'WORKS_ORDERS'
        THEN
          OPEN c_get_wor_refno(p1.lpva_legacy_ref);
          FETCH c_get_wor_refno into l_reusable_refno;
          CLOSE c_get_wor_refno;
        ELSIF l_obj = 'SURVEY_RESULTS'
        THEN
          OPEN c_get_srt_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref,p1.lpva_further_ref);
          FETCH c_get_srt_refno into l_reusable_refno;
          CLOSE c_get_srt_refno;
        ELSIF l_obj = 'CONTRACT_VERSIONS'
        THEN
          OPEN c_get_cve_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref );
          FETCH c_get_cve_refno into l_reusable_refno;
          CLOSE c_get_cve_refno;
        ELSIF l_obj = 'CONTRACT_TASKS'
        THEN
          OPEN c_get_tve_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref,p1.lpva_further_ref,p1.lpva_further_ref2,p1.lpva_further_ref3);
          FETCH c_get_tve_refno into l_reusable_refno;
          CLOSE c_get_tve_refno;
        ELSIF l_obj = 'DELIVERABLES'
        THEN
          --
          -- Get the pro_refno or use Admin Unit Code supplied
          --
          IF p1.lpva_further_ref = 'P'
           THEN
            l_pro_aun_code := s_properties.get_refno_for_propref(p1.lpva_secondary_ref);
          ELSE
            l_pro_aun_code := p1.lpva_secondary_ref;
          END IF;
                  --
          OPEN  c_get_dve_refno(p1.lpva_legacy_ref,l_pro_aun_code,p1.lpva_further_ref,p1.lpva_further_ref2);
          FETCH c_get_dve_refno into l_reusable_refno;
          CLOSE c_get_dve_refno;
        END IF;
        IF p1.lpva_pdu_pob_table_name = 'INTERESTED_PARTIES'
        THEN
          OPEN c_get_ipp_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref);
          FETCH c_get_ipp_refno INTO l_reusable_refno;
          CLOSE c_get_ipp_refno;
        ELSIF p1.lpva_pdu_pob_table_name = 'BUSINESS_ACTIONS'
        THEN
          OPEN c_get_ban_refno(p1.lpva_legacy_ref);
          FETCH c_get_ban_refno INTO l_reusable_refno;
          CLOSE c_get_ban_refno;
        ELSIF p1.lpva_pdu_pob_table_name = 'BUSINESS_ACTION_EVENTS'
        THEN
          OPEN c_get_bae_refno(p1.lpva_legacy_ref,p1.lpva_secondary_ref,p1.lpva_secondary_date,p1.lpva_further_ref);
          FETCH c_get_bae_refno INTO l_reusable_refno;
          CLOSE c_get_bae_refno;
        ELSIF p1.lpva_pdu_pob_table_name = 'ADVICE_CASE_HOUSING_OPTIONS'
        THEN
          l_lebe_refno := NULL;
          l_reusable_refno := p1.lpva_lebe_reusable_refno;
          l_lebe_refno     := p1.lpva_lebe_refno;
        ELSIF p1.lpva_pdu_pob_table_name = 'PREVENTION_PAYMENTS'
        THEN
          OPEN c_get_ppyt_refno(p1.lpva_legacy_ref);
          FETCH c_get_ppyt_refno into l_reusable_refno;
          CLOSE c_get_ppyt_refno;
        ELSIF p1.lpva_pdu_pob_table_name = 'REGISTERED_ADDRESS_LETTINGS'
        THEN
          OPEN c_get_real_refno(p1.lpva_legacy_ref);
          FETCH c_get_real_refno into l_reusable_refno;
          CLOSE c_get_real_refno;
        ELSIF p1.lpva_pdu_pob_table_name = 'PROPERTY_ELEMENTS'
        THEN
          l_reusable_refno := p1.lpva_lebe_reusable_refno;
        ELSIF p1.lpva_pdu_pob_table_name = 'ADMIN_UNIT_ELEMENTS'
        THEN
          l_reusable_refno := p1.lpva_lebe_reusable_refno;
        ELSIF p1.lpva_pdu_pob_table_name = 'TENANCIES2'
        THEN
          OPEN c_get_tcy_refno2(p1.lpva_legacy_ref);
          FETCH c_get_tcy_refno2 into l_reusable_refno;
          CLOSE c_get_tcy_refno2;
        ELSIF (p1.lpva_pdu_pob_table_name = 'ORGANISATIONS')
        THEN
          l_reusable_refno := p1.lpva_lebe_reusable_refno;
        ELSIF (p1.lpva_pdu_pob_table_name = 'REVENUE_ACCOUNTS')
        THEN
          l_reusable_refno := p1.lpva_lebe_reusable_refno;
        ELSIF (p1.lpva_pdu_pob_table_name = 'ORGANISATION_CONTACTS')
        THEN
          l_reusable_refno := p1.lpva_lebe_reusable_refno;
        END IF;
        DELETE
        FROM  parameter_values
        WHERE pva_reusable_refno = l_reusable_refno
        AND   pva_pdu_pdf_name = p1.lpva_pdf_name
        AND   pva_pdu_pdf_param_type = 'OTHER FIELDS'
        AND   pva_pdu_pgp_refno = nvl(p1.lpva_pdu_pgp_refno,pva_pdu_pgp_refno)
        AND   pva_pdu_pob_table_name = nvl(p1.lpva_pdu_table_name,pva_pdu_pob_table_name)
        AND   pva_pdu_display_seqno = nvl(p1.lpva_pdu_display_seqno,pva_pdu_display_seqno);
        --
        -- For Advice Case Housing Options we also want to remove the
        -- lettings benchmark record
        --
        IF p1.lpva_pdu_pob_table_name = 'ADVICE_CASE_HOUSING_OPTIONS'
        THEN
          DELETE
          FROM  lettings_benchmarks
          WHERE lebe_refno = l_lebe_refno;
        END IF;
		--
		-- Now remove values stored during create process from data load table
        --
        UPDATE dl_mad_other_field_values
        SET    lpva_lebe_reusable_refno = null
              ,lpva_pdu_pgp_refno = null
              ,lpva_pdu_table_name = null	
              ,lpva_pdu_display_seqno = null			  
        WHERE  lpva_dlb_batch_id = cb
        AND    lpva_dl_seqno = cs		
        AND    ROWID = p1.rec_rowid;
		--
        -- keep a count of the rows processed and commit after every 1000
        --
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        set_record_status_flag(l_id,'V');
        i := i+1;
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
    --
    -- Section to anayze the table(s) populated by this dataload
    --
    l_an_tab := s_dl_hem_utils.dl_comp_stats('PARAMETER_VALUES');
    fsc_utils.proc_end;
  EXCEPTION
  WHEN OTHERS
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_mad_other_field_values;
/
show errors
