CREATE OR REPLACE PACKAGE BODY s_dl_mad_other_field_val_hist
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      1.0  5.16.0    VS   07-NOV-2009  Initial version.
--
--      2.0  5.16.0    VS   07-DEC-2009  No history for ADVICE_CASE_HOUSING_OPTION.
--                                       to be loaded. A new lettings benchmark record
--                                       always created. All code has been commented out#
--      3.0  6.9.0.1   AJ   15-DEC-2014  Amended get reusable ref for contractors and contractor site was referencing 
--                                       cursor to get for parties incorrectly lines 467/73 1253/63 and 1970/76
--      3.1  6.10/11   AJ   03-JUN-2015  added show errors at bottom no further change required as Advice Case Housing Options
--                                       does not create a history record (see version 2.0 comment above)
--      3.1  6.14/15   AJ   27-MAR-2017  added blank line below show errors at bottom
--
-- ***********************************************************************   
--  
--  declare package variables AND constants
--
--
PROCEDURE set_record_status_flag(p_rowid  IN ROWID,
                                 p_status IN VARCHAR2)
AS
--
BEGIN
--
  UPDATE dl_mad_other_field_val_hist
     SET lpvh_dl_load_status = p_status
   WHERE rowid               = p_rowid;
--
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_mad_other_field_val_hist');
          RAISE;
--
END set_record_status_flag;
--
-- ************************************************************************************
--
--
PROCEDURE dataload_create(p_batch_id          IN VARCHAR2,
                          p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LPVH_DLB_BATCH_ID,
       LPVH_DL_SEQNO,
       LPVH_DL_LOAD_STATUS,
       LPVH_LEGACY_REF,
       LPVH_PVA_PDU_PDF_NAME,
       LPVH_PVA_DATE_VALUE,
       LPVH_PVA_NUMBER_VALUE,
       LPVH_PVA_CHAR_VALUE,
       LPVH_SECONDARY_REF,
       LPVH_SECONDARY_DATE,
       LPVH_PVA_PDU_POB_TABLE_NAME,
       NVL(LPVH_CREATED_BY,'DATALOAD')  LPVH_CREATED_BY, 
       NVL(LPVH_CREATED_DATE,SYSDATE)   LPVH_CREATED_DATE,
       NVL(LPVH_MODIFIED_BY,'DATALOAD') LPVH_MODIFIED_BY,
       NVL(LPVH_MODIFIED_DATE,SYSDATE)  LPVH_MODIFIED_DATE,
       LPVH_PVA_FURTHER_REF,
       LPVH_PVA_HRV_LOC_CODE,
       LPVH_PVA_FURTHER_REF2,
       LPVH_PVA_FURTHER_REF3,
       LPVH_DESC
  FROM dl_mad_other_field_val_hist
 WHERE lpvh_dlb_batch_id   = p_batch_id
   AND lpvh_dl_load_status = 'V';
--
-- ************************************************************************************
--
CURSOR c_pdu(p_pdf_name   VARCHAR2, 
             p_table_name VARCHAR2) 
IS
SELECT PDU_PDF_NAME,
       PDU_POB_TABLE_NAME,
       PDU_PGP_REFNO,
       PDU_DISPLAY_SEQNO,
       PDU_PDF_PARAM_TYPE
  FROM parameter_definition_usages
 WHERE pdu_pdf_name       = p_pdf_name
   AND pdu_pdf_param_type = 'OTHER FIELDS'
   AND pdu_pob_table_name = p_table_name;      
--
-- ************************************************************************************
--
CURSOR c_get_aun_refno(p_aun_ref VARCHAR2) 
is
SELECT aun_reusable_refno
  FROM admin_units
 WHERE p_aun_ref = aun_code;
--
-- ************************************************************************************
--
CURSOR c_get_aca_refno(p_pay_ref  VARCHAR2
                      ,p_ara_code VARCHAR2
                      ,p_date     DATE) is
SELECT aca_reusable_refno
  FROM account_arrears_actions a,
       revenue_accounts r
 WHERE p_pay_ref       = r.rac_pay_ref
   AND a.aca_rac_accno = r.rac_accno
   AND p_ara_code      = a.aca_ara_code
   AND p_date          = trunc(a.aca_created_date);
--
-- ************************************************************************************
--
CURSOR c_get_con_refno(p_con_ref VARCHAR2) 
is
SELECT con_reusable_refno
  FROM contractors
 WHERE con_code = p_con_ref;
--
-- ************************************************************************************
--
CURSOR c_get_cos_refno(p_cos_ref VARCHAR2) 
is
SELECT cos_reusable_refno
  FROM contractor_sites
 WHERE cos_code = p_cos_ref;
--
-- ************************************************************************************
--
CURSOR c_get_par_refno(p_per_alt_ref VARCHAR2) 
is
SELECT par_reusable_refno
  FROM parties
 WHERE par_per_alt_ref = p_per_alt_ref;
--
-- ************************************************************************************
--
CURSOR c_get_pro_refno(p_pro_propref VARCHAR2) 
is
SELECT pro_reusable_refno
  FROM properties
 WHERE pro_propref = p_pro_propref;
--
-- ************************************************************************************
--
CURSOR c_get_srq_refno(p_srq_ref VARCHAR2) is
SELECT srq_reusable_refno
  FROM service_requests
 WHERE srq_legacy_refno = p_srq_ref;
--
-- ************************************************************************************
--
CURSOR c_get_tcy_refno(p_tcy_alt_ref VARCHAR2) 
is
SELECT tcy_reusable_refno
  FROM tenancies
 WHERE tcy_refno = p_tcy_alt_ref;
--
-- ************************************************************************************
--
CURSOR c_get_wor_refno(p_wor_ref VARCHAR2) 
is
SELECT wor_reusable_refno
  FROM works_orders
 WHERE wor_legacy_ref = p_wor_ref;
--
-- ************************************************************************************
--
--
CURSOR c_get_srt_refno(p_srt_sud_pro_aun_code VARCHAR2,
                       p_srt_ele_code         VARCHAR2,
                       p_scs_reference        VARCHAR2)
IS
SELECT srt_reusable_refno
  FROM survey_results,
       stock_condition_surveys
 WHERE srt_sud_pro_aun_code = p_srt_sud_pro_aun_code
   AND srt_ele_code         = p_srt_ele_code
   AND scs_reference        = p_scs_reference
   AND scs_refno            = srt_sud_scs_refno;
--
-- ************************************************************************************
--
CURSOR c_obj(p_obj VARCHAR2, 
             p_table_name VARCHAR2) 
IS
SELECT pdu_pob_table_name,
       pdu_pgp_refno,
       pdu_display_seqno,
       pdu_pdf_param_type
  FROM parameter_definition_usages
 WHERE pdu_pdf_name       = p_obj
   AND pdu_pob_table_name = p_table_name;
--
-- ************************************************************************************
--
CURSOR c_pro_refno(p_propref VARCHAR2) 
IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = p_propref;
--
-- ************************************************************************************
--
CURSOR c_get_cve_refno(p_cnt_reference VARCHAR2,
                       p_version_no    VARCHAR2)   
IS
SELECT cve_reusable_refno
  FROM contract_versions
 WHERE cve_cnt_reference  = p_cnt_reference
   AND cve_version_number = p_version_no;
--
-- ************************************************************************************
--
CURSOR c_get_tve_refno (p_tkg_src_reference  VARCHAR2,
                        p_tkg_code           VARCHAR2,
                        p_tkg_src_type       VARCHAR2,
                        p_stk_code           VARCHAR2,
                        p_version_number     NUMBER)  
IS
SELECT tve_reusable_refno
  FROM task_versions,
       tasks
 WHERE tsk_id                    = tve_tsk_id
   AND tve_tsk_tkg_src_reference = p_tkg_src_reference
   AND tve_tsk_tkg_code          = p_tkg_code
   AND tve_tsk_tkg_src_type      = p_tkg_src_type
   AND tsk_stk_code              = p_stk_code
   AND tve_version_number        = p_version_number;
--
-- ************************************************************************************
--
CURSOR c_get_ara(p_obj      VARCHAR2,
                 p_ara_code VARCHAR2) 
IS
SELECT pdu_pgp_refno
  FROM parameter_definition_usages,
       arrears_actions
 WHERE pdu_pdf_name  = p_obj
   AND pdu_pgp_refno = ara_pgp_refno
   AND ara_code      = p_ara_code;
--
-- ************************************************************************************
--
CURSOR c_get_dve_refno(p_cnt_reference VARCHAR2,
                       p_pro_aun_code  VARCHAR2,
                       p_pro_aun_ind   VARCHAR2,
                       p_display_seqno VARCHAR2)   
IS
SELECT dve_reusable_refno
  FROM deliverables,
       deliverable_versions
 WHERE dlv_refno            = dve_dlv_refno
   AND dlv_cnt_reference    = p_cnt_reference
   AND dlv_cad_pro_aun_code = p_pro_aun_code
   AND dlv_cad_type_ind     = p_pro_aun_ind
   AND dve_display_sequence = p_display_seqno
   AND dve_current_ind      = 'Y';
--
-- ************************************************************************************
--
CURSOR c_get_ipp_refno(p_ipp_shortname VARCHAR2,
                       p_ipp_ipt_code  VARCHAR2)   
IS
SELECT ipp_reusable_refno
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_shortname
   AND ipp_ipt_code  = p_ipp_ipt_code;
--
-- ************************************************************************************
--
CURSOR c_get_ipt_pgp_refno(p_ipt_code  VARCHAR2)   
IS
SELECT ipt_pgp_refno
  FROM interested_party_types
 WHERE ipt_code = p_ipt_code;
--
-- ************************************************************************************
--
CURSOR c_get_hoop_pgp_refno(p_hoop_code  VARCHAR2)   
IS
SELECT hoop_pgp_refno
  FROM housing_options
 WHERE hoop_code = p_hoop_code;
--
-- ************************************************************************************
--
CURSOR c_get_aet_pgp_refno(p_aet_code  VARCHAR2)   
IS
SELECT aet_pgp_refno
  FROM action_event_types
 WHERE aet_code = p_aet_code;
--
-- ************************************************************************************
--
CURSOR c_get_bro_pgp_refno(p_bro_code  VARCHAR2)   
IS
SELECT bro_pgp_action_refno
  FROM business_reasons
 WHERE bro_code = p_bro_code;
--
-- ************************************************************************************
--
CURSOR c_get_ban_bro_code(p_ban_reference VARCHAR2)   
IS
SELECT ban_bro_code
  FROM business_actions
 WHERE ban_reference = p_ban_reference;
--
-- ************************************************************************************
--
CURSOR c_get_pdu_details(p_pdf_name  VARCHAR2, 
                         p_pgp_refno NUMBER) 
IS
SELECT PDU_PDF_NAME,
       PDU_POB_TABLE_NAME,
       PDU_PGP_REFNO,
       PDU_DISPLAY_SEQNO,
       PDU_PDF_PARAM_TYPE
  FROM parameter_definition_usages
 WHERE pdu_pdf_name       = p_pdf_name
   AND pdu_pgp_refno      = p_pgp_refno
   AND pdu_pdf_param_type = 'OTHER FIELDS';
--
-- ************************************************************************************
--
CURSOR c_get_ban_refno(p_ban_reference VARCHAR2)   
IS
SELECT ban_reusable_refno
  FROM business_actions
 WHERE ban_reference = p_ban_reference;
--
-- ************************************************************************************
--
CURSOR c_get_real_refno(p_real_reference VARCHAR2)   
IS
SELECT real_reuseable_refno
  FROM registered_address_lettings
 WHERE real_reference = p_real_reference;
--
-- ************************************************************************************
--
CURSOR c_get_bae_refno(p_bae_ban_reference    VARCHAR2,
                       p_bae_aet_code         VARCHAR2,
                       p_bae_status_date      DATE,
                       p_bae_seqno            NUMBER)   
IS
SELECT bae_reusable_refno
  FROM business_action_events
 WHERE bae_ban_reference = p_bae_ban_reference
   AND bae_aet_code      = p_bae_aet_code
   AND bae_status_date   = p_bae_status_date
   AND bae_sequence      = p_bae_seqno;
--
-- ************************************************************************************
--
CURSOR c_get_acho_reference(p_acho_reference VARCHAR2)
IS
SELECT acho_reference
  FROM advice_case_housing_options
 WHERE acho_alternative_reference = p_acho_reference;
--
-- ************************************************************************************
--
CURSOR c_get_lebe_refno(p_acho_reference NUMBER,
                        p_created_date   DATE,
                        p_description    VARCHAR2)   
IS
SELECT lebe_reusable_refno
  FROM lettings_benchmarks
 WHERE lebe_acho_reference = p_acho_reference
   AND lebe_created_date   = p_created_date
   AND lebe_description    = p_description
   AND rownum = 1;
--
-- ************************************************************************************
--
-- Constants for process_summary
--
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'CREATE';
ct       		VARCHAR2(30) := 'DL_MAD_OTHER_FIELD_VAL_HIST';
cs       		INTEGER;
ce	   		VARCHAR2(200);
l_id            	ROWID;
l_an_tab 		VARCHAR2(1);
--
-- Other variables
--
i                 	INTEGER := 0;
--
--
l_reusable_refno  	NUMBER(10);
l_pdf_name		VARCHAR2(30);
l_param_type      	VARCHAR2(12);
l_pob_table_name        VARCHAR2(30);
l_pgp_refno       	NUMBER(10);
l_display_seqno   	NUMBER(3);
--
l_pro_aun_code    	VARCHAR2(20);
--
--
l_ara_pgp_refno   	INTEGER;
l_ipt_pgp_refno		INTEGER;
l_hoop_pgp_refno        INTEGER;
l_aet_pgp_refno         INTEGER;
l_bro_pgp_refno         INTEGER;
l_ban_bro_code          VARCHAR2(10);
--
l_lebe_refno		NUMBER(10);
l_acho_reference        NUMBER(10);
--
BEGIN
--
--    execute immediate 'alter trigger FSC.PVA_CREATED_BY_DEFAULT disable';
--
    fsc_utils.proc_start('s_dl_mad_other_field_val_hist.dataload_create');
    fsc_utils.debug_message( 's_dl_mad_other_field_val_hist.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs := p1.lpvh_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- get the object and param def information
--
--
          l_reusable_refno := NULL;
          l_pdf_name       := NULL;
          l_param_type     := NULL;
          l_pob_table_name := NULL;
          l_pgp_refno      := NULL;
          l_display_seqno  := NULL;
--
--
          l_pro_aun_code   := NULL;
          l_ara_pgp_refno  := NULL;
          l_ipt_pgp_refno  := NULL;
          l_hoop_pgp_refno := NULL;
          l_aet_pgp_refno  := NULL;
          l_bro_pgp_refno  := NULL;
          l_ban_bro_code   := NULL;
--
--
          IF (p1.lpvh_pva_pdu_pob_table_name NOT IN ('INTERESTED_PARTIES',
                                                     'BUSINESS_ACTIONS',
                                                     'BUSINESS_ACTION_EVENTS')) THEN
--
            OPEN c_pdu(p1.lpvh_pva_pdu_pdf_name, p1.lpvh_pva_pdu_pob_table_name);
           FETCH c_pdu INTO l_pdf_name,l_pob_table_name,l_pgp_refno,l_display_seqno,l_param_type;
           CLOSE c_pdu;
--
          END IF;
--
          IF  l_pob_table_name = 'ADMIN_UNITS' THEN
--
            OPEN c_get_aun_refno(p1.lpvh_legacy_ref);
           FETCH c_get_aun_refno into l_reusable_refno;
           CLOSE c_get_aun_refno;
--
          ELSIF l_pob_table_name = 'CONTRACTORS' THEN
--
               OPEN c_get_con_refno(p1.lpvh_legacy_ref);
              FETCH c_get_con_refno into l_reusable_refno;
              CLOSE c_get_con_refno;
--
          ELSIF l_pob_table_name = 'CONTRACTOR_SITES' THEN
--
               OPEN c_get_cos_refno(p1.lpvh_legacy_ref);
              FETCH c_get_cos_refno into l_reusable_refno;
              CLOSE c_get_cos_refno;
--
          ELSIF l_pob_table_name = 'PARTIES' THEN
--
               OPEN c_get_par_refno(p1.lpvh_legacy_ref);
              FETCH c_get_par_refno into l_reusable_refno;
              CLOSE c_get_par_refno;
--
          ELSIF l_pob_table_name = 'PROPERTIES' THEN
-- 
               OPEN c_get_pro_refno(p1.lpvh_legacy_ref);
              FETCH c_get_pro_refno into l_reusable_refno;
              CLOSE c_get_pro_refno;
--
          ELSIF l_pob_table_name = 'SERVICE_REQUESTS' THEN
--
               OPEN c_get_srq_refno(p1.lpvh_legacy_ref);
              FETCH c_get_srq_refno into l_reusable_refno;
              CLOSE c_get_srq_refno;
--
          ELSIF l_pob_table_name = 'TENANCIES' THEN
--
               OPEN c_get_tcy_refno(p1.lpvh_legacy_ref);
              FETCH c_get_tcy_refno into l_reusable_refno;
              CLOSE c_get_tcy_refno;
--
          ELSIF l_pob_table_name = 'WORKS_ORDERS' THEN
--
               OPEN c_get_wor_refno(p1.lpvh_legacy_ref);
              FETCH c_get_wor_refno into l_reusable_refno;
              CLOSE c_get_wor_refno;
--
          ELSIF l_pob_table_name = 'SURVEY_RESULTS' THEN
--
               OPEN c_get_srt_refno(p1.lpvh_legacy_ref,p1.lpvh_secondary_ref,p1.lpvh_pva_further_ref);
              FETCH c_get_srt_refno into l_reusable_refno;
              CLOSE c_get_srt_refno;
--
          ELSIF l_pob_table_name = 'CONTRACT_VERSIONS' THEN
--
               OPEN c_get_cve_refno(p1.lpvh_legacy_ref, p1.lpvh_secondary_ref);
              FETCH c_get_cve_refno into l_reusable_refno;
              CLOSE c_get_cve_refno;
--
          ELSIF l_pob_table_name = 'CONTRACT_TASKS' THEN
--
               OPEN c_get_tve_refno(p1.lpvh_legacy_ref, p1.lpvh_secondary_ref,
                                    p1.lpvh_pva_further_ref, p1.lpvh_pva_further_ref2,
                                    p1.lpvh_pva_further_ref3);
              FETCH c_get_tve_refno into l_reusable_refno;
              CLOSE c_get_tve_refno;
--
          ELSIF l_pob_table_name = 'DELIVERABLES' THEN
--
-- Get the pro_refno or use Admin Unit Code supplied
--
--
              IF p1.lpvh_pva_further_ref = 'P' THEN
               l_pro_aun_code := s_properties.get_refno_for_propref(p1.lpvh_secondary_ref);
              ELSE
                 l_pro_aun_code := p1.lpvh_secondary_ref;
--
                  OPEN c_get_dve_refno(p1.lpvh_legacy_ref, l_pro_aun_code,
                                       p1.lpvh_pva_further_ref, p1.lpvh_pva_further_ref2);
                 FETCH c_get_dve_refno into l_reusable_refno;
                 CLOSE c_get_dve_refno;
--
              END IF;
--
          ELSIF l_pob_table_name = 'REGISTERED_ADDRESS_LETTINGS' THEN
--
               OPEN c_get_real_refno(p1.lpvh_legacy_ref);
              FETCH c_get_real_refno into l_reusable_refno;
              CLOSE c_get_real_refno;
--
          END IF;
--
-- ***********************************************************************
--
--
-- The following are new additions to the otherfields load. 
-- INTERESTED_PARTIES are held against the individual ipp_shortname therefore
-- we need to get the ipp_pgp_refno held against the ipt_code 
--
          IF (p1.LPVH_PVA_PDU_POB_TABLE_NAME = 'INTERESTED_PARTIES') THEN
--
            OPEN c_get_ipt_pgp_refno(p1.lpvh_secondary_ref);
           FETCH c_get_ipt_pgp_refno INTO l_ipt_pgp_refno;
--
           IF (c_get_ipt_pgp_refno%FOUND) THEN
--
            l_pgp_refno      := l_ipt_pgp_refno;
            l_pob_table_name := 'NULL';
--
           END IF;
--
           CLOSE c_get_ipt_pgp_refno;
--
--
-- Now get parameter definition details for the otherfield name
--
            OPEN c_get_pdu_details(p1.lpvh_pva_pdu_pdf_name, l_ipt_pgp_refno);
--
           FETCH c_get_pdu_details INTO l_pdf_name,
                                        l_pob_table_name,
                                        l_pgp_refno,
                                        l_display_seqno,
                                        l_param_type;
--
           CLOSE c_get_pdu_details;
--
            OPEN c_get_ipp_refno(p1.lpvh_legacy_ref, p1.lpvh_secondary_ref);
           FETCH c_get_ipp_refno into l_reusable_refno;
           CLOSE c_get_ipp_refno;
-- 
          END IF;
--
-- ***********************************************************************
--
          IF (p1.LPVH_PVA_PDU_POB_TABLE_NAME = 'BUSINESS_ACTIONS') THEN
-- 
            OPEN c_get_ban_bro_code(p1.lpvh_legacy_ref);
           FETCH c_get_ban_bro_code into l_ban_bro_code;
           CLOSE c_get_ban_bro_code;
--
            OPEN c_get_bro_pgp_refno(l_ban_bro_code);
           FETCH c_get_bro_pgp_refno INTO l_bro_pgp_refno;
--
           IF (c_get_bro_pgp_refno%FOUND) THEN
--
            l_pgp_refno      := l_bro_pgp_refno;
            l_pob_table_name := 'NULL';
--
           END IF;
--
           CLOSE c_get_bro_pgp_refno;
--
--
-- Now get parameter definition details for the otherfield name
--
            OPEN c_get_pdu_details(p1.lpvh_pva_pdu_pdf_name, l_bro_pgp_refno);
--
           FETCH c_get_pdu_details INTO l_pdf_name,
                                        l_pob_table_name,
                                        l_pgp_refno,
                                        l_display_seqno,
                                        l_param_type;
--
           CLOSE c_get_pdu_details;
--
            OPEN c_get_ban_refno(p1.lpvh_legacy_ref);
           FETCH c_get_ban_refno into l_reusable_refno;
           CLOSE c_get_ban_refno;
-- 
          END IF;
--
-- ***********************************************************************
--
--
          IF (p1.LPVH_PVA_PDU_POB_TABLE_NAME = 'BUSINESS_ACTION_EVENTS') THEN
--
            OPEN c_get_aet_pgp_refno(p1.lpvh_secondary_ref);
           FETCH c_get_aet_pgp_refno INTO l_aet_pgp_refno;
--
           IF (c_get_aet_pgp_refno%FOUND) THEN
--
            l_pgp_refno      := l_aet_pgp_refno;
            l_pob_table_name := 'NULL';
--
           END IF;
--
           CLOSE c_get_aet_pgp_refno;
--
            OPEN c_get_pdu_details(p1.lpvh_pva_pdu_pdf_name, l_aet_pgp_refno);
--
           FETCH c_get_pdu_details INTO l_pdf_name,
                                        l_pob_table_name,
                                        l_pgp_refno,
                                        l_display_seqno,
                                        l_param_type;
--
           CLOSE c_get_pdu_details;
--
            OPEN c_get_bae_refno(p1.lpvh_legacy_ref, 
                                 p1.lpvh_secondary_ref, 
                                 p1.lpvh_secondary_date, 
                                 p1.lpvh_pva_further_ref);
--
           FETCH c_get_bae_refno into l_reusable_refno;
           CLOSE c_get_bae_refno;
--
          END IF;
--
-- ***********************************************************************
--
-- For Advice Case Housing Options 
-- 1) we have to get the pgp_refno assigned to the hoop_code
-- 2) The other fields will be held against the lettings_benchmark reusable_refno
--    against the acho_reference, created_date. These will be dealt within another 
--    loop in the create process.
--
--          IF (p1.LPVH_PVA_PDU_POB_TABLE_NAME = 'ADVICE_CASE_HOUSING_OPTIONS') THEN
--
--           l_acho_reference := NULL;
--
--            OPEN c_get_acho_reference(p1.lpvh_legacy_ref);
--           FETCH c_get_acho_reference INTO l_acho_reference;
--           CLOSE c_get_acho_reference;
--
--            OPEN c_get_lebe_refno(l_acho_reference, p1.lpvh_created_date, p1.lpvh_desc);
--           FETCH c_get_lebe_refno into l_reusable_refno;
--           CLOSE c_get_lebe_refno;
--
--            OPEN c_get_hoop_pgp_refno(p1.lpvh_secondary_ref);
--           FETCH c_get_hoop_pgp_refno INTO l_hoop_pgp_refno;
--
--           IF (c_get_hoop_pgp_refno%FOUND) THEN
--
--            l_pgp_refno      := l_hoop_pgp_refno;
--            l_pob_table_name := 'NULL';
--
--           END IF;
--
--           CLOSE c_get_hoop_pgp_refno;
--
--            OPEN c_get_pdu_details(p1.lpvh_pva_pdu_pdf_name, l_hoop_pgp_refno);
--
--           FETCH c_get_pdu_details INTO l_pdf_name,
--                                        l_pob_table_name,
--                                        l_pgp_refno,
--                                        l_display_seqno,
--                                        l_param_type;
--
--           CLOSE c_get_pdu_details;
-- 
--
--          END IF;
--
--
-- ***********************************************************************
--
-- Amended 26-MAR-2009. The other field can be held against the individual
-- arrears action and if this is the case we need to get the ara_pgp_refno
-- and set the table name to be 'NULL' not ARREARS_ACTIONS. This is because you
-- can set an other field to be against ALL actions in parameter setup or
-- specific ones (eg court details) in arrears actions setup.
-- If we don't do this the other fields will not display.
--
          IF (p1.LPVH_PVA_PDU_POB_TABLE_NAME = 'ARREARS_ACTIONS') THEN
--
            OPEN c_get_ara(p1.lpvh_pva_pdu_pdf_name, p1.lpvh_secondary_ref);
           FETCH c_get_ara INTO l_ara_pgp_refno;
--
           IF c_get_ara%FOUND THEN
            l_pgp_refno      := l_ara_pgp_refno;
            l_pob_table_name := 'NULL';
           END IF;
--
           CLOSE c_get_ara;
--
            OPEN c_get_aca_refno(p1.lpvh_legacy_ref,p1.lpvh_secondary_ref,p1.lpvh_secondary_date);
           FETCH c_get_aca_refno into l_reusable_refno;
           CLOSE c_get_aca_refno;
--
          END IF;
--
--
-- ***********************************************************************
--
          INSERT INTO PARAMETER_VALUES_HISTORY(pvh_pva_reusable_refno,
                                               pvh_pva_pdu_pdf_name,
                                               pvh_pva_pdu_pdf_param_type,
                                               pvh_pva_pdu_pob_table_name,
                                               pvh_pva_pdu_pgp_refno,
                                               pvh_pva_pdu_display_seqno,
                                               pvh_modified_date,
                                               pvh_modified_by,
                                               pvh_created_date,
                                               pvh_created_by,
                                               pvh_pva_date_value,
                                               pvh_pva_number_value,
                                               pvh_pva_char_value
                                              )
--
                                       VALUES (l_reusable_refno,
                                               p1.lpvh_pva_pdu_pdf_name,
                                               l_param_type,
                                               l_pob_table_name,
                                               l_pgp_refno ,
                                               l_display_seqno,
                                               p1.lpvh_modified_date,
                                               p1.lpvh_modified_by,
                                               p1.lpvh_created_date,
                                               p1.lpvh_created_by,
                                               p1.lpvh_pva_date_value,
                                               p1.lpvh_pva_number_value,
                                               p1.lpvh_pva_char_value
                                              );
--
--
-- keep a count of the rows processed and commit after every 5000
--
          i := i+1; 
--
          IF MOD(i,5000)=0 THEN 
           COMMIT; 
          END If;
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'C');
--
          EXCEPTION
               WHEN OTHERS THEN
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
--
-- Section to analyze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PARAMETER_VALUES');
--
    fsc_utils.proc_end;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
            RAISE;
--
--    execute immediate 'alter trigger FSC.PVA_CREATED_BY_DEFAULT enable';
--
END dataload_create;
--
-- ************************************************************************************
--
--
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
                            p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LPVH_DLB_BATCH_ID,
       LPVH_DL_SEQNO,
       LPVH_DL_LOAD_STATUS,
       LPVH_LEGACY_REF,
       LPVH_PVA_PDU_PDF_NAME,
       LPVH_PVA_DATE_VALUE,
       LPVH_PVA_NUMBER_VALUE,
       LPVH_PVA_CHAR_VALUE,
       LPVH_SECONDARY_REF,
       LPVH_SECONDARY_DATE,
       LPVH_PVA_PDU_POB_TABLE_NAME,
       NVL(LPVH_CREATED_BY,'DATALOAD')  LPVH_CREATED_BY, 
       NVL(LPVH_CREATED_DATE,SYSDATE)   LPVH_CREATED_DATE,
       NVL(LPVH_MODIFIED_BY,'DATALOAD') LPVH_MODIFIED_BY,
       NVL(LPVH_MODIFIED_DATE,SYSDATE)  LPVH_MODIFIED_DATE,
       LPVH_PVA_FURTHER_REF,
       LPVH_PVA_HRV_LOC_CODE,
       LPVH_PVA_FURTHER_REF2,
       LPVH_PVA_FURTHER_REF3,
       LPVH_DESC
  FROM dl_mad_other_field_val_hist
 WHERE lpvh_dlb_batch_id   = p_batch_id
   AND lpvh_dl_load_status in ('L','F','O');
--
-- ************************************************************************************
--
CURSOR c_pdu(p_pdf_name   VARCHAR2, 
             p_table_name VARCHAR2) 
IS
SELECT PDU_PDF_NAME,
       PDU_POB_TABLE_NAME,
       PDU_PGP_REFNO,
       PDU_DISPLAY_SEQNO,
       PDU_PDF_PARAM_TYPE
  FROM parameter_definition_usages
 WHERE pdu_pdf_name       = p_pdf_name
   AND pdu_pdf_param_type = 'OTHER FIELDS'
   AND pdu_pob_table_name = p_table_name;      
--
-- ************************************************************************************
--
CURSOR c_get_aun_refno(p_aun_ref VARCHAR2) 
is
SELECT aun_reusable_refno
  FROM admin_units
 WHERE aun_code = p_aun_ref;
--
-- ************************************************************************************
--
CURSOR c_get_aca_refno(p_pay_ref  VARCHAR2
                      ,p_ara_code VARCHAR2
                      ,p_date     DATE) 
is
SELECT aca_reusable_refno
  FROM account_arrears_actions a,revenue_accounts r
 WHERE p_pay_ref       = r.rac_pay_ref
   AND a.aca_rac_accno = r.rac_accno
   AND p_ara_code      = a.aca_ara_code
   AND p_date          = trunc(a.aca_created_date);
--
-- ************************************************************************************
--
CURSOR c_get_con_refno(p_con_ref VARCHAR2) 
is
SELECT con_reusable_refno
  FROM contractors
 WHERE con_code = p_con_ref;
--
-- ************************************************************************************
--
CURSOR c_get_cos_refno(p_cos_ref VARCHAR2) 
is
SELECT cos_reusable_refno
  FROM contractor_sites
 WHERE cos_code = p_cos_ref;
--
-- ************************************************************************************
--
CURSOR c_get_par_refno(p_per_alt_ref VARCHAR2) 
is
SELECT par_reusable_refno
  FROM parties
 WHERE par_per_alt_ref = p_per_alt_ref;
--
-- ************************************************************************************
--
CURSOR c_get_pro_refno(p_pro_propref VARCHAR2) 
is
SELECT pro_reusable_refno
  FROM properties
 WHERE pro_propref = p_pro_propref;
--
-- ************************************************************************************
--
CURSOR c_get_srq_refno(p_srq_ref VARCHAR2) 
is
SELECT srq_reusable_refno
  FROM service_requests
 WHERE srq_legacy_refno = p_srq_ref;
--
-- ************************************************************************************
--
CURSOR c_get_tcy_refno(p_tcy_alt_ref VARCHAR2) 
is
SELECT tcy_reusable_refno
  FROM tenancies
 WHERE tcy_refno = p_tcy_alt_ref;
--
-- ************************************************************************************
--
CURSOR c_get_wor_refno(p_wor_ref VARCHAR2) 
is
SELECT wor_reusable_refno
  FROM works_orders
 WHERE wor_legacy_ref = p_wor_ref;
--
-- ************************************************************************************
--
CURSOR c_get_srt_refno(p_srt_sud_pro_aun_code VARCHAR2,
                       p_srt_ele_code         VARCHAR2,
                       p_scs_reference        VARCHAR2)
IS
SELECT srt_reusable_refno
  FROM survey_results,
       stock_condition_surveys
 WHERE srt_sud_pro_aun_code = p_srt_sud_pro_aun_code
   AND srt_ele_code         = p_srt_ele_code
   AND scs_reference        = p_scs_reference
   AND scs_refno            = srt_sud_scs_refno;
--
-- ************************************************************************************
--
CURSOR c_get_cve_refno(p_cnt_reference VARCHAR2,
                       p_version_no    VARCHAR2)   
IS
SELECT cve_reusable_refno
  FROM contract_versions
 WHERE cve_cnt_reference  = p_cnt_reference 
   AND cve_version_number = p_version_no;
--
-- ************************************************************************************
--
CURSOR c_get_tve_refno (p_tkg_src_reference  VARCHAR2
                       ,p_tkg_code           VARCHAR2
                       ,p_tkg_src_type       VARCHAR2
                       ,p_stk_code           VARCHAR2
                       ,p_version_number     NUMBER)  
IS
SELECT tve_reusable_refno
  FROM task_versions,
       tasks
 WHERE tsk_id                    = tve_tsk_id
   AND tve_tsk_tkg_src_reference = p_tkg_src_reference
   AND tve_tsk_tkg_code          = p_tkg_code
   AND tve_tsk_tkg_src_type      = p_tkg_src_type
   AND tsk_stk_code              = p_stk_code
   AND tve_version_number        = p_version_number;
--
-- ************************************************************************************
--
CURSOR c_get_dve_refno(p_cnt_reference VARCHAR2,
                       p_pro_aun_code  VARCHAR2,
                       p_pro_aun_ind   VARCHAR2,
                       p_display_seqno VARCHAR2)   
IS
SELECT dve_reusable_refno
  FROM deliverables,
       deliverable_versions
 WHERE dlv_refno            = dve_dlv_refno
   AND dlv_cnt_reference    = p_cnt_reference
   AND dlv_cad_pro_aun_code = p_pro_aun_code
   AND dlv_cad_type_ind     = p_pro_aun_ind
   AND dve_display_sequence = p_display_seqno
   AND dve_current_ind      = 'Y';
--
-- ************************************************************************************
--
CURSOR c_get_ipp_refno(p_ipp_shortname VARCHAR2,
                       p_ipp_ipt_code  VARCHAR2)   
IS
SELECT ipp_reusable_refno
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_shortname
   AND ipp_ipt_code  = p_ipp_ipt_code;
--
-- ************************************************************************************
--
CURSOR c_get_ipt_pgp_refno(p_ipt_code  VARCHAR2)   
IS
SELECT ipt_pgp_refno
  FROM interested_party_types
 WHERE ipt_code = p_ipt_code;
--
-- ************************************************************************************
--
CURSOR c_get_hoop_pgp_refno(p_hoop_code  VARCHAR2)   
IS
SELECT hoop_pgp_refno
  FROM housing_options
 WHERE hoop_code = p_hoop_code;
--
-- ************************************************************************************
--
CURSOR c_get_aet_pgp_refno(p_aet_code  VARCHAR2)   
IS
SELECT aet_pgp_refno
  FROM action_event_types
 WHERE aet_code = p_aet_code;
--
-- ************************************************************************************
--
CURSOR c_get_bro_pgp_refno(p_bro_code  VARCHAR2)   
IS
SELECT bro_pgp_action_refno
  FROM business_reasons
 WHERE bro_code = p_bro_code;
--
-- ************************************************************************************
--
CURSOR c_get_ban_bro_code(p_ban_reference VARCHAR2)   
IS
SELECT ban_bro_code
  FROM business_actions
 WHERE ban_reference = p_ban_reference;
--
-- ************************************************************************************
--
CURSOR c_get_pdu_details(p_pdf_name  VARCHAR2, 
                         p_pgp_refno NUMBER) 
IS
SELECT 'X',
       PDU_PDF_NAME,
       PDU_POB_TABLE_NAME,
       PDU_PGP_REFNO,
       PDU_DISPLAY_SEQNO,
       PDU_PDF_PARAM_TYPE
  FROM parameter_definition_usages
 WHERE pdu_pdf_name       = p_pdf_name
   AND pdu_pgp_refno      = p_pgp_refno
   AND pdu_pdf_param_type = 'OTHER FIELDS';
--
-- ************************************************************************************
--
CURSOR c_get_real_refno(p_real_reference VARCHAR2)   
IS
SELECT real_reuseable_refno
  FROM registered_address_lettings
 WHERE real_reference = p_real_reference;
--
-- ************************************************************************************
--
CURSOR c_get_bae_refno(p_bae_ban_reference    VARCHAR2,
                       p_bae_aet_code         VARCHAR2,
                       p_bae_status_date      DATE,
                       p_bae_seqno            NUMBER)   
IS
SELECT bae_reusable_refno
  FROM business_action_events
 WHERE bae_ban_reference = p_bae_ban_reference
   AND bae_aet_code      = p_bae_aet_code
   AND bae_status_date   = p_bae_status_date
   AND bae_sequence      = p_bae_seqno;
--
-- ************************************************************************************
--
CURSOR c_chk_pva_exists(P_PVA_REUSABLE_REFNO     NUMBER,
                        P_PVA_PDU_PDF_NAME       VARCHAR2,
                        P_PVA_PDU_PDF_PARAM_TYPE VARCHAR2,
                        P_PVA_PDU_POB_TABLE_NAME VARCHAR2,
                        P_PVA_PDU_PGP_REFNO      NUMBER,
                        P_PVA_PDU_DISPLAY_SEQNO  NUMBER)
IS
SELECT 'X'
  FROM parameter_values
 WHERE PVA_REUSABLE_REFNO     = P_PVA_REUSABLE_REFNO
   AND PVA_PDU_PDF_NAME       = P_PVA_PDU_PDF_NAME
   AND PVA_PDU_PDF_PARAM_TYPE = P_PVA_PDU_PDF_PARAM_TYPE
   AND PVA_PDU_POB_TABLE_NAME = P_PVA_PDU_POB_TABLE_NAME
   AND PVA_PDU_PGP_REFNO      = P_PVA_PDU_PGP_REFNO
   AND PVA_PDU_DISPLAY_SEQNO  = P_PVA_PDU_DISPLAY_SEQNO;
--
-- ************************************************************************************
--
CURSOR chk_acho_exists(p_acho_reference VARCHAR2) 
IS
SELECT acho_reference
  FROM advice_case_housing_options
 WHERE acho_alternative_reference = p_acho_reference;
--
-- ************************************************************************************
--
-- Constants for process_summary
--
cb       		VARCHAR2(30);
cd      		DATE;
cp       		VARCHAR2(30) := 'VALIDATE';
ct       		VARCHAR2(30) := 'DL_MAD_OTHER_FIELD_VAL_HIST';
cs       		INTEGER;
ce       		VARCHAR2(200);
l_id            	ROWID;
--
-- Other Constants
--
l_exists         	VARCHAR2(1);
l_errors         	VARCHAR2(10);
l_error_ind      	VARCHAR2(10);
i                	INTEGER :=0;
--
--
l_reusable_refno  	NUMBER(10);
l_pdf_name		VARCHAR2(30);
l_param_type      	VARCHAR2(12);
l_pob_table_name        VARCHAR2(30);
l_pgp_refno       	NUMBER(10);
l_display_seqno   	NUMBER(3);
--
l_ara_pgp_refno   	INTEGER;
l_pro_aun_code    	VARCHAR2(20);
--
l_ipt_pgp_refno         INTEGER;
l_ipt_pdu_exists        VARCHAR2(1);
--
l_hoop_pgp_refno        INTEGER;
l_hoop_pdu_exists       VARCHAR2(1);
--
l_aet_pgp_refno         INTEGER;
l_aet_pdu_exists        VARCHAR2(1);
--
l_bro_pgp_refno         INTEGER;
l_bro_pdu_exists        VARCHAR2(1);
l_ban_bro_code          VARCHAR2(10);
--
l_pva_exists            VARCHAR2(1);
--
l_acho_reference       	NUMBER(10);
--
BEGIN
--
    fsc_utils.proc_start('s_dl_mad_other_field_val_hist.dataload_validate');
    fsc_utils.debug_message('s_dl_mad_other_field_val_hist.dataload_validate',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lpvh_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
--
          l_reusable_refno := NULL;
          l_pdf_name       := NULL;
          l_param_type     := NULL;
          l_pob_table_name := NULL;
          l_pgp_refno      := NULL;
          l_display_seqno  := NULL;
--
--
          l_ara_pgp_refno  := NULL;
          l_pro_aun_code   := NULL;
          l_ipt_pgp_refno  := NULL;
          l_hoop_pgp_refno := NULL;
          l_aet_pgp_refno  := NULL;
          l_bro_pgp_refno  := NULL;
--
          l_ipt_pdu_exists  := NULL;
          l_hoop_pdu_exists := NULL;
          l_aet_pdu_exists  := NULL;
          l_bro_pdu_exists  := NULL;
          l_ban_bro_code    := NULL;
--
          l_pva_exists      := NULL;
--
-- ************************************************************************************
--
-- Check the Links to Other Tables
--
-- Check the Definition Name is valid
--
--
          IF (p1.lpvh_pva_pdu_pob_table_name NOT IN ('INTERESTED_PARTIES',
                                                     'BUSINESS_ACTIONS',
                                                     'BUSINESS_ACTION_EVENTS')) THEN
--
--
-- Get the parameter details so we can check the datatype is correct
--
            OPEN c_pdu(p1.lpvh_pva_pdu_pdf_name, p1.lpvh_pva_pdu_pob_table_name);
           FETCH c_pdu INTO l_pdf_name,l_pob_table_name,l_pgp_refno,l_display_seqno,l_param_type;
           CLOSE c_pdu;
--
           IF l_pdf_name IS NULL THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',933);
           END IF;
--
          END IF;
--
-- ***************************
--
          IF l_pob_table_name = 'ADMIN_UNITS' THEN
--
            OPEN c_get_aun_refno(p1.lpvh_legacy_ref);
           FETCH c_get_aun_refno into l_reusable_refno;
           CLOSE c_get_aun_refno;
--
           IF l_reusable_refno IS NULL THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
           END IF;
--
          ELSIF l_pob_table_name = 'ARREARS_ACTIONS' THEN
--
               OPEN c_get_aca_refno(p1.lpvh_legacy_ref,p1.lpvh_secondary_ref,p1.lpvh_secondary_date);
              FETCH c_get_aca_refno into l_reusable_refno;
              CLOSE c_get_aca_refno;
--
              IF l_reusable_refno IS NULL THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',941);
              END IF;
--
          ELSIF l_pob_table_name = 'CONTRACTORS' THEN
--
               OPEN c_get_con_refno(p1.lpvh_legacy_ref);
              FETCH c_get_con_refno into l_reusable_refno;
              CLOSE c_get_con_refno;
--
              IF l_reusable_refno IS NULL THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',939);
              END IF;
--
          ELSIF l_pob_table_name = 'CONTRACTOR_SITES' THEN
--
               OPEN c_get_cos_refno(p1.lpvh_legacy_ref);
              FETCH c_get_cos_refno into l_reusable_refno;
              CLOSE c_get_cos_refno;
--
              IF l_reusable_refno IS NULL THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',718);
              END IF;
--
          ELSIF l_pob_table_name = 'PARTIES' THEN
--
               OPEN c_get_par_refno(p1.lpvh_legacy_ref);
              FETCH c_get_par_refno into l_reusable_refno;
              CLOSE c_get_par_refno;
--
              IF l_reusable_refno IS NULL THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',510);
              END IF;
--
          ELSIF l_pob_table_name = 'PROPERTIES' THEN 
--
               OPEN c_get_pro_refno(p1.lpvh_legacy_ref);
              FETCH c_get_pro_refno into l_reusable_refno;
              CLOSE c_get_pro_refno;
--
              IF l_reusable_refno IS NULL THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
              END IF;
--
          ELSIF l_pob_table_name = 'SERVICE_REQUESTS' THEN
--
               OPEN c_get_srq_refno(p1.lpvh_legacy_ref);
              FETCH c_get_srq_refno into l_reusable_refno;
              CLOSE c_get_srq_refno;
--
              IF l_reusable_refno IS NULL THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',693);
              END IF;
--
          ELSIF l_pob_table_name = 'TENANCIES' THEN
--
               OPEN c_get_tcy_refno(p1.lpvh_legacy_ref);
              FETCH c_get_tcy_refno into l_reusable_refno;
              CLOSE c_get_tcy_refno;
--
              IF l_reusable_refno IS NULL THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',080);
              END IF;
--
          ELSIF l_pob_table_name = 'WORKS_ORDERS' THEN
--
               OPEN c_get_wor_refno(p1.lpvh_legacy_ref);
              FETCH c_get_wor_refno into l_reusable_refno;
              CLOSE c_get_wor_refno;
--
              IF l_reusable_refno IS NULL THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',938);
              END IF;
--
          ELSIF l_pob_table_name = 'SURVEY_RESULTS' THEN
--
               OPEN c_get_srt_refno(p1.lpvh_legacy_ref,p1.lpvh_secondary_ref,p1.lpvh_pva_further_ref);
              FETCH c_get_srt_refno into l_reusable_refno;
              CLOSE c_get_srt_refno;
--
              IF l_reusable_refno IS NULL THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',047);
              END IF;
--
          ELSIF l_pob_table_name = 'CONTRACT_VERSIONS' THEN
--
               OPEN c_get_cve_refno(p1.lpvh_legacy_ref, p1.lpvh_secondary_ref );
              FETCH c_get_cve_refno into l_reusable_refno;
              CLOSE c_get_cve_refno;
--
              IF l_reusable_refno IS NULL THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',507);
              END IF;
--
          ELSIF l_pob_table_name = 'CONTRACT_TASKS' THEN
--
               OPEN c_get_tve_refno(p1.lpvh_legacy_ref, p1.lpvh_secondary_ref,
                                    p1.lpvh_pva_further_ref, p1.lpvh_pva_further_ref2,
                                    p1.lpvh_pva_further_ref3);
--
              FETCH c_get_tve_refno into l_reusable_refno;
              CLOSE c_get_tve_refno;
--
              IF l_reusable_refno IS NULL THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',507);
              END IF;
--
          ELSIF l_pob_table_name = 'DELIVERABLES' THEN
--
-- Get the pro_refno or use Admin Unit Code supplied
--
              IF p1.lpvh_pva_further_ref = 'P' THEN
               l_pro_aun_code := s_properties.get_refno_for_propref(p1.lpvh_secondary_ref);
              ELSE
                 l_pro_aun_code := p1.lpvh_secondary_ref;
--
                  OPEN c_get_dve_refno(p1.lpvh_legacy_ref,
                                       l_pro_aun_code,
                                       p1.lpvh_pva_further_ref,
                                       p1.lpvh_pva_further_ref2);
--
                 FETCH c_get_dve_refno into l_reusable_refno;
                 CLOSE c_get_dve_refno;
--
                 IF l_reusable_refno IS NULL THEN
                  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',569);
                 END IF;
--
              END IF;
--
          ELSIF l_pob_table_name = 'REGISTERED_ADDRESS_LETTINGS' THEN
--
               OPEN c_get_real_refno(p1.lpvh_legacy_ref);
              FETCH c_get_real_refno into l_reusable_refno;
              CLOSE c_get_real_refno;
-- 
              IF l_reusable_refno IS NULL THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',572);
              END IF;
--
          END IF;
--
--
-- For interested parties 
-- 1) we have to get the pgp_refno assigned to the ipt_code
-- 2) Check the parameter definition usages record exists for parameter name
--    pgp_refno
-- 3) get reusable_refno for ip based on shortname and ipt_code
--
--
          IF (p1.lpvh_pva_pdu_pob_table_name = 'INTERESTED_PARTIES') THEN
--
            OPEN c_get_ipt_pgp_refno(p1.lpvh_secondary_ref);
           FETCH c_get_ipt_pgp_refno INTO l_ipt_pgp_refno;
           CLOSE c_get_ipt_pgp_refno;
--
           IF (l_ipt_pgp_refno IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',635);
--
           ELSE 
--
               OPEN c_get_pdu_details(p1.lpvh_pva_pdu_pdf_name, l_ipt_pgp_refno);
              FETCH c_get_pdu_details INTO l_ipt_pdu_exists,
                                           l_pdf_name,
                                           l_pob_table_name,
                                           l_pgp_refno,
                                           l_display_seqno,
                                           l_param_type;
--
              CLOSE c_get_pdu_details;
--
              IF (l_ipt_pdu_exists IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',636);
              END IF;
--
           END IF;
-- 
            OPEN c_get_ipp_refno(p1.lpvh_legacy_ref, p1.lpvh_secondary_ref);
           FETCH c_get_ipp_refno into l_reusable_refno;
           CLOSE c_get_ipp_refno;
--
           IF l_reusable_refno IS NULL THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',570);
           END IF;
-- 
          END IF;
--
--
-- For Advice Case Housing Options 
-- 1) we have to get the pgp_refno assigned to the hoop_code
-- 2) Check the parameter definition usages record exists for parameter name
--    pgp_refno
-- 3) 
--
--          IF (p1.lpvh_pva_pdu_pob_table_name = 'ADVICE_CASE_HOUSING_OPTIONS') THEN
--
--           l_acho_reference := NULL;
--
--            OPEN chk_acho_exists(p1.lpvh_legacy_ref);
--           FETCH chk_acho_exists INTO l_acho_reference;
--           CLOSE chk_acho_exists;
--
--           IF (l_acho_reference IS NULL) THEN
--            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',185);
--           END IF;
--
--            OPEN c_get_hoop_pgp_refno(p1.lpvh_secondary_ref);
--           FETCH c_get_hoop_pgp_refno INTO l_hoop_pgp_refno;
--           CLOSE c_get_hoop_pgp_refno;
--
--           IF (l_hoop_pgp_refno IS NULL) THEN
--
--            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',638);
--
--           ELSE 
--
--               OPEN c_get_pdu_details(p1.lpvh_pva_pdu_pdf_name, l_hoop_pgp_refno);
--              FETCH c_get_pdu_details INTO l_hoop_pdu_exists,
--                                           l_pdf_name,
--                                           l_pob_table_name,
--                                           l_pgp_refno,
--                                           l_display_seqno,
--                                           l_param_type;
--
--              CLOSE c_get_pdu_details;
--
--              IF (l_hoop_pdu_exists IS NULL) THEN
--               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',639);
--              END IF;
--
--           END IF;
-- 
--          END IF;
--
--
-- For business actions
-- 1) we have to get the action pgp_refno assigned to the bro_code 
-- 2) Check the parameter definition usages record exists for parameter name
--    pgp_refno
-- 3) get reusable_refno for ban based on ban_reference
--
          IF (p1.lpvh_pva_pdu_pob_table_name = 'BUSINESS_ACTIONS') THEN
-- 
            OPEN c_get_ban_bro_code(p1.lpvh_legacy_ref);
           FETCH c_get_ban_bro_code into l_ban_bro_code;
           CLOSE c_get_ban_bro_code;
--
           IF (l_ban_bro_code IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',571);
--
           ELSE
--
               OPEN c_get_bro_pgp_refno(l_ban_bro_code);
              FETCH c_get_bro_pgp_refno INTO l_bro_pgp_refno;
              CLOSE c_get_bro_pgp_refno;
--
              IF (l_bro_pgp_refno IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',640);
--
              ELSE 
--
                  OPEN c_get_pdu_details(p1.lpvh_pva_pdu_pdf_name, l_bro_pgp_refno);
                 FETCH c_get_pdu_details INTO l_bro_pdu_exists,
                                              l_pdf_name,
                                              l_pob_table_name,
                                              l_pgp_refno,
                                              l_display_seqno,
                                              l_param_type;
--
                 CLOSE c_get_pdu_details;
--
                 IF (l_bro_pdu_exists IS NULL) THEN
                  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',641);
                 END IF;
--
              END IF;
--
           END IF;
--
          END IF;
--
--
-- For business action events
-- 1) we have to get the pgp_refno assigned to the aet_code
-- 2) Check the parameter definition usages record exists for parameter name
--    pgp_refno
-- 3) get reusable_refno for bae based on ban_reference, aet_code, status_date
--
--
          IF (p1.lpvh_pva_pdu_pob_table_name = 'BUSINESS_ACTION_EVENTS') THEN
--
            OPEN c_get_aet_pgp_refno(p1.lpvh_secondary_ref);
           FETCH c_get_aet_pgp_refno INTO l_aet_pgp_refno;
           CLOSE c_get_aet_pgp_refno;
--
           IF (l_aet_pgp_refno IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',642);
--
           ELSE 
--
               OPEN c_get_pdu_details(p1.lpvh_pva_pdu_pdf_name, l_aet_pgp_refno);
              FETCH c_get_pdu_details INTO l_aet_pdu_exists,
                                           l_pdf_name,
                                           l_pob_table_name,
                                           l_pgp_refno,
                                           l_display_seqno,
                                           l_param_type;
--
              CLOSE c_get_pdu_details;
--
              IF (l_aet_pdu_exists IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',643);
              END IF;
--
           END IF;
-- 
            OPEN c_get_bae_refno(p1.lpvh_legacy_ref, 
                                 p1.lpvh_secondary_ref, 
                                 p1.lpvh_secondary_date,
                                 p1.lpvh_pva_further_ref);
--
           FETCH c_get_bae_refno into l_reusable_refno;
           CLOSE c_get_bae_refno;
--
           IF l_reusable_refno IS NULL THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',573);
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- Now check that there is an entry in parameter values
--
          IF (    l_pdf_name       IS NOT NULL
              AND l_reusable_refno IS NOT NULL) THEN
--
            OPEN c_chk_pva_exists(l_reusable_refno,
                                  l_pdf_name,
                                  l_param_type,
                                  l_pob_table_name,
                                  l_pgp_refno,
                                  l_display_seqno);
--
           FETCH c_chk_pva_exists into l_pva_exists;
           CLOSE c_chk_pva_exists;
--
           IF l_pva_exists IS NULL THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',644);
           END IF;
--
          END IF;
--
--
--
-- ************************************************************************************
--
--
-- Now UPDATE the record count AND error code
--
          IF l_errors = 'F' THEN
           l_error_ind := 'Y';
          ELSE
             l_error_ind := 'N';
          END IF;
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
          set_record_status_flag(l_id,l_errors);
--
--
-- keep a count of the rows processed and commit after every 1000
--
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
--
          EXCEPTION
               WHEN OTHERS THEN
                  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
                  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
                  set_record_status_flag(l_id,'O');
--
      END;
--
    END LOOP;
--
    COMMIT;
--
    fsc_utils.proc_END;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
-- ************************************************************************************
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT rowid rec_rowid,
       LPVH_DLB_BATCH_ID,
       LPVH_DL_SEQNO,
       LPVH_DL_LOAD_STATUS,
       LPVH_LEGACY_REF,
       LPVH_PVA_PDU_PDF_NAME,
       LPVH_PVA_DATE_VALUE,
       LPVH_PVA_NUMBER_VALUE,
       LPVH_PVA_CHAR_VALUE,
       LPVH_SECONDARY_REF,
       LPVH_SECONDARY_DATE,
       LPVH_PVA_PDU_POB_TABLE_NAME,
       NVL(LPVH_CREATED_BY,'DATALOAD')  LPVH_CREATED_BY, 
       NVL(LPVH_CREATED_DATE,SYSDATE)   LPVH_CREATED_DATE,
       NVL(LPVH_MODIFIED_BY,'DATALOAD') LPVH_MODIFIED_BY,
       NVL(LPVH_MODIFIED_DATE,SYSDATE)  LPVH_MODIFIED_DATE,
       LPVH_PVA_FURTHER_REF,
       LPVH_PVA_HRV_LOC_CODE,
       LPVH_PVA_FURTHER_REF2,
       LPVH_PVA_FURTHER_REF3,
       LPVH_DESC
  FROM dl_mad_other_field_val_hist
 WHERE lpvh_dlb_batch_id   = p_batch_id
   AND lpvh_dl_load_status = 'C';
--
-- ************************************************************************************
--
CURSOR c_pdu(p_pdf_name   VARCHAR2, 
             p_table_name VARCHAR2) 
IS
SELECT PDU_PDF_NAME,
       PDU_POB_TABLE_NAME,
       PDU_PGP_REFNO,
       PDU_DISPLAY_SEQNO,
       PDU_PDF_PARAM_TYPE
  FROM parameter_definition_usages
 WHERE pdu_pdf_name       = p_pdf_name
   AND pdu_pdf_param_type = 'OTHER FIELDS'
   AND pdu_pob_table_name = p_table_name;      
--
-- ************************************************************************************
--
CURSOR c_get_aun_refno(p_aun_ref VARCHAR2) 
is
SELECT aun_reusable_refno
  FROM admin_units
 WHERE aun_code = p_aun_ref;
--
-- ************************************************************************************
--
CURSOR c_get_aca_refno(p_pay_ref  VARCHAR2
                      ,p_ara_code VARCHAR2
                      ,p_date     DATE) 
is
SELECT aca_reusable_refno
  FROM account_arrears_actions a,revenue_accounts r
 WHERE p_pay_ref       = r.rac_pay_ref
   AND a.aca_rac_accno = r.rac_accno
   AND p_ara_code      = a.aca_ara_code
   AND p_date          = trunc(a.aca_created_date);
--
-- ************************************************************************************
--
CURSOR c_get_con_refno(p_con_ref VARCHAR2) 
is
SELECT con_reusable_refno
  FROM contractors
 WHERE con_code = p_con_ref;
--
-- ************************************************************************************
--
CURSOR c_get_cos_refno(p_cos_ref VARCHAR2) 
is
SELECT cos_reusable_refno
  FROM contractor_sites
 WHERE cos_code = p_cos_ref;
--
-- ************************************************************************************
--
CURSOR c_get_par_refno(p_per_alt_ref VARCHAR2)
is
SELECT par_reusable_refno
  FROM parties
 WHERE par_per_alt_ref = p_per_alt_ref;
--
-- ************************************************************************************
--
CURSOR c_get_pro_refno(p_pro_propref VARCHAR2) 
is
SELECT pro_reusable_refno
  FROM properties
 WHERE p_pro_propref = pro_propref;
--
-- ************************************************************************************
--
CURSOR c_get_srq_refno(p_srq_ref VARCHAR2) 
is
SELECT srq_reusable_refno
  FROM service_requests
 WHERE srq_legacy_refno = p_srq_ref;
--
-- ************************************************************************************
--
CURSOR c_get_tcy_refno(p_tcy_alt_ref VARCHAR2) 
is
SELECT tcy_reusable_refno
  FROM tenancies
 WHERE tcy_refno = p_tcy_alt_ref;
--
-- ************************************************************************************
--
CURSOR c_get_wor_refno(p_wor_ref VARCHAR2) 
is
SELECT wor_reusable_refno
  FROM works_orders
 WHERE wor_legacy_ref = p_wor_ref;
--
-- ************************************************************************************
--
CURSOR c_get_srt_refno(p_srt_sud_pro_aun_code VARCHAR2,
                       p_srt_ele_code         VARCHAR2,
                       p_scs_reference        VARCHAR2) 
IS
SELECT srt_reusable_refno
  FROM survey_results,
       stock_condition_surveys
 WHERE srt_sud_pro_aun_code = p_srt_sud_pro_aun_code
   AND srt_ele_code         = p_srt_ele_code
   AND scs_reference        = p_scs_reference
   AND scs_refno            = srt_sud_scs_refno;
--
-- ************************************************************************************
--
CURSOR c_get_cve_refno(p_cnt_reference VARCHAR2,
                       p_version_no    VARCHAR2)   
IS
SELECT cve_reusable_refno
  FROM contract_versions
 WHERE cve_cnt_reference  = p_cnt_reference
   AND cve_version_number = p_version_no;
--
-- ************************************************************************************
--
CURSOR c_get_tve_refno (p_tkg_src_reference  VARCHAR2
                       ,p_tkg_code           VARCHAR2
                       ,p_tkg_src_type       VARCHAR2
                       ,p_stk_code           VARCHAR2
                       ,p_version_number     NUMBER)  
IS
SELECT tve_reusable_refno
  FROM task_versions,
       tasks
 WHERE tsk_id                    = tve_tsk_id
   AND tve_tsk_tkg_src_reference = p_tkg_src_reference
   AND tve_tsk_tkg_code          = p_tkg_code
   AND tve_tsk_tkg_src_type      = p_tkg_src_type
   AND tsk_stk_code              = p_stk_code
   AND tve_version_number        = p_version_number;
--
-- ************************************************************************************
--
CURSOR c_get_dve_refno(p_cnt_reference VARCHAR2,
                       p_pro_aun_code  VARCHAR2,
                       p_pro_aun_ind   VARCHAR2,
                       p_display_seqno VARCHAR2)   
IS
SELECT dve_reusable_refno
  FROM deliverables,
       deliverable_versions
 WHERE dlv_refno            = dve_dlv_refno
   AND dlv_cnt_reference    = p_cnt_reference
   AND dlv_cad_pro_aun_code = p_pro_aun_code
   AND dlv_cad_type_ind     = p_pro_aun_ind
   AND dve_display_sequence = p_display_seqno
   AND dve_current_ind      = 'Y';
--
-- ************************************************************************************
--
CURSOR c_get_ipp_refno(p_ipp_shortname VARCHAR2,
                       p_ipp_ipt_code  VARCHAR2)   
IS
SELECT ipp_reusable_refno
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_shortname
   AND ipp_ipt_code  = p_ipp_ipt_code;
--
-- ************************************************************************************
--
CURSOR c_get_ban_refno(p_ban_reference VARCHAR2)   
IS
SELECT ban_reusable_refno
  FROM business_actions
 WHERE ban_reference = p_ban_reference;
--
-- ************************************************************************************
--
CURSOR c_get_real_refno(p_real_reference VARCHAR2)   
IS
SELECT real_reuseable_refno
  FROM registered_address_lettings
 WHERE real_reference = p_real_reference;
--
-- ************************************************************************************
--
CURSOR c_get_bae_refno(p_bae_ban_reference VARCHAR2,
                       p_bae_aet_code      VARCHAR2,
                       p_bae_status_date   DATE,
                       p_bae_seqno         NUMBER)   
IS
SELECT bae_reusable_refno
  FROM business_action_events
 WHERE bae_ban_reference = p_bae_ban_reference
   AND bae_aet_code      = p_bae_aet_code
   AND bae_status_date   = p_bae_status_date
   AND bae_sequence      = p_bae_seqno;
--
-- ************************************************************************************
--
CURSOR c_get_acho_reference(p_acho_reference VARCHAR2)
IS
SELECT acho_reference
  FROM advice_case_housing_options
 WHERE acho_alternative_reference = p_acho_reference;
--
-- ************************************************************************************
--
CURSOR c_get_lebe_refno(p_acho_reference NUMBER,
                        p_created_date   DATE,
                        p_description    VARCHAR2)   
IS
SELECT lebe_reusable_refno
  FROM lettings_benchmarks
 WHERE lebe_acho_reference = p_acho_reference
   AND lebe_created_date   = p_created_date
   AND lebe_description    = p_description
   AND rownum = 1;
--
-- ************************************************************************************
--
-- Constants for process_summary
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'DELETE';
ct       		VARCHAR2(30) := 'DL_MAD_OTHER_FIELD_VAL_HIST';
cs       		INTEGER;
ce       		VARCHAR2(200);
l_id                 	ROWID;
l_an_tab          	VARCHAR2(1);
--
-- Other Variables
--
l_reusable_refno  	NUMBER(10);
l_pdf_name		VARCHAR2(30);
l_param_type      	VARCHAR2(12);
l_pob_table_name        VARCHAR2(30);
l_pgp_refno       	NUMBER(10);
l_display_seqno   	NUMBER(3);
--
i                 	INTEGER := 0;
l_pro_aun_code   	VARCHAR2(20);
--
l_acho_reference	NUMBER(10);

BEGIN
--
    fsc_utils.proc_start('s_dl_mad_other_field_val_hist.dataload_delete');
    fsc_utils.debug_message( 's_dl_mad_other_field_val_hist.dataload_delete',3 );
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lpvh_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
--
          l_reusable_refno := NULL;
          l_pdf_name       := NULL;
          l_param_type     := NULL;
          l_pob_table_name := NULL;
          l_pgp_refno      := NULL;
          l_display_seqno  := NULL;
--
--
          IF (p1.lpvh_pva_pdu_pob_table_name NOT IN ('INTERESTED_PARTIES',
                                                     'BUSINESS_ACTIONS',
                                                     'BUSINESS_ACTION_EVENTS')) THEN
--
            OPEN c_pdu(p1.lpvh_pva_pdu_pdf_name, p1.lpvh_pva_pdu_pob_table_name);
           FETCH c_pdu INTO l_pdf_name,l_pob_table_name,l_pgp_refno,l_display_seqno,l_param_type;
           CLOSE c_pdu;
--
          END IF;
--
--
          IF l_pob_table_name = 'ADMIN_UNITS' THEN
--
            OPEN c_get_aun_refno(p1.lpvh_legacy_ref);
           FETCH c_get_aun_refno into l_reusable_refno;
           CLOSE c_get_aun_refno;
--
          ELSIF l_pob_table_name = 'ARREARS_ACTIONS' THEN
--
               OPEN c_get_aca_refno(p1.lpvh_legacy_ref,p1.lpvh_secondary_ref,p1.lpvh_secondary_date);
              FETCH c_get_aca_refno into l_reusable_refno;
              CLOSE c_get_aca_refno;
--
          ELSIF l_pob_table_name = 'CONTRACTORS' THEN
--
               OPEN c_get_con_refno(p1.lpvh_legacy_ref);
              FETCH c_get_con_refno into l_reusable_refno;
              CLOSE c_get_con_refno;
--
          ELSIF l_pob_table_name = 'CONTRACTOR_SITES' THEN
--
               OPEN c_get_cos_refno(p1.lpvh_legacy_ref);
              FETCH c_get_cos_refno into l_reusable_refno;
              CLOSE c_get_cos_refno;
--
          ELSIF l_pob_table_name = 'PARTIES' THEN
--
               OPEN c_get_par_refno(p1.lpvh_legacy_ref);
              FETCH c_get_par_refno into l_reusable_refno;
              CLOSE c_get_par_refno;
--
          ELSIF l_pob_table_name = 'PROPERTIES' THEN 
--
               OPEN c_get_pro_refno(p1.lpvh_legacy_ref);
              FETCH c_get_pro_refno into l_reusable_refno;
              CLOSE c_get_pro_refno;
--
          ELSIF l_pob_table_name = 'SERVICE_REQUESTS' THEN
--
               OPEN c_get_srq_refno(p1.lpvh_legacy_ref);
              FETCH c_get_srq_refno into l_reusable_refno;
              CLOSE c_get_srq_refno;
--
          ELSIF l_pob_table_name = 'TENANCIES' THEN

               OPEN c_get_tcy_refno(p1.lpvh_legacy_ref);
              FETCH c_get_tcy_refno into l_reusable_refno;
              CLOSE c_get_tcy_refno;
--
          ELSIF l_pob_table_name = 'WORKS_ORDERS' THEN
--
               OPEN c_get_wor_refno(p1.lpvh_legacy_ref);
              FETCH c_get_wor_refno into l_reusable_refno;
              CLOSE c_get_wor_refno;
-- 
          ELSIF l_pob_table_name = 'SURVEY_RESULTS' THEN
--
               OPEN c_get_srt_refno(p1.lpvh_legacy_ref,p1.lpvh_secondary_ref,p1.lpvh_pva_further_ref);
              FETCH c_get_srt_refno into l_reusable_refno;
              CLOSE c_get_srt_refno;
--
          ELSIF l_pob_table_name = 'CONTRACT_VERSIONS' THEN
--
               OPEN c_get_cve_refno(p1.lpvh_legacy_ref, p1.lpvh_secondary_ref );
              FETCH c_get_cve_refno into l_reusable_refno;
              CLOSE c_get_cve_refno;
--
          ELSIF l_pob_table_name = 'CONTRACT_TASKS' THEN
--
               OPEN c_get_tve_refno(p1.lpvh_legacy_ref,
                                    p1.lpvh_secondary_ref,
                                    p1.lpvh_pva_further_ref,
                                    p1.lpvh_pva_further_ref2,
                                    p1.lpvh_pva_further_ref3);
--
              FETCH c_get_tve_refno into l_reusable_refno;
              CLOSE c_get_tve_refno;
-- 
          ELSIF l_pob_table_name = 'DELIVERABLES' THEN
--
-- Get the pro_refno or use Admin Unit Code supplied
--
              IF p1.lpvh_pva_further_ref = 'P' THEN
--
               l_pro_aun_code := s_properties.get_refno_for_propref(p1.lpvh_secondary_ref);
--
              ELSE
--
                 l_pro_aun_code := p1.lpvh_secondary_ref;
--
                  OPEN c_get_dve_refno(p1.lpvh_legacy_ref,
                                       l_pro_aun_code,
                                       p1.lpvh_pva_further_ref,
                                       p1.lpvh_pva_further_ref2);
--
                 FETCH c_get_dve_refno into l_reusable_refno;
                 CLOSE c_get_dve_refno;
--
              END IF;
--
          ELSIF l_pob_table_name = 'REGISTERED_ADDRESS_LETTINGS' THEN
--
               OPEN c_get_real_refno(p1.lpvh_legacy_ref);
              FETCH c_get_real_refno into l_reusable_refno;
              CLOSE c_get_real_refno;
-- 
          END IF;
--
          IF (p1.lpvh_pva_pdu_pob_table_name = 'INTERESTED_PARTIES') THEN
--
               OPEN c_get_ipp_refno(p1.lpvh_legacy_ref, p1.lpvh_secondary_ref);
              FETCH c_get_ipp_refno into l_reusable_refno;
              CLOSE c_get_ipp_refno;
-- 
          ELSIF (p1.lpvh_pva_pdu_pob_table_name = 'BUSINESS_ACTIONS') THEN
--
               OPEN c_get_ban_refno(p1.lpvh_legacy_ref);
              FETCH c_get_ban_refno into l_reusable_refno;
              CLOSE c_get_ban_refno;
--
          ELSIF (p1.lpvh_pva_pdu_pob_table_name = 'BUSINESS_ACTION_EVENTS') THEN
--
               OPEN c_get_bae_refno(p1.lpvh_legacy_ref, 
                                    p1.lpvh_secondary_ref, 
                                    p1.lpvh_secondary_date,
                                    p1.lpvh_pva_further_ref);
--
              FETCH c_get_bae_refno into l_reusable_refno;
              CLOSE c_get_bae_refno;
--
--          ELSIF (p1.lpvh_pva_pdu_pob_table_name = 'ADVICE_CASE_HOUSING_OPTIONS') THEN
--
--              l_acho_reference := NULL;
--
--               OPEN c_get_acho_reference(p1.lpvh_legacy_ref);
--              FETCH c_get_acho_reference INTO l_acho_reference;
--              CLOSE c_get_acho_reference;
--
--               OPEN c_get_lebe_refno(l_acho_reference, p1.lpvh_created_date, p1.lpvh_desc);
--              FETCH c_get_lebe_refno into l_reusable_refno;
--              CLOSE c_get_lebe_refno;
--  
          END IF;
--
--
          DELETE 
            FROM parameter_values_history
           WHERE pvh_pva_reusable_refno = l_reusable_refno
             AND pvh_pva_pdu_pdf_name   = p1.lpvh_pva_pdu_pdf_name;
--
--
-- keep a count of the rows processed and commit after every 1000
--
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
--
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
--
          EXCEPTION
               WHEN OTHERS THEN
                  ROLLBACK TO SP1;
                  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
                  set_record_status_flag(l_id,'C');
                  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
    COMMIT;
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PARAMETER_VALUES');
--
    fsc_utils.proc_end;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
            RAISE;
--
END dataload_delete;
--
--
END s_dl_mad_other_field_val_hist;
/
show errors
