CREATE or replace force EDITIONABLE view "HOU"."RED_APPL_ACC_BALANCE_VIEW" (
APP_REFNO,
ACCOUNT_BALANCE
)
AS
SELECT
APPLICATIONS.APP_REFNO,
S_REVENUE_ACCOUNTS4.GET_SUM_RAC_BAL_FOR_TCY(NVL(APPLICATIONS.APP_TCY_REFNO,0)) as ACCOUNT_BALANCE
from HOU.APPLICATIONS APPLICATIONS;

      
CREATE OR REPLACE PUBLIC SYNONYM RED_APPL_ACC_BALANCE_VIEW  for RED_APPL_ACC_BALANCE_VIEW;


create or replace force EDITIONABLE view "HOU"."REQ_RQC_UNION_VIEW" ("REQU_REFNO", "REQU_REQ_REFERENCE", 
"REQU_RQC_SEQUENCE", "REQU_REUSABLE_REFNO", "REQU_RQC_DISPLAY_SEQUENCE", "REQU_SCO_CODE", 
"REQU_STATUS_DATE", "REQU_CREATED_BY", "REQU_CREATED_DATE", "REQU_PRO_AUN_CODE", 
"REQU_PRO_AUN_TYPE_IND", "REQU_HRV_LOC_CODE", "REQU_SOR_CODE", "REQU_DESCRIPTION", 
"REQU_REQ_STD_CODE", "REQU_BUD_REFNO", "REQU_HRV_RUR_CODE", "REQU_HRV_RET_CODE",
"REQU_HRV_SYA_CODE", "REQU_PLANNED_DATE", "REQU_QUANTITY", "REQU_UNIT_COST", "REQU_HRV_PMU_CODE", 
"REQU_ESTIMATED_COST", "REQU_COMMENTS", "REQU_PREV_SCO_CODE", "REQU_PREV_STATUS_DATE", "REQU_MODIFIED_BY", 
"REQU_MODIFIED_DATE", "REQU_SRC_TYPE")
AS 
  SELECT REQ_REFNO,
    REQ_REFERENCE,
    0 ,--TO_NUMBER(NULL),
    REQ_REUSABLE_REFNO,
    TO_NUMBER(NULL),
    REQ_SCO_CODE,
    REQ_STATUS_DATE,
    REQ_CREATED_BY,
    REQ_CREATED_DATE,
    REQ_PRO_AUN_CODE,
    REQ_PRO_AUN_TYPE_IND,
    REQ_HRV_LOC_CODE,
    REQ_SOR_CODE,
    REQ_DESCRIPTION,
    REQ_STD_CODE,
    REQ_BUD_REFNO,
    REQ_HRV_RUR_CODE,
    REQ_HRV_RET_CODE,
    REQ_HRV_SYA_CODE,
    REQ_PLANNED_DATE,
    REQ_QUANTITY,
    REQ_UNIT_COST,
    REQ_HRV_PMU_CODE,
    REQ_ESTIMATED_COST,
    REQ_COMMENTS,
    REQ_PREV_SCO_CODE,
    REQ_PREV_STATUS_DATE,
    REQ_MODIFIED_BY,
    REQ_MODIFIED_DATE,
    'REQ'
  FROM REQUIREMENTS
  UNION ALL
  SELECT RQC_REQ_REFNO,
    TO_CHAR(NULL),
    RQC_SEQUENCE,
    RQC_REUSABLE_REFNO,
    RQC_DISPLAY_SEQUENCE,
    RQC_SCO_CODE,
    RQC_STATUS_DATE,
    RQC_CREATED_BY,
    RQC_CREATED_DATE,
    RQC_PRO_AUN_CODE,
    RQC_PRO_AUN_TYPE_IND,
    RQC_HRV_LOC_CODE,
    RQC_SOR_CODE,
    RQC_DESCRIPTION,
    TO_CHAR(NULL),
    RQC_BUD_REFNO,
    RQC_HRV_RUR_CODE,
    RQC_HRV_RET_CODE,
    RQC_HRV_SYA_CODE,
    RQC_PLANNED_DATE,
    RQC_QUANTITY,
    RQC_UNIT_COST,
    RQC_HRV_PMU_CODE,
    RQC_ESTIMATED_COST,
    RQC_COMMENTS,
    RQC_PREV_SCO_CODE,
    RQC_PREV_STATUS_DATE,
    RQC_MODIFIED_BY,
    RQC_MODIFIED_DATE,
    'CMP'
  FROM REQUIREMENT_COMPONENTS ;
CREATE OR REPLACE PUBLIC SYNONYM REQ_RQC_UNION_VIEW  for REQ_RQC_UNION_VIEW;

CREATE OR REPLACE VIEW HOU.DW_TEN_NAME_ADDRESS_VIEW ("TENANCY_REFNO", "TENANTS_NAME", "CORR_ADDRESS") AS 
  select 
HOU.TENANCIES.TCY_REFNO tenancy_refno
, s_tenancies2.get_tennants_name(HOU.TENANCIES.TCY_REFNO)tenants_name
, s_tenancies.get_address(tenancies.tcy_refno,'PAR','CONTACT')corr_address from hou.tenancies;
CREATE OR REPLACE PUBLIC SYNONYM DW_TEN_NAME_ADDRESS_VIEW  for DW_TEN_NAME_ADDRESS_VIEW;

CREATE OR REPLACE FORCE VIEW "HOU"."RED_INSPECTIONS_IVI_VIEW" ("INS_SRQ_NO","INS_SEQNO","INS_RAISED_DATETIME", "INS_TARGET_DATETIME","INS_PRINTED_IND","INS_CREATED_BY","INS_CREATED_DATE","INS_ARC_CODE","INS_ARC_SYS_CODE","INS_HRV_ITY_CODE","INS_HRV_IRN_CODE","INS_PRI_CODE","INS_SCO_CODE","INS_AUN_CODE","INS_PRO_REFNO","INS_DESCRIPTION","INS_ISSUED_DATETIME","INS_COMPLETED_DATETIME","INS_STATUS_DATE","INS_ALTERNATIVE_REFNO","INS_LEGACY_REFNO","INS_REUSABLE_REFNO","INS_MODIFIED_BY","INS_MODIFIED_DATE", "INS_WOR_SEQNO", "IVI_INS_SRQ_NO","IVI_INS_SEQNO", "IVI_VISIT_NO","IVI_STATUS_DATE","IVI_SCO_CODE","IVI_CREATED_BY","IVI_CREATED_DATE","IVI_INSPECTION_PATCH","IVI_HRV_LOC_CODE","IVI_IPP_REFNO","IVI_VISIT_DATETIME","IVI_VISIT_DESCRIPTION","IVI_RESULT_DESCRIPTION","IVI_ACCESS_AM","IVI_ACCESS_PM","IVI_ACCESS_NOTES","IVI_LOCATION_NOTES","IVI_IRE_CODE","IVI_SPR_PRINTER_NAME","IVI_HRV_ACC_CODE","IVI_MODIFIED_BY","IVI_MODIFIED_DATE","IVI_HH_DOWNLOAD_DATE","IVI_HH_DOWNLOAD_TO_USER","IVI_HH_DOWNLOAD_BY_USER","IVI_HH_DOWNLOAD_BATCHREF","IVI_HH_UPLOAD_DATE","IVI_HH_UPLOAD_BY_USER","IVI_HH_UPLOAD_BATCHREF") AS Select
    INS_SRQ_NO,
    INS_SEQNO ,
    INS_RAISED_DATETIME ,
    INS_TARGET_DATETIME ,
    INS_PRINTED_IND ,
    INS_CREATED_BY ,
    INS_CREATED_DATE ,
    INS_ARC_CODE ,
    INS_ARC_SYS_CODE ,
    INS_HRV_ITY_CODE ,
    INS_HRV_IRN_CODE ,
    INS_PRI_CODE ,
    INS_SCO_CODE ,
    INS_AUN_CODE ,
    INS_PRO_REFNO ,
    INS_DESCRIPTION ,
    INS_ISSUED_DATETIME ,
    INS_COMPLETED_DATETIME ,
    INS_STATUS_DATE ,
    INS_ALTERNATIVE_REFNO ,
    INS_LEGACY_REFNO ,
    INS_REUSABLE_REFNO ,
    INS_MODIFIED_BY ,
    INS_MODIFIED_DATE ,
    INS_WOR_SEQNO ,
    IVI_INS_SRQ_NO ,
    IVI_INS_SEQNO ,
    IVI_VISIT_NO ,
    IVI_STATUS_DATE ,
    IVI_SCO_CODE ,
    IVI_CREATED_BY ,
    IVI_CREATED_DATE ,
    IVI_INSPECTION_PATCH ,
    IVI_HRV_LOC_CODE ,
    IVI_IPP_REFNO ,
    IVI_VISIT_DATETIME ,
    IVI_VISIT_DESCRIPTION ,
    IVI_RESULT_DESCRIPTION ,
    IVI_ACCESS_AM ,
    IVI_ACCESS_PM ,
    IVI_ACCESS_NOTES ,
    IVI_LOCATION_NOTES ,
    IVI_IRE_CODE ,
    IVI_SPR_PRINTER_NAME ,
    IVI_HRV_ACC_CODE ,
    IVI_MODIFIED_BY ,
    IVI_MODIFIED_DATE ,
    IVI_HH_DOWNLOAD_DATE ,
    IVI_HH_DOWNLOAD_TO_USER ,
    IVI_HH_DOWNLOAD_BY_USER ,
    IVI_HH_DOWNLOAD_BATCHREF ,
    IVI_HH_UPLOAD_DATE ,
    IVI_HH_UPLOAD_BY_USER ,
    IVI_HH_UPLOAD_BATCHREF
  from HOU.INSPECTIONS
  LEFT OUTER JOIN (select *
      from HOU.INSPECTION_VISITS CIV
      where CIV.IVI_VISIT_NO =
      (select max(CURRENT_INSPECTION_VISITS2.IVI_VISIT_NO)
      from HOU.INSPECTION_VISITS CURRENT_INSPECTION_VISITS2
      where CIV.IVI_INS_SRQ_NO = CURRENT_INSPECTION_VISITS2.IVI_INS_SRQ_NO 
      and   CIV.IVI_INS_SEQNO    = CURRENT_INSPECTION_VISITS2.IVI_INS_SEQNO 
     ) ) CURRENT_INSPECTION_VISITS   
   on   HOU.INSPECTIONS.INS_SRQ_NO = CURRENT_INSPECTION_VISITS.IVI_INS_SRQ_NO
  and   HOU.INSPECTIONS.INS_SEQNO  = CURRENT_INSPECTION_VISITS.IVI_INS_SEQNO ;
CREATE OR REPLACE PUBLIC SYNONYM RED_INSPECTIONS_IVI_VIEW  for RED_INSPECTIONS_IVI_VIEW;


 CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOU"."FQV_CCT_NSC_SCN_VIEW" ("CNS_CCT_REFERENCE", "CNS_CODE", "CNS_SCN_REFNO", "CNS_BRO_CODE", "CNS_MAJOR_SUBJECT_IND", "CNS_S_N_TYPE", "CNS_BRC_CODE", "CNS_SCO_CODE", "CNS_CREATED_BY", "CNS_CREATED_DATE", "CNS_COMMENTS", "CNS_TARGET_DATE", "CNS_MODIFIED_DATE", "CNS_ACTUAL_DATE", "D_SUBJECT", "CNS_SCN_PAR_REFNO", "CNS_SCN_TCY_REFNO", "CNS_SCN_IPP_REFNO", "CNS_SCN_APP_REFNO", "CNS_SCN_PEG_CODE", "CNS_SCN_COS_CODE", "CNS_SCN_PRO_REFNO", "CNS_SCN_AUN_CODE", "CNS_
A_PRO_REFNO", "CNS_SCN_LAS_LEA_START_DATE", "CNS_SCN_LAS_START_DATE", "CNS_SCN_PAR_ORG_REFNO", "CNS_SCN_LOAP_REFNO", "CNS_SCN_SRQ_NO", "CNS_SCN_BAN_REF", "CNS_REUSABLE_REFNO", "CNS_SCN_ADR_REFNO", "CNS_CCT_RECEIVED_DATE", "CNS_CCT_HRV_CCM_CODE", "CNS_CCT_STATUS_DATE", "CNS_CCT_JRO_CODE", "CNS_CCT_USR_USERNAME", "CNS_CCT_CNY_CODE", "CNS_CCT_COMMENTS", "CNS_CCT_CREATED_BY", "CNS_CCT_CREATED_DATE", "CNS_CCT_HRV_ANO_CODE", "CNS_CCT_CORRESPOND_REFERENCE", "CNS_CCT_OUTCOME_COMMENTS", "CNS_CCT_ADR_REFNO", "CNS_CCT_PAR_REFNO_SPECIFIC_TO", "CNS_CCT_PAR_REFNO", "CNS_CCT_TCY_REFNO", "CNS_CCT_IPP_REFNO", "CNS_CCT_APP_REFNO", "CNS_CCT_PEG_CODE", "CNS_CCT_COS_CODE", "CNS_CCT_SCO_CODE", "CNS_CCT_ANSWERED_DATE", "CNS_CCT_LEA_PRO_REFNO", "CNS_CCT_LEA_START_DATE", "CNS_CCT_LAS_START_DATE", "CNS_CCT_PAR_ORG_REFNO", "CNS_CCT_LOAP_REFNO", "D_APP_EXISTS", "D_NUM_REASONS", "D_WHO_FROM", "CNS_STATUS_DATE", "CNS_STATUS", "CNS_CCT_SOURCE_ADDRESS", "CNS_SUBJECT_ADDRESS") AS 
  SELECT ilv.cct_reference
       ,ilv.cns_code
       ,ilv.scn_refno
       ,ilv.bro_code
       ,ilv.major_subject_ind
       ,ilv.s_n_type
       ,ilv.brc_code
       ,ilv.sco_code
       ,ilv.created_by
       ,ilv.created_date
       ,ilv.comments
       ,ilv.target_date
       ,ilv.modified_date
       ,ilv.actual_date
       ,ilv.d_subject
       ,ilv.scn_par_refno
       ,ilv.scn_tcy_refno
       ,ilv.scn_ipp_refno
       ,ilv.scn_app_refno
       ,ilv.scn_peg_code
       ,ilv.scn_cos_code
       ,ilv.scn_pro_refno
       ,ilv.scn_aun_code
,ilv.scn_las_lea_pro_refno
,ilv.scn_las_lea_start_date
,ilv.scn_las_start_date
       ,ilv.scn_par_org_refno
       ,ilv.scn_loap_refno
       ,ilv.scn_srq_no
       ,ilv.scn_ban_ref
       ,ilv.reusable_refno
       ,ilv.scn_adr_refno
       ,cct.cct_received_date
       ,cct.cct_hrv_ccm_code
       ,trunc(cct.cct_status_date)
       ,cct.cct_jro_code
       ,cct.cct_usr_username
       ,cct.cct_cny_code
       ,cct.cct_comments
       ,cct.cct_created_by
       ,cct.cct_created_date
       ,cct.cct_hrv_ano_code
       ,cct.cct_correspond_reference
       ,cct.cct_outcome_comments
       ,cct.cct_adr_refno
       ,cct.cct_par_refno_specific_to
       ,cct.cct_par_refno
       ,cct.cct_tcy_refno
       ,cct.cct_ipp_refno
       ,cct.cct_app_refno
       ,cct.cct_peg_code
       ,cct.cct_cos_code
       ,cct.cct_sco_code
       ,cct.cct_answered_date
       ,cct.cct_las_lea_pro_refno
       ,cct.cct_las_lea_start_date
       ,cct.cct_las_start_date
       ,cct.cct_par_org_refno
       ,cct.cct_loap_refno
       ,s_appointments.check_apo_exists_for_cct(cct.cct_reference) d_app_exists
       ,s_cct_nsc_scn_view.count_num_reasons(cct.cct_reference) d_num_reasons
       ,decode(cct.cct_par_refno_specific_to
           ,NULL,decode(cct.cct_par_refno
             ,NULL,DECODE(cct.cct_ipp_refno
               ,NULL,DECODE(cct.cct_app_refno
                ,NULL,DECODE(cct.cct_las_lea_pro_refno
                  ,NULL,DECODE(cct.cct_par_org_refno
                   ,NULL,DECODE(cct.cct_loap_refno
                    ,NULL,DECODE(cct.cct_cos_code
                      ,NULL,DECODE(cct.cct_tcy_refno
                        ,NULL,DECODE(cct.cct_peg_code
                         ,NULL,NULL,s_people_groups.get_peg_description
(cct.cct_peg_code))
                   ,s_tenancies2.get_tcy_correspond_name(cct.cct_tcy_refno))
                 ,s_contractor_sites.get_cos_name(cct.cct_cos_code))
                 ,s_loan_applications.get_corr_name(cct_loap_refno))
                 ,s_parties.party_name(cct.cct_par_org_refno))
                ,s_lease_assignments.get_corr_name(cct.cct_las_lea_pro_refno,
                                                   cct.cct_las_lea_start_date,
                                                   cct.cct_las_start_date))
               ,s_applications.app_correspondence_name(cct.cct_app_refno))
             ,decode(s_interested_parties.get_ipp_par_refno(cct.cct_ipp_refno)
                ,NULL,decode(s_interested_parties.get_ipp_username(cct.cct_ipp_refno)
                   ,NULL,NULL ,s_users.get_standard_form_username(
                     s_interested_parties.get_ipp_username(cct.cct_ipp_refno)))
                      ,s_parties.party_name(s_interested_parties.get_ipp_par_refno(
                        cct.cct_ipp_refno))))
           ,s_parties.party_name(cct.cct_par_refno))
       ,s_parties.party_name(cct.cct_par_refno_specific_to)) d_who_from
       ,ilv.scn_status_date
       ,ilv.scn_sco_code
       ,s_cct_nsc_scn_view.address
          (cct.cct_adr_refno,
           cct.cct_tcy_refno,
           cct.cct_par_refno,
           cct.cct_ipp_refno,
           cct.cct_app_refno,
           cct.cct_peg_code,
           cct.cct_cos_code,
           cct.cct_las_lea_pro_refno,
           cct.cct_par_org_refno,
           cct.cct_loap_refno) cns_cct_source_address
       ,s_cct_nsc_scn_view.address
          (ilv.scn_adr_refno,
           ilv.scn_tcy_refno,
           ilv.scn_par_refno,
           ilv.scn_ipp_refno,
           ilv.scn_app_refno,
           ilv.scn_peg_code,
           ilv.scn_cos_code,
           ilv.scn_par_org_refno,
           ilv.scn_loap_refno,
           --***REV 2.0 ilv.scn_las_lea_pro_refno) cns_subject_address
           NVL(ilv.scn_las_lea_pro_refno,ilv.scn_pro_refno) ) cns_subject_address
FROM contacts cct,
   (SELECT  scn.scn_cct_reference cct_reference
            ,to_char(scn.scn_refno) cns_code
            ,scn.scn_refno scn_refno
            ,scn.scn_bro_code bro_code
            ,scn.scn_major_subject_ind major_subject_ind
            ,'S' s_n_type
            ,scn.scn_brc_code brc_code
            ,scn.scn_sco_code sco_code
            ,scn.scn_created_by created_by
            ,scn.scn_created_date created_date
            ,scn.scn_comments comments
            ,scn.scn_target_date target_date
            ,scn.scn_modified_date modified_date
            ,scn.scn_actual_date actual_date
            ,decode(scn_par_refno
              ,NULL,decode(scn_pro_refno
                ,NULL,DECODE(scn_aun_code
                  ,NULL,DECODE(scn_tcy_refno
                    ,NULL,DECODE(scn_ipp_refno
                      ,NULL,DECODE(scn_app_refno
                        ,NULL,DECODE(scn_peg_code
                          ,NULL,DECODE(scn_las_lea_pro_refno				                           ,NULL,DECODE(scn_par_org_refno
                            ,NULL, DECODE(scn_loap_refno
                              ,NULL,DECODE(scn_srq_no
                                ,NULL,DECODE(scn_cos_code
                                  ,NULL,NULL ,s_contractor_sites.get_cos_name
(scn_cos_code))
                           ,'Service Request: '|| scn_srq_no)
                           ,'Loan Application:  '||scn_loap_refno)
                           ,'Organisation Reference:  '||scn_par_org_refno)
                          ,'LAS Prop Ref :'||s_properties.get_propref_for_refno (scn_las_lea_pro_refno)
  ||'/'||TO_CHAR(scn_las_lea_start_date,'DD-MON-RRRR')||'/'||s_lease_assignments.get_corr_name
(scn_las_lea_pro_refno,
scn_las_lea_start_date,                                                             
scn_las_start_date)),s_people_groups.get_peg_description(scn_peg_code))                  ,'App Ref: '|| scn_app_refno ||' ' ||s_applications.app_correspondence_name(scn_app_refno)),decode(s_interested_parties.get_ipp_par_refno(scn_ipp_refno)             ,NULL,decode(s_interested_parties.get_ipp_username(scn_ipp_refno),NULL,NULL,s_users.get_standard_form_username(                          s_interested_parties.get_ipp_username(scn_ipp_refno))),s_parties.party_name(s_interested_parties.get_ipp_par_refno(scn_ipp_refno)))),'Tcy Ref: '|| scn_tcy_refno ||' ' ||s_tenancies2.get_tcy_correspond_name(scn_tcy_refno)),'Admin Unit: '|| scn_aun_code),'Property Ref: '|| s_properties.get_propref_for_refno(scn_pro_refno)),s_parties.party_name(scn_par_refno)) d_subject
,scn.scn_par_refno scn_par_refno
,scn.scn_tcy_refno scn_tcy_refno
,scn.scn_ipp_refno scn_ipp_refno
,scn.scn_app_refno scn_app_refno
,scn.scn_peg_code scn_peg_code
,scn.scn_cos_code scn_cos_code
,scn.scn_pro_refno scn_pro_refno
,scn.scn_aun_code scn_aun_code
,scn.scn_las_lea_pro_refno
,scn.scn_las_lea_start_date
,scn.scn_las_start_date
,scn.scn_par_org_refno
,scn.scn_loap_refno
,scn.scn_srq_no scn_srq_no
,scn.scn_ban_reference scn_ban_ref
,scn.scn_reusable_refno reusable_refno
,scn.scn_adr_refno scn_adr_refno
,scn.scn_status_date
,scn.scn_sco_code
,' ' 
FROM subj_cont_bus_reasons scn 
UNION ALL
   SELECT nsc.nsc_cct_reference  cct_reference
          ,nsc.nsc_bro_code cns_code
          ,to_number(null) scn_refno
          ,nsc.nsc_bro_code  bro_code
          ,nsc.nsc_major_subject_ind  major_subject_ind
          ,'N' s_n_type
          ,nsc.nsc_brc_code brc_code
          ,nsc.nsc_sco_code sco_code
          ,nsc.nsc_created_by created_by
          ,nsc.nsc_created_date created_date
          ,nsc.nsc_comments comments
          ,nsc.nsc_target_date target_date
          ,nsc.nsc_modified_date modified_date
          ,nsc.nsc_actual_date actual_date
          ,to_char(NULL) d_subject
          ,to_number(NULL) scn_par_refno
          ,to_number(NULL) scn_tcy_refno
          ,to_number(NULL) scn_ipp_refno
          ,to_number(NULL) scn_app_refno
          ,to_char(NULL) scn_peg_code
          ,to_char(NULL) scn_cos_code
          ,to_number(NULL) scn_pro_refno
          ,to_char(NULL) scn_aun_code
          ,to_number(NULL) scn_las_lea_pro_refno
          ,to_date(NULL) scn_las_lea_start_date
          ,to_date(NULL) scn_las_start_date
          ,to_number(NULL) scn_par_org_refno
          ,to_number(NULL) scn_loap_refno
          ,to_number(NULL) scn_srq_no
          ,to_number(NULL) scn_ban_ref
          ,nsc.nsc_reusable_refno reusable_refno
          ,to_number(NULL) scn_adr_refno
          ,nsc.nsc_status_date scn_status_date
          ,nsc.nsc_sco_code scn_sco_code
          ,' '
FROM  non_subj_cont_bus_reasons nsc) ilv
WHERE ilv.cct_reference = cct.cct_reference
Union
SELECT to_number(NULL)  cct_reference
          ,BAN_BRO_CODE cns_code
          ,to_number(null) scn_refno
          ,BAN_BRO_CODE  bro_code
          ,NULL  major_subject_ind
          ,'S' s_n_type
          ,NULL brc_code
          ,BAN_sco_code sco_code
          ,BAN_created_by created_by
          ,BAN_created_date created_date
          ,NULL comments
          ,BAN_target_date target_date
          ,BAN_modified_date modified_date
          ,to_date(NULL) actual_date
          ,decode(ban_par_refno
              ,NULL,decode(ban_pro_refno
                ,NULL,DECODE(ban_aun_code
                  ,NULL,DECODE(ban_tcy_refno
                    ,NULL,DECODE(ban_ipp_refno
                      ,NULL,DECODE(ban_app_refno
                        ,NULL,DECODE(ban_peg_code
                          ,NULL,DECODE(ban_las_lea_pro_refno
                           ,NULL, DECODE(ban_par_org_refno
                            ,NULL, DECODE(ban_loap_refno
                              ,NULL,DECODE(ban_srq_no
                                ,NULL,DECODE(ban_cos_code
                                  ,NULL,NULL ,s_contractor_sites.get_cos_name
(ban_cos_code))
                           ,'Service Request: '|| ban_srq_no)
                           ,'Loan Application: '||ban_loap_refno)
                           ,'Organisation Reference '||ban_par_org_refno)
                          ,'LAS Prop Ref :'||s_properties.get_propref_for_refno
(ban_las_lea_pro_refno)                                           ||'/'||TO_CHAR(ban_las_lea_start_date,'DD-MON-RRRR')                                           ||'/'||s_lease_assignments.get_corr_name
(ban_las_lea_pro_refno,                                                             
ban_las_lea_start_date, 
ban_las_start_date)),s_people_groups.get_peg_description(ban_peg_code)) ,'App Ref: '|| ban_app_refno ||' ' ||
s_applications.app_correspondence_name(ban_app_refno))
                    ,decode(s_interested_parties.get_ipp_par_refno(ban_ipp_refno)
                       ,NULL,decode(s_interested_parties.get_ipp_username
(ban_ipp_refno)
                        ,NULL,NULL,s_users.get_standard_form_username(
                          s_interested_parties.get_ipp_username(ban_ipp_refno)))
                          ,s_parties.party_name(s_interested_parties.get_ipp_par_refno
(ban_ipp_refno))))
                  ,'Tcy Ref: '|| ban_tcy_refno ||' ' ||
s_tenancies2.get_tcy_correspond_name(ban_tcy_refno))
               ,'Admin Unit: '|| ban_aun_code)
             ,'Property Ref: '|| s_properties.get_propref_for_refno(ban_pro_refno))
            ,s_parties.party_name(ban_par_refno)) d_subject
          ,ban_par_refno scn_par_refno
          ,ban_tcy_refno scn_tcy_refno
          ,ban_ipp_refno scn_ipp_refno
          ,ban_app_refno scn_app_refno
          ,ban_peg_code scn_peg_code
          ,ban_cos_code scn_cos_code
          ,ban_pro_refno scn_pro_refno
          ,ban_aun_code scn_aun_code
          ,ban_las_lea_pro_refno scn_las_lea_pro_refno
          ,ban_las_lea_start_date scn_las_lea_start_date
          ,ban_las_start_date scn_las_start_date
          ,ban_par_org_refno scn_par_org_refno
          ,ban_loap_refno scn_loap_refno
          ,ban_srq_no scn_srq_no
          ,ban_reference scn_ban_ref
          ,ban_reusable_refno reusable_refno
          ,ban_adr_refno scn_adr_refno
	,to_date(NULL) cns_cct_received_date
       ,NULL cns_cct_hrv_ccm_code
       ,to_date(NULL) cns_cct_status_date
       ,NULL cns_cct_jro_code
       ,NULL cns_cct_usr_username
       ,NULL cns_cct_cny_code
       ,NULL cns_cct_comments
       ,NULL cns_cct_created_by
       ,to_date(NULL) cns_cct_created_date
       ,NULL cns_cct_hrv_ano_code
       ,NULL cns_cct_correspond_reference
       ,NULL  cns_cct_outcome_comments
       ,to_number(NULL) cns_cct_adr_refno
       ,to_number(NULL) cns_cct_par_refno_specific_to
       ,to_number(NULL) cns_cct_par_refno
       ,to_number(NULL) cns_cct_tcy_refno
       ,to_number(NULL) cns_cct_ipp_refno
       ,to_number(NULL) cns_cct_app_refno
       ,NULL cns_cct_peg_code
       ,NULL cns_cct_cos_code
       ,NULL cns_cct_sco_code
       ,to_date(NULL) cns_cct_answered_date
       ,to_number(NULL) cns_cct_lea_pro_refno
       ,to_date(NULL) cns_cct_lea_start_date
       ,to_date(NULL) cns_cct_las_start_date
       ,to_number(NULL) cns_cct_par_org_refno
       ,to_number(NULL) cns_cct_loap_refno
       ,NULL d_app_exists
       ,to_number(NULL) d_num_reasons
       ,NULL d_who_from
       ,ban_status_date cns_status_date
       ,ban_sco_code cns_status
       ,NULL cns_cct_source_address
          ,s_cct_nsc_scn_view.address
          (ban_adr_refno,
           ban_tcy_refno,
           ban_par_refno,
           ban_ipp_refno,
           ban_app_refno,
           ban_peg_code,
           ban_cos_code,
           ban_par_org_refno,
           ban_loap_refno,
           --***REV 2.0 ban_las_lea_pro_refno) ban_subject_address 
           NVL(ban_las_lea_pro_refno,ban_pro_refno) ) ban_subject_address 
   FROM  business_actions
  WHERE ban_reference NOT IN
	(Select ban_reference from business_actions, subj_cont_bus_reasons
	where ban_reference = scn_ban_reference);
CREATE OR REPLACE PUBLIC SYNONYM FQV_CCT_NSC_SCN_VIEW for FQV_CCT_NSC_SCN_VIEW;


CREATE OR REPLACE VIEW HCS_CONTACTS_VIEW
(HCV_CCT_REFERENCE, HCV_CCT_TYPE, HCV_CONTACT_NAME, HCV_ADDRESS, HCV_ETHNIC_ORIGIN, 
 HCV_ETHNIC_ORIGIN_DESC, HCV_GEOGRAPHIC_ORIGIN, HCV_GEOGRAPHIC_ORIGIN_DESC, HCV_GENDER, 
HCV_DISABILITY,HCV_RELIGION, HCV_NATIONALITY, HCV_SEXUALITY, HCV_ECONOMIC_STATUS,
 HCV_DATE_OF_BIRTH, HCV_TCY_TTY_CODE, HCV_TCY_TENURE)
AS 
(
select cct_reference hcv_cct_reference,
       'TCY',
       tcy_correspond_name hcv_contact_name,
        s_cct_nsc_scn_view.address (cct_adr_refno,
                                      cct_tcy_refno,
                                      cct_par_refno,
                                      cct_ipp_refno,
                                      cct_app_refno,
                                      cct_peg_code,
                                      cct_cos_code,
                                      cct_las_lea_pro_refno)         hcv_address,
       PAR_PER_FRV_FEO_CODE hcv_ethnic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_FRV_FEO_CODE
           and frv_frd_domain = 'ETHNIC2') hcv_ethnic_origin_desc,
       PAR_PER_HOU_HRV_HGO_CODE hcv_geographic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_HOU_HRV_HGO_CODE
           and frv_frd_domain = 'ETHNIC') hcv_geographic_origin_desc,
       PAR_PER_FRV_FGE_CODE hcv_gender,
       PAR_PER_HOU_DISABLED_IND hcv_disability,
       PAR_PER_HOU_HRV_RLGN_CODE,
       PAR_PER_HOU_HRV_NTLY_CODE,
       PAR_PER_HOU_HRV_SEXO_CODE,
       PAR_PER_HOU_HRV_ECST_CODE,
       PAR_PER_DATE_OF_BIRTH,
       tcy_tty_code,
       TCY_HRV_TTYP_CODE
  from contacts,
       tenancies,
       household_persons,
       tenancy_instances,
       parties
 where tin_hop_refno = hop_refno
   and sysdate between hop_start_date and nvl(hop_end_date, sysdate+1)
   and sysdate between tin_start_date and nvl(tin_end_date, sysdate+1)
   and tin_tcy_refno = cct_tcy_refno
   and tin_main_tenant_ind = 'Y'
   and cct_tcy_refno is not null
   and tcy_refno=cct_tcy_refno
   and hop_par_refno = par_refno
union all 
select cct_reference,
       'PAR',
       s_parties.party_name(cct_par_refno) contact_name,
       s_cct_nsc_scn_view.address (cct_adr_refno,
                                      cct_tcy_refno,
                                      cct_par_refno,
                                      cct_ipp_refno,
                                      cct_app_refno,
                                      cct_peg_code,
                                      cct_cos_code,
                                      cct_las_lea_pro_refno) hcv_address,
       PAR_PER_FRV_FEO_CODE hcv_ethnic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_FRV_FEO_CODE
           and frv_frd_domain = 'ETHNIC2') hcv_ethnic_origin_desc,
       PAR_PER_HOU_HRV_HGO_CODE hcv_geographic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_HOU_HRV_HGO_CODE
           and frv_frd_domain = 'ETHNIC') hcv_geographic_origin_desc,
       PAR_PER_FRV_FGE_CODE hcv_gender,
       PAR_PER_HOU_DISABLED_IND hcv_disability,
       PAR_PER_HOU_HRV_RLGN_CODE,
       PAR_PER_HOU_HRV_NTLY_CODE,
       PAR_PER_HOU_HRV_SEXO_CODE,
       PAR_PER_HOU_HRV_ECST_CODE,
       PAR_PER_DATE_OF_BIRTH,
       null,
       null
  from contacts,
       parties
 where cct_par_refno is not null
   and par_refno=cct_par_refno 
union all
select cct_reference,
       'IPP',
       s_interested_parties.ipp_name(cct_ipp_refno) contact_name,
       s_cct_nsc_scn_view.address (cct_adr_refno,
                                      cct_tcy_refno,
                                      cct_par_refno,
                                      cct_ipp_refno,
                                      cct_app_refno,
                                      cct_peg_code,
                                      cct_cos_code,
                                      cct_las_lea_pro_refno) hcv_address,
       PAR_PER_FRV_FEO_CODE hcv_ethnic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_FRV_FEO_CODE
           and frv_frd_domain = 'ETHNIC2') hcv_ethnic_origin_desc,
       PAR_PER_HOU_HRV_HGO_CODE hcv_geographic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_HOU_HRV_HGO_CODE
           and frv_frd_domain = 'ETHNIC') hcv_geographic_origin_desc,
       PAR_PER_FRV_FGE_CODE hcv_gender,
       PAR_PER_HOU_DISABLED_IND hcv_disability,
       PAR_PER_HOU_HRV_RLGN_CODE,
       PAR_PER_HOU_HRV_NTLY_CODE,
       PAR_PER_HOU_HRV_SEXO_CODE,
       PAR_PER_HOU_HRV_ECST_CODE,
       PAR_PER_DATE_OF_BIRTH,
       null,
       null
  from contacts,
       interested_parties,
       parties
 where cct_ipp_refno is not null
   and cct_ipp_refno = ipp_refno
   and par_refno=ipp_par_refno 
union all
select cct_reference,
       'APP',
       app_corr_name contact_name,
      s_cct_nsc_scn_view.address (cct_adr_refno,
                                      cct_tcy_refno,
                                      cct_par_refno,
                                      cct_ipp_refno,
                                      cct_app_refno,
                                      cct_peg_code,
                                      cct_cos_code,
                                      cct_las_lea_pro_refno) hcv_address,
       PAR_PER_FRV_FEO_CODE hcv_ethnic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_FRV_FEO_CODE
           and frv_frd_domain = 'ETHNIC2') hcv_ethnic_origin_desc,
       PAR_PER_HOU_HRV_HGO_CODE hcv_geographic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_HOU_HRV_HGO_CODE
           and frv_frd_domain = 'ETHNIC') hcv_geographic_origin_desc,
       PAR_PER_FRV_FGE_CODE hcv_gender,
       PAR_PER_HOU_DISABLED_IND hcv_disability,
       PAR_PER_HOU_HRV_RLGN_CODE,
       PAR_PER_HOU_HRV_NTLY_CODE,
       PAR_PER_HOU_HRV_SEXO_CODE,
       PAR_PER_HOU_HRV_ECST_CODE,
       PAR_PER_DATE_OF_BIRTH,
       null,
       null
  from contacts,
       applications,
       involved_parties,
       parties
 where cct_app_refno is not null
   and cct_app_refno = app_refno
   and ipa_app_refno = app_refno
   and IPA_MAIN_APPLICANT_IND = 'Y'
   and par_refno=ipa_par_refno
 union all 
select cct_reference,
       'OTH',
       null contact_name,
       s_cct_nsc_scn_view.address (cct_adr_refno,
                                      cct_tcy_refno,
                                      cct_par_refno,
                                      cct_ipp_refno,
                                      cct_app_refno,
                                      cct_peg_code,
                                      cct_cos_code,
                                      cct_las_lea_pro_refno) hcv_address,
       null ethnic_origin,
       null hcv_ethnic_origin_desc,
       null geographic_origin,
       null hcv_geographic_origin_desc,
       null gender,
       null disability,
       null, 
       null,
       null,
       null,
       to_date(null),
       null,
       null
  from contacts
 where cct_app_refno is null
   and cct_par_refno is null
   and cct_ipp_refno is null
   and cct_tcy_refno is null) 
UNION ALL
Select cct_reference,
       'ORG',
       s_parties.party_name(cct_par_org_refno) contact_name,
       s_cct_nsc_scn_view.address (cct_adr_refno,
                                      cct_tcy_refno,
                                      cct_par_refno,
                                      cct_ipp_refno,
                                      cct_app_refno,
                                      cct_peg_code,
                                      cct_cos_code,
                                      cct_las_lea_pro_refno) hcv_address,
       NULL,
       NULL,
       NULL,
      NULL,
        NULL,
        NULL,
      NULL,
NULL,
NULL,
NULL,
to_date(NULL),
            null,
       null
  from contacts,
       fsc.parties
 where cct_par_org_refno is not null
   and par_refno=cct_par_org_refno 
union all
select cct_reference,
       'LOAP',
       LOAP_CORRESPONDENCE_NAME contact_name,
      s_cct_nsc_scn_view.address (cct_adr_refno,
                                      cct_tcy_refno,
                                      cct_par_refno,
                                      cct_ipp_refno,
                                      cct_app_refno,
                                      cct_peg_code,
                                      cct_cos_code,
                                      cct_las_lea_pro_refno,loap_refno) hcv_address,
       PAR_PER_FRV_FEO_CODE hcv_ethnic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_FRV_FEO_CODE
           and frv_frd_domain = 'ETHNIC2') hcv_ethnic_origin_desc,
       PAR_PER_HOU_HRV_HGO_CODE hcv_geographic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_HOU_HRV_HGO_CODE
           and frv_frd_domain = 'ETHNIC') hcv_geographic_origin_desc,
       PAR_PER_FRV_FGE_CODE hcv_gender,
       PAR_PER_HOU_DISABLED_IND hcv_disability,
       PAR_PER_HOU_HRV_RLGN_CODE,
       PAR_PER_HOU_HRV_NTLY_CODE,
       PAR_PER_HOU_HRV_SEXO_CODE,
       PAR_PER_HOU_HRV_ECST_CODE,
       PAR_PER_DATE_OF_BIRTH,
       null,
       null
  from contacts,
       loan_applications,
       loan_parties,
       parties
 where cct_loap_refno is not null
   and cct_loap_refno = loap_refno
   and loap_refno = lopa_loap_refno
    and par_refno=lopa_par_refno
    and lopa_main_ind = 'Y'
   ;

CREATE OR REPLACE PUBLIC SYNONYM HCS_CONTACTS_VIEW for HCS_CONTACTS_VIEW;

 CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOU"."FQV_BPA_VIEW" ("FQV_BPA_BAN_REFERENCE", "FQV_BPA_BAN_CONTACT_TYPE", "FQV_ROLE", "FQV_MAIN_PARTY_IND", "FQV_COMMENTS", "FQV_BPA_START_DATE", "FQV_BPA_END_DATE", "FQV_PAR_REFNO", "FQV_BPA_REFNO", "FQV_BPA_NAME", "FQV_BPA_ADDRESS", "FQV_BPA_ETHNIC_ORIGIN", "FQV_BPA_ETHNIC_ORIGIN_DESC", "FQV_BPA_GEOGRAPHIC_ORIGIN", "FQV_BPA_GEOGRAPHIC_ORIGIN_DESC", "FQV_BPA_GENDER", "FQV_BPA_DISABILITY", "FQV_BPA_DATE_OF_BIRTH", "FQV_BPA_TCY_TTY_CODE", "FQV_BPA_TCY_TENURE") AS 
  (
select bpa_ban_reference fqv_bpa_ban_reference,
       'TCY',
	   bpa_hrv_bac_code fqv_role,
	   BPA_MAIN_PARTY_IND fqv_main_party_ind,
	   BPA_COMMENTS fqv_comments,
	   BPA_START_DATE fqv_bpa_start_date,
	   BPA_END_DATE fqv_bpa_end_date,
	   par_refno fqv_par_refno,
	   bpa_refno fqv_bpa_refno,
	   tcy_correspond_name fqv_bpa_name,
       (select adr_line_all
          from fsc.address_usages, addresses
         where sysdate between aus_start_date and nvl(aus_end_date, sysdate+1)
           and adr_refno = aus_adr_refno
           and aus_aut_far_code = 'CONTACT'
		   and aus_par_refno = (select distinct hop_par_refno
                                   from household_persons, tenancy_instances
                                  where tin_hop_refno = hop_refno
                                    and sysdate between hop_start_date and nvl(hop_end_date, sysdate+1)
                                    and sysdate between tin_start_date and nvl(tin_end_date, sysdate+1)
                                    and tin_tcy_refno = bpa_tcy_refno
                                    and tin_main_tenant_ind = 'Y')) fqv_bpa_address,
       PAR_PER_FRV_FEO_CODE fqv_bpa_ethnic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_FRV_FEO_CODE
           and frv_frd_domain = 'ETHNIC2') fqv_bpa_ethnic_origin_desc,
       PAR_PER_HOU_HRV_HGO_CODE fqv_bpa_geographic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_HOU_HRV_HGO_CODE
           and frv_frd_domain = 'ETHNIC') fqv_bpa_geographic_origin_desc,
       PAR_PER_FRV_FGE_CODE fqv_bpa_gender,
       PAR_PER_HOU_DISABLED_IND fqv_bpa_disability,
       PAR_PER_DATE_OF_BIRTH,
       tcy_tty_code,
       TCY_HRV_TTYP_CODE
  from business_action_parties,
       tenancies,
       household_persons,
       tenancy_instances,
       parties
 where tin_hop_refno = hop_refno
   and sysdate between hop_start_date and nvl(hop_end_date, sysdate+1)
   and sysdate between tin_start_date and nvl(tin_end_date, sysdate+1)
   and tin_tcy_refno = bpa_tcy_refno
   and tin_main_tenant_ind = 'Y'
   and bpa_tcy_refno is not null
   and tcy_refno=bpa_tcy_refno
   and hop_par_refno = par_refno
union all
select ban_reference ,
       'PAR',
	   NULL fqv_role,
	   NULL fqv_main_party_ind,
	   NULL fqv_comments,
	   to_date(NULL) fqv_bpa_start_date,
	   to_date(NULL) fqv_bpa_end_date,
	   par_refno fqv_par_refno,
	   0,
       s_parties.party_name(ban_par_refno) ,
       adr_line_all,
             PAR_PER_FRV_FEO_CODE ,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_FRV_FEO_CODE
           and frv_frd_domain = 'ETHNIC2') ,
       PAR_PER_HOU_HRV_HGO_CODE ,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_HOU_HRV_HGO_CODE
           and frv_frd_domain = 'ETHNIC') ,
       PAR_PER_FRV_FGE_CODE ,
       PAR_PER_HOU_DISABLED_IND ,
       PAR_PER_DATE_OF_BIRTH,
       null,
       null
  from business_actions,
       parties,fsc.address_usages, addresses
         where sysdate between aus_start_date and nvl(aus_end_date, sysdate+1)
           and adr_refno = aus_adr_refno
           and aus_aut_far_code = 'CONTACT'
           and aus_par_refno = ban_par_refno
           and ban_par_refno is not null
   and par_refno=ban_par_refno
union all
select bpa_ban_reference fqv_bpa_ban_reference,
       'IPP',
	   bpa_hrv_bac_code fqv_role,
	   BPA_MAIN_PARTY_IND fqv_main_party_ind,
	   BPA_COMMENTS fqv_comments,
	   BPA_START_DATE fqv_bpa_start_date,
	   BPA_END_DATE fqv_bpa_end_date,
	   par_refno fqv_par_refno,
	   bpa_refno fqv_bpa_refno,
       s_interested_parties.ipp_name(bpa_ipp_refno) fqv_bpa_name,
       adr_line_all fqv_bpa_address,
       PAR_PER_FRV_FEO_CODE fqv_bpa_ethnic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_FRV_FEO_CODE
           and frv_frd_domain = 'ETHNIC2') fqv_bpa_ethnic_origin_desc,
       PAR_PER_HOU_HRV_HGO_CODE fqv_bpa_geographic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_HOU_HRV_HGO_CODE
           and frv_frd_domain = 'ETHNIC') fqv_bpa_geographic_origin_desc,
       PAR_PER_FRV_FGE_CODE fqv_bpa_gender,
       PAR_PER_HOU_DISABLED_IND fqv_bpa_disability,
       PAR_PER_DATE_OF_BIRTH,
       null,
       null
  from business_action_parties,
       interested_parties,
       parties,
	   address_usages,
	   addresses
 where bpa_ipp_refno is not null
   and bpa_ipp_refno = ipp_refno
   and par_refno=ipp_par_refno
   and sysdate between aus_start_date and nvl(aus_end_date, sysdate+1)
           and adr_refno = aus_adr_refno
           and aus_aut_far_code = 'CONTACT'
           and aus_par_refno = ipp_par_refno
union all
select bpa_ban_reference fqv_bpa_ban_reference,
       'APP',
	   bpa_hrv_bac_code fqv_role,
	   BPA_MAIN_PARTY_IND fqv_main_party_ind,
	   BPA_COMMENTS fqv_comments,
	   BPA_START_DATE fqv_bpa_start_date,
	   BPA_END_DATE fqv_bpa_end_date,
	   par_refno fqv_par_refno,
	   bpa_refno fqv_bpa_refno,
       app_corr_name fqv_bpa_name,
       adr_line_all fqv_bpa_address,
         PAR_PER_FRV_FEO_CODE fqv_bpa_ethnic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_FRV_FEO_CODE
           and frv_frd_domain = 'ETHNIC2') fqv_bpa_ethnic_origin_desc,
       PAR_PER_HOU_HRV_HGO_CODE fqv_bpa_geographic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_HOU_HRV_HGO_CODE
           and frv_frd_domain = 'ETHNIC') fqv_bpa_geographic_origin_desc,
       PAR_PER_FRV_FGE_CODE fqv_bpa_gender,
       PAR_PER_HOU_DISABLED_IND fqv_bpa_disability,
       PAR_PER_DATE_OF_BIRTH,
       null,
       null
  from business_action_parties,
       applications,
       involved_parties,
       parties,
	   address_usages,
	   addresses
 where bpa_app_refno is not null
   and bpa_app_refno = app_refno
   and ipa_app_refno = app_refno
   and IPA_MAIN_APPLICANT_IND = 'Y'
   and par_refno=ipa_par_refno
   and sysdate between aus_start_date and nvl(aus_end_date, sysdate+1)
           and adr_refno = aus_adr_refno
		   and aus_aut_far_code = 'APPLICATN'
           and aus_par_refno = ipa_par_refno
union all
select bpa_ban_reference fqv_bpa_ban_reference,
       'PEG',
	   bpa_hrv_bac_code fqv_role,
	   BPA_MAIN_PARTY_IND fqv_main_party_ind,
	   BPA_COMMENTS fqv_comments,
	   BPA_START_DATE fqv_bpa_start_date,
	   BPA_END_DATE fqv_bpa_end_date,
	   par_refno fqv_par_refno,
	   bpa_refno fqv_bpa_refno,
       s_parties.party_name(pge_par_refno) fqv_bpa_name,
       adr_line_all fqv_bpa_address,
         PAR_PER_FRV_FEO_CODE fqv_bpa_ethnic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_FRV_FEO_CODE
           and frv_frd_domain = 'ETHNIC2') fqv_bpa_ethnic_origin_desc,
       PAR_PER_HOU_HRV_HGO_CODE fqv_bpa_geographic_origin,
       (select FRV_NAME
          from first_ref_values
         where frv_code = PAR_PER_HOU_HRV_HGO_CODE
           and frv_frd_domain = 'ETHNIC') fqv_bpa_geographic_origin_desc,
       PAR_PER_FRV_FGE_CODE fqv_bpa_gender,
       PAR_PER_HOU_DISABLED_IND fqv_bpa_disability,
       PAR_PER_DATE_OF_BIRTH,
       null,
       null
  from business_action_parties,
       parties,
	   people_group_members,
	   address_usages,
	   addresses
 where bpa_peg_code is not null
   and par_refno=pge_par_refno
   and bpa_peg_code = pge_peg_code
   and adr_refno = aus_adr_refno
		   and sysdate between aus_start_date and nvl(aus_end_date, sysdate+1)
           and aus_par_refno = pge_par_refno
           and aus_aut_far_code = 'CONTACT') ;

CREATE OR REPLACE PUBLIC SYNONYM FQV_BPA_VIEW for FQV_BPA_VIEW;

CREATE OR REPLACE FORCE EDITIONABLE VIEW HOU.FQV_PGR_AGR_VIEW ("PROP_AUN_CODE", "PROP_OR_AUN", "AUN_CODE", "AUN_TYPE", "FULL_ADDRESS")
AS
  SELECT agr_aun_code_child,
    'A',
    agr_aun_code_parent,
    agr_auy_code_parent,
    FSC.ADDRESSES.ADR_LINE_ALL
  FROM FSC.ADDRESSES,
    FSC.ADDRESS_USAGES,
    fsc.admin_groupings_self
  WHERE ( FSC.ADDRESS_USAGES.AUS_AUN_CODE(+)   =fsc.admin_groupings_self.agr_aun_code_child )
  AND ( FSC.ADDRESSES.ADR_REFNO(+)             =FSC.ADDRESS_USAGES.AUS_ADR_REFNO )
  AND ( FSC.ADDRESS_USAGES.AUS_AUT_FAR_CODE(+) = 'PHYSICAL'
  AND FSC.ADDRESS_USAGES.AUS_AUT_FAO_CODE(+)   = 'AUN'
  AND FSC.ADDRESS_USAGES.AUS_END_DATE(+)      IS NULL )
  UNION ALL
  SELECT TO_CHAR(pgr_pro_refno),
    'P',
    pgr_aun_code,
    pgr_aun_type,
    FSC.ADDRESSES.ADR_LINE_ALL
  FROM FSC.ADDRESS_USAGES,
    FSC.ADDRESSES,
    HOU.PROP_GROUPINGS
  WHERE ( HOU.PROP_GROUPINGS.PGR_PRO_REFNO =FSC.ADDRESS_USAGES.AUS_PRO_REFNO )
  AND ( FSC.ADDRESS_USAGES.AUS_ADR_REFNO   =FSC.ADDRESSES.ADR_REFNO )
  AND ( FSC.ADDRESS_USAGES.AUS_AUT_FAR_CODE='PHYSICAL'
  AND FSC.ADDRESS_USAGES.AUS_END_DATE     IS NULL );

CREATE OR REPLACE PUBLIC SYNONYM fqv_pgr_agr_view for fqv_pgr_agr_view;

 CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOU"."RED_BUDGETS_BUD_VIEW" ("BHE_REFNO", "BHE_CODE", "BHE_DESCRIPTION", "BHE_CREATED_BY", "BHE_CREATED_DATE", "BHE_MODIFIED_BY", "BHE_MODIFIED_DATE", "BHE_CODE_MLANG", "BHE_DESCRIPTION_MLANG", "BUD_REFNO", "BUD_BHE_REFNO", "BUD_BCA_YEAR", "BUD_TYPE", "BUD_AUN_CODE", "BUD_AMOUNT", "BUD_ALLOW_NEGATIVE_IND", "BUD_REPEAT_WARNING_IND", "BUD_WARNING_ISSUED_IND", "BUD_SCO_CODE", "BUD_CREATED_BY", "BUD_CREATED_DATE", "BUD_BUD_REFNO", "BUD_BPCA_BPR_CODE", "BUD_WARNING_PERCENT", "BUD_COMMENTS", "BUD_COMMITTED", "BUD_ACCRUED", "BUD_INVOICED", "BUD_EXPENDED", "BUD_CREDITED", "BUD_TAX_COMMITTED", "BUD_TAX_ACCRUED", "BUD_TAX_INVOICED", "BUD_TAX_EXPENDED", "BUD_TAX_CREDITED", "BUD_MODIFIED_BY", "BUD_MODIFIED_DATE") AS 
  select 
  BUDGET_HEADS.BHE_REFNO
, BUDGET_HEADS.BHE_CODE
, BUDGET_HEADS.BHE_DESCRIPTION
, BUDGET_HEADS.BHE_CREATED_BY
, BUDGET_HEADS.BHE_CREATED_DATE
, BUDGET_HEADS.BHE_MODIFIED_BY
, BUDGET_HEADS.BHE_MODIFIED_DATE
, BUDGET_HEADS.BHE_CODE_MLANG
, BUDGET_HEADS.BHE_DESCRIPTION_MLANG
, BUDGETS.BUD_REFNO
, BUDGETS.BUD_BHE_REFNO
, BUDGETS.BUD_BCA_YEAR
, BUDGETS.BUD_TYPE
, BUDGETS.BUD_AUN_CODE
, BUDGETS.BUD_AMOUNT
, BUDGETS.BUD_ALLOW_NEGATIVE_IND
, BUDGETS.BUD_REPEAT_WARNING_IND
, BUDGETS.BUD_WARNING_ISSUED_IND
, BUDGETS.BUD_SCO_CODE
, BUDGETS.BUD_CREATED_BY
, BUDGETS.BUD_CREATED_DATE
, BUDGETS.BUD_BUD_REFNO
, BUDGETS.BUD_BPCA_BPR_CODE
, BUDGETS.BUD_WARNING_PERCENT
, BUDGETS.BUD_COMMENTS
, BUDGETS.BUD_COMMITTED
, BUDGETS.BUD_ACCRUED
, BUDGETS.BUD_INVOICED
, BUDGETS.BUD_EXPENDED
, BUDGETS.BUD_CREDITED
, BUDGETS.BUD_TAX_COMMITTED
, BUDGETS.BUD_TAX_ACCRUED
, BUDGETS.BUD_TAX_INVOICED
, BUDGETS.BUD_TAX_EXPENDED
, BUDGETS.BUD_TAX_CREDITED
, BUDGETS.BUD_MODIFIED_BY
, BUDGETS.BUD_MODIFIED_DATE
FROM BUDGET_HEADS 
INNER JOIN BUDGETS 
ON BUDGET_HEADS.BHE_REFNO = BUDGETS.BUD_BHE_REFNO;

CREATE OR REPLACE PUBLIC SYNONYM RED_BUDGETS_BUD_VIEW for RED_BUDGETS_BUD_VIEW;

 CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOU"."RED_PSL_INVOICES_PIND_VIEW" ("PINH_REFNO", "PINH_PSLS_CODE", "PINH_SCO_CODE", "PINH_STATUS_DATE", "PINH_CREATED_BY", "PINH_CREATED_DATE", "PINH_OVERALL_AMOUNT_INC_TAX", "PINH_OVERALL_TAX_AMOUNT", "PINH_OVERALL_AMOUNT_EXC_TAX", "PINH_CERTIFIED_DATE", "PINH_CERTIFIED_BY", "PINH_AUTHORISED_BY", "PINH_AUTHORISED_DATE", "PINH_PPE_YEAR", "PINH_PPE_PERIOD_NO", "PINH_PPE_TYPE", "PINH_MODIFIED_BY", "PINH_MODIFIED_DATE", "PINH_MGMT_FEE_AMOUNT_INC_TAX", "PINH_MGMT_FEE_TAX", "PINH_RELET_FEE_AMOUNT_INC_TAX", "PINH_RELET_FEE_TAX", "PINH_RELET_CREDIT_AMT_INC_TAX", "PINH_RELET_CREDIT_TAX_AMOUNT", "PINH_NOM_FAIL_AMOUNT_INC_TAX", "PINH_NOM_FAIL_TAX_AMOUNT", "PINH_COMMENTS", "PIND_REFNO", "PIND_PINH_REFNO", "PIND_AMOUNT_INC_TAX", "PIND_CREATED_BY", "PIND_CREATED_DATE", "PIND_PSL_REFNO", "PIND_PIF_CODE", "PIND_PPE_YEAR", "PIND_PPE_PERIOD_NO", "PIND_PPE_TYPE", "PIND_TAX_AMOUNT", "PIND_AMOUNT_EXC_TAX", "PIND_MODIFIED_BY", "PIND_MODIFIED_DATE", "PIND_COMMENTS") AS 
  select 
  PSL_INVOICE_HEADERS.PINH_REFNO
, PSL_INVOICE_HEADERS.PINH_PSLS_CODE
, PSL_INVOICE_HEADERS.PINH_SCO_CODE
, PSL_INVOICE_HEADERS.PINH_STATUS_DATE
, PSL_INVOICE_HEADERS.PINH_CREATED_BY
, PSL_INVOICE_HEADERS.PINH_CREATED_DATE
, PSL_INVOICE_HEADERS.PINH_OVERALL_AMOUNT_INC_TAX
, PSL_INVOICE_HEADERS.PINH_OVERALL_TAX_AMOUNT
, PSL_INVOICE_HEADERS.PINH_OVERALL_AMOUNT_EXC_TAX
, PSL_INVOICE_HEADERS.PINH_CERTIFIED_DATE
, PSL_INVOICE_HEADERS.PINH_CERTIFIED_BY
, PSL_INVOICE_HEADERS.PINH_AUTHORISED_BY
, PSL_INVOICE_HEADERS.PINH_AUTHORISED_DATE
, PSL_INVOICE_HEADERS.PINH_PPE_YEAR
, PSL_INVOICE_HEADERS.PINH_PPE_PERIOD_NO
, PSL_INVOICE_HEADERS.PINH_PPE_TYPE
, PSL_INVOICE_HEADERS.PINH_MODIFIED_BY
, PSL_INVOICE_HEADERS.PINH_MODIFIED_DATE
, PSL_INVOICE_HEADERS.PINH_MGMT_FEE_AMOUNT_INC_TAX
, PSL_INVOICE_HEADERS.PINH_MGMT_FEE_TAX
, PSL_INVOICE_HEADERS.PINH_RELET_FEE_AMOUNT_INC_TAX
, PSL_INVOICE_HEADERS.PINH_RELET_FEE_TAX
, PSL_INVOICE_HEADERS.PINH_RELET_CREDIT_AMT_INC_TAX
, PSL_INVOICE_HEADERS.PINH_RELET_CREDIT_TAX_AMOUNT
, PSL_INVOICE_HEADERS.PINH_NOM_FAIL_AMOUNT_INC_TAX
, PSL_INVOICE_HEADERS.PINH_NOM_FAIL_TAX_AMOUNT
, PSL_INVOICE_HEADERS.PINH_COMMENTS
, PSL_INVOICE_DETAILS.PIND_REFNO
, PSL_INVOICE_DETAILS.PIND_PINH_REFNO
, PSL_INVOICE_DETAILS.PIND_AMOUNT_INC_TAX
, PSL_INVOICE_DETAILS.PIND_CREATED_BY
, PSL_INVOICE_DETAILS.PIND_CREATED_DATE
, PSL_INVOICE_DETAILS.PIND_PSL_REFNO
, PSL_INVOICE_DETAILS.PIND_PIF_CODE
, PSL_INVOICE_DETAILS.PIND_PPE_YEAR
, PSL_INVOICE_DETAILS.PIND_PPE_PERIOD_NO
, PSL_INVOICE_DETAILS.PIND_PPE_TYPE
, PSL_INVOICE_DETAILS.PIND_TAX_AMOUNT
, PSL_INVOICE_DETAILS.PIND_AMOUNT_EXC_TAX
, PSL_INVOICE_DETAILS.PIND_MODIFIED_BY
, PSL_INVOICE_DETAILS.PIND_MODIFIED_DATE
, PSL_INVOICE_DETAILS.PIND_COMMENTS
FROM PSL_INVOICE_HEADERS 
INNER JOIN PSL_INVOICE_DETAILS 
ON PSL_INVOICE_HEADERS.PINH_REFNO = PSL_INVOICE_DETAILS.PIND_PINH_REFNO;

CREATE OR REPLACE PUBLIC SYNONYM RED_PSL_INVOICES_PIND_VIEW for RED_PSL_INVOICES_PIND_VIEW;


  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOU"."RED_PSL_LANDLORD_PAY_PYD_VIEW" ("PYH_REFNO", "PYH_PLD_REFNO", "PYH_PSL_REFNO", "PYH_SCO_CODE", "PYH_CREDIT_AMOUNT_INC_TAX", "PYH_PAYMENT_FROM_DATE", "PYH_PAYMENT_TO_DATE", "PYH_STATUS_DATE", "PYH_CREATED_BY", "PYH_CREATED_DATE", "PYH_CREDIT_TAX_AMOUNT", "PYH_DEBIT_TAX_AMOUNT", "PYH_RETURNED_IND", "PYH_PASSED_DATE", "PYH_ISSUED_DATE", "PYH_AUTHORISED_BY", "PYH_AUTHORISED_DATE", "PYH_CERTIFIED_BY", "PYH_CERTIFIED_DATE", "PYH_COMMENTS", "PYH_MODIFIED_BY", "PYH_MODIFIED_DATE", "PYH_CREDIT_AMOUNT_EXC_TAX", "PYH_PERIOD_FROM_DATE", "PYH_PERIOD_TO_DATE", "PYD_REFNO", "PYD_PYH_REFNO", "PYD_TYPE", "PYD_PPTR_CODE", "PYD_PAYMENT_EFFECTIVE_DATE", "PYD_CREATED_BY", "PYD_CREATED_DATE", "PYD_CREDIT_AMOUNT_INC_TAX", "PYD_DEBIT_AMOUNT_INC_TAX", "PYD_CREDIT_TAX_AMOUNT", "PYD_DEBIT_TAX_AMOUNT", "PYD_COMMENTS", "PYD_MODIFIED_BY", "PYD_MODIFIED_DATE", "PYD_CREDIT_AMOUNT_EXC_TAX", "PYD_DEBIT_AMOUNT_EXC_TAX", "PYD_RAT_TRA_REFNO", "PYD_RAT_RRE_RAC_ACCNO", "PYD_RAT_RRE_JOB_REFNO", "PYD_DATE_EXTRACTED", "PYD_CONTRA_TRA_REFNO") AS 
  select 
  PSL_LANDLORD_PAYMENT_HDRS.PYH_REFNO
, PSL_LANDLORD_PAYMENT_HDRS.PYH_PLD_REFNO
, PSL_LANDLORD_PAYMENT_HDRS.PYH_PSL_REFNO
, PSL_LANDLORD_PAYMENT_HDRS.PYH_SCO_CODE
, PSL_LANDLORD_PAYMENT_HDRS.PYH_CREDIT_AMOUNT_INC_TAX
, PSL_LANDLORD_PAYMENT_HDRS.PYH_PAYMENT_FROM_DATE
, PSL_LANDLORD_PAYMENT_HDRS.PYH_PAYMENT_TO_DATE
, PSL_LANDLORD_PAYMENT_HDRS.PYH_STATUS_DATE
, PSL_LANDLORD_PAYMENT_HDRS.PYH_CREATED_BY
, PSL_LANDLORD_PAYMENT_HDRS.PYH_CREATED_DATE
, PSL_LANDLORD_PAYMENT_HDRS.PYH_CREDIT_TAX_AMOUNT
, PSL_LANDLORD_PAYMENT_HDRS.PYH_DEBIT_TAX_AMOUNT
, PSL_LANDLORD_PAYMENT_HDRS.PYH_RETURNED_IND
, PSL_LANDLORD_PAYMENT_HDRS.PYH_PASSED_DATE
, PSL_LANDLORD_PAYMENT_HDRS.PYH_ISSUED_DATE
, PSL_LANDLORD_PAYMENT_HDRS.PYH_AUTHORISED_BY
, PSL_LANDLORD_PAYMENT_HDRS.PYH_AUTHORISED_DATE
, PSL_LANDLORD_PAYMENT_HDRS.PYH_CERTIFIED_BY
, PSL_LANDLORD_PAYMENT_HDRS.PYH_CERTIFIED_DATE
, PSL_LANDLORD_PAYMENT_HDRS.PYH_COMMENTS
, PSL_LANDLORD_PAYMENT_HDRS.PYH_MODIFIED_BY
, PSL_LANDLORD_PAYMENT_HDRS.PYH_MODIFIED_DATE
, PSL_LANDLORD_PAYMENT_HDRS.PYH_CREDIT_AMOUNT_EXC_TAX
, PSL_LANDLORD_PAYMENT_HDRS.PYH_PERIOD_FROM_DATE
, PSL_LANDLORD_PAYMENT_HDRS.PYH_PERIOD_TO_DATE
, PSL_LANDLORD_PAY_DTLS.PYD_REFNO
, PSL_LANDLORD_PAY_DTLS.PYD_PYH_REFNO
, PSL_LANDLORD_PAY_DTLS.PYD_TYPE
, PSL_LANDLORD_PAY_DTLS.PYD_PPTR_CODE
, PSL_LANDLORD_PAY_DTLS.PYD_PAYMENT_EFFECTIVE_DATE
, PSL_LANDLORD_PAY_DTLS.PYD_CREATED_BY
, PSL_LANDLORD_PAY_DTLS.PYD_CREATED_DATE
, PSL_LANDLORD_PAY_DTLS.PYD_CREDIT_AMOUNT_INC_TAX
, PSL_LANDLORD_PAY_DTLS.PYD_DEBIT_AMOUNT_INC_TAX
, PSL_LANDLORD_PAY_DTLS.PYD_CREDIT_TAX_AMOUNT
, PSL_LANDLORD_PAY_DTLS.PYD_DEBIT_TAX_AMOUNT
, PSL_LANDLORD_PAY_DTLS.PYD_COMMENTS
, PSL_LANDLORD_PAY_DTLS.PYD_MODIFIED_BY
, PSL_LANDLORD_PAY_DTLS.PYD_MODIFIED_DATE
, PSL_LANDLORD_PAY_DTLS.PYD_CREDIT_AMOUNT_EXC_TAX
, PSL_LANDLORD_PAY_DTLS.PYD_DEBIT_AMOUNT_EXC_TAX
, PSL_LANDLORD_PAY_DTLS.PYD_RAT_TRA_REFNO
, PSL_LANDLORD_PAY_DTLS.PYD_RAT_RRE_RAC_ACCNO
, PSL_LANDLORD_PAY_DTLS.PYD_RAT_RRE_JOB_REFNO
, PSL_LANDLORD_PAY_DTLS.PYD_DATE_EXTRACTED
, PSL_LANDLORD_PAY_DTLS.PYD_CONTRA_TRA_REFNO
FROM PSL_LANDLORD_PAYMENT_HDRS 
INNER JOIN PSL_LANDLORD_PAY_DTLS 
ON PSL_LANDLORD_PAYMENT_HDRS.PYH_REFNO = PSL_LANDLORD_PAY_DTLS.PYD_PYH_REFNO;

CREATE OR REPLACE PUBLIC SYNONYM RED_PSL_LANDLORD_PAY_PYD_VIEW for RED_PSL_LANDLORD_PAY_PYD_VIEW;


  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOU"."RED_WORKS_ORDER_WOV_VIEW" ("WOR_SEQNO", "WOR_SRQ_NO", "WOR_RAISED_DATETIME", "WOR_STATUS_DATE", "WOR_UPDATE_CHILD_ELEMS_IND", "WOR_CONFIRMATION_IND", "WOR_PRINT_IND", "WOR_TENANT_TICKET_PRINT_IND", "WOR_REASSIGN_IND", "WOR_DEF_CONTR_IND", "WOR_CREATED_BY", "WOR_CREATED_DATE", "WOR_PPC_COS_CODE", "WOR_PPC_PPP_START_DATE", "WOR_PPC_PPP_PPG_CODE", "WOR_PPC_PPP_WPR_CODE", "WOR_REUSABLE_REFNO", "WOR_SCO_CODE", "WOR_AUTHORISED_DATETIME", "WOR_HELD_DATETIME", "WOR_ISSUED_DATETIME", "WOR_ELEMENT_UPDATE_DATE", "WOR_ACT_HBC_REFNO", "WOR_EST_HBC_REFNO", "WOR_REPORTED_COMP_DATETIME", "WOR_SYSTEM_COMP_DATETIME", "WOR_ALTERNATIVE_REF", "WOR_LEGACY_REF", "WOR_MODIFIED_BY", "WOR_MODIFIED_DATE", "WOR_INS_SEQNO", "WOR_CSPG_REFNO", "WOR_WOR_SEQNO", "WOR_WOR_SRQ_NO", "WOR_HRV_CBY_CODE", "WOR_AUN_CODE", "WOR_PRO_REFNO", "WOR_CSPG_START_DATE", "WOR_PREV_STATUS_CODE", "WOR_PREV_STATUS_DATE", "WOR_AUTHORISED_BY", "WOR_EMAIL_IND", "WOV_WOR_SRQ_NO", "WOV_WOR_SEQNO", "WOV_VERSION_NO", "WOV_TYPE", "WOV_SCO_CODE", "WOV_PRI_CODE", "WOV_RAISED_DATETIME", "WOV_STATUS_DATE", "WOV_TARGET_DATETIME", "WOV_PRINTED_IND", "WOV_HRV_LOC_CODE", "WOV_SUNDRY_CLEARED_IND", "WOV_RTR_IND", "WOV_ACCESS_AM", "WOV_ACCESS_PM", "WOV_LOCATION_NOTES", "WOV_CREATED_BY", "WOV_CREATED_DATE", "WOV_SPR_PRINTER_NAME", "WOV_HELD_DATETIME", "WOV_AUTHORISED_DATETIME", "WOV_INVOICED_DATETIME", "WOV_ESTIMATED_COST", "WOV_ESTIMATED_TAX_AMOUNT", "WOV_INVOICED_COST", "WOV_INVOICED_TAX_AMOUNT", "WOV_FINANCIALS_EXTRACT_DATE", "WOV_CONTRACTOR_EXTRACT_DATE", "WOV_HRV_VRE_CODE", "WOV_RAC_ACCNO", "WOV_HRV_UST_CODE", "WOV_HRV_ACC_CODE", "WOV_MODIFIED_BY", "WOV_MODIFIED_DATE", "WOV_DESCRIPTION", "WOV_COMMENTS", "WOV_VAR_REASON_COMMENTS", "WOV_ACCESS_NOTES", "WOV_RECHARGE_PAYMENT_TYPE", "WOV_AUTHORISED_BY", "WOV_PRI_CALC_DATE", "WOV_TUR_CODE", "WOV_COS_EMAIL_ADDRESS", "WOV_RETURN_EMAIL_ADDRESS", "WOV_EMAILED_IND", "WOV_HRV_CBY_CODE") AS 
  select
    WOR_SEQNO
   ,WOR_SRQ_NO
   ,WOR_RAISED_DATETIME
   ,WOR_STATUS_DATE
   ,WOR_UPDATE_CHILD_ELEMS_IND
   ,WOR_CONFIRMATION_IND
   ,WOR_PRINT_IND
   ,WOR_TENANT_TICKET_PRINT_IND
   ,WOR_REASSIGN_IND
   ,WOR_DEF_CONTR_IND
   ,WOR_CREATED_BY
   ,WOR_CREATED_DATE
   ,WOR_PPC_COS_CODE
   ,WOR_PPC_PPP_START_DATE
   ,WOR_PPC_PPP_PPG_CODE
   ,WOR_PPC_PPP_WPR_CODE
   ,WOR_REUSABLE_REFNO
   ,WOR_SCO_CODE
   ,WOR_AUTHORISED_DATETIME
   ,WOR_HELD_DATETIME
   ,WOR_ISSUED_DATETIME
   ,WOR_ELEMENT_UPDATE_DATE
   ,WOR_ACT_HBC_REFNO
   ,WOR_EST_HBC_REFNO
   ,WOR_REPORTED_COMP_DATETIME
   ,WOR_SYSTEM_COMP_DATETIME
   ,WOR_ALTERNATIVE_REF
   ,WOR_LEGACY_REF
   ,WOR_MODIFIED_BY
   ,WOR_MODIFIED_DATE
   ,WOR_INS_SEQNO
   ,WOR_CSPG_REFNO
   ,WOR_WOR_SEQNO
   ,WOR_WOR_SRQ_NO
   ,WOR_HRV_CBY_CODE
   ,WOR_AUN_CODE
   ,WOR_PRO_REFNO
   ,WOR_CSPG_START_DATE
   ,WOR_PREV_STATUS_CODE
   ,WOR_PREV_STATUS_DATE
   ,WOR_AUTHORISED_BY
   ,WOR_EMAIL_IND
   ,WOV_WOR_SRQ_NO
   ,WOV_WOR_SEQNO
   ,WOV_VERSION_NO
   ,WOV_TYPE
   ,WOV_SCO_CODE
   ,WOV_PRI_CODE
   ,WOV_RAISED_DATETIME
   ,WOV_STATUS_DATE
   ,WOV_TARGET_DATETIME
   ,WOV_PRINTED_IND
   ,WOV_HRV_LOC_CODE
   ,WOV_SUNDRY_CLEARED_IND
   ,WOV_RTR_IND
   ,WOV_ACCESS_AM
   ,WOV_ACCESS_PM
   ,WOV_LOCATION_NOTES
   ,WOV_CREATED_BY
   ,WOV_CREATED_DATE
   ,WOV_SPR_PRINTER_NAME
   ,WOV_HELD_DATETIME
   ,WOV_AUTHORISED_DATETIME
   ,WOV_INVOICED_DATETIME
   ,WOV_ESTIMATED_COST
   ,WOV_ESTIMATED_TAX_AMOUNT
   ,WOV_INVOICED_COST
   ,WOV_INVOICED_TAX_AMOUNT
   ,WOV_FINANCIALS_EXTRACT_DATE
   ,WOV_CONTRACTOR_EXTRACT_DATE
   ,WOV_HRV_VRE_CODE
   ,WOV_RAC_ACCNO
   ,WOV_HRV_UST_CODE
   ,WOV_HRV_ACC_CODE
   ,WOV_MODIFIED_BY
   ,WOV_MODIFIED_DATE
   ,WOV_DESCRIPTION
   ,WOV_COMMENTS
   ,WOV_VAR_REASON_COMMENTS
   ,WOV_ACCESS_NOTES
   ,WOV_RECHARGE_PAYMENT_TYPE
   ,WOV_AUTHORISED_BY
   ,WOV_PRI_CALC_DATE
   ,WOV_TUR_CODE
   ,WOV_COS_EMAIL_ADDRESS
   ,WOV_RETURN_EMAIL_ADDRESS
   ,WOV_EMAILED_IND
   ,WOV_HRV_CBY_CODE
FROM HOU.WORKS_ORDERS 
   , HOU.WORKS_ORDER_VERSIONS
WHERE HOU.WORKS_ORDERS.WOR_SEQNO=HOU.WORKS_ORDER_VERSIONS.WOV_WOR_SEQNO 
and HOU.WORKS_ORDERS.WOR_SRQ_NO=HOU.WORKS_ORDER_VERSIONS.WOV_WOR_SRQ_NO and HOU.WORKS_ORDER_VERSIONS.WOV_TYPE = 'C';

CREATE OR REPLACE PUBLIC SYNONYM RED_WORKS_ORDER_WOV_VIEW for RED_WORKS_ORDER_WOV_VIEW;


create or replace force editionable view hou.red_programmes_view (
  prg_reference,
    prg_sco_code,
    prg_status_date,
    prg_aun_code,
    prg_created_by,
    prg_created_date,
    prg_total_task_value,
    prg_authorised_by,
    prg_authorised_datetime,
    prg_modified_by,
    prg_modified_date,
    pgv_version_number,
    pgv_current_ind,
    pgv_description,
    pgv_reusable_refno,
    pgv_created_by,
    pgv_created_date,
    pgv_alternate_reference,
    pgv_estimated_start_date,
    pgv_estimated_end_date,
    pgv_maximum_value,
    pgv_planned_cost,
    pgv_comments,
    PGV_MODIFIED_BY,
    PGV_MODIFIED_DATE )
as
  select
    prg_reference,
    prg_sco_code,
    prg_status_date,
    prg_aun_code,
    prg_created_by,
    prg_created_date,
    prg_total_task_value,
    prg_authorised_by,
    prg_authorised_datetime,
    prg_modified_by,
    prg_modified_date,
    pgv_version_number,
    pgv_current_ind,
    pgv_description,
    pgv_reusable_refno,
    pgv_created_by,
    pgv_created_date,
    pgv_alternate_reference,
    pgv_estimated_start_date,
    pgv_estimated_end_date,
    pgv_maximum_value,
    pgv_planned_cost,
    pgv_comments,
    PGV_MODIFIED_BY,
    PGV_MODIFIED_DATE
 FROM HOU.PROGRAMMES
  LEFT OUTER JOIN HOU.PROGRAMME_VERSIONS ON 
  HOU.PROGRAMMES.PRG_REFERENCE =   HOU.PROGRAMME_VERSIONS.PGV_PRG_REFERENCE AND 
 HOU.PROGRAMME_VERSIONS.PGV_CURRENT_IND = 'Y';
 
 CREATE OR REPLACE PUBLIC SYNONYM red_programmes_view for red_programmes_view;
 
 create or replace force editionable view hou.RED_PROJECTS_VIEW (
   PRJ_REFERENCE
  ,PRJ_PRG_REFERENCE
  ,PRJ_SCO_CODE
  ,PRJ_STATUS_DATE
  ,PRJ_AUN_CODE
  ,PRJ_CREATED_BY
  ,PRJ_CREATED_DATE
  ,PRJ_TOTAL_TASK_VALUE
  ,PRJ_COMMITTED_AMOUNT
  ,PRJ_ACCRUED_AMOUNT
  ,PRJ_INVOICED_AMOUNT
  ,PRJ_EXPENDED_AMOUNT
  ,PRJ_TAX_COMMITTED_AMOUNT
  ,PRJ_TAX_ACCRUED_AMOUNT
  ,PRJ_TAX_INVOICED_AMOUNT
  ,PRJ_TAX_EXPENDED_AMOUNT
  ,PRJ_NET_ALLOCATED_AMOUNT
  ,PRJ_TAX_ALLOCATED_AMOUNT
  ,PRJ_AUTHORISED_BY
  ,PRJ_AUTHORISED_DATETIME
  ,PRJ_MODIFIED_BY
  ,PRJ_MODIFIED_DATE
  ,PJV_VERSION_NUMBER
  ,PJV_CURRENT_IND
  ,PJV_DESCRIPTION
  ,PJV_HRV_PRT_CODE
  ,PJV_REUSABLE_REFNO
  ,PJV_CREATED_BY
  ,PJV_CREATED_DATE
  ,PJV_ESTIMATED_START_DATE
  ,PJV_ESTIMATED_END_DATE
  ,PJV_ALTERNATE_REFERENCE
  ,PJV_MAXIMUM_VALUE
  ,PJV_PLANNED_COST
  ,PJV_WORKS_AMOUNT
  ,PJV_FEES_AMOUNT
  ,PJV_COMMENTS
  ,PJV_MODIFIED_BY
  ,PJV_MODIFIED_DATE
 )
AS
  SELECT
   PRJ_REFERENCE
  ,PRJ_PRG_REFERENCE
  ,PRJ_SCO_CODE
  ,PRJ_STATUS_DATE
  ,PRJ_AUN_CODE
  ,PRJ_CREATED_BY
  ,PRJ_CREATED_DATE
  ,PRJ_TOTAL_TASK_VALUE
  ,PRJ_COMMITTED_AMOUNT
  ,PRJ_ACCRUED_AMOUNT
  ,PRJ_INVOICED_AMOUNT
  ,PRJ_EXPENDED_AMOUNT
  ,PRJ_TAX_COMMITTED_AMOUNT
  ,PRJ_TAX_ACCRUED_AMOUNT
  ,PRJ_TAX_INVOICED_AMOUNT
  ,PRJ_TAX_EXPENDED_AMOUNT
  ,PRJ_NET_ALLOCATED_AMOUNT
  ,PRJ_TAX_ALLOCATED_AMOUNT
  ,PRJ_AUTHORISED_BY
  ,PRJ_AUTHORISED_DATETIME
  ,PRJ_MODIFIED_BY
  ,PRJ_MODIFIED_DATE
  ,PJV_VERSION_NUMBER
  ,PJV_CURRENT_IND
  ,PJV_DESCRIPTION
  ,PJV_HRV_PRT_CODE
  ,PJV_REUSABLE_REFNO
  ,PJV_CREATED_BY
  ,PJV_CREATED_DATE
  ,PJV_ESTIMATED_START_DATE
  ,PJV_ESTIMATED_END_DATE
  ,PJV_ALTERNATE_REFERENCE
  ,PJV_MAXIMUM_VALUE
  ,PJV_PLANNED_COST
  ,PJV_WORKS_AMOUNT
  ,PJV_FEES_AMOUNT
  ,PJV_COMMENTS
  ,PJV_MODIFIED_BY
  ,PJV_MODIFIED_DATE
 FROM HOU.PROJECTS
 LEFT OUTER JOIN HOU.PROJECT_VERSIONS ON 
  HOU.PROJECTS.PRJ_REFERENCE =  HOU.PROJECT_VERSIONS.PJV_PRJ_REFERENCE AND 
 HOU.PROJECT_VERSIONS.PJV_CURRENT_IND = 'Y';
 
 CREATE OR REPLACE PUBLIC SYNONYM RED_PROJECTS_VIEW  for RED_PROJECTS_VIEW;
 
 create or replace force editionable view hou.RED_CONTRACTS_VIEW (
     CNT_REFERENCE
  ,CNT_PRJ_REFERENCE
  ,CNT_AUN_CODE
  ,CNT_SCO_CODE
  ,CNT_STATUS_DATE
  ,CNT_WARN_HRM_USERS_IND
  ,CNT_DRAWINGS_IND
  ,CNT_CREATED_BY
  ,CNT_CREATED_DATE
  ,CNT_COS_CODE
  ,CNT_NET_AMOUNT_ALLOCATED
  ,CNT_TAX_AMOUNT_ALLOCATED
  ,CNT_COMMITTED_AMOUNT
  ,CNT_ACCRUED_AMOUNT
  ,CNT_INVOICED_AMOUNT
  ,CNT_EXPENDED_AMOUNT
  ,CNT_TAX_COMMITTED_AMOUNT
  ,CNT_TAX_ACCRUED_AMOUNT
  ,CNT_TAX_INVOICED_AMOUNT
  ,CNT_TAX_EXPENDED_AMOUNT
  ,CNT_FILE_REF
  ,CNT_ALTERNATIVE_REFERENCE
  ,CNT_COMMENTS
  ,CNT_AUTHORISED_DATETIME
  ,CNT_AUTHORISED_BY
  ,CNT_MODIFIED_BY
  ,CNT_MODIFIED_DATE
  ,CNT_BULK_SOR_UPD_DATETIME
  ,CNT_RESCHEDULE_ALLOWED_IND
  ,CVE_VERSION_NUMBER
  ,CVE_CURRENT_IND
  ,CVE_DESCRIPTION
  ,CVE_RPT_IN_PLANNED_WORK_IND
  ,CVE_CREATED_BY
  ,CVE_CREATED_DATE
  ,CVE_REUSABLE_REFNO
  ,CVE_BUD_REFNO
  ,CVE_CNT_REF_ASSOCIATED_WITH
  ,CVE_HRV_PYR_CODE
  ,CVE_ESTIMATED_START_DATE
  ,CVE_ESTIMATED_END_DATE
  ,CVE_PROJECTED_COST
  ,CVE_PROJECTED_COST_TAX
  ,CVE_CONTRACT_VALUE
  ,CVE_MAX_VARIATION_AMOUNT
  ,CVE_MAX_VARIATION_TAX_AMT
  ,CVE_NON_COMP_DAMAGES_AMT
  ,CVE_NON_COMP_DAMAGES_UNIT
  ,CVE_LIABILITY_PERIOD
  ,CVE_PENULT_RETENTION_PCT
  ,CVE_INTERIM_RETENTION_PCT
  ,CVE_INTERIM_PYMNT_INTERVAL
  ,CVE_INTERIM_PYMNT_INT_UNIT
  ,CVE_FINAL_MEASURE_PERIOD
  ,CVE_MAX_NO_OF_REPEATS
  ,CVE_REPEAT_PERIOD
  ,CVE_REPEAT_PERIOD_UNIT
  ,CVE_MODIFIED_BY
  ,CVE_MODIFIED_DATE
  ,CVE_RETENTIONS_IND
 )
AS
  SELECT
   CNT_REFERENCE
  ,CNT_PRJ_REFERENCE
  ,CNT_AUN_CODE
  ,CNT_SCO_CODE
  ,CNT_STATUS_DATE
  ,CNT_WARN_HRM_USERS_IND
  ,CNT_DRAWINGS_IND
  ,CNT_CREATED_BY
  ,CNT_CREATED_DATE
  ,CNT_COS_CODE
  ,CNT_NET_AMOUNT_ALLOCATED
  ,CNT_TAX_AMOUNT_ALLOCATED
  ,CNT_COMMITTED_AMOUNT
  ,CNT_ACCRUED_AMOUNT
  ,CNT_INVOICED_AMOUNT
  ,CNT_EXPENDED_AMOUNT
  ,CNT_TAX_COMMITTED_AMOUNT
  ,CNT_TAX_ACCRUED_AMOUNT
  ,CNT_TAX_INVOICED_AMOUNT
  ,CNT_TAX_EXPENDED_AMOUNT
  ,CNT_FILE_REF
  ,CNT_ALTERNATIVE_REFERENCE
  ,CNT_COMMENTS
  ,CNT_AUTHORISED_DATETIME
  ,CNT_AUTHORISED_BY
  ,CNT_MODIFIED_BY
  ,CNT_MODIFIED_DATE
  ,CNT_BULK_SOR_UPD_DATETIME
  ,CNT_RESCHEDULE_ALLOWED_IND
  ,CVE_VERSION_NUMBER
  ,CVE_CURRENT_IND
  ,CVE_DESCRIPTION
  ,CVE_RPT_IN_PLANNED_WORK_IND
  ,CVE_CREATED_BY
  ,CVE_CREATED_DATE
  ,CVE_REUSABLE_REFNO
  ,CVE_BUD_REFNO
  ,CVE_CNT_REF_ASSOCIATED_WITH
  ,CVE_HRV_PYR_CODE
  ,CVE_ESTIMATED_START_DATE
  ,CVE_ESTIMATED_END_DATE
  ,CVE_PROJECTED_COST
  ,CVE_PROJECTED_COST_TAX
  ,CVE_CONTRACT_VALUE
  ,CVE_MAX_VARIATION_AMOUNT
  ,CVE_MAX_VARIATION_TAX_AMT
  ,CVE_NON_COMP_DAMAGES_AMT
  ,CVE_NON_COMP_DAMAGES_UNIT
  ,CVE_LIABILITY_PERIOD
  ,CVE_PENULT_RETENTION_PCT
  ,CVE_INTERIM_RETENTION_PCT
  ,CVE_INTERIM_PYMNT_INTERVAL
  ,CVE_INTERIM_PYMNT_INT_UNIT
  ,CVE_FINAL_MEASURE_PERIOD
  ,CVE_MAX_NO_OF_REPEATS
  ,CVE_REPEAT_PERIOD
  ,CVE_REPEAT_PERIOD_UNIT
  ,CVE_MODIFIED_BY
  ,CVE_MODIFIED_DATE
  ,CVE_RETENTIONS_IND
 FROM HOU.CONTRACTS
  LEFT OUTER JOIN HOU.CONTRACT_VERSIONS ON 
  HOU.CONTRACTS.CNT_REFERENCE = HOU.CONTRACT_VERSIONS.CVE_CNT_REFERENCE AND
 HOU.CONTRACT_VERSIONS.CVE_CURRENT_IND = 'Y';
 
 CREATE OR REPLACE PUBLIC SYNONYM RED_CONTRACTS_VIEW  FOR RED_CONTRACTS_VIEW;
 
 
 CREATE OR REPLACE FORCE editionable VIEW  hou.RED_TKG_TVE_TASKS_VIEW 
(  TKG_SRC_REFERENCE
  ,TKG_CODE
  ,TKG_SRC_TYPE
  ,TSK_ID
  ,TVE_VERSION_NUMBER
  ,TKG_DESCRIPTION
  ,TKG_START_DATE
  ,TKG_GROUP_TYPE
  ,TKG_CREATED_BY
  ,TKG_CREATED_DATE
  ,TKG_STM_CODE
  ,TKG_MODIFIED_BY
  ,TKG_MODIFIED_DATE
  ,TSK_TYPE_IND
  ,TSK_STK_CODE
  ,TSK_SCO_CODE
  ,TSK_STATUS_DATE
  ,TSK_CREATED_BY
  ,TSK_CREATED_DATE
  ,TSK_ALT_REFERENCE
  ,TSK_ACTUAL_END_DATE
  ,TSK_AUTHORISED_BY
  ,TSK_AUTHORISED_DATETIME
  ,TSK_MODIFIED_BY
  ,TSK_MODIFIED_DATE
  ,TVE_CURRENT_IND
  ,TVE_DISPLAY_SEQUENCE
  ,TVE_REUSABLE_REFNO
  ,TVE_CREATED_BY
  ,TVE_CREATED_DATE
  ,TVE_HRV_TUS_CODE
  ,TVE_BCA_YEAR
  ,TVE_VCA_CODE
  ,TVE_PLANNED_START_DATE
  ,TVE_NET_AMOUNT
  ,TVE_TAX_AMOUNT
  ,TVE_RETENTION_PERCENT
  ,TVE_RETENTION_PERIOD
  ,TVE_RETENTION_PERIOD_UNITS
  ,TVE_COMMENTS
  ,TVE_MODIFIED_BY
  ,TVE_MODIFIED_DATE)
AS 
SELECT TASK_GROUPS.TKG_SRC_REFERENCE
  ,TASK_GROUPS.TKG_CODE
  ,TASK_GROUPS.TKG_SRC_TYPE
  ,TASKS.TSK_ID
  ,TASK_VERSIONS.TVE_VERSION_NUMBER
  ,TASK_GROUPS.TKG_DESCRIPTION
  ,TASK_GROUPS.TKG_START_DATE
  ,TASK_GROUPS.TKG_GROUP_TYPE
  ,TASK_GROUPS.TKG_CREATED_BY
  ,TASK_GROUPS.TKG_CREATED_DATE
  ,TASK_GROUPS.TKG_STM_CODE
  ,TASK_GROUPS.TKG_MODIFIED_BY
  ,TASK_GROUPS.TKG_MODIFIED_DATE
  ,TASKS.TSK_TYPE_IND
  ,TASKS.TSK_STK_CODE
  ,TASKS.TSK_SCO_CODE
  ,TASKS.TSK_STATUS_DATE
  ,TASKS.TSK_CREATED_BY
  ,TASKS.TSK_CREATED_DATE
  ,TASKS.TSK_ALT_REFERENCE
  ,TASKS.TSK_ACTUAL_END_DATE
  ,TASKS.TSK_AUTHORISED_BY
  ,TASKS.TSK_AUTHORISED_DATETIME
  ,TASKS.TSK_MODIFIED_BY
  ,TASKS.TSK_MODIFIED_DATE
  ,TASK_VERSIONS.TVE_CURRENT_IND
  ,TASK_VERSIONS.TVE_DISPLAY_SEQUENCE
  ,TASK_VERSIONS.TVE_REUSABLE_REFNO
  ,TASK_VERSIONS.TVE_CREATED_BY
  ,TASK_VERSIONS.TVE_CREATED_DATE
  ,TASK_VERSIONS.TVE_HRV_TUS_CODE
  ,TASK_VERSIONS.TVE_BCA_YEAR
  ,TASK_VERSIONS.TVE_VCA_CODE
  ,TASK_VERSIONS.TVE_PLANNED_START_DATE
  ,TASK_VERSIONS.TVE_NET_AMOUNT
  ,TASK_VERSIONS.TVE_TAX_AMOUNT
  ,TASK_VERSIONS.TVE_RETENTION_PERCENT
  ,TASK_VERSIONS.TVE_RETENTION_PERIOD
  ,TASK_VERSIONS.TVE_RETENTION_PERIOD_UNITS
  ,TASK_VERSIONS.TVE_COMMENTS
  ,TASK_VERSIONS.TVE_MODIFIED_BY
  ,TASK_VERSIONS.TVE_MODIFIED_DATE
FROM
    HOU.tasks
   INNER JOIN HOU.task_groups ON
      HOU.TASKS.TSK_TKG_CODE = HOU.TASK_GROUPS.TKG_CODE AND
      HOU.TASKS.TSK_TKG_SRC_REFERENCE = HOU.TASK_GROUPS.TKG_SRC_REFERENCE AND
      HOU.TASKS.TSK_TKG_SRC_TYPE = HOU.TASK_GROUPS.TKG_SRC_TYPE 
    LEFT OUTER JOIN HOU.task_versions ON    
      HOU.TASKS.TSK_ID = HOU.TASK_VERSIONS.TVE_TSK_ID AND
      HOU.TASKS.TSK_TKG_CODE = HOU.TASK_VERSIONS.TVE_TSK_TKG_CODE AND
      HOU.TASKS.TSK_TKG_SRC_REFERENCE = HOU.TASK_VERSIONS.TVE_TSK_TKG_SRC_REFERENCE AND
      HOU.TASKS.TSK_TKG_SRC_TYPE = HOU.TASK_VERSIONS.TVE_TSK_TKG_SRC_TYPE AND
      HOU.TASK_VERSIONS.TVE_CURRENT_IND  =  'Y';
      
CREATE OR REPLACE PUBLIC SYNONYM RED_TKG_TVE_TASKS_VIEW  for RED_TKG_TVE_TASKS_VIEW;


  CREATE OR REPLACE FORCE VIEW HOU.RED_BANK_ACCOUNT_DETAILS_VIEW 
  (BANK_ACCOUNT_REFNO,
    PARTY_REFNO,
    BANK_ACCOUNT_TYPE,
    BANK_ACCOUNT_SORT_CODE,
    BANK_ACCOUNT_BANK_REFNO,
    BANK_ACCOUNT_NO,
    BANK_ACCOUNT_NAME,
    BANK_ACCOUNT_START_DATE,
    BANK_ACCOUNT_CREATED_BY,
    BANK_ACCOUNT_CREATED_DATE,
    BANK_ACCOUNT_END_DATE,
    BANK_ACCOUNT_USER_ALT_REF,
    BANK_ACCOUNT_MODIFIED_BY,
    BANK_ACCOUNT_MODIFIED_DATE,
    PARTY_ACCOUNT_START_DATE,
    PARTY_ACCOUNT_CREATED_DATE,
    PARTY_ACCOUNT_CREATED_BY,
    PARTY_ACCOUNT_END_DATE,
    PARTY_ACCOUNT_MODIFIED_DATE,
    PARTY_ACCOUNT_MODIFIED_BY) AS
  SELECT
    bank_account_details.bad_refno,
    party_bank_accounts.pba_par_refno,
    bank_account_details.bad_type,
    bank_account_details.bad_sort_code,
    bank_account_details.bad_bde_refno,
    bank_account_details.bad_account_no,
    bank_account_details.bad_account_name,
    bank_account_details.bad_start_date,
    bank_account_details.bad_created_by,
    bank_account_details.bad_created_date,
    bank_account_details.bad_end_date,
    bank_account_details.bad_user_alt_ref,
    bank_account_details.bad_modified_by,
    bank_account_details.bad_modified_date,
    party_bank_accounts.pba_start_date,
    party_bank_accounts.pba_created_date,
    party_bank_accounts.pba_created_by,
    party_bank_accounts.pba_end_date,
    party_bank_accounts.pba_modified_date,
    party_bank_accounts.pba_modified_by
  FROM
    bank_account_details, party_bank_accounts
WHERE bank_account_details.bad_refno = party_bank_accounts.pba_bad_refno;

CREATE OR REPLACE PUBLIC SYNONYM RED_BANK_ACCOUNT_DETAILS_VIEW   for RED_BANK_ACCOUNT_DETAILS_VIEW ;

 CREATE OR REPLACE FORCE EDITIONABLE VIEW HOU.RED_PROP_RENT_VIEW 
(PROPERTY_REFNO
,PROPERTY_REFERENCE
,RENTS_AUN_CODE
,ELEMENT_CODE
,ATTRIBUTE_CODE
,REBATEABLE_INDICATOR
,GROSS_AMOUNT
,NET_AMOUNT) AS
 SELECT 
pro.pro_refno
,pro.pro_propref
,prav.prav_par_ren_aun_code,
ele_code,
rer_att_code,
rer_rebateable_ind,
(round(((rer.rer_elem_rate
 * decode(rer.rer_elem_period,'W',aye.aye_weeks_in_year,'M',12,'Y',1))
 / decode(aye.aye_debit_basis,'W',aye.aye_weeks_in_year,'M',12))  *  decode(rer.rer_apply_multiplier_ind,'Y',s_admin_year_multipliers.get_multiplier(aye.aye_aun_code,  aye.aye_year, SYSDATE),'N',1)
 * decode(ele.ele_value_type,'C',1,'N',pel_numeric_value),2)) ,
 (round((round((( rer.rer_elem_rate
 * decode(rer.rer_elem_period, 'W',aye.aye_weeks_in_year, 'M',12,'Y',1))
 / decode(aye.aye_debit_basis, 'W',aye.aye_weeks_in_year, 'M',12))
 * decode(rer.rer_apply_multiplier_ind, 'Y',   s_admin_year_multipliers.get_multiplier(aye.aye_aun_code, aye.aye_year, SYSDATE),'N',1)
  * decode(ele.ele_value_type, 'C', 1, 'N', pel.pel_numeric_value),2)
  * nvl(vra.vra_vat_rate,0) /100),2)) 
FROM  properties  	    pro,
      admin_properties 	    apr,
      property_elements     pel,
      vat_rates  	    vra,
      rent_elemrates  	    rer,
      admin_years  	    aye,
      admin_units  	    aun,
      elements		    ele,
      parent_ren_aun_view   prav
    WHERE apr.apr_pro_refno = pro.pro_refno
AND   apr.apr_aun_code      = aun.aun_code
AND   aun.aun_code          = prav.prav_aun_code
AND   prav.prav_par_ren_aun_code = aye.aye_aun_code
AND   aye.aye_start_date   	 <= TRUNC(sysdate)
AND   aye.aye_end_date     	 >= TRUNC(sysdate)
AND   pel.pel_pro_refno   	 = pro.pro_refno
AND   ele.ele_code	         = pel.pel_ele_code
AND   ele.ele_value_type    	 in ('C', 'N')
AND   pel.pel_start_date   	 <= TRUNC(sysdate)
AND  (pel.pel_end_date     	 >= TRUNC(sysdate) OR pel.pel_end_date IS NULL)
AND   rer.rer_ele_code       	 = pel.pel_ele_code
AND   rer.rer_ele_value_type 	 = ele.ele_value_type
AND   rer.rer_status         	 = 'A'
AND   NVL(rer.rer_att_code, 'NUL') = NVL(pel.pel_att_code, 'NUL')
AND   rer.rer_start_date   	   <= TRUNC(sysdate)
AND   (rer.rer_end_date    	   >= TRUNC(sysdate) OR rer.rer_end_date IS NULL)
AND   vra.vra_vca_code(+)    	   = rer.rer_vca_code
AND   TRUNC(sysdate) BETWEEN vra.vra_start_date(+)
  AND NVL(vra.vra_end_date(+),TRUNC(sysdate));

CREATE OR REPLACE PUBLIC SYNONYM RED_PROP_RENT_VIEW   for RED_PROP_RENT_VIEW;

