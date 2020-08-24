-- *********************************************************************
--
-- Version  dbver   Who   Date         Why
--   1.0    6.13    AJ    08-FEB-2016  Initial Creation WITH Change Control
--                                     as added LPAR_ORG_CURRENT_IND
--   1.1    6.14    AJ    07-NOV-2017  Added LPAR_HOP_HEAD_HHOLD_IND and
--                                     LPAR_HOP_HHOLD_GROUP_NO new at this release 
-- *********************************************************************
--
load data
APPEND
into table DL_HEM_PEOPLE
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LPAR_DLB_BATCH_ID CONSTANT "$batch_no"
,LPAR_DL_SEQNO RECNUM
,LPAR_DL_LOAD_STATUS CONSTANT "L"
,LPAR_HOP_START_DATE DATE "DD-MON-YYYY" NULLIF LPAR_HOP_START_DATE=blanks
,LPAR_PER_SURNAME CHAR "rtrim(:LPAR_PER_SURNAME)"
,LPAR_TCY_ALT_REF CHAR "rtrim(:LPAR_TCY_ALT_REF)"
,LPAR_PER_FORENAME CHAR "rtrim(:LPAR_PER_FORENAME)"
,LPAR_HOP_HPSR_CODE CHAR "rtrim(:LPAR_HOP_HPSR_CODE)"
,LPAR_PER_TITLE CHAR "rtrim(:LPAR_PER_TITLE)"
,LPAR_PER_INITIALS CHAR "rtrim(:LPAR_PER_INITIALS)"
,LPAR_PER_DATE_OF_BIRTH DATE "DD-MON-YYYY" NULLIF LPAR_PER_DATE_OF_BIRTH=blanks
,LPAR_PER_HOU_DISABLED_IND CHAR "rtrim(:LPAR_PER_HOU_DISABLED_IND)"
,LPAR_PER_HOU_OAP_IND CHAR "rtrim(:LPAR_PER_HOU_OAP_IND)"
,LPAR_PER_FRV_FGE_CODE CHAR "rtrim(:LPAR_PER_FRV_FGE_CODE)"
,LPAR_HOP_HRV_REL_CODE CHAR "rtrim(:LPAR_HOP_HRV_REL_CODE)"
,LPAR_PER_HOU_EMPLOYER CHAR "rtrim(:LPAR_PER_HOU_EMPLOYER)"
,LPAR_PER_HOU_HRV_HMS_CODE CHAR "rtrim(:LPAR_PER_HOU_HRV_HMS_CODE)"
,LPAR_PHONE CHAR "rtrim(:LPAR_PHONE)"
,LPAR_HOP_END_DATE DATE "DD-MON-YYYY" NULLIF LPAR_HOP_END_DATE=blanks
,LPAR_HOP_HPER_CODE CHAR "rtrim(:LPAR_HOP_HPER_CODE)"
,LPAR_TCY_IND CHAR "rtrim(:LPAR_TCY_IND)"
,LPAR_TIN_MAIN_TENANT_IND "rtrim(:LPAR_TIN_MAIN_TENANT_IND)"
,LPAR_TIN_START_DATE DATE "DD-MON-YYYY" NULLIF LPAR_TIN_START_DATE=blanks
,LPAR_TIN_END_DATE DATE "DD-MON-YYYY" NULLIF LPAR_TIN_END_DATE=blanks
,LPAR_TIN_HRV_TIR_CODE "rtrim(:LPAR_TIN_HRV_TIR_CODE)"
,LPAR_TIN_STAT_SUCCESSOR_IND CHAR "rtrim(:LPAR_TIN_STAT_SUCCESSOR_IND)"
,LPAR_PER_ALT_REF CHAR "rtrim(:LPAR_PER_ALT_REF)"
,LPAR_PER_FRV_FEO_CODE CHAR "rtrim(:LPAR_PER_FRV_FEO_CODE)"
,LPAR_PER_NI_NO CHAR "rtrim(:LPAR_PER_NI_NO)"
,LPAR_PER_FRV_HGO_CODE CHAR "rtrim(:LPAR_PER_FRV_HGO_CODE)"
,LPAR_PER_FRV_FNL_CODE CHAR "rtrim(:LPAR_PER_FRV_FNL_CODE)"
,LPAR_PER_OTHER_NAME CHAR "rtrim(:LPAR_PER_OTHER_NAME)"
,LPAR_PER_HOU_SURNAME_PREFIX CHAR "rtrim(:LPAR_PER_HOU_SURNAME_PREFIX)"
,LPAR_HOU_LEGACY_REF CHAR "rtrim(:LPAR_HOU_LEGACY_REF)"
,LPAR_IPP_SHORTNAME CHAR "rtrim(:LPAR_IPP_SHORTNAME)"
,LPAR_IPP_PLACEMENT_IND CHAR "rtrim(:LPAR_IPP_PLACEMENT_IND)"
,LPAR_IPP_CURRENT_IND CHAR "rtrim(:LPAR_IPP_CURRENT_IND)"
,LPAR_IPP_IPT_CODE CHAR "rtrim(:LPAR_IPP_IPT_CODE)"
,LPAR_IPP_USR_USERNAME CHAR "rtrim(:LPAR_IPP_USR_USERNAME)"
,LPAR_IPP_SPR_PRINTER_NAME CHAR "rtrim(:LPAR_IPP_SPR_PRINTER_NAME)"
,LPAR_IPP_COMMENTS CHAR "rtrim(:LPAR_IPP_COMMENTS)"
,LPAR_IPP_VCA_CODE CHAR "rtrim(:LPAR_IPP_VCA_CODE)"
,LPAR_IPU_AUN_CODE CHAR "rtrim(:LPAR_IPU_AUN_CODE)"
,LPAR_IPP_STAFF_ID CHAR "rtrim(:LPAR_IPP_STAFF_ID)"
,LPAR_IPP_COS_CODE CHAR "rtrim(:LPAR_IPP_COS_CODE)"
,LPAR_IPP_HRV_FIT_CODE CHAR "rtrim(:LPAR_IPP_HRV_FIT_CODE)"
,LPAR_TYPE CHAR "rtrim(:LPAR_TYPE)"
,LPAR_ORG_SORT_CODE CHAR "rtrim(:LPAR_ORG_SORT_CODE)"
,LPAR_ORG_NAME CHAR "rtrim(:LPAR_ORG_NAME)"
,LPAR_ORG_SHORT_NAME CHAR "rtrim(:LPAR_ORG_SHORT_NAME)"
,LPAR_ORG_FRV_OTY_CODE CHAR "rtrim(:LPAR_ORG_FRV_OTY_CODE)"
,LPAR_PER_HOU_AT_RISK_IND CHAR "rtrim(:LPAR_PER_HOU_AT_RISK_IND)"
,LPAR_PER_HOU_HRV_NTLY_CODE  CHAR "rtrim(:LPAR_PER_HOU_HRV_NTLY_CODE)"
,LPAR_PER_HOU_HRV_SEXO_CODE  CHAR "rtrim(:LPAR_PER_HOU_HRV_SEXO_CODE)"
,LPAR_PER_HOU_HRV_RLGN_CODE  CHAR "rtrim(:LPAR_PER_HOU_HRV_RLGN_CODE)"
,LPAR_PER_HOU_HRV_ECST_CODE  CHAR "rtrim(:LPAR_PER_HOU_HRV_ECST_CODE)"
,LPAR_ORG_CURRENT_IND  CHAR "rtrim(:LPAR_ORG_CURRENT_IND)"
,LPAR_HOP_HEAD_HHOLD_IND  CHAR "rtrim(:LPAR_HOP_HEAD_HHOLD_IND)"
,LPAR_HOP_HHOLD_GROUP_NO  CHAR "rtrim(:LPAR_HOP_HHOLD_GROUP_NO)"
)

