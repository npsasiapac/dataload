--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0                    27-JUL-2010  latest version used as base.
--  1.1     6.13      AJ   29-FEB-2016  Control comments section added
--                                      time stamp added to LIPA_CREATED_DATE
--                                      LPAR_CREATED_DATE
--  *****   bespoke for GNB ************
--  1.2     6.14      AJ   05-JAN-2018  Modified Date and By added for GNB Migration
--  1.3     6.14      AJ   12-JAN-2018  Modified by Date slightly amended
--  1.4     6.14      AJ   22-JAN-2018  Added LIPA_LEGACY_REF
--  1.5     6.14      AJ   23-JAN-2018  LIPA_HHOLD_GROUP_NO and LIPA_HEAD_HHOLD_IND                                      
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HAT_INVOLVED_PARTIES
fields terminated by "," optionally enclosed by '"'
trailing nullcols          
( LIPA_DLB_BATCH_ID            CONSTANT "$batch_no",             
  LIPA_DL_SEQNO                RECNUM,  
  LIPA_DL_LOAD_STATUS          CONSTANT "L",
  LAPP_LEGACY_REF              CHAR "rtrim(:LAPP_LEGACY_REF)", 
  LPAR_PER_ALT_REF             CHAR "rtrim(:LPAR_PER_ALT_REF)",  
  LIPA_JOINT_APPL_IND          CHAR "rtrim(:LIPA_JOINT_APPL_IND)", 
  LIPA_LIVING_APART_IND        CHAR "rtrim(:LIPA_LIVING_APART_IND)",  
  LIPA_REHOUSE_IND             CHAR "rtrim(:LIPA_REHOUSE_IND)",  
  LIPA_MAIN_APPLICANT_IND      CHAR "rtrim(:LIPA_MAIN_APPLICANT_IND)",  
  LIPA_CREATED_DATE            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LIPA_CREATED_DATE=blanks,
  LIPA_CREATED_BY              CHAR "rtrim(:LIPA_CREATED_BY)",
  LIPA_START_DATE              DATE "DD-MON-YYYY" NULLIF LIPA_START_DATE=blanks,  
  LIPA_END_DATE                DATE "DD-MON-YYYY" NULLIF LIPA_END_DATE=blanks,  
  LIPA_GROUPNO                 CHAR "rtrim(:LIPA_GROUPNO)", 
  LIPA_ACT_ROOMNO              CHAR "rtrim(:LIPA_ACT_ROOMNO)",   
  LIPA_HRV_HPER_CODE           CHAR "rtrim(:LIPA_HRV_HPER_CODE)",   
  LIPA_HRV_REL_CODE            CHAR "rtrim(:LIPA_HRV_REL_CODE)",  
  LPAR_TYPE                    CHAR "rtrim(:LPAR_TYPE)",   
  LPAR_CREATED_DATE            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPAR_CREATED_DATE=blanks,  
  LPAR_CREATED_BY              CHAR "rtrim(:LPAR_CREATED_BY)",  
  LPAR_PER_SURNAME             CHAR "rtrim(:LPAR_PER_SURNAME)", 
  LPAR_PER_FORENAME            CHAR "rtrim(:LPAR_PER_FORENAME)",   
  LPAR_PER_INITIALS            CHAR "rtrim(:LPAR_PER_INITIALS)",  
  LPAR_PER_TITLE               CHAR "rtrim(:LPAR_PER_TITLE)",   
  LPAR_PER_DATE_OF_BIRTH       DATE "DD-MON-YYYY" NULLIF LPAR_PER_DATE_OF_BIRTH=blanks,   
  LPAR_PER_HOU_HRV_HMS_CODE    CHAR "rtrim(:LPAR_PER_HOU_HRV_HMS_CODE)",  
  LPAR_PER_HOU_OAP_IND         CHAR "rtrim(:LPAR_PER_HOU_OAP_IND)",
  LPAR_PER_HOU_DISABLED_IND    CHAR "rtrim(:LPAR_PER_HOU_DISABLED_IND)", 
  LPAR_PER_FRV_FGE_CODE        CHAR "rtrim(:LPAR_PER_FRV_FGE_CODE)", 
  LPAR_PHONE_NO                CHAR "rtrim(:LPAR_PHONE_NO)",  
  LPAR_PER_FRV_FEO_CODE        CHAR "rtrim(:LPAR_PER_FRV_FEO_CODE)",
  LPAR_PER_HOU_HRV_HGO_CODE    CHAR "rtrim(:LPAR_PER_HOU_HRV_HGO_CODE)",
  LPAR_PER_FRV_FNL_CODE        CHAR "rtrim(:LPAR_PER_FRV_FNL_CODE)",
  LPAR_PER_NI_NO               CHAR "rtrim(:LPAR_PER_NI_NO)",
  LPAR_PER_HOU_SURNAME_PREFIX  CHAR "rtrim(:LPAR_PER_HOU_SURNAME_PREFIX)",
  LPAR_PER_HOU_AT_RISK_IND     CHAR "rtrim(:LPAR_PER_HOU_AT_RISK_IND)",
  LPAR_PER_HOU_HRV_NTLY_CODE   CHAR "rtrim(:LPAR_PER_HOU_HRV_NTLY_CODE)",
  LPAR_PER_HOU_HRV_SEXO_CODE   CHAR "rtrim(:LPAR_PER_HOU_HRV_SEXO_CODE)",
  LPAR_PER_HOU_HRV_RLGN_CODE   CHAR "rtrim(:LPAR_PER_HOU_HRV_RLGN_CODE)",
  LPAR_PER_HOU_HRV_ECST_CODE   CHAR "rtrim(:LPAR_PER_HOU_HRV_ECST_CODE)",
  LIPA_MODIFIED_DATE           DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LIPA_MODIFIED_DATE=blanks,
  LIPA_MODIFIED_BY             CHAR "rtrim(:LIPA_MODIFIED_BY)",
  LIPA_LEGACY_REF              CHAR "rtrim(:LIPA_LEGACY_REF)",
  LIPA_HEAD_HHOLD_IND          CHAR "rtrim(:LIPA_HEAD_HHOLD_IND)",
  LIPA_HHOLD_GROUP_NO          CHAR "rtrim(:LIPA_HHOLD_GROUP_NO)"
)

