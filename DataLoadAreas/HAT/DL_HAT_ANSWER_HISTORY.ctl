--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.14      AJ   13-OCT-2017 Initial creation for GNB Migrate Project
--                                     AHS_APP_REFNO => LAHS_APP_LEGACY_REF
--  1.1     6.14      AJ   03-JAN-2017 several column names amended and added
--                                     LAHS_REC_TYPE
--  1.2     6.14      AJ   03-JAN-2017 1) Removed the following as not required
--                                     lahs_ipa_refno  lahs_mrf_assessment_refno and 
--                                     lahs_hia_hin_instance_refno
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HAT_ANSWER_HISTORY
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LAHS_DLB_BATCH_ID           CONSTANT "$batch_no",    
LAHS_DL_SEQNO                RECNUM,
LAHS_DL_LOAD_STATUS          CONSTANT "L",
LAHS_REC_TYPE                CHAR "rtrim(:LAHS_REC_TYPE)",
LAHS_APP_LEGACY_REF          CHAR "rtrim(:LAHS_APP_LEGACY_REF)",
LAHS_QUE_REFNO               CHAR "rtrim(:LAHS_QUE_REFNO)",
LAHS_LAR_CODE                CHAR "rtrim(:LAHS_LAR_CODE)",
LAHS_ACTION_IND              CHAR "rtrim(:LAHS_ACTION_IND)",
LAHS_MODIFIED_BY             CHAR "rtrim(:LAHS_MODIFIED_BY)",
LAHS_MODIFIED_DATE           DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LAHS_MODIFIED_DATE=blanks,
LAHS_DATE_VALUE              DATE "DD-MON-YYYY" NULLIF LAHS_DATE_VALUE=blanks,
LAHS_NUMBER_VALUE            CHAR "rtrim(:LAHS_NUMBER_VALUE)",
LAHS_CHAR_VALUE              CHAR "rtrim(:LAHS_CHAR_VALUE)",
LAHS_CREATED_BY              CHAR "rtrim(:LAHS_CREATED_BY)",
LAHS_CREATED_DATE            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LAHS_CREATED_DATE=blanks,
LAHS_QOR_CODE                CHAR "rtrim(:LAHS_QOR_CODE)",
LAHS_OTHER_CODE              CHAR "rtrim(:LAHS_OTHER_CODE)",
LAHS_OTHER_DATE              DATE "DD-MON-YYYY" NULLIF LAHS_OTHER_DATE=blanks,
LAHS_COMMENTS                CHAR(2000) "rtrim(:LAHS_COMMENTS)"
)




   
