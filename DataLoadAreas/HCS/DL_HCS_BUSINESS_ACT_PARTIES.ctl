--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.18      RH   05-FEB-2019  Initial creation for SAHT
--
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HCS_BUSINESS_ACT_PARTIES
fields terminated by "," optionally enclosed by '"'
trailing nullcols       
( LBPA_DLB_BATCH_ID             CONSTANT "$batch_no",             
  LBPA_DL_SEQNO                 RECNUM,  
  LBPA_DL_LOAD_STATUS           CONSTANT "L",
  LBPA_BAN_REFERENCE            CHAR "upper(rtrim(:LBPA_BAN_REFERENCE))",
  LBPA_START_DATE               DATE "DD-MON-YYYY" NULLIF LBPA_START_DATE=blanks,
  LBPA_MAIN_PARTY_IND           CHAR "upper(rtrim(:LBPA_MAIN_PARTY_IND))",
  LBPA_HRV_BAC_CODE             CHAR "upper(rtrim(:LBPA_HRV_BAC_CODE))",
  LBPA_CREATED_BY               CHAR "upper(rtrim(:LBPA_CREATED_BY))",
  LBPA_CREATED_DATE             DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LBPA_CREATED_DATE=blanks,
  LBPA_OBJECT_TYPE              CHAR "upper(rtrim(:LBPA_OBJECT_TYPE))",
  LBPA_OBJECT_REF               CHAR "upper(rtrim(:LBPA_OBJECT_REF))",
  LBPA_END_DATE                 DATE "DD-MON-YYYY" NULLIF LBPA_END_DATE=blanks,
  LBPA_COMMENTS                 CHAR "rtrim(:LBPA_COMMENTS)"
)


