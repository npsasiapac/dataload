--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.18      RH   29-JAN-2019  Initial creation for SAHT
--
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HCS_PEOPLE_GROUP_MEMBERS
fields terminated by "," optionally enclosed by '"'
trailing nullcols       
( LPGE_DLB_BATCH_ID             CONSTANT "$batch_no",             
  LPGE_DL_SEQNO                 RECNUM,  
  LPGE_DL_LOAD_STATUS           CONSTANT "L",
  LPGE_PEG_CODE                 CHAR "upper(rtrim(:LPGE_PEG_CODE))",
  LPGE_PAR_REFNO                CHAR "rtrim(:LPGE_PAR_REFNO)",
  LPGE_START_DATE               DATE "DD-MON-YYYY" NULLIF LPGE_START_DATE=blanks,
  LPGE_KEY_MEMBER_IND           CHAR "upper(rtrim(:LPGE_KEY_MEMBER_IND))",
  LPGE_END_DATE                 DATE "DD-MON-YYYY" NULLIF LPGE_END_DATE=blanks,
  LPGE_CREATED_DATE             DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPGE_CREATED_DATE=blanks,
  LPGE_CREATED_BY               CHAR "upper(rtrim(:LPGE_CREATED_BY))"
)


