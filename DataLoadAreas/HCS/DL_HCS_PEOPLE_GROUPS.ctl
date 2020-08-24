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
into table DL_HCS_PEOPLE_GROUPS
fields terminated by "," optionally enclosed by '"'
trailing nullcols       
( LPEG_DLB_BATCH_ID             CONSTANT "$batch_no",
  LPEG_DL_SEQNO                 RECNUM,
  LPEG_DL_LOAD_STATUS           CONSTANT "L",
  LPEG_CODE                     CHAR "upper(rtrim(:LPEG_CODE))",
  LPEG_DESCRIPTION              CHAR "rtrim(:LPEG_DESCRIPTION)",
  LPEG_START_DATE               DATE "DD-MON-YYYY" NULLIF LPEG_START_DATE=blanks,
  LPEG_PGT_CODE                 CHAR "upper(rtrim(:LPEG_PGT_CODE))",
  LPEG_SCO_CODE                 CHAR "upper(rtrim(:LPEG_SCO_CODE))",
  LPEG_COMMENTS                 CHAR "rtrim(:LPEG_COMMENTS)",
  LPEG_AUN_CODE                 CHAR "upper(rtrim(:LPEG_AUN_CODE))",
  LPEG_END_DATE                 DATE "DD-MON-YYYY" NULLIF LPEG_END_DATE=blanks,
  LPEG_CREATED_DATE             DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPEG_CREATED_DATE=blanks,
  LPEG_CREATED_BY               CHAR "upper(rtrim(:LPEG_CREATED_BY))"
)

