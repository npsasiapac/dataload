--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.16      AJ   28-MAR-2018  Initial creation for OHMS Migrate Project
--
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HEM_FIRST_REF_VALUES
fields terminated by "," optionally enclosed by '"'
trailing nullcols       
( LFRV_DLB_BATCH_ID             CONSTANT "$batch_no",             
  LFRV_DL_SEQNO                 RECNUM,  
  LFRV_DL_LOAD_STATUS           CONSTANT "L",
  LFRV_FRD_DOMAIN               CHAR "rtrim(:LFRV_FRD_DOMAIN)",
  LFRV_CODE                     CHAR "rtrim(:LFRV_CODE)",
  LFRV_NAME                     CHAR "rtrim(:LFRV_NAME)",
  LFRV_CURRENT_IND              CHAR "rtrim(:LFRV_CURRENT_IND)",
  LFRV_USAGE                    CHAR "rtrim(:LFRV_USAGE)",
  LFRV_CREATION_DATE            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LFRV_CREATION_DATE=blanks,
  LFRV_CREATED_BY               CHAR "rtrim(:LWHI_CREATED_BY)",
  LFRV_DEFAULT_IND              CHAR "rtrim(:LFRV_DEFAULT_IND)",
  LFRV_SEQUENCE                 CHAR "rtrim(:LFRV_SEQUENCE)",
  LFRV_TEXT                     CHAR(240)"rtrim(:LFRV_TEXT)",
  LFRV_CODE_MLANG               CHAR "rtrim(:LFRV_CODE_MLANG)",
  LFRV_NAME_MLANG               CHAR "rtrim(:LFRV_NAME_MLANG)"
)

