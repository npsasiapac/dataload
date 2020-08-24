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
into table DL_HEM_FIRST_REF_DOMAINS
fields terminated by "," optionally enclosed by '"'
trailing nullcols       
( LFRD_DLB_BATCH_ID             CONSTANT "$batch_no",             
  LFRD_DL_SEQNO                 RECNUM,  
  LFRD_DL_LOAD_STATUS           CONSTANT "L",
  LFRD_DOMAIN                   CHAR "rtrim(:LFRD_DOMAIN)",
  LFRD_NAME                     CHAR "rtrim(:LFRD_NAME)",
  LFRD_CURRENT_IND              CHAR "rtrim(:LFRD_CURRENT_IND)",
  LFRD_DEFAULT_OPT_IND          CHAR "rtrim(:LFRD_DEFAULT_OPT_IND)",
  LFRD_LENGTH                   CHAR "rtrim(:LFRD_LENGTH)",
  LFRD_USAGE                    CHAR "rtrim(:LFRD_USAGE)",
  LFRD_CREATION_DATE            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LFRD_CREATION_DATE=blanks,
  LFRD_CREATED_BY               CHAR "rtrim(:LFRD_CREATED_BY)",
  LFRD_PRODUCT_IND              CHAR "rtrim(:LFRD_PRODUCT_IND)",
  LFRD_DOMAIN_MLANG             CHAR "rtrim(:LFRD_DOMAIN_MLANG)",
  LFRD_NAME_MLANG               CHAR "rtrim(:LFRD_NAME_MLANG)"
)

