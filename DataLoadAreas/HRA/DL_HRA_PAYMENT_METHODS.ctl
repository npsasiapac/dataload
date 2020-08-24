-- *********************************************************************
--
-- Ver   DB     Who   Date         Why
-- 1.00  6.13   AJ    26-APR-2016  Initial Creation WITH Change Control
--                                 as PCT Start and End dates not in file
--                                 yet in data load table and guide
-- 1.1   6:13  PJD    03-NOV-2016  Corrected a couple of typos in the
--                                 new mlang field names
-- *********************************************************************
--
load data
infile $GRI_DATAFILE
APPEND
into table DL_HRA_PAYMENT_METHODS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPME_DLB_BATCH_ID		      CONSTANT "$batch_no"
,LPME_DL_LOAD_STATUS		  CONSTANT "L"
,LPME_DL_SEQNO			      RECNUM
,LPME_PAY_REF			      CHAR "rtrim(:LPME_PAY_REF)"
,LPME_PME_PMY_CODE		      CHAR "rtrim(:LPME_PME_PMY_CODE)"
,LPME_PME_START_DATE		  DATE "DD-MON-YYYY" NULLIF LPME_PME_START_DATE=blanks
,LPME_PME_HRV_PPC_CODE		  CHAR "rtrim(:LPME_PME_HRV_PPC_CODE)"
,LPME_PME_FIRST_DD_TAKEN_IND  CHAR "rtrim(:LPME_PME_FIRST_DD_TAKEN_IND)"
,LPME_BDE_BANK_NAME		      CHAR "rtrim(:LPME_BDE_BANK_NAME)"
,LPME_BAD_ACCOUNT_NO		  CHAR "rtrim(:LPME_BAD_ACCOUNT_NO)"
,LPME_BAD_ACCOUNT_NAME		  CHAR "rtrim(:LPME_BAD_ACCOUNT_NAME)"
,LPME_BAD_SORT_CODE		      CHAR "rtrim(:LPME_BAD_SORT_CODE)"
,LPME_BDE_BRANCH_NAME		  CHAR "rtrim(:LPME_BDE_BRANCH_NAME)"
,LPME_BAD_START_DATE		  DATE "DD-MON-YYYY" NULLIF LPME_BAD_START_DATE=blanks
,LPME_AUN_BAD_ACCOUNT_NO	  CHAR "rtrim(:LPME_AUN_BAD_ACCOUNT_NO)"
,LPME_SOURCE_ACC_IND		  CHAR "rtrim(:LPME_SOURCE_ACC_IND)"
,LPME_BAD_PAR_PER_ALT_REF	  CHAR "rtrim(:LPME_BAD_PAR_PER_ALT_REF)"
,LPME_PCT_AMOUNT		      CHAR "rtrim(:LPME_PCT_AMOUNT)"
,LPME_PCT_PERCENTAGE		  CHAR "rtrim(:LPME_PCT_PERCENTAGE)"
,LPME_BDE_BTY_CODE		      CHAR "rtrim(:LPME_BDE_BTY_CODE)"
,LPME_PME_END_DATE            DATE "DD-MON-YYYY" NULLIF LPME_PME_END_DATE=blanks
,LPME_PCT_START_DATE          DATE "DD-MON-YYYY" NULLIF LPME_PCT_START_DATE=blanks
,LPME_PCT_END_DATE            DATE "DD-MON-YYYY" NULLIF LPME_PCT_END_DATE=blanks
,LPME_BDE_BANK_NAME_MLANG     CHAR "rtrim(:LPME_BDE_BANK_NAME_MLANG)"                          
,LPME_BDE_BRANCH_NAME_MLANG   CHAR "rtrim(:LPME_BDE_BRANCH_NAME_MLANG)"
)
