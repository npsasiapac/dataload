-- *********************************************************************
--
-- Ver   DB     Who   Date         Why
-- 1.0   6.14   AJ    20-DEC-2016  Initial Creation WITH Change Control
--                                 for Manitoba
-- 1.1   6.14   AJ    21-DEC-2016  Amended Syntax when testing in housup14
--
-- *********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HRA_BANK_DETAILS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LBDE_DLB_BATCH_ID               CONSTANT "$batch_no"
,LBDE_DL_SEQNO                   RECNUM
,LBDE_DL_LOAD_STATUS             CONSTANT "L"
,LBDE_TYPE                       CHAR "rtrim(:LBDE_TYPE)"
,LBDE_BRANCH                     CHAR "rtrim(:LBDE_BRANCH)"
,LBDE_BRANCH_CODE                CHAR "rtrim(:LBDE_BRANCH_CODE)"
,LBDE_ADR1                       CHAR "rtrim(:LBDE_ADR1)"
,LBDE_ADR2                       CHAR "rtrim(:LBDE_ADR2)"
,LBDE_ADR3                       CHAR "rtrim(:LBDE_ADR3)"
,LBDE_PCODE                      CHAR "rtrim(:LBDE_PCODE)"
,LBDE_PHONE                      CHAR "rtrim(:LBDE_PHONE)"
,LBDE_BANK_CODE                  CHAR "rtrim(:LBDE_BANK_CODE)"
,LBDE_BANK_NAME                  CHAR "rtrim(:LBDE_BANK_NAME)"
,LBDE_AMENDED                    DATE "DD-MON-YYYY" NULLIF LBDE_AMENDED=blanks     
)


