-- *********************************************************************
--
-- Version      Who         Date       Why
-- 01.00        Ian Rowell  25-Sep-09  Initial Creation
--
-- 02.00        V. Shah     17-Oct-11  Removed LSGPE_SURV_LEGACY_REF
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_SUBSIDY_GRACE_PERIODS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LSGPE_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LSGPE_DL_SEQNO                   RECNUM
,LSGPE_DL_LOAD_STATUS             CONSTANT "L"
,LSGPE_SUAP_LEGACY_REF            CHAR "rtrim(UPPER(:LSGPE_SUAP_LEGACY_REF))"
,LSGPE_SEQ		          CHAR "rtrim(UPPER(:LSGPE_SEQ))"
,LSGPE_HGPR_CODE                  CHAR "rtrim(UPPER(:LSGPE_HGPR_CODE))"
,LSGPE_START_DATE                 DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSGPE_START_DATE=blanks
,LSGPE_RENT_PAYABLE	          CHAR "rtrim(UPPER(:LSGPE_RENT_PAYABLE))"
,LSGPE_CREATED_DATE	          DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSGPE_CREATED_DATE=blanks
,LSGPE_CREATED_BY	          CHAR "rtrim(UPPER(:LSGPE_CREATED_BY))"
,LSGPE_END_DATE                   DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSGPE_END_DATE=blanks
,LSGPE_MODIFIED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSGPE_MODIFIED_DATE=blanks
,LSGPE_MODIFIED_BY                CHAR "rtrim(UPPER(:LSGPE_MODIFIED_BY))"
)


