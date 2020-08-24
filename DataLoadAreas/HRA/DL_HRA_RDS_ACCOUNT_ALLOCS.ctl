-- *********************************************************************
--
-- Version      Who         Date       Why
-- 1.0          Ian Rowell  25-Sep-09  Initial Creation
--
-- 1.0          V. Shah     13-NOV-09  Change for defect id 2494 fix
--
-- 2.0          V. Shah     09-DEC-09  Change for defect id 2847 fix
-- 2.1          T.Goodley   05-Feb-18  Ensure date only columns only accept date.
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_RDS_ACCOUNT_ALLOCS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LRAAL_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LRAAL_DL_SEQNO                   RECNUM
,LRAAL_DL_LOAD_STATUS             CONSTANT "L"
,LRAAL_RDSA_HA_REFERENCE          CHAR "rtrim(UPPER(:LRAAL_RDSA_HA_REFERENCE))"
,LRAAL_PAR_PER_ALT_REF            CHAR "rtrim(UPPER(:LRAAL_PAR_PER_ALT_REF))"
,LRAAL_RAUD_START_DATE            DATE "DD-MON-YYYY" NULLIF LRAAL_RAUD_START_DATE=blanks
,LRAAL_HRV_DEDT_CODE              CHAR "rtrim(UPPER(:LRAAL_HRV_DEDT_CODE))"
,LRAAL_HRV_RBEG_CODE              CHAR "rtrim(UPPER(:LRAAL_HRV_RBEG_CODE))"
,LRAAL_RACD_PAY_REF               CHAR "rtrim(UPPER(:LRAAL_RACD_PAY_REF))"
,LRAAL_RACD_START_DATE            DATE "DD-MON-YYYY" NULLIF LRAAL_RACD_START_DATE=blanks
,LRAAL_RACD_RADT_CODE             CHAR "rtrim(UPPER(:LRAAL_RACD_RADT_CODE))"
,LRAAL_RDAL_EFFECTIVE_DATE        DATE "DD-MON-YYYY" NULLIF LRAAL_RDAL_EFFECTIVE_DATE=blanks
,LRAAL_REQUESTED_AMOUNT           CHAR "rtrim(:LRAAL_REQUESTED_AMOUNT)"
,LRAAL_FIXED_AMOUNT_IND           CHAR "rtrim(UPPER(:LRAAL_FIXED_AMOUNT_IND))"
,LRAAL_ALLOCATED_AMOUNT           CHAR "rtrim(:LRAAL_ALLOCATED_AMOUNT)"
,LRAAL_PRIORITY                   CHAR "rtrim(UPPER(:LRAAL_PRIORITY))"
,LRAAL_REQUESTED_PERCENTAGE       CHAR "rtrim(:LRAAL_REQUESTED_PERCENTAGE)"
,LRAAL_RACD_REQUESTED_AMOUNT      CHAR "rtrim(:LRAAL_RACD_REQUESTED_AMOUNT)"
,LRAAL_RDAL_DEDUCT_ACTION_TYPE    CHAR "rtrim(:LRAAL_RDAL_DEDUCT_ACTION_TYPE)"
,LRAAL_RDIN_EFFECTIVE_DATE        DATE "DD-MON-YYYY" NULLIF LRAAL_RDIN_EFFECTIVE_DATE=blanks
)

