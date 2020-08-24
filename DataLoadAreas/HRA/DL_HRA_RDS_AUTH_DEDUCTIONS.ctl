-- *********************************************************************
--
-- Version      Who         Date       Why
-- 01.00        Ian Rowell  25-Sep-09  Initial Creation
-- 01.01        T.Goodley   05-Feb-18  Ensure date only columns only accept date.
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_RDS_AUTH_DEDUCTIONS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LRAUD_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LRAUD_DL_SEQNO                   RECNUM
,LRAUD_DL_LOAD_STATUS             CONSTANT "L"
,LRAUD_RDSA_HA_REFERENCE          CHAR "rtrim(UPPER(:LRAUD_RDSA_HA_REFERENCE))"
,LRAUD_START_DATE                 DATE "DD-MON-YYYY" NULLIF LRAUD_START_DATE=blanks
,LRAUD_HRV_DEDT_CODE              CHAR "rtrim(UPPER(:LRAUD_HRV_DEDT_CODE))"
,LRAUD_CURRENT_SCO_CODE           CHAR "rtrim(UPPER(:LRAUD_CURRENT_SCO_CODE))"
,LRAUD_STATUS_DATE                DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRAUD_STATUS_DATE=blanks
,LRAUD_CREATED_BY                 CHAR "rtrim(UPPER(:LRAUD_CREATED_BY))"
,LRAUD_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRAUD_CREATED_DATE=blanks
,LRAUD_HRV_RBEG_CODE              CHAR "rtrim(UPPER(:LRAUD_HRV_RBEG_CODE))"
,LRAUD_PENDING_SCO_CODE           CHAR "rtrim(UPPER(:LRAUD_PENDING_SCO_CODE))"
,LRAUD_END_DATE                   DATE "DD-MON-YYYY" NULLIF LRAUD_END_DATE=blanks
,LRAUD_SUSPEND_FROM_DATE          DATE "DD-MON-YYYY" NULLIF LRAUD_SUSPEND_FROM_DATE=blanks
,LRAUD_SUSPEND_TO_DATE            DATE "DD-MON-YYYY" NULLIF LRAUD_SUSPEND_TO_DATE=blanks
,LRAUD_ACTION_SENT_DATETIME       DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRAUD_ACTION_SENT_DATETIME=blanks
,LRAUD_NEXT_PAY_DATE              DATE "DD-MON-YYYY" NULLIF LRAUD_NEXT_PAY_DATE=blanks
,LRAUD_HRV_SUSR_CODE              CHAR "rtrim(UPPER(:LRAUD_HRV_SUSR_CODE))"
,LRAUD_HRV_TERR_CODE              CHAR "rtrim(UPPER(:LRAUD_HRV_TERR_CODE))"
,LRAUD_REFNO                      INTEGER EXTERNAL "raud_seq.nextval"
)


