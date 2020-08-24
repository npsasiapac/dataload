-- *********************************************************************
--
-- Version      Who         Date       Why
-- 01.00        Ian Rowell  25-Sep-09  Initial Creation
-- 01.01        T.Goodley   05-Feb-18  Ensure date only columns only accept date.
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_RDS_PYI530
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LP530_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LP530_DL_SEQNO                   RECNUM
,LP530_DL_LOAD_STATUS             CONSTANT "L"
,LP530_RDTF_ALT_REF               CHAR "rtrim(UPPER(:LP530_RDTF_ALT_REF))"
,LP530_HRV_RBEG_CODE              CHAR "rtrim(UPPER(:LP530_HRV_RBEG_CODE))"
,LP530_TIMESTAMP                  DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LP530_TIMESTAMP=blanks
,LP530_ENVIRONMENT_ID             CHAR "rtrim(UPPER(:LP530_ENVIRONMENT_ID))"
,LP530_DEDUCTION_ACTION_TYPE      CONSTANT "CST"
,LP530_COSTING_DATE               DATE "DD-MON-YYYY" NULLIF LP530_COSTING_DATE=blanks
,LP530_SERVICE_TYPE               CHAR "rtrim(UPPER(:LP530_SERVICE_TYPE))"
,LP530_UNIT_COUNT                 CHAR "rtrim(UPPER(:LP530_UNIT_COUNT))"
,LP530_UNIT_COST                  CHAR "rtrim(UPPER(:LP530_UNIT_COST))"
,LP530_TOTAL_COST                 CHAR "rtrim(UPPER(:LP530_TOTAL_COST))"
,LP530_REFNO                      INTEGER EXTERNAL "rint_seq.nextval"
)



