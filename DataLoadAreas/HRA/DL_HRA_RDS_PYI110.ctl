-- *********************************************************************
--
-- Version      Who         Date       Why
--
-- 1.0          Ian Rowell  25-Sep-09  Initial Creation
--
-- 2.0          V Shah      10-APR-10  Defect id 4162 fix. Added 
--                                     LP110_RAUD_START_DATE
-- 2.1          T.Goodley   05-Feb-18  Ensure date only columns only accept date.
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_RDS_PYI110
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LP110_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LP110_DL_SEQNO                   RECNUM
,LP110_DL_LOAD_STATUS             CONSTANT "L"
,LP110_RDTF_ALT_REF               CHAR "rtrim(UPPER(:LP110_RDTF_ALT_REF))"
,LP110_RDSA_HA_REFERENCE          CHAR "rtrim(UPPER(:LP110_RDSA_HA_REFERENCE))"
,LP110_RAUD_START_DATE            DATE "DD-MON-YYYY" NULLIF LP110_RAUD_START_DATE=blanks
,LP110_HRV_DEDT_CODE              CHAR "rtrim(UPPER(:LP110_HRV_DEDT_CODE))"
,LP110_HRV_RBEG_CODE              CHAR "rtrim(UPPER(:LP110_HRV_RBEG_CODE))"
,LP110_TRANS_REF                  CHAR "rtrim(UPPER(:LP110_TRANS_REF))"
,LP110_CRN                        CHAR "rtrim(UPPER(:LP110_CRN))"
,LP110_TIMESTAMP                  DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LP110_TIMESTAMP=blanks
,LP110_REQUEST_ACTION_CODE        CHAR "rtrim(UPPER(:LP110_REQUEST_ACTION_CODE))"
,LP110_INSTRUCTION_AMOUNT         CHAR "rtrim(UPPER(:LP110_INSTRUCTION_AMOUNT))"
,LP110_TP_ID                      CHAR "rtrim(UPPER(:LP110_TP_ID))"
,LP110_START_DATE                 DATE "DD-MON-YYYY" NULLIF LP110_START_DATE=blanks
,LP110_END_DATE                   DATE "DD-MON-YYYY" NULLIF LP110_END_DATE=blanks
)

