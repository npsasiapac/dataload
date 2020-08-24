-- *********************************************************************
--
-- Version      Who         Date       Why
--
-- 1.0          Ian Rowell  25-Sep-09  Initial Creation
--
-- 2.0          V Shah      10-APR-10  Defect id 4162 fix. Added 
--                                     LP100_RAUD_START_DATE
-- 2.1          T.Goodley   05-Feb-18  Ensure date only columns only accept date.
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_RDS_PYI100
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LP100_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LP100_DL_SEQNO                   RECNUM
,LP100_DL_LOAD_STATUS             CONSTANT "L"
,LP100_RDTF_ALT_REF               CHAR "rtrim(UPPER(:LP100_RDTF_ALT_REF))"
,LP100_RDSA_HA_REFERENCE          CHAR "rtrim(UPPER(:LP100_RDSA_HA_REFERENCE))"
,LP100_RAUD_START_DATE            DATE "DD-MON-YYYY" NULLIF LP100_RAUD_START_DATE=blanks
,LP100_HRV_DEDT_CODE              CHAR "rtrim(UPPER(:LP100_HRV_DEDT_CODE))"
,LP100_HRV_RBEG_CODE              CHAR "rtrim(UPPER(:LP100_HRV_RBEG_CODE))"
,LP100_CUSTOMER_BIRTH_DATE        DATE "DD-MON-YYYY" NULLIF LP100_CUSTOMER_BIRTH_DATE=blanks
,LP100_CUSTOMER_SURNAME           CHAR "rtrim(UPPER(:LP100_CUSTOMER_SURNAME))"
,LP100_CUSTOMER_POSTCODE          CHAR "rtrim(UPPER(:LP100_CUSTOMER_POSTCODE))"
,LP100_TRANS_REF                  CHAR "rtrim(UPPER(:LP100_TRANS_REF))"
,LP100_CRN                        CHAR "rtrim(UPPER(:LP100_CRN))"
,LP100_TIMESTAMP                  DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LP100_TIMESTAMP=blanks
,LP100_REQUEST_ACTION_CODE        CONSTANT "NEW"
,LP100_INSTRUCTION_AMOUNT         CHAR "rtrim(UPPER(:LP100_INSTRUCTION_AMOUNT))"
,LP100_TP_ID                      CHAR "rtrim(UPPER(:LP100_TP_ID))"
,LP100_START_DATE                 DATE "DD-MON-YYYY" NULLIF LP100_START_DATE=blanks
,LP100_END_DATE                   DATE "DD-MON-YYYY" NULLIF LP100_END_DATE=blanks
)

