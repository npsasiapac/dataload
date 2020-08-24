-- *********************************************************************
--
-- Version      Who         Date       Why
--
-- 1.0          Ian Rowell  25-Sep-09  Initial Creation
--
-- 2.0          V Shah      10-APR-10  Defect id 4162 fix. Added 
--                                     LP500_RAUD_START_DATE
-- 2.1          T.Goodley   05-Feb-18  Ensure date only columns only accept date.
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_RDS_PYI500
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LP500_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LP500_DL_SEQNO                   RECNUM
,LP500_DL_LOAD_STATUS             CONSTANT "L"
,LP500_RDTF_ALT_REF               CHAR "rtrim(UPPER(:LP500_RDTF_ALT_REF))"
,LP500_RDSA_HA_REFERENCE          CHAR "rtrim(UPPER(:LP500_RDSA_HA_REFERENCE))"
,LP500_RAUD_START_DATE            DATE "DD-MON-YYYY" NULLIF LP500_RAUD_START_DATE=blanks
,LP500_HRV_DEDT_CODE              CHAR "rtrim(UPPER(:LP500_HRV_DEDT_CODE))"
,LP500_HRV_RBEG_CODE              CHAR "rtrim(UPPER(:LP500_HRV_RBEG_CODE))"
,LP500_CRN                        CHAR "rtrim(UPPER(:LP500_CRN))"
,LP500_EXT_REF_ID                 CHAR "rtrim(UPPER(:LP500_EXT_REF_ID))"
,LP500_SOURCE_TRAN_REF            CHAR "rtrim(UPPER(:LP500_SOURCE_TRAN_REF))"
,LP500_TIMESTAMP                  DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LP500_TIMESTAMP=blanks
,LP500_INSTRUCTION_AMOUNT         CHAR "rtrim(UPPER(:LP500_INSTRUCTION_AMOUNT))"
,LP500_ENVIRONMENT_ID             CHAR "rtrim(UPPER(:LP500_ENVIRONMENT_ID))"
,LP500_ERROR_CODE                 CHAR "rtrim(UPPER(:LP500_ERROR_CODE))"
,LP500_ERROR_MESSAGE              CHAR "rtrim(UPPER(:LP500_ERROR_MESSAGE))"
,LP500_DEDUCTION_ACTION           CONSTANT "REJ"
,LP500_REFNO                      INTEGER EXTERNAL "rint_seq.nextval"
)

