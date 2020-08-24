-- *********************************************************************
--
-- Version      Who         Date       Why
--
-- 1.0          Ian Rowell  25-Sep-09  Initial Creation
--
-- 2.0          V Shah      10-APR-10  Defect id 4162 fix. Added 
--                                     LP520_RAUD_START_DATE
-- 2.1          T.Goodley   05-Feb-18  Ensure date only columns only accept date.
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_RDS_PYI520
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LP520_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LP520_DL_SEQNO                   RECNUM
,LP520_DL_LOAD_STATUS             CONSTANT "L"
,LP520_RDTF_ALT_REF               CHAR "rtrim(UPPER(:LP520_RDTF_ALT_REF))"
,LP520_RDSA_HA_REFERENCE          CHAR "rtrim(UPPER(:LP520_RDSA_HA_REFERENCE))"
,LP520_RAUD_START_DATE            DATE "DD-MON-YYYY" NULLIF LP520_RAUD_START_DATE=blanks
,LP520_HRV_DEDT_CODE              CHAR "rtrim(UPPER(:LP520_HRV_DEDT_CODE))"
,LP520_HRV_RBEG_CODE              CHAR "rtrim(UPPER(:LP520_HRV_RBEG_CODE))"
,LP520_CRN                        CHAR "rtrim(UPPER(:LP520_CRN))"
,LP520_EXT_REF_ID                 CHAR "rtrim(UPPER(:LP520_EXT_REF_ID))"
,LP520_SOURCE_TRAN_REF            CHAR "rtrim(UPPER(:LP520_SOURCE_TRAN_REF))"
,LP520_TIMESTAMP                  DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LP520_TIMESTAMP=blanks
,LP520_INSTRUCTION_AMOUNT         CHAR "rtrim(UPPER(:LP520_INSTRUCTION_AMOUNT))"
,LP520_ENVIRONMENT_ID             CHAR "rtrim(UPPER(:LP520_ENVIRONMENT_ID))"
,LP520_DEDUCTION_ACTION_TYPE      CONSTANT "PAY"
,LP520_PAY_DATE                   DATE "DD-MON-YYYY" NULLIF LP520_PAY_DATE=blanks
,LP520_PAYMENT_STRIP              CHAR "rtrim(UPPER(:LP520_PAYMENT_STRIP))"
,LP520_PAY_AMOUNT                 CHAR "rtrim(UPPER(:LP520_PAY_AMOUNT))"
,LP520_BSB_NUMBER                 CHAR "rtrim(UPPER(:LP520_BSB_NUMBER))"
,LP520_ACCOUNT_NUMBER             CHAR "rtrim(UPPER(:LP520_ACCOUNT_NUMBER))"
,LP520_PAYMENT_STATUS_CODE        CHAR "rtrim(UPPER(:LP520_PAYMENT_STATUS_CODE))"
,LP520_CUSTOMER_NAME              CHAR "rtrim(UPPER(:LP520_CUSTOMER_NAME))"
,LP520_TRANS_REF                  CHAR "rtrim(UPPER(:LP520_TRANS_REF))"
,LP520_LODGEMENT_PREFIX           CHAR "rtrim(UPPER(:LP520_LODGEMENT_PREFIX))"
,LP520_REFNO                      INTEGER EXTERNAL "rint_seq.nextval"
)

