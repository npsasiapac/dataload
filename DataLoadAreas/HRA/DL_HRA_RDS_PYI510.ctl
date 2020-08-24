-- *********************************************************************
--
-- Version      Who         Date       Why
--
-- 1.0          Ian Rowell  25-Sep-09  Initial Creation
--
-- 2.0          V Shah      10-APR-10  Defect id 4162 fix. Added 
--                                     LP510_RAUD_START_DATE
-- 2.1          T.Goodley   05-Feb-18  Ensure date only columns only accept date.
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_RDS_PYI510
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LP510_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LP510_DL_SEQNO                   RECNUM
,LP510_DL_LOAD_STATUS             CONSTANT "L"
,LP510_RDTF_ALT_REF               CHAR "rtrim(UPPER(:LP510_RDTF_ALT_REF))"
,LP510_RDSA_HA_REFERENCE          CHAR "rtrim(UPPER(:LP510_RDSA_HA_REFERENCE))"
,LP510_RAUD_START_DATE            DATE "DD-MON-YYYY" NULLIF LP510_RAUD_START_DATE=blanks
,LP510_HRV_DEDT_CODE              CHAR "rtrim(UPPER(:LP510_HRV_DEDT_CODE))"
,LP510_HRV_RBEG_CODE              CHAR "rtrim(UPPER(:LP510_HRV_RBEG_CODE))"
,LP510_CRN                        CHAR "rtrim(UPPER(:LP510_CRN))"
,LP510_EXT_REF_ID                 CHAR "rtrim(UPPER(:LP510_EXT_REF_ID))"
,LP510_SOURCE_TRAN_REF            CHAR "rtrim(UPPER(:LP510_SOURCE_TRAN_REF))"
,LP510_TIMESTAMP                  DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LP510_TIMESTAMP=blanks
,LP510_INSTRUCTION_AMOUNT         CHAR "rtrim(UPPER(:LP510_INSTRUCTION_AMOUNT))"
,LP510_ENVIRONMENT_ID             CHAR "rtrim(UPPER(:LP510_ENVIRONMENT_ID))"
,LP510_DEDUCTION_ACTION_TYPE      CHAR "rtrim(UPPER(:LP510_DEDUCTION_ACTION_TYPE))"
,LP510_EFFECTIVE_DATE             DATE "DD-MON-YYYY" NULLIF LP510_EFFECTIVE_DATE=blanks
,LP510_REFNO                      INTEGER EXTERNAL "rint_seq.nextval"
)

