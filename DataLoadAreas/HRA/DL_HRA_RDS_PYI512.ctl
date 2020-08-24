-- *********************************************************************
--
-- Version      Who         Date       Why
--
-- 1.0          Ian Rowell  25-Sep-09  Initial Creation
--
-- 2.0          V Shah      10-APR-10  Defect id 4162 fix. Added 
--                                     LP512_RAUD_START_DATE
-- 2.1          T.Goodley   05-Feb-18  Ensure date only columns only accept date.
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_RDS_PYI512
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LP512_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LP512_DL_SEQNO                   RECNUM
,LP512_DL_LOAD_STATUS             CONSTANT "L"
,LP512_RDTF_ALT_REF               CHAR "rtrim(UPPER(:LP512_RDTF_ALT_REF))"
,LP512_RDSA_HA_REFERENCE          CHAR "rtrim(UPPER(:LP512_RDSA_HA_REFERENCE))"
,LP512_RAUD_START_DATE            DATE "DD-MON-YYYY" NULLIF LP512_RAUD_START_DATE=blanks
,LP512_HRV_DEDT_CODE              CHAR "rtrim(UPPER(:LP512_HRV_DEDT_CODE))"
,LP512_HRV_RBEG_CODE              CHAR "rtrim(UPPER(:LP512_HRV_RBEG_CODE))"
,LP512_CRN                        CHAR "rtrim(UPPER(:LP512_CRN))"
,LP512_EXT_REF_ID                 CHAR "rtrim(UPPER(:LP512_EXT_REF_ID))"
,LP512_SOURCE_TRAN_REF            CHAR "rtrim(UPPER(:LP512_SOURCE_TRAN_REF))"
,LP512_TIMESTAMP                  DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LP512_TIMESTAMP=blanks
,LP512_INSTRUCTION_AMOUNT         CHAR "rtrim(UPPER(:LP512_INSTRUCTION_AMOUNT))"
,LP512_ENVIRONMENT_ID             CHAR "rtrim(UPPER(:LP512_ENVIRONMENT_ID))"
,LP512_DEDUCTION_ACTION_TYPE      CHAR "rtrim(UPPER(:LP512_DEDUCTION_ACTION_TYPE))"
,LP512_EFFECTIVE_DATE             DATE "DD-MON-YYYY" NULLIF LP512_EFFECTIVE_DATE=blanks
,LP512_ALLOCATED_AMOUNT           CHAR "rtrim(:LP512_ALLOCATED_AMOUNT)"
,LP512_FUTURE_ACTION_DATE         DATE "DD-MON-YYYY" NULLIF LP512_FUTURE_ACTION_DATE=blanks
,LP512_REFNO                      INTEGER EXTERNAL "rint_seq.nextval"
)

