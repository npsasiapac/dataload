-- *********************************************************************
--
-- Version      Who         Date       Why
--
-- 1.0          Ian Rowell  25-Sep-09  Initial Creation
--
-- 2.0          V Shah      10-APR-10  Defect id 4162 fix. Added 
--                                     LP513_RAUD_START_DATE
-- 2.1          T.Goodley   05-Feb-18  Ensure date only columns only accept date.
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_RDS_PYI513
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LP513_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LP513_DL_SEQNO                   RECNUM
,LP513_DL_LOAD_STATUS             CONSTANT "L"
,LP513_RDTF_ALT_REF               CHAR "rtrim(UPPER(:LP513_RDTF_ALT_REF))"
,LP513_RDSA_HA_REFERENCE          CHAR "rtrim(UPPER(:LP513_RDSA_HA_REFERENCE))"
,LP513_RAUD_START_DATE            DATE "DD-MON-YYYY" NULLIF LP513_RAUD_START_DATE=blanks
,LP513_HRV_DEDT_CODE              CHAR "rtrim(UPPER(:LP513_HRV_DEDT_CODE))"
,LP513_HRV_RBEG_CODE              CHAR "rtrim(UPPER(:LP513_HRV_RBEG_CODE))"
,LP513_CRN                        CHAR "rtrim(UPPER(:LP513_CRN))"
,LP513_EXT_REF_ID                 CHAR "rtrim(UPPER(:LP513_EXT_REF_ID))"
,LP513_TIMESTAMP                  DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LP513_TIMESTAMP=blanks
,LP513_ENVIRONMENT_ID             CHAR "rtrim(UPPER(:LP513_ENVIRONMENT_ID))"
,LP513_DEDUCTION_ACTION_TYPE      CONSTANT "PPC"
,LP513_EFFECTIVE_DATE             DATE "DD-MON-YYYY" NULLIF LP513_EFFECTIVE_DATE=blanks
,LP513_REFNO                      INTEGER EXTERNAL "rint_seq.nextval"
)

