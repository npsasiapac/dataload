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
into table DL_HRA_RDS_PYI540
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LP540_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LP540_DL_SEQNO                   RECNUM
,LP540_DL_LOAD_STATUS             CONSTANT "L"
,LP540_RDTF_ALT_REF               CHAR "rtrim(UPPER(:LP540_RDTF_ALT_REF))"
,LP540_TIMESTAMP                  DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LP540_TIMESTAMP=blanks
,LP540_DEDUCTION_ACTION_TYPE      CONSTANT "REC"
,LP540_ENVIRONMENT_ID             CHAR "rtrim(UPPER(:LP540_ENVIRONMENT_ID))"
,LP540_BENEFIT_GROUP              CHAR "rtrim(UPPER(:LP540_BENEFIT_GROUP))"
,LP540_PAY_DATE                   DATE "DD-MON-YYYY" NULLIF LP540_PAY_DATE=blanks
,LP540_TOTAL_COUNT                CHAR "rtrim(UPPER(:LP540_TOTAL_COUNT))"
,LP540_TOTAL_COST                 CHAR "rtrim(UPPER(:LP540_TOTAL_COST))"
,LP540_TOTAL_AMOUNT_PAID          CHAR "rtrim(UPPER(:LP540_TOTAL_AMOUNT_PAID))"
,LP540_BANK_FAX_AMOUNT            CHAR "rtrim(UPPER(:LP540_BANK_FAX_AMOUNT))"
,LP540_FAX_INPUT_DATETIME         DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LP540_FAX_INPUT_DATETIME=blanks
,LP540_FAX_INPUT_BY               CHAR "rtrim(UPPER(:LP540_FAX_INPUT_BY))"
,LP540_REC_ORIDE_IND              CHAR "rtrim(UPPER(:LP540_REC_ORIDE_IND))"
,LP540_REC_ORIDE_DATETIME         DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LP540_REC_ORIDE_DATETIME=blanks
,LP540_REC_ORIDE_BY               CHAR "rtrim(UPPER(:LP540_REC_ORIDE_BY))"
,LP540_REFNO                      INTEGER EXTERNAL "rint_seq.nextval"
)







