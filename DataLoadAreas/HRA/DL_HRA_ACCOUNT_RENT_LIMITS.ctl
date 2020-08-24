-- *********************************************************************
--
-- Version      Who         Date       Why
-- 1.0          Ian Rowell  25-Sep-09  Initial Creation
--
-- 2.0          V.Shah      03-DEC-09  Fix for Defect id 2703 - Rename 
--                                     LARLI_TCY_ALT_REF to LARLI_REFERENCE
--
-- 3.0          V.Shah      09-JUL-10  Fix for Defect id 4731 - Remove 
--                                     LARLI_REFNO external population. This 
--                                     will now be done in the VALIDATE process
-- 4.0          P.Hearty    30-JUN-10  Added in ARLI_REFNO
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_ACCOUNT_RENT_LIMITS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LARLI_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LARLI_DL_SEQNO                   RECNUM
,LARLI_DL_LOAD_STATUS             CONSTANT "L"
,LARLI_SURV_LEGACY_REF            CHAR "rtrim(UPPER(:LARLI_SURV_LEGACY_REF))"
,LARLI_REFERENCE                  CHAR "rtrim(UPPER(:LARLI_REFERENCE))"
,LARLI_SURV_EFFECTIVE_DATE        DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LARLI_SURV_EFFECTIVE_DATE=blanks
,LARLI_RLTY_CODE                  CHAR "rtrim(UPPER(:LARLI_RLTY_CODE))"
,LARLI_START_DATE                 DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LARLI_START_DATE=blanks
,LARLI_AMOUNT                     CHAR "rtrim(UPPER(:LARLI_AMOUNT))"
,LARLI_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LARLI_CREATED_DATE=blanks
,LARLI_CREATED_BY                 CHAR "rtrim(UPPER(:LARLI_CREATED_BY))"
,LARLI_END_DATE                   DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LARLI_END_DATE=blanks
,LARLI_REFNO                      INTEGER EXTERNAL "arli_refno_seq.nextval"
)

