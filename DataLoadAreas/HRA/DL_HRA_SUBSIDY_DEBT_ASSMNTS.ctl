-- *********************************************************************
--
-- Version      Who         Date       Why
-- 1.0          P Hearty    01-JUL-11  Initial Creation
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_SUBSIDY_DEBT_ASSMNTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LSUDA_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LSUDA_DL_SEQNO                   RECNUM
,LSUDA_DL_LOAD_STATUS             CONSTANT "L"
,LSUDA_LEGACY_REF                 CHAR "rtrim(UPPER(:LSUDA_LEGACY_REF))"
,LSUDA_SUAP_LEGACY_REF            CHAR "rtrim(UPPER(:LSUDA_SUAP_LEGACY_REF))"
,LSUDA_START_DATE                 DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSUDA_START_DATE=blanks
,LSUDA_SCO_CODE                   CHAR "rtrim(UPPER(:LSUDA_SCO_CODE))"
,LSUDA_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSUDA_CREATED_DATE=blanks
,LSUDA_CREATED_BY                 CHAR "rtrim(UPPER(:LSUDA_CREATED_BY))"
,LSUDA_PAY_REF                    CHAR "rtrim(UPPER(:LSUDA_PAY_REF))"
,LSUDA_COMMENTS                   CHAR(4000) "rtrim(:LSUDA_COMMENTS)"
,LSUDA_TOTAL_DEBT                 CHAR "rtrim(:LSUDA_TOTAL_DEBT)"
,LSUDA_TOTAL_ACCRUED_DEBT         CHAR "rtrim(:LSUDA_TOTAL_ACCRUED_DEBT)"
,LSUDA_TOTAL_NON_ACCRUED_DEBT     CHAR "rtrim(:LSUDA_TOTAL_NON_ACCRUED_DEBT)"
,LSUDA_CALCULATED_DATE            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSUDA_CALCULATED_DATE=blanks
,LSUDA_CALCULATED_BY              CHAR "rtrim(UPPER(:LSUDA_CALCULATED_BY))"
,LSUDA_ESTABLISHED_DATE           DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSUDA_ESTABLISHED_DATE=blanks
,LSUDA_ESTABLISHED_BY             CHAR "rtrim(UPPER(:LSUDA_ESTABLISHED_BY))"
,LSUDA_SDAR_CODE                  CHAR "rtrim(UPPER(:LSUDA_SDAR_CODE))"
,LSUDA_SDWR_CODE                  CHAR "rtrim(UPPER(:LSUDA_SDWR_CODE))"
,LSUDA_REFNO                      INTEGER EXTERNAL "SUDA_REFNO_SEQ.nextval"
)

