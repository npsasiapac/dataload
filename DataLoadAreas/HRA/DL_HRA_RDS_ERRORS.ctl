-- *********************************************************************
--
-- Version      Who         Date       Why
-- 01.00        Ian Rowell  25-Sep-09  Initial Creation
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_RDS_ERRORS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LRERR_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LRERR_DL_SEQNO                   RECNUM
,LRERR_DL_LOAD_STATUS             CONSTANT "L"
,LRERR_RDTF_ALT_REF               CHAR "rtrim(UPPER(:LRERR_RDTF_ALT_REF))"
,LRERR_SOURCE_TRAN_REF            CHAR "rtrim(UPPER(:LRERR_SOURCE_TRAN_REF))"
,LRERR_TRANS_CODE                 CHAR "rtrim(UPPER(:LRERR_TRANS_CODE))"
,LRERR_EDX_ERROR_CODE             CHAR "rtrim(UPPER(:LRERR_EDX_ERROR_CODE))"
,LRERR_TIMESTAMP                  DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRERR_TIMESTAMP=blanks
,LRERR_CRN                        CHAR "rtrim(UPPER(:LRERR_CRN))"
,LRERR_EXT_REF_ID                 CHAR "rtrim(UPPER(:LRERR_EXT_REF_ID))"
,LRERR_DATA_LENGTH                CHAR "rtrim(UPPER(:LRERR_DATA_LENGTH))"
,LRERR_TRANSACTION_DATA           CHAR "rtrim(UPPER(:LRERR_TRANSACTION_DATA))"
,LRERR_REFNO                      INTEGER EXTERNAL "rerr_seq.nextval"
)



