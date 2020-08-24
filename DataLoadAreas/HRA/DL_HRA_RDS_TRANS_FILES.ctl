-- *********************************************************************
--
-- Version      Who         Date       Why
-- 01.00        Ian Rowell  25-Sep-09  Initial Creation
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_RDS_TRANS_FILES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LRDTF_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LRDTF_DL_SEQNO                   RECNUM
,LRDTF_DL_LOAD_STATUS             CONSTANT "L"
,LRDTF_ALT_REF                    CHAR "rtrim(UPPER(:LRDTF_ALT_REF))"
,LRDTF_HRV_RPAG_CODE              CHAR "rtrim(UPPER(:LRDTF_HRV_RPAG_CODE))"
,LRDTF_TYPE                       CHAR "rtrim(UPPER(:LRDTF_TYPE))"
,LRDTF_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRDTF_CREATED_DATE=blanks
,LRDTF_CREATED_BY                 CHAR "rtrim(UPPER(:LRDTF_CREATED_BY))"
,LRDTF_SCO_CODE                   CHAR "rtrim(UPPER(:LRDTF_SCO_CODE))"
,LRDTF_FILE_NUMBER                CHAR "rtrim(UPPER(:LRDTF_FILE_NUMBER))"
,LRDTF_TIMESTAMP                  DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRDTF_TIMESTAMP=blanks
,LRDTF_SENDING_AGENCY             CHAR "rtrim(UPPER(:LRDTF_SENDING_AGENCY))"
,LRDTF_RECEIVING_AGENCY           CHAR "rtrim(UPPER(:LRDTF_RECEIVING_AGENCY))"
,LRDTF_PROCESSED_DATETIME         DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRDTF_PROCESSED_DATETIME=blanks
,LRDTF_TRANSACTION_COUNT          CHAR "rtrim(UPPER(:LRDTF_TRANSACTION_COUNT))"
,LRDTF_REC_AUTHD_DATETIME         DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRDTF_REC_AUTHD_DATETIME=blanks
,LRDTF_REC_AUTHD_BY               CHAR "rtrim(UPPER(:LRDTF_REC_AUTHD_BY))"
,LRDTF_REFNO                      INTEGER EXTERNAL "rdtf_refno_seq.nextval"
)





