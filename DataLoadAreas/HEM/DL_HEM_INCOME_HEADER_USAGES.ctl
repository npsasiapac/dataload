load data
APPEND
into table DL_HEM_INCOME_HEADER_USAGES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LIHU_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LIHU_DL_SEQNO                   RECNUM
,LIHU_DL_LOAD_STATUS             CONSTANT "L"
,LIHU_INH_LEGACY_REF             CHAR "rtrim(UPPER(:LIHU_INH_LEGACY_REF))"
,LIHU_CREATED_BY                 CHAR "rtrim(UPPER(:LIHU_CREATED_BY))"
,LIHU_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LIHU_CREATED_DATE=blanks
,LIHU_REFERENCE_TYPE             CHAR "rtrim(UPPER(:LIHU_REFERENCE_TYPE))"
,LIHU_REFERENCE_VALUE            CHAR "rtrim(UPPER(:LIHU_REFERENCE_VALUE))"
,LIHU_REFNO                      INTEGER EXTERNAL "IHU_REFNO_SEQ.NEXTVAL"
)
