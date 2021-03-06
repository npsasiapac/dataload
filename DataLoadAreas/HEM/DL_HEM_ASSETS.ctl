load data
APPEND
into table DL_HEM_ASSETS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LASSE_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LASSE_DL_SEQNO                   RECNUM
,LASSE_DL_LOAD_STATUS             CONSTANT "L"
,LASSE_INH_LEGACY_REF             CHAR "rtrim(UPPER(:LASSE_INH_LEGACY_REF))"
,LASSE_ASCO_CODE                  CHAR "rtrim(UPPER(:LASSE_ASCO_CODE))"
,LASSE_AMOUNT                     CHAR "rtrim(UPPER(:LASSE_AMOUNT))"
,LASSE_PERCENTAGE_OWNED           CHAR "rtrim(UPPER(:LASSE_PERCENTAGE_OWNED))"
,LASSE_CREATED_BY                 CHAR "rtrim(UPPER(:LASSE_CREATED_BY))"
,LASSE_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LASSE_CREATED_DATE=blanks
,LASSE_ASSESSMENT_DATE            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LASSE_ASSESSMENT_DATE=blanks
,LASSE_COMMENTS                   CHAR(2000) "rtrim(:LASSE_COMMENTS)"
,LASSE_MODIFIED_BY                CHAR "rtrim(UPPER(:LASSE_MODIFIED_BY))"
,LASSE_MODIFIED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LASSE_MODIFIED_DATE=blanks
,LASSE_ANNUAL_INCOME              CHAR "rtrim(UPPER(:LASSE_ANNUAL_INCOME))"
,LASSE_REFNO                      INTEGER EXTERNAL "asse_refno_seq.nextval"
)
