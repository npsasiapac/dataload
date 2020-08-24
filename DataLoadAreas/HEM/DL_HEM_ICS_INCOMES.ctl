load data
APPEND
into table DL_HEM_ICS_INCOMES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LINC_DLB_BATCH_ID               CONSTANT "$BATCH_NO",
 LINC_DL_SEQNO                   RECNUM,
 LINC_DL_LOAD_STATUS             CONSTANT "L",
 LINC_INDR_REFERENCE             CHAR "rtrim(UPPER(:LINC_INDR_REFERENCE))",
 LINC_TYPE                       CHAR "rtrim(UPPER(:LINC_TYPE))",
 LINC_HRV_IITY_CODE              CHAR "rtrim(UPPER(:LINC_HRV_IITY_CODE))",
 LINC_AMOUNT                     CHAR "rtrim(UPPER(:LINC_AMOUNT))",
 LINC_INCOME_DATE                DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LINC_INCOME_DATE=blanks,
 LINC_CREATED_BY                 CHAR "rtrim(UPPER(:LINC_CREATED_BY))",
 LINC_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LINC_CREATED_DATE=blanks,
 LINC_INF_CODE                   CHAR "rtrim(UPPER(:LINC_INF_CODE))",
 LINC_REFNO                      INTEGER EXTERNAL "inc_refno_seq.nextval"
)
