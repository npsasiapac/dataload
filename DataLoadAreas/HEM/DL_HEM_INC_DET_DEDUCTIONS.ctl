load data
APPEND
into table DL_HEM_INC_DET_DEDUCTIONS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LINDD_DLB_BATCH_ID               CONSTANT "$BATCH_NO",
 LINDD_DL_SEQNO                   RECNUM,
 LINDD_DL_LOAD_STATUS             CONSTANT "L",
 LINDD_INDT_LEGACY_REF            CHAR "rtrim(UPPER(:LINDD_INDT_LEGACY_REF))",
 LINDD_INCO_CODE                  CHAR "rtrim(UPPER(:LINDD_INCO_CODE))",
 LINDD_HRV_DDCO_CODE              CHAR "rtrim(UPPER(:LINDD_HRV_DDCO_CODE))",
 LINDD_AMOUNT                     CHAR "rtrim(UPPER(:LINDD_AMOUNT))",
 LINDD_REGULAR_AMOUNT             CHAR "rtrim(UPPER(:LINDD_REGULAR_AMOUNT))",
 LINDD_CREATED_BY                 CHAR "rtrim(UPPER(:LINDD_CREATED_BY))",
 LINDD_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LINDD_CREATED_DATE=blanks,
 LINDD_COMMENTS                   CHAR(2000) "rtrim(UPPER(:LINDD_COMMENTS))",
 LINDD_MODIFIED_BY                CHAR "rtrim(UPPER(:LINDD_MODIFIED_BY))",
 LINDD_MODIFIED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LINDD_MODIFIED_DATE=blanks
)
