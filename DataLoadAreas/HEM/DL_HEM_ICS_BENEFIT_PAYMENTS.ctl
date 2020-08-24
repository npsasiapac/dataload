load data
APPEND
into table DL_HEM_ICS_BENEFIT_PAYMENTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LIBP_DLB_BATCH_ID               CONSTANT "$BATCH_NO",
 LIBP_DL_SEQNO                   RECNUM,
 LIBP_DL_LOAD_STATUS             CONSTANT "L",
 LIBP_INDR_REFERENCE             CHAR "rtrim(UPPER(:LIBP_INDR_REFERENCE))",
 LIBP_HRV_IBEN_CODE              CHAR "rtrim(UPPER(:LIBP_HRV_IBEN_CODE))",
 LIBP_HRV_IPS_CODE               CHAR "rtrim(UPPER(:LIBP_HRV_IPS_CODE))",
 LIBP_HRV_ICPT_CODE              CHAR "rtrim(UPPER(:LIBP_HRV_ICPT_CODE))",
 LIBP_TYPE                       CHAR "rtrim(UPPER(:LIBP_TYPE))",
 LIBP_ACTUAL_AMOUNT              CHAR "rtrim(UPPER(:LIBP_ACTUAL_AMOUNT))",
 LIBP_CANCELLED_IND              CHAR "rtrim(UPPER(:LIBP_CANCELLED_IND))",
 LIBP_CREATED_BY                 CHAR "rtrim(UPPER(:LIBP_CREATED_BY))",
 LIBP_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LIBP_CREATED_DATE=blanks,
 LIBP_INF_CODE                   CHAR "rtrim(UPPER(:LIBP_INF_CODE))",
 LIBP_GRANT_DATE                 DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LIBP_GRANT_DATE=blanks,
 LIBP_LEGISLATIVE_AMOUNT         CHAR "rtrim(UPPER(:LIBP_LEGISLATIVE_AMOUNT))",
 LIBP_MAX_RATE_IND               CHAR "rtrim(UPPER(:LIBP_MAX_RATE_IND))",
 LIBP_PAYMENT_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LIBP_PAYMENT_DATE=blanks,
 LIBP_NUM_OF_PAID_DAYS           CHAR "rtrim(UPPER(:LIBP_NUM_OF_PAID_DAYS))",
 LIBP_REFNO                      INTEGER EXTERNAL "ibp_refno_seq.nextval"
)
