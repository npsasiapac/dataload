load data
APPEND
into table DL_HEM_ICS_DEDUCTIONS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LICD_DLB_BATCH_ID               CONSTANT "$BATCH_NO",
 LICD_DL_SEQNO                   RECNUM,
 LICD_DL_LOAD_STATUS             CONSTANT "L",
 LICD_INDR_REFERENCE             CHAR "rtrim(UPPER(:LICD_INDR_REFERENCE))",
 LICD_IBP_HRV_IBEN_CODE          CHAR "rtrim(UPPER(:LICD_IBP_HRV_IBEN_CODE))",
 LICD_IBP_HRV_IPS_CODE           CHAR "rtrim(UPPER(:LICD_IBP_HRV_IPS_CODE))",
 LICD_IBP_HRV_ICPT_CODE          CHAR "rtrim(UPPER(:LICD_IBP_HRV_ICPT_CODE))",
 LICD_TYPE                       CHAR "rtrim(UPPER(:LICD_TYPE))",
 LICD_AMOUNT                     CHAR "rtrim(UPPER(:LICD_AMOUNT))",
 LICD_HRV_ICDT_CODE              CHAR "rtrim(UPPER(:LICD_HRV_ICDT_CODE))",
 LICD_CREATED_BY                 CHAR "rtrim(UPPER(:LICD_CREATED_BY))",
 LICD_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LICD_CREATED_DATE=blanks,
 LICD_DEDUCTION_DATE             DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LICD_DEDUCTION_DATE=blanks,
 LICD_REFNO                      INTEGER EXTERNAL "icd_refno_seq.nextval"
)
