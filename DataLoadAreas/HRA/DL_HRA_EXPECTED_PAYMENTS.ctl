load data
APPEND
into table DL_HRA_EXPECTED_PAYMENTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
 LEXPA_DLB_BATCH_ID                 CONSTANT "$batch_no"
,LEXPA_DL_SEQNO                     RECNUM
,LEXPA_DL_LOAD_STATUS               CONSTANT "L"
,LEXPA_TYPE                         CHAR "rtrim(:LEXPA_TYPE)"
,LEXPA_RAC_PAY_REF                  CHAR "rtrim(:LEXPA_RAC_PAY_REF)"
,LEXPA_COMMENT                      CHAR "rtrim(:LEXPA_COMMENT)"
,LEXPA_PAYMENT_AMOUNT               CHAR NULLIF LEXPA_PAYMENT_AMOUNT=blanks "TO_NUMBER(:LEXPA_PAYMENT_AMOUNT)"
,LEXPA_PAYMENT_DUE_DATE             DATE "DD-MON-YYYY" NULLIF LEXPA_PAYMENT_DUE_DATE=blanks
,LEXPA_PAYMENT_OVERDUE_DATE         DATE "DD-MON-YYYY" NULLIF LEXPA_PAYMENT_OVERDUE_DATE=blanks
,LEXPA_UNPAID_BALANCE               CHAR NULLIF LEXPA_UNPAID_BALANCE=blanks "TO_NUMBER(:LEXPA_UNPAID_BALANCE)"
,LEXPA_EXTRACTED_IND                CHAR "rtrim(:LEXPA_EXTRACTED_IND)"
,LEXPA_EXTRACTED_DATE               DATE "DD-MON-YYYY" NULLIF LEXPA_EXTRACTED_DATE=blanks
,LEXPA_REFNO                        INTEGER EXTERNAL "expa_seq.NEXTVAL"
)
