load data
infile $GRI_DATAFILE
APPEND
into table DL_HRA_PAYMENT_CONTRACTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPCT_DLB_BATCH_ID               CONSTANT "$batch_no"
,LPCT_DL_LOAD_STATUS             CONSTANT "L"
,LPCT_DL_SEQNO                   RECNUM
,LPCT_PAY_REF           	 CHAR "rtrim(:LPCT_PAY_REF)"
,LPCT_PAR_PER_ALT_REF         	 CHAR "rtrim(:LPCT_PAR_PER_ALT_REF)"
,LPCT_START_DATE                 DATE "DD-MON-YYYY" NULLIF LPCT_START_DATE=blanks
,LPCT_STATUS                     CHAR "rtrim(:LPCT_STATUS)"
,LPCT_END_DATE                   DATE "DD-MON-YYYY" NULLIF LPCT_END_DATE=blanks
,LPCT_AMOUNT                     CHAR "rtrim(:LPCT_AMOUNT)"
,LPCT_PERCENTAGE                 CHAR "rtrim(:LPCT_PERCENTAGE)"
,LPCT_REFNO                      INTEGER EXTERNAL "PCT_REFNO_SEQ.NEXTVAL"
)
