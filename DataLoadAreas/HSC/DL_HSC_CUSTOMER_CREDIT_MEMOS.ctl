load data                                                                       
infile $GRI_DATAFILE                                                                                                                    
APPEND                                                                          
into table DL_HSC_CUSTOMER_CREDIT_MEMOS                                                     
fields terminated by "," optionally enclosed by '"'                             
trailing nullcols
(LCCME_DLB_BATCH_ID CONSTANT "$BATCH_NO"
,LCCME_DL_SEQNO RECNUM
,LCCME_DL_LOAD_STATUS CONSTANT "L"
,LCCME_CREDIT_MEMO_REF         CHAR "rtrim(:LCCME_CREDIT_MEMO_REF)"
,LCCME_SCO_CODE                CHAR "rtrim(:LCCME_SCO_CODE)"
,LCCME_CLIN_INVOICE_REF        CHAR "rtrim(:LCCME_CLIN_INVOICE_REF)"
,LCCME_AUTHORISED_BY           CHAR "rtrim(:LCCME_AUTHORISED_BY)"
,LCCME_AUTHORISED_DATE         DATE "DD-MON-YYYY"
,LCCME_ISSUED_BY               CHAR "rtrim(:LCCME_ISSUED_BY)"
,LCCME_ISSUED_DATE             DATE "DD-MON-YYYY"
,LCCME_LEVEL2_AUTHORISED_BY    CHAR "rtrim(:LCCME_LEVEL2_AUTHORISED_BY)"
,LCCME_LEVEL2_AUTHORISED_DATE  DATE "DD-MON-YYYY"
,LCCME_PAY_REF                 CHAR "rtrim(:LCCME_PAY_REF)"
,LCCME_REFNO INTEGER EXTERNAL "ccme_refno_seq.nextval"
)
