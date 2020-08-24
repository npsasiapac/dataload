load data                                                                       
infile $GRI_DATAFILE                                                                                                                   
APPEND                                                                          
into table DL_HSC_CREDIT_MEMO_BALANCES                                                    
fields terminated by "," optionally enclosed by '"'                             
trailing nullcols
(LCMBA_DLB_BATCH_ID         CONSTANT "$BATCH_NO"
,LCMBA_DL_SEQNO             RECNUM
,LCMBA_DL_LOAD_STATUS       CONSTANT "L"
,LCMBA_CCME_CREDIT_MEMO_REF CHAR "rtrim(:LCMBA_CCME_CREDIT_MEMO_REF)"
,LCMBA_SEQNO NULLIF         LCMBA_SEQNO=BLANKS
,LCMBA_BALANCE_DATE         DATE "DD-MON-YYYY"
,LCMBA_TOTAL_BALANCE        NULLIF LCMBA_TOTAL_BALANCE=BLANKS
)

