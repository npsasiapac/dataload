load data                                      
APPEND                                                                          
into table DL_HRA_ACCOUNT_BALANCES                                                 
fields terminated by "," optionally enclosed by '"'                             
trailing nullcols
(
LABA_DLB_BATCH_ID CONSTANT "$batch_no"
,LABA_DL_SEQNO RECNUM
,LABA_DL_LOAD_STATUS CONSTANT "L",                                                                                                                                       
LABA_PAY_REF CHAR "rtrim(:LABA_PAY_REF)",                                                                                                                       
LABA_BALANCE CHAR "rtrim(:LABA_BALANCE)",                                                                                                                       
LABA_DATE DATE "DD-MON-YYYY" NULLIF laba_date=BLANKS,                                                                                                                                   
LABA_SUMMARISE_IND CHAR "rtrim(:LABA_SUMMARISE_IND)" )
