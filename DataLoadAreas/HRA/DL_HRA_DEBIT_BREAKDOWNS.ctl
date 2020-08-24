load data                     
APPEND                                                                          
into table DL_HRA_DEBIT_BREAKDOWNS                                                 
fields terminated by "," optionally enclosed by '"'                             
trailing nullcols
(
LDBR_DLB_BATCH_ID CONSTANT "$batch_no"
,LDBR_DL_SEQNO RECNUM
,LDBR_DL_LOAD_STATUS CONSTANT "L",                                                                                                                                       
LDBR_PAY_REF CHAR "rtrim(:LDBR_PAY_REF)",                                                                                                                       
LDBR_PRO_REFNO CHAR "rtrim(:LDBR_PRO_REFNO)",                                                                                                               
LDBR_ELE_CODE CHAR "rtrim(:LDBR_ELE_CODE)",                                                                                                                     
LDBR_START_DATE DATE "DD-MON-YYYY" NULLIF ldbr_start_date=BLANKS,                                                                                                                                  
LDBR_END_DATE DATE "DD-MON-YYYY" NULLIF ldbr_end_date=BLANKS,                                                                                                             
LDBR_ATT_CODE CHAR "rtrim(:LDBR_ATT_CODE)",                                                                                                                   
LDBR_ELE_VALUE CHAR "rtrim(:LDBR_ELE_VALUE)",                                                                                                                   
LDBR_VCA_CODE CHAR "rtrim(:LDBR_VCA_CODE)",
LDBR_PAR_ALT_REF CHAR "rtrim(:LDBR_PAR_ALT_REF)")
