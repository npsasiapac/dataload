-- *********************************************************************
--
-- Ver   DB     Who   Date         Why
-- 1.00  6.13   AJ    26-APR-2016  Initial Creation WITH Change Control
--                                 amended the following
--                                 LTRA_STATEMENT_IND added RTRIM
--                                 LTRA_SUMMARISE_IND CHAR added RTRIM
--                                 LTRA_REFNO next val from seq now done in package
-- 1.01  6.14   AJ    04-APR-2017  LTRA_SUMMARISE_IND CHAR syntax error
--
-- *********************************************************************
--
load data  
APPEND    
into table DL_HRA_TRANSACTIONS     
fields terminated by "," optionally enclosed by '"'                             
trailing nullcols
(
LTRA_DLB_BATCH_ID CONSTANT "$batch_no",
LTRA_DL_SEQNO RECNUM,
LTRA_DL_LOAD_STATUS CONSTANT "L",
LTRA_DATE DATE "DD-MON-YYYY" NULLIF LTRA_DATE=blanks, 
LTRA_PAY_REF CHAR "rtrim(:LTRA_PAY_REF)",                                                                                                                     
LTRA_EFFECTIVE_DATE DATE "DD-MON-YYYY",                                                                                                                        
LTRA_STATEMENT_IND CHAR "rtrim(:LTRA_STATEMENT_IND)", 
LTRA_TRT_CODE CHAR "rtrim(:LTRA_TRT_CODE)",  
LTRA_TRS_CODE CHAR "rtrim(:LTRA_TRS_CODE)", 
LTRA_PMY_CODE CHAR "rtrim(:LTRA_PMY_CODE)", 
LTRA_BALANCE_IND CHAR "rtrim(:LTRA_BALANCE_IND)",                                                                                                             
LTRA_DR CHAR NULLIF LTRA_dr=BLANKS,                                                                                                                           
LTRA_CR CHAR NULLIF LTRA_cr=BLANKS,                                                                                                                        
LTRA_VAT_DR CHAR NULLIF LTRA_VAT_DR=BLANKS,                                                                                                                   
LTRA_VAT_CR CHAR NULLIF LTRA_VAT_CR=BLANKS,                                                                                                                   
LTRA_PAYMENT_DATE DATE "DD-MON-YYYY" NULLIF LTRA_PAYMENT_DATE=BLANKS,                                                                                         
LTRA_HDE_CLAIM_NO CHAR "rtrim(:LTRA_HDE_CLAIM_NO)",                                                                                                           
LTRA_EXTERNAL_REF CHAR "rtrim(:LTRA_EXTERNAL_REF)",                                                                                                           
LTRA_BALANCE_YEAR CHAR NULLIF LTRA_BALANCE_YEAR=BLANKS,                                                                                                       
LTRA_BALANCE_PERIOD CHAR NULLIF LTRA_BALANCE_PERIOD=BLANKS,                                                                                                   
LTRA_SUMMARISE_IND CHAR "rtrim(:LTRA_SUMMARISE_IND)", 
LTRA_TEXT CHAR "rtrim(:LTRA_TEXT)", 
LTRA_DEBIT_EFFECTIVE_DATE DATE "DD-MON-YYYY" NULLIF LTRA_DEBIT_EFFECTIVE_DATE=blanks,
LTRA_SUSP_PAY_REF CHAR "rtrim(:LTRA_SUSP_PAY_REF)",
LTRA_EXT_DESCRIPTION CHAR "rtrim(:LTRA_EXT_DESCRIPTION)",
LTRA_CLIN_INVOICE_REF CHAR "rtrim(:LTRA_CLIN_INVOICE_REF)",
LTRA_ALLOCATE_TO_CLIN CHAR "rtrim(:LTRA_ALLOCATE_TO_CLIN)"
)



