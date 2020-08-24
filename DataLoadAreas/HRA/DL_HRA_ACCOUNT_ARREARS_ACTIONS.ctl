-- *********************************************************************
--
-- Ver   DB     Who   Date         Why
-- 1.00  6.13   AJ    26-APR-2016  Initial Creation WITH Change Control
--                                 as LACA_NOP_TEXT increased from 2000
--                                 to 4000
-- *********************************************************************
--
load data  
APPEND
into table DL_HRA_ACCOUNT_ARREARS_ACTIONS    
fields terminated by "," optionally enclosed by '"'   
trailing nullcols
(
LACA_DLB_BATCH_ID CONSTANT "$batch_no"
,LACA_DL_SEQNO RECNUM
,LACA_DL_LOAD_STATUS CONSTANT "L",
LACA_BALANCE CHAR NULLIF LACA_BALANCE=BLANKS, 
LACA_PAY_REF CHAR "rtrim(:LACA_PAY_REF)",
LACA_TYPE CHAR "rtrim(:LACA_TYPE)", 
LACA_CREATED_BY CHAR "rtrim(:LACA_CREATED_BY)", 
LACA_CREATED_DATE DATE "DD-MON-YYYY" NULLIF laca_created_date=BLANKS, 
LACA_ARREARS_DISPUTE_IND CHAR "rtrim(:LACA_ARREARS_DISPUTE_IND)", 
LACA_ARA_CODE CHAR "rtrim(:LACA_ARA_CODE)",
LACA_STATUS CHAR "rtrim(:LACA_STATUS)",
LACA_HRV_ADL_CODE CHAR "rtrim(:LACA_HRV_ADL_CODE)", 
LACA_EAC_EPO_CODE CHAR "rtrim(:LACA_EAC_EPO_CODE)", 
LACA_EFFECTIVE_DATE DATE "DD-MON-YYYY" NULLIF laca_effective_date=BLANKS,
LACA_EXPIRY_DATE DATE "DD-MON-YYYY" NULLIF laca_expiry_date=BLANKS,
LACA_NEXT_ACTION_DATE DATE "DD-MON-YYYY" NULLIF laca_next_action_date=BLANKS,
LACA_AUTH_DATE DATE "DD-MON-YYYY" NULLIF laca_auth_date=BLANKS,
LACA_AUTH_USERNAME CHAR "rtrim(:LACA_AUTH_USERNAME)", 
LACA_PRINT_DATE DATE "DD-MON-YYYY" NULLIF laca_print_date=BLANKS,
LACA_DEL_DATE DATE "DD-MON-YYYY" NULLIF laca_del_date=BLANKS,
LACA_DEL_USERNAME CHAR "rtrim(:LACA_DEL_USERNAME)", 
LACA_PRINT_USERNAME CHAR "rtrim(:LACA_PRINT_USERNAME)",
LACA_NOP_TEXT CHAR(4000) "rtrim(:LACA_NOP_TEXT)",
LACA_REFNO INTEGER EXTERNAL "aca_refno_seq.nextval") 
