-- *********************************************************************
--
-- Ver   DB     Who   Date         Why
-- 1.00  6.14   AJ    14-NOV-2017  Initial Creation WITH Change Control
--
-- *********************************************************************
--
load data                          
APPEND                   
into table DL_HEM_ADMIN_UNITS            
fields terminated by "," optionally enclosed by '"'        
trailing nullcols
(
LAUN_DLB_BATCH_ID CONSTANT "$batch_no",
LAUN_DL_SEQNO RECNUM,
LAUN_DL_LOAD_STATUS CONSTANT "L", 
LAUN_CODE CHAR "rtrim(:LAUN_CODE)", 
LAUN_NAME CHAR "rtrim(:LAUN_NAME)", 
LAUN_AUY_CODE CHAR "rtrim(:LAUN_AUY_CODE)",
LAUN_CURRENT_IND CHAR "rtrim(:LAUN_CURRENT_IND)",
LAUN_TENANCY_WK_START CHAR "rtrim(:LAUN_TENANCY_WK_START)",
LAUN_HB_PERIOD CHAR "rtrim(:LAUN_HB_PERIOD)",
LAUN_ALT_REF CHAR "rtrim(:LAUN_ALT_REF)",
LAUN_COMMENTS CHAR "rtrim(:LAUN_COMMENTS)",          
LAUN_BDE_BANK_NAME CHAR "rtrim(:LAUN_BDE_BANK_NAME)",                           
LAUN_BDE_BRANCH_NAME CHAR "rtrim(:LAUN_BDE_BRANCH_NAME)",                          
LAUN_BAD_ACCOUNT_NO CHAR "rtrim(:LAUN_BAD_ACCOUNT_NO)",                        
LAUN_BAD_ACCOUNT_NAME CHAR "rtrim(:LAUN_BAD_ACCOUNT_NAME)",                          
LAUN_BAD_SORT_CODE CHAR "rtrim(:LAUN_BAD_SORT_CODE)",                           
LAUN_BAD_START_DATE DATE "DD-MON-YYYY" NULLIF LAUN_BAD_START_DATE=BLANKS,
LAUN_CODE_MLANG CHAR "rtrim(:LAUN_CODE_MLANG)", 
LAUN_NAME_MLANG CHAR "rtrim(:LAUN_NAME_MLANG)",
LAUN_BDE_BANK_NAME_MLANG CHAR "rtrim(:LAUN_BDE_BANK_NAME_MLANG)",                           
LAUN_BDE_BRANCH_NAME_MLANG CHAR "rtrim(:LAUN_BDE_BRANCH_NAME_MLANG)" 
)

