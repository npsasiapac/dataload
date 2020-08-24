load data
APPEND
into table DL_HEM_LINK_TENANCIES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LLTE_DLB_BATCH_ID         CONSTANT "$batch_no"
,LLTE_DL_SEQNO            RECNUM
,LLTE_DL_LOAD_STATUS      CONSTANT "L"
,LLTE_START_DATE	  DATE "DD-MON-YYYY" NULLIF LLTE_START_DATE=blanks
,LLTE_TCY_ALT_REF	  CHAR "rtrim(:LLTE_TCY_ALT_REF)"
,LLTE_TCY_ALT_REF_IS_FOR  CHAR "rtrim(:LLTE_TCY_ALT_REF_IS_FOR)"
,LLTE_HRV_FTLR_CODE	  CHAR "rtrim(:LLTE_HRV_FTLR_CODE)"
,LLTE_COMMENTS            CHAR "rtrim(:LLTE_COMMENTS)"
)
