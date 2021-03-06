load data
infile $GRI_DATAFILE
APPEND
into table DL_HRM_CON_SITE_PRICES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LCSPG_DLB_BATCH_ID CONSTANT "$batch_no"
,LCSPG_DL_SEQNO RECNUM
,LCSPG_DL_LOAD_STATUS CONSTANT "L"
,LCSPG_PPC_PPP_PPG_CODE CHAR "rtrim(:LCSPG_PPC_PPP_PPG_CODE)"
,LCSPG_PPC_PPP_WPR_CODE CHAR "rtrim(:LCSPG_PPC_PPP_WPR_CODE)"
,LCSPG_PPC_PPP_START_DATE DATE "DD-MON-YYYY" NULLIF LCSPG_PPC_PPP_START_DATE=blanks
,LCSPG_PPC_COS_CODE CHAR "rtrim(:LCSPG_PPC_COS_CODE)"
,LCSPG_START_DATE DATE "DD-MON-YYYY" NULLIF LCSPG_START_DATE=blanks
,LCSPG_END_DATE DATE "DD-MON-YYYY" NULLIF LCSPG_END_DATE=blanks
,LCSP_SOR_CODE CHAR "rtrim(:LCSP_SOR_CODE)"
,LCSP_PRICE CHAR NULLIF LCSP_PRICE=blanks "TO_NUMBER(:LCSP_PRICE)"
,LCSP_PREFERRED_IND CHAR "rtrim(:LCSP_PREFERRED_IND)"
)
