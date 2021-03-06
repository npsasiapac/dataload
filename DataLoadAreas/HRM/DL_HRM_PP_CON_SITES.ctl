load data
infile $GRI_DATAFILE
APPEND
into table DL_HRM_PP_CON_SITES 
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LPPC_DLB_BATCH_ID      	        CONSTANT "$batch_no"
,LPPC_DL_SEQNO          	RECNUM
,LPPC_DL_LOAD_STATUS    	CONSTANT "L"
,LPPC_PPP_WPR_CODE              CHAR "rtrim(:LPPC_PPP_WPR_CODE)"
,LPPC_PPP_PPG_CODE              CHAR "rtrim(:LPPC_PPP_PPG_CODE)"
,LPPC_START_DATE                DATE "DD-MON-YYYY" NULLIF LPPC_START_DATE=blanks
,LPPC_COS_CODE                  CHAR "rtrim(:LPPC_COS_CODE)"
,LPPC_CREATED_BY		CHAR "rtrim(:LPPC_CREATED_BY)"
,LPPC_CREATED_DATE		DATE "DD-MON-YYYY" NULLIF LPPC_CREATED_DATE=blanks
,LPPC_FCA_CODE                  CHAR "rtrim(:LPPC_FCA_CODE)"
,LPPC_AGREED_PERCENTAGE		CHAR NULLIF LPPC_AGREED_PERCENTAGE=blanks
,LPPC_WORKS_ORDERS_COUNT        CHAR NULLIF LPPC_WORKS_ORDERS_COUNT=blanks
,LPPC_MIN_WO_VALUE              CHAR NULLIF LPPC_MIN_WO_VALUE=blanks
)

