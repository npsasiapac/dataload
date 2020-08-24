load data
APPEND
into table DL_HRM_SERVICE_REQUESTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LSRQ_DLB_BATCH_ID CONSTANT "$batch_no"
,LSRQ_DL_SEQNO RECNUM
,LSRQ_DL_LOAD_STATUS CONSTANT "L"
,LSRQ_LEGACY_REFNO CHAR "rtrim(:LSRQ_LEGACY_REFNO)"
,LSRQ_DESCRIPTION CHAR "rtrim(:LSRQ_DESCRIPTION)"
,LSRQ_SOURCE CHAR "rtrim(:LSRQ_SOURCE)"
,LSRQ_RTR_IND CHAR "rtrim(:LSRQ_RTR_IND)"
,LSRQ_RECHARGEABLE_IND CHAR "rtrim(:LSRQ_RECHARGEABLE_IND)"
,LSRQ_INSPECTION_IND CHAR "rtrim(:LSRQ_INSPECTION_IND)"
,LSRQ_WORKS_ORDER_IND CHAR "rtrim(:LSRQ_WORKS_ORDER_IND)"
,LSRQ_SCO_CODE CHAR "rtrim(:LSRQ_SCO_CODE)"
,LSRQ_STATUS_DATE DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSRQ_STATUS_DATE=blanks
,LSRQ_PRINTED_IND CHAR "rtrim(:LSRQ_PRINTED_IND)"
,LSRQ_HRV_LOC_CODE CHAR "rtrim(:LSRQ_HRV_LOC_CODE)"
,LSRQ_PRO_PROPREF CHAR "rtrim(:LSRQ_PRO_PROPREF)"
,LSRQ_AUN_CODE CHAR "rtrim(:LSRQ_AUN_CODE)"
,LSRQ_TARGET_DATETIME DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSRQ_TARGET_DATETIME=blanks
,LSRQ_ACCESS_NOTES CHAR "rtrim(:LSRQ_ACCESS_NOTES)"
,LSRQ_LOCATION_NOTES CHAR "rtrim(:LSRQ_LOCATION_NOTES)"
,LSRQ_REPORTED_BY CHAR "rtrim(:LSRQ_REPORTED_BY)"
,LSRQ_REPORTED_DATETIME DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSRQ_REPORTED_DATETIME=blanks
,LSRQ_PRI_CODE CHAR "rtrim(:LSRQ_PRI_CODE)"
,LSRQ_ACCESS_AM CHAR "rtrim(:LSRQ_ACCESS_AM)"
,LSRQ_ACCESS_PM CHAR "rtrim(:LSRQ_ACCESS_PM)"
,LSRQ_COMMENTS CHAR(2000) "rtrim(:LSRQ_COMMENTS)"
,LSRQ_ALTERNATIVE_REFNO CHAR "rtrim(:LSRQ_ALTERNATIVE_REFNO)"
,LSRQ_HRV_RBR_CODE CHAR "rtrim(:LSRQ_HRV_RBR_CODE)"
,LSRQ_HRV_RMT_CODE CHAR "rtrim(:LSRQ_HRV_RMT_CODE)"
,LSRQ_HRV_ACC_CODE CHAR "rtrim(:LSRQ_HRV_ACC_CODE)"
,LSRQ_HRV_CBY_CODE CHAR "rtrim(:LSRQ_HRV_CBY_CODE)"
,LSRQ_CREATED_DATE DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSRQ_CREATED_DATE=blanks
)

