load data
infile $GRI_DATAFILE
APPEND
into table DL_HSC_SERVICE_ASSIGNMENTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LSEA_DLB_BATCH_ID              CONSTANT "$batch_no"
,LSEA_DL_LOAD_STATUS            CONSTANT "L"
,LSEA_DL_SEQNO                  RECNUM
,LSEA_AUN_CODE                  CHAR "rtrim(:LSEA_AUN_CODE)"
,LSEA_SVC_ATT_ELE_CODE          CHAR "rtrim(:LSEA_SVC_ATT_ELE_CODE)"
,LSEA_SVC_ATT_CODE              CHAR "rtrim(:LSEA_SVC_ATT_CODE)"
,LSEA_START_DATE                DATE "DD-MON-YYYY" NULLIF LSEA_START_DATE=blanks
,LSEA_END_DATE                  DATE "DD-MON-YYYY" NULLIF LSEA_END_DATE=blanks
,LSEA_SEA_AUN_CODE              CHAR "rtrim(:LSEA_SEA_AUN_CODE)"
,LSEA_SEA_SVC_ATT_ELE_CODE      CHAR "rtrim(:LSEA_SEA_SVC_ATT_ELE_CODE)"
,LSEA_SEA_SVC_ATT_CODE          CHAR "rtrim(:LSEA_SEA_SVC_ATT_CODE)"
,LSEA_SEA_START_DATE            DATE "DD-MON-YYYY" NULLIF LSEA_SEA_START_DATE=blanks)


