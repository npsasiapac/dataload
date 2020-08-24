load data
infile $gri_datafile
APPEND
into table DL_HAT_APPLIC_STATUSES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LALE_DLB_BATCH_ID           CONSTANT "$batch_no",  
 LALE_DL_SEQNO               RECNUM,
 LALE_DL_LOAD_STATUS         CONSTANT "L",
 LAPP_LEGACY_REF             CHAR "rtrim(:LAPP_LEGACY_REF)", 
 LALE_RLI_CODE               CHAR "rtrim(:LALE_RLI_CODE)",
 LALE_LST_CODE               CHAR "rtrim(:LALE_LST_CODE)",
 LALE_ALS_ACTIVE_IND         CHAR "rtrim(:LALE_ALS_ACTIVE_IND)",
 LALE_STATUS_START_DATE      DATE "DD-MON-YYYY" NULLIF LALE_STATUS_START_DATE=blanks,
 LALE_STATUS_REVIEW_DATE     DATE "DD-MON-YYYY" NULLIF LALE_STATUS_REVIEW_DATE=blanks,
 LALE_HRV_APS_CODE           CHAR "rtrim(:LALE_HRV_APS_CODE)"
)







