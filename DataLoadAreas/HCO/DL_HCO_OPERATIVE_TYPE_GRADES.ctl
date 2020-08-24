load data
APPEND
into table DL_HCO_OPERATIVE_TYPE_GRADES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LOTGR_DLB_BATCH_ID              CONSTANT "$BATCH_NO"
,LOTGR_DL_SEQNO                  RECNUM
,LOTGR_DL_LOAD_STATUS            CONSTANT "L"
,LOTGR_IPT_CODE                  CHAR "rtrim(:LOTGR_IPT_CODE)"
,LOTGR_HRV_GRA_CODE              CHAR "rtrim(:LOTGR_HRV_GRA_CODE)"
,LOTGR_CURRENT_IND               CHAR "rtrim(:LOTGR_CURRENT_IND)"
,LOTGR_MAX_WKLY_STD_WORKING_TIM  CHAR "rtrim(:LOTGR_MAX_WKLY_STD_WORKING_TIM)"
,LOTGR_MAX_WKLY_OVERTIME         CHAR "rtrim(:LOTGR_MAX_WKLY_OVERTIME)"
,LOTGR_DEFAULT_HOURLY_RATE       NULLIF LOTGR_DEFAULT_HOURLY_RATE=BLANKS
,LOTGR_DEFAULT_OVERTIME_RATE     NULLIF LOTGR_DEFAULT_OVERTIME_RATE=BLANKS
) 
