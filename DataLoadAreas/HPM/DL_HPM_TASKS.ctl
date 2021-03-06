load data
infile $GRI_DATAFILE
APPEND
into table DL_HPM_TASKS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
 LTSK_DLB_BATCH_ID 		CONSTANT "$batch_no",
 LTSK_DL_SEQNO 			RECNUM,
 LTSK_DL_LOAD_STATUS 		CONSTANT "L",
 LTSK_TKG_SRC_REFERENCE		CHAR "rtrim(:LTSK_TKG_SRC_REFERENCE)",
 LTSK_TKG_CODE 			CHAR "rtrim(:LTSK_TKG_CODE)",
 LTSK_TKG_SRC_TYPE		CHAR "rtrim(:LTSK_TKG_SRC_TYPE)",
 LTSK_TYPE_IND	 		CHAR "rtrim(:LTSK_TYPE_IND)",
 LTSK_STK_CODE	 		CHAR "rtrim(:LTSK_STK_CODE)",
 LTSK_SCO_CODE	 		CHAR "rtrim(:LTSK_SCO_CODE)",
 LTSK_STATUS_DATE 		DATE "DD-MON-YYYY" NULLIF LTSK_STATUS_DATE=BLANKS,
 LTSK_ALT_REFERENCE 		CHAR "rtrim(:LTSK_ALT_REFERENCE)",
 LTSK_ACTUAL_END_DATE 		DATE "DD-MON-YYYY" NULLIF LTSK_ACTUAL_END_DATE=BLANKS,
 LTVE_VERSION_NUMBER 		CHAR "rtrim(:LTVE_VERSION_NUMBER)",
 LTVE_CURRENT_IND     		CHAR "rtrim(:LTVE_CURRENT_IND)",
 LTVE_DISPLAY_SEQUENCE 		CHAR "rtrim(:LTVE_DISPLAY_SEQUENCE)",
 LTVE_HRV_TUS_CODE     		CHAR "rtrim(:LTVE_HRV_TUS_CODE)",
 LTVE_BCA_YEAR     		CHAR "rtrim(:LTVE_BCA_YEAR)",
 LTVE_VCA_CODE     		CHAR "rtrim(:LTVE_VCA_CODE)",
 LTVE_PLANNED_START_DATE 	DATE "DD-MON-YYYY" NULLIF LTVE_PLANNED_START_DATE=BLANKS,
 LTVE_NET_AMOUNT		CHAR "rtrim(:LTVE_NET_AMOUNT)",
 LTVE_TAX_AMOUNT     		CHAR "rtrim(:LTVE_TAX_AMOUNT)",
 LTVE_RETENTION_PERCENT     	CHAR "rtrim(:LTVE_RETENTION_PERCENT)",
 LTVE_RETENTION_PERIOD     	CHAR "rtrim(:LTVE_RETENTION_PERIOD)",
 LTVE_RETENTION_PERIOD_UNITS	CHAR "rtrim(:LTVE_RETENTION_PERIOD_UNITS)",
 LTVE_COMMENTS     		CHAR "rtrim(:LTVE_COMMENTS)",
 LTBA_BHE_CODE     		CHAR "rtrim(:LTBA_BHE_CODE)",
 LTBA_BCA_YEAR     		CHAR "rtrim(:LTBA_BCA_YEAR)",
 LTBA_NET_AMOUNT     		CHAR "rtrim(:LTBA_NET_AMOUNT)",
 LTBA_TAX_AMOUNT     		CHAR "rtrim(:LTBA_TAX_AMOUNT)"
)
