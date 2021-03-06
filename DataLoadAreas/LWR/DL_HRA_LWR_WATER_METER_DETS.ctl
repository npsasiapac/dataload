load data
APPEND
into table DL_HRA_LWR_WATER_METER_DETS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
 LWMD_DLB_BATCH_ID          	CONSTANT "$batch_no",
 LWMD_DL_SEQNO              	RECNUM,
 LWMD_DL_LOAD_STATUS        	CONSTANT "L",
 LWMD_LWRB_REFNO		CHAR "rtrim(:LWMD_LWRB_REFNO)",
 LWMD_WRA_CURR_ASSESSMENT_REF  	CHAR "rtrim(:LWMD_WRA_CURR_ASSESSMENT_REF)",
 LWRA_RATE_PERIOD_START_DATE	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LWRA_RATE_PERIOD_START_DATE=blanks,
 LWRA_RATE_PERIOD_END_DATE	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LWRA_RATE_PERIOD_END_DATE=blanks,
 LWMD_RCCO_CODE		    	CHAR "rtrim(:LWMD_RCCO_CODE)",
 LWMD_CURRENT_METER_READING    	CHAR "rtrim(:LWMD_CURRENT_METER_READING)",
 LWMD_PREVIOUS_METER_READING   	CHAR "rtrim(:LWMD_PREVIOUS_METER_READING)",
 LWMD_CHARGEABLE_WATER_USAGE   	CHAR "rtrim(:LWMD_CHARGEABLE_WATER_USAGE)",
 LWMD_CURRENT_READING_DATE     	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LWMD_CURRENT_READING_DATE=blanks,
 LWMD_PREVIOUS_READING_DATE    	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LWMD_PREVIOUS_READING_DATE=blanks,
 LWMD_DAYS_SINCE_LAST_READING  	CHAR "rtrim(:LWMD_DAYS_SINCE_LAST_READING)",
 LWMD_WATER_METER_NUMBER   	CHAR "rtrim(:LWMD_WATER_METER_NUMBER)",
 LWMD_CREATED_BY            	CHAR "rtrim(:LWMD_CREATED_BY)",
 LWMD_CREATED_DATE          	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LWMD_CREATED_DATE=blanks,
 LWMD_COMMENTS           	CHAR "rtrim(:LWMD_COMMENTS)",
 LWMD_MODIFIED_BY           	CHAR "rtrim(:LWMD_MODIFIED_BY)",
 LWMD_MODIFIED_DATE           	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LWMD_MODIFIED_DATE=blanks,
 LWMD_REFNO                     INTEGER EXTERNAL "wmd_seq.nextval"
)

