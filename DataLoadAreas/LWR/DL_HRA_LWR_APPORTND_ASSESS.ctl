load data
APPEND
into table DL_HRA_LWR_APPORTND_ASSESS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
 LAAS_DLB_BATCH_ID          	CONSTANT "$batch_no",
 LAAS_DL_SEQNO              	RECNUM,
 LAAS_DL_LOAD_STATUS        	CONSTANT "L",
 LAAS_LWRB_REFNO		CHAR "rtrim(:LAAS_LWRB_REFNO)",
 LAAS_WRA_CURR_ASSESSMENT_REF  	CHAR "rtrim(:LAAS_WRA_CURR_ASSESSMENT_REF)",
 LWRA_RATE_PERIOD_START_DATE	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LWRA_RATE_PERIOD_START_DATE=blanks,
 LWRA_RATE_PERIOD_END_DATE	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LWRA_RATE_PERIOD_END_DATE=blanks,
 LAAS_PRO_PROPREF		CHAR "rtrim(:LAAS_PRO_PROPREF)",
 LAAS_AMOUNT		    	CHAR "rtrim(:LAAS_AMOUNT)",
 LAAS_CREATED_BY            	CHAR "rtrim(:LAAS_CREATED_BY)",
 LAAS_CREATED_DATE          	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LAAS_CREATED_DATE=blanks
)

