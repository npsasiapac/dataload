load data
APPEND
into table DL_HRA_LWR_RATE_ASSESS_DETS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
 LRAD_DLB_BATCH_ID          	CONSTANT "$batch_no",
 LRAD_DL_SEQNO              	RECNUM,
 LRAD_DL_LOAD_STATUS        	CONSTANT "L",
 LRAD_LWRB_REFNO		CHAR "rtrim(:LRAD_LWRB_REFNO)",
 LRAD_WRA_CURR_ASSESSMENT_REF  	CHAR "rtrim(:LRAD_WRA_CURR_ASSESSMENT_REF)",
 LRAD_CLASS_CODE		CHAR "rtrim(:LRAD_CLASS_CODE)",
 LWRA_RATE_PERIOD_START_DATE	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LWRA_RATE_PERIOD_START_DATE=blanks,
 LWRA_RATE_PERIOD_END_DATE	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LWRA_RATE_PERIOD_END_DATE=blanks,
 LRAD_RCCO_CODE		    	CHAR "rtrim(:LRAD_RCCO_CODE)",
 LRAD_CATEGORY_RATE_AMOUNT     	CHAR "rtrim(:LRAD_CATEGORY_RATE_AMOUNT)",
 LRAD_CR_DR_INDICATOR         	CHAR "rtrim(:LRAD_CR_DR_INDICATOR)",
 LRAD_CREATED_BY            	CHAR "rtrim(:LRAD_CREATED_BY)",
 LRAD_CREATED_DATE          	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRAD_CREATED_DATE=blanks,
 LRAD_CATEGORY_START_DATE	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRAD_CATEGORY_START_DATE=blanks,
 LRAD_CATEGORY_END_DATE		DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRAD_CATEGORY_END_DATE=blanks,
 LRAD_COMMENTS           	CHAR "rtrim(:LRAD_COMMENTS)",
 LRAD_MODIFIED_BY           	CHAR "rtrim(:LRAD_MODIFIED_BY)",
 LRAD_MODIFIED_DATE           	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRAD_MODIFIED_DATE=blanks,
 LRAD_RAAM_SEQ_NO           	CHAR "rtrim(:LRAD_RAAM_SEQ_NO)",
 LRAD_REFNO                     INTEGER EXTERNAL "lrad_seq.nextval"
)

