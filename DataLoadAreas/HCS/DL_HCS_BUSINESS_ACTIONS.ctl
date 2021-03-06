load data
APPEND
into table DL_HCS_BUSINESS_ACTIONS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
 LBAN_DLB_BATCH_ID          	CONSTANT "$batch_no",             
 LBAN_DL_SEQNO              	RECNUM,     
 LBAN_DL_LOAD_STATUS        	CONSTANT "L",    
 LBAN_ALT_REF			CHAR "rtrim(:LBAN_ALT_REF)",   
 LBAN_OBJ_LEGACY_REF	    	CHAR "rtrim(:LBAN_OBJ_LEGACY_REF)",
 LBAN_OBJ_SECONDARY_REF	    	CHAR "rtrim(:LBAN_OBJ_SECONDARY_REF)",
 LBAN_LEGACY_TYPE	    		CHAR "rtrim(:LBAN_LEGACY_TYPE)",
 LBAN_TYPE                  	CHAR "rtrim(:LBAN_TYPE)",
 LBAN_BRO_CODE              	CHAR "rtrim(:LBAN_BRO_CODE)",
 LBAN_AUN_CODE_RESPONSIBLE  	CHAR "rtrim(:LBAN_AUN_CODE_RESPONSIBLE)",
 LBAN_SCO_CODE              	CHAR "rtrim(:LBAN_SCO_CODE)",
 LBAN_STATUS_DATE           	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LBAN_STATUS_DATE=blanks,
 LBAN_CREATED_BY            	CHAR "rtrim(:LBAN_CREATED_BY)",
 LBAN_CREATED_DATE          	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LBAN_CREATED_DATE=blanks,
 LBAN_USR_USERNAME          	CHAR "rtrim(:LBAN_USR_USERNAME)",
 LBAN_BAN_ALT_REF           	CHAR "rtrim(:LBAN_BAN_ALT_REF)",
 LBAN_TARGET_DATE           	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LBAN_TARGET_DATE=blanks,
 LBAN_LAS_LEA_START_DATE    	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LBAN_LAS_LEA_START_DATE=blanks,
 LBAN_LAS_START_DATE        	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LBAN_LAS_START_DATE=blanks
)
