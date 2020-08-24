load data
infile $gri_datafile
APPEND
INTO TABLE DL_HCO_STORES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LSTO_DLB_BATCH_ID               CONSTANT "$batch_no",
LSTO_DL_SEQNO                   RECNUM,
LSTO_DL_LOAD_STATUS             CONSTANT "L",
LSTO_LOCATION              	CHAR "rtrim(:LSTO_LOCATION)",
LSTO_TYPE                  	CHAR "rtrim(:LSTO_TYPE)",
LSTO_DESCRIPTION           	CHAR "rtrim(:LSTO_DESCRIPTION)",
LSTO_START_DATE            	DATE "DD-MON-YYYY" NULLIF lsto_start_date=blanks,
LSTO_STRT_CODE             	CHAR "rtrim(:LSTO_STRT_CODE)",
LSTO_END_DATE              	DATE "DD-MON-YYYY" NULLIF lsto_end_date=blanks,
LSTO_COMMENTS              	CHAR "rtrim(:LSTO_COMMENTS)",
LSTO_CDEP_DEP_CODE         	CHAR "rtrim(:LSTO_CDEP_DEP_CODE)",
LSTO_CDEP_COS_CODE         	CHAR "rtrim(:LSTO_CDEP_COS_CODE)",
LSTO_COS_CODE              	CHAR "rtrim(:LSTO_COS_CODE)",
LSTO_VEHICLE_REG           	CHAR "rtrim(:LSTO_VEHICLE_REG)",
LSTO_HRV_FFTY_CODE         	CHAR "rtrim(:LSTO_HRV_FFTY_CODE)",
LSTO_HRV_VEHI_CODE         	CHAR "rtrim(:LSTO_HRV_VEHI_CODE)",
LSTO_HRV_VMM_CODE          	CHAR "rtrim(:LSTO_HRV_VMM_CODE)",
LSTO_TAX_DUE_DATE          	DATE "DD-MON-YYYY" NULLIF lsto_tax_due_date=blanks,
LSTO_INSURANCE_DUE_DATE    	DATE "DD-MON-YYYY" NULLIF lsto_insurance_due_date=blanks,
LSTO_INSURANCE_REFERENCE   	CHAR "rtrim(:LSTO_INSURANCE_REFERENCE)",
LSTO_FIRST_REGISTERED_DATE 	DATE "DD-MON-YYYY" NULLIF lsto_first_registered_date=blanks,
LSTO_CC                    	CHAR "rtrim(:LSTO_CC)",
LSTO_MOT_DUE_DATE          	DATE "DD-MON-YYYY" NULLIF lsto_mot_due_date=blanks,
LSTO_SERVICE_DUE_DATE      	DATE "DD-MON-YYYY" NULLIF lsto_service_due_date=blanks,
LSTO_SUSPENDED_IND         	CHAR "rtrim(:LSTO_SUSPENDED_IND)"
)
