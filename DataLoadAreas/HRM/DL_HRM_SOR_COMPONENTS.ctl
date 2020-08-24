load data
infile $gri_datafile
APPEND
INTO TABLE DL_HRM_SOR_COMPONENTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LSCMP_DLB_BATCH_ID          	CONSTANT "$batch_no",
LSCMP_DL_SEQNO              	RECNUM,
LSCMP_DL_LOAD_STATUS        	CONSTANT "L",
LSCMP_SOR_CODE      		CHAR "rtrim(:LSCMP_SOR_CODE)",
LSCMP_START_DATE      		CHAR "rtrim(:LSCMP_START_DATE)", 
LSCMP_SCMT_CODE         	CHAR "rtrim(:LSCMP_SCMT_CODE)",
LSCMP_COST      		CHAR "rtrim(:LSCMP_COST)",
LSCMP_PERCENTAGE_IND     	CHAR "rtrim(:LSCMP_PERCENTAGE_IND)"
)
