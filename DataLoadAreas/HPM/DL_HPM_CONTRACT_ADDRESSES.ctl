load data
infile $GRI_DATAFILE
APPEND
into table DL_HPM_CONTRACT_ADDRESSES 
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
 LCAD_DLB_BATCH_ID 		CONSTANT "$batch_no",
 LCAD_DL_SEQNO 			RECNUM,
 LCAD_DL_LOAD_STATUS 		CONSTANT "L",
 LCAD_CAD_CNT_REFERENCE 	CHAR "rtrim(:LCAD_CAD_CNT_REFERENCE)",
 LCAD_CAD_PRO_AUN_CODE     	CHAR "rtrim(:LCAD_CAD_PRO_AUN_CODE)",
 LCAD_CAD_TYPE_IND		CHAR "rtrim(:LCAD_CAD_TYPE_IND)",
 LCAD_CAI_START_DATE		DATE "DD-MON-YYYY" NULLIF LCAD_CAI_START_DATE=BLANKS,
 LCAD_CAI_END_DATE		DATE "DD-MON-YYYY" NULLIF LCAD_CAI_END_DATE=BLANKS,
 LCAD_CAI_COMMENTS		CHAR "rtrim(:LCAD_CAI_COMMENTS)",
 LCAD_CAI_CSE_SECTION_NUMBER	CHAR "rtrim(:LCAD_CAI_CSE_SECTION_NUMBER)",
 LCAD_CAI_HRV_CAA_CODE          CHAR "rtrim(:LCAD_CAI_HRV_CAA_CODE)",
 LCAD_CAI_HRV_CAT_CODE 		CHAR "rtrim(:LCAD_CAI_HRV_CAT_CODE)")
