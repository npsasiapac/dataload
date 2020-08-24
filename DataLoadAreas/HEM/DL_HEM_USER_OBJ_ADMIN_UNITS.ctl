load data
infile $gri_datafile
APPEND
into table DL_HEM_USER_OBJ_ADMIN_UNITS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LUOA_DLB_BATCH_ID       CONSTANT "$batch_no",
 LUOA_DL_SEQNO           RECNUM,
 LUOA_DL_LOAD_STATUS     CONSTANT "L",
 LUOA_USR_USERNAME       CHAR "rtrim(:LUOA_USR_USERNAME)",
 LUOA_AUN_CODE           CHAR "rtrim(:LUOA_AUN_CODE)",
 LUOA_OBJ_NAME           CHAR "rtrim(:LUOA_OBJ_NAME)",
 LUOA_ACCESS_LEVEL       CHAR "rtrim(:LUOA_ACCESS_LEVEL)",
 LUOA_START_DATE         DATE "DD-MON-YYYY" NULLIF LUOA_START_DATE=blanks,
 LUOA_END_DATE           DATE "DD-MON-YYYY" NULLIF LUOA_END_DATE=blanks,
 LUOA_COMMENTS           CHAR "rtrim(:LUOA_COMMENTS)"
)

