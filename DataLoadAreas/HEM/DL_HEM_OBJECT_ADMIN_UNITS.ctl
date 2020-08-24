load data
infile $gri_datafile
APPEND
into table DL_HEM_OBJECT_ADMIN_UNITS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LOAU_DLB_BATCH_ID       CONSTANT "$batch_no",
 LOAU_DL_SEQNO           RECNUM,
 LOAU_DL_LOAD_STATUS     CONSTANT "L",
 LOAU_AUN_CODE           CHAR "rtrim(:LOAU_AUN_CODE)",
 LOAU_START_DATE         DATE "DD-MON-YYYY" NULLIF LOAU_START_DATE=blanks,
 LOAU_END_DATE           DATE "DD-MON-YYYY" NULLIF LOAU_END_DATE=blanks,
 LOAU_REC_TYPE           CHAR "rtrim(:LOAU_REC_TYPE)",
 LOAU_OBJ_REF            CHAR "rtrim(:LOAU_OBJ_REF)",
 LOAU_COMMENTS           CHAR "rtrim(:LOAU_COMMENTS)"
)

