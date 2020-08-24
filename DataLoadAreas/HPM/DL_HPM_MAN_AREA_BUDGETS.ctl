load data
infile $gri_datafile
APPEND
into table DL_HPM_MAN_AREA_BUDGETS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LMAB_DLB_BATCH_ID       CONSTANT "$batch_no",
 LMAB_DL_SEQNO           RECNUM,
 LMAB_DL_LOAD_STATUS     CONSTANT "L",
 LMAB_AUN_CODE           CHAR "rtrim(:LMAB_AUN_CODE)",
 LMAB_BHE_CODE           CHAR "rtrim(:LMAB_BHE_CODE)",
 LMAB_BCA_YEAR           CHAR NULLIF LMAB_BCA_YEAR=BLANKS
)

