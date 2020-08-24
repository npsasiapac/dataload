load data
infile $gri_datafile
APPEND
into table DL_HSC_LEASE_ASSIGNMENTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LLAS_DLB_BATCH_ID         CONSTANT "$batch_no"
,LLAS_DL_SEQNO             RECNUM
,LLAS_DL_LOAD_STATUS       CONSTANT "L"
,LLAS_LEA_PRO_PROPREF      CHAR "rtrim(:LLAS_LEA_PRO_PROPREF)"
,LLAS_LEA_START_DATE       DATE "DD-MON-YYYY" NULLIF LLAS_LEA_START_DATE=blanks
,LLAS_START_DATE           DATE "DD-MON-YYYY" NULLIF LLAS_START_DATE=blanks
,LLAS_END_DATE             DATE "DD-MON-YYYY" NULLIF LLAS_END_DATE=blanks
,LLAS_CORRESPOND_NAME      CHAR "rtrim(:LLAS_CORRESPOND_NAME)"
)
