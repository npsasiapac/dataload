load data
infile $gri_datafile
APPEND
into table DL_HPP_PP_EVENTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPEV_DLB_BATCH_ID                 CONSTANT "$batch_no"
,LPEV_DL_SEQNO                     RECNUM
,LPEV_DL_LOAD_STATUS               CONSTANT "L"
,LPEV_PAPP_DISPLAYED_REFERENCE     CHAR "rtrim(:LPEV_PAPP_DISPLAYED_REFERENCE)"
,LPEV_PET_CODE                     CHAR "rtrim(:LPEV_PET_CODE)"
,LPEV_SEQNO                        CHAR "rtrim(:LPEV_SEQNO)"
,LPEV_ACTUAL_DATE                  DATE "DD-MON-YYYY" NULLIF LPEV_ACTUAL_DATE=blanks
,LPEV_TARGET_DATE                  DATE "DD-MON-YYYY" NULLIF LPEV_TARGET_DATE=blanks
,LPEV_STATUTORY_DATE               DATE "DD-MON-YYYY" NULLIF LPEV_STATUTORY_DATE=blanks
,LPEV_COMMENTS                     CHAR "rtrim(:LPEV_COMMENTS)"
)
