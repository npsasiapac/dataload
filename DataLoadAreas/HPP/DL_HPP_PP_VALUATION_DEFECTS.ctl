load data
infile $gri_datafile
APPEND
into table DL_HPP_PP_VALUATION_DEFECTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPVD_DLB_BATCH_ID               CONSTANT "$batch_no"
,LPVD_DL_SEQNO                   RECNUM
,LPVD_DL_LOAD_STATUS             CONSTANT "L"
,LPVD_PAPP_DISPLAYED_REFERENCE   CHAR "rtrim(:LPVD_PAPP_DISPLAYED_REFERENCE)"
,LPVD_PVAL_SEQNO                 CHAR "rtrim(:LPVD_PVAL_SEQNO)"
,LPVD_VDT_CODE                   CHAR "rtrim(:LPVD_VDT_CODE)"
,LPVD_AMOUNT                     CHAR "rtrim(:LPVD_AMOUNT)"
,LPVD_COMMENTS                   CHAR "rtrim(:LPVD_COMMENTS)"
)
