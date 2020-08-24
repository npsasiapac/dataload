load data
infile $gri_datafile
APPEND
into table DL_HPP_PP_TENANT_IMPROVS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LTIM_DLB_BATCH_ID               CONSTANT "$batch_no"
,LTIM_DL_SEQNO                   RECNUM
,LTIM_DL_LOAD_STATUS             CONSTANT "L"
,LTIM_PAPP_DISPLAYED_REFERENCE   CHAR "rtrim(:LTIM_PAPP_DISPLAYED_REFERENCE)"
,LTIM_SEQNO                      CHAR "rtrim(:LTIM_SEQNO)"
,LTIM_DESCRIPTION                CHAR "rtrim(:LTIM_DESCRIPTION)"
,LTIM_VERIFIED_IND               CHAR "rtrim(:LTIM_VERIFIED_IND)"
,LTIM_PVR_AMOUNT                 CHAR "rtrim(:LTIM_PVR_AMOUNT)"
,LTIM_COMMENTS                   CHAR "rtrim(:LTIM_COMMENTS)"
,LTIM_PVAL_SEQNO                 CHAR "rtrim(:LTIM_PVAL_SEQNO)"
)
