load data
infile $gri_datafile
APPEND
into table DL_HPP_PP_TENANCY_HISTORIES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LTHI_DLB_BATCH_ID                 CONSTANT "$batch_no"
,LTHI_DL_SEQNO                     RECNUM
,LTHI_DL_LOAD_STATUS               CONSTANT "L"
,LTHI_PAPP_DISPLAYED_REFERENCE     CHAR "rtrim(:LTHI_PAPP_DISPLAYED_REFERENCE)"
,LTHI_PAR_PER_ALT_REF              CHAR "rtrim(:LTHI_PAR_PER_ALT_REF)"
,LTHI_THT_CODE                     CHAR "rtrim(:LTHI_THT_CODE)"
,LTHI_PERIOD_START                 DATE "DD-MON-YYYY" NULLIF LTHI_PERIOD_START=blanks
,LTHI_PERIOD_END                   DATE "DD-MON-YYYY" NULLIF LTHI_PERIOD_END=blanks
,LTHI_TENANT_NAME                  CHAR "rtrim(:LTHI_TENANT_NAME)"
,LTHI_TENANCY_ADDRESS              CHAR "rtrim(:LTHI_TENANCY_ADDRESS)"
,LTHI_LANDLORD_IPP_SHORTNAME       CHAR "rtrim(:LTHI_LANDLORD_IPP_SHORTNAME)"
,LTHI_LANDLORD_FREE_TEXT           CHAR "rtrim(:LTHI_LANDLORD_FREE_TEXT)"
,LTHI_COMMENTS                     CHAR "rtrim(:LTHI_COMMENTS)"
,LTHI_VERIFIED_IND                 CHAR "rtrim(:LTHI_VERIFIED_IND)"
,LTHI_CURRENT_LANDLORD_IND         CHAR "rtrim(:LTHI_CURRENT_LANDLORD_IND)"
)
