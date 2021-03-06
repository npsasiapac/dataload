load data
infile $gri_datafile
APPEND
into table DL_HPP_PP_APPLN_PARTIES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPAP_DLB_BATCH_ID                CONSTANT "$batch_no"
,LPAP_DL_SEQNO                    RECNUM
,LPAP_DL_LOAD_STATUS              CONSTANT "L"
,LPAP_PAPP_DISPLAYED_REFERENCE    CHAR "rtrim(:LPAP_PAPP_DISPLAYED_REFERENCE)"
,LPAP_PAR_PER_ALT_REF             CHAR "rtrim(:LPAP_PAR_PER_ALT_REF)"
,LPAP_PARTY_TYPE                  CHAR "rtrim(:LPAP_PARTY_TYPE)"
,LPAP_PRINCIPAL_HOME_IND          CHAR "rtrim(:LPAP_PRINCIPAL_HOME_IND)"
,LPAP_WISH_TO_BUY_IND             CHAR "rtrim(:LPAP_WISH_TO_BUY_IND)"
,LPAP_LIVED_ONE_YEAR              CHAR "rtrim(:LPAP_LIVED_ONE_YEAR)"
,LPAP_PARTY_VERIFIED_IND          CHAR "rtrim(:LPAP_PARTY_VERIFIED_IND)"
,LPAP_SIGNATURE_VERIFIED_IND      CHAR "rtrim(:LPAP_SIGNATURE_VERIFIED_IND)"
,LPAP_SIGNATURE_DATE              DATE "DD-MON-YYYY" NULLIF LPAP_SIGNATURE_DATE=blanks
,LPAP_COMMENTS                    CHAR "rtrim(:LPAP_COMMENTS)"
,LPAP_ADMITTED_IND                CHAR "rtrim(:LPAP_ADMITTED_IND)"
,LPAP_DENIED_IND                  CHAR "rtrim(:LPAP_DENIED_IND)"
,LPAP_HRV_DENIED_REASON           CHAR "rtrim(:LPAP_HRV_DENIED_REASON)"
)
