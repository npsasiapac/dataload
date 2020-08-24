load data
infile $gri_datafile
APPEND
into table DL_HAT_HLESS_INS_STAGE_DECIS
fields terminated by "," optionally enclosed by '"'
trailing nullcols

(LHID_DLB_BATCH_ID          CONSTANT "$batch_no",        
LHID_DL_SEQNO                   RECNUM,
LHID_DL_LOAD_STATUS             CONSTANT "L",
LHIN_INSTANCE_REFNO             CHAR "rtrim(:LHIN_INSTANCE_REFNO)",
LHID_RLS_CODE                   CHAR "rtrim(:LHID_RLS_CODE)",
LHID_SCO_CODE                   CHAR "rtrim(:LHID_SCO_CODE)",
LHID_STATUS_DATE                DATE "DD-MON-YYYY" NULLIF LHID_status_date=blanks,
LHID_CREATED_DATE               DATE "DD-MON-YYYY" NULLIF LHID_created_date=blanks,
LHID_CREATED_BY                 CHAR  "rtrim(:LHID_CREATED_BY)",
LHID_COMMENTS                   CHAR  "rtrim(:LHID_COMMENTS)",
LHID_DECISION_DATE              DATE "DD-MON-YYYY" NULLIF LHID_decision_date=blanks,
LHID_DECISION_BY                CHAR  "rtrim(:LHID_DECISION_BY)",
LHID_AUTHORISED_DATE            DATE "DD-MON-YYYY" NULLIF LHID_authorised_date=blanks,
LHID_AUTHORISED_BY              CHAR  "rtrim(:LHID_AUTHORISED_BY)",
LHID_AUTH_STATUS_START_DATE     DATE "DD-MON-YYYY" NULLIF LHID_auth_status_start_date=blanks,
LHID_AUTH_STATUS_REVIEW_DATE    DATE "DD-MON-YYYY" NULLIF LHID_auth_status_review_date=blanks,
LHID_HRV_APS_CODE               CHAR  "rtrim(:LHID_HRV_APS_CODE)",
LHID_HRV_SDR_CODE               CHAR  "rtrim(:LHID_HRV_SDR_CODE)",
LHID_RSD_HRV_LSD_CODE           CHAR  "rtrim(:LHID_RSD_HRV_LSD_CODE)",
LHID_PROVISIONAL_LST_CODE       CHAR  "rtrim(:LHID_PROVISIONAL_LST_CODE)"
)


  
