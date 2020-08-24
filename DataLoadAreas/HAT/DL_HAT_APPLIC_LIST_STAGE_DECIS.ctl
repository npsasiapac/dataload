--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    PH   02-MAR-2014  Initial Creation.
--  1.1     6.13      AJ   26-FEB-2016  Control comments section added
--                                      time stamp added to LALS_STATUS_DATE
--                                      LALS_CREATED_DATE, LALS_AUTHORISED_DATE 
--  1.2     6.18      PN   01-MAR-2019  LALS_COMMENTS definition update
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HAT_APPLIC_LIST_STAGE_DECIS
fields terminated by "," optionally enclosed by '"'
trailing nullcols

(LALS_DLB_BATCH_ID          CONSTANT "$batch_no",        
LALS_DL_SEQNO                   RECNUM,
LALS_DL_LOAD_STATUS             CONSTANT "L",
LAPP_LEGACY_REF                 CHAR  "rtrim(:LAPP_LEGACY_REF)",
LALS_ALE_RLI_CODE               CHAR "rtrim(:LALS_ALE_RLI_CODE)",
LALS_RLS_CODE                   CHAR "rtrim(:LALS_RLS_CODE)",
LALS_SCO_CODE                   CHAR "rtrim(:LALS_SCO_CODE)",
LALS_STATUS_DATE                DATE "DD-MON-YYYY HH24:MI:SS" NULLIF lals_status_date=blanks,
LALS_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF lals_created_date=blanks,
LALS_CREATED_BY                 CHAR  "rtrim(:LALS_CREATED_BY)",
LALS_COMMENTS                   CHAR(2000)  "rtrim(:LALS_COMMENTS)",
LALS_DECISION_DATE              DATE "DD-MON-YYYY" NULLIF lals_decision_date=blanks,
LALS_DECISION_BY                CHAR  "rtrim(:LALS_DECISION_BY)",
LALS_AUTHORISED_DATE            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF lals_authorised_date=blanks,
LALS_AUTHORISED_BY              CHAR  "rtrim(:LALS_AUTHORISED_BY)",
LALS_AUTH_STATUS_START_DATE     DATE "DD-MON-YYYY" NULLIF lals_auth_status_start_date=blanks,
LALS_AUTH_STATUS_REVIEW_DATE    DATE "DD-MON-YYYY" NULLIF lals_auth_status_review_date=blanks,
LALS_HRV_APS_CODE               CHAR  "rtrim(:LALS_HRV_APS_CODE)",
LALS_HRV_SDR_CODE               CHAR  "rtrim(:LALS_HRV_SDR_CODE)",
LALS_RSD_HRV_LSD_CODE           CHAR  "rtrim(:LALS_RSD_HRV_LSD_CODE)",
LALS_PROVISIONAL_LST_CODE       CHAR  "rtrim(:LALS_PROVISIONAL_LST_CODE)"
)


  
