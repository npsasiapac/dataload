-- ***********************************************************************
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.7.0     VS   18-MAR-2013  Initial Version
--
-- ***********************************************************************
load data
APPEND
into table DL_HEM_ICS_REQUEST_STATUSES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LIRS_DLB_BATCH_ID               CONSTANT "$BATCH_NO",
 LIRS_DL_SEQNO                   RECNUM,
 LIRS_DL_LOAD_STATUS             CONSTANT "L",
 LIRS_INDR_REFERENCE             CHAR "rtrim(UPPER(:LIRS_INDR_REFERENCE))",
 LIRS_SCO_CODE                   CHAR "rtrim(UPPER(:LIRS_SCO_CODE))",
 LIRS_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LIRS_CREATED_DATE=blanks,
 LIRS_CREATED_BY                 CHAR "rtrim(UPPER(:LIRS_CREATED_BY))",
 LIRS_HRV_IEC_CODE               CHAR "rtrim(UPPER(:LIRS_HRV_IEC_CODE))",
 LIRS_ERROR_TEXT                 CHAR "rtrim(UPPER(:LIRS_ERROR_TEXT))",
 LIRS_CURRENT_STATUS_IND         CHAR "rtrim(UPPER(:LIRS_CURRENT_STATUS_IND))"
)
