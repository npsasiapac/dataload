-- ***********************************************************************
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.7.0     VS   18-MAR-2013  Initial Versions
--
-- ***********************************************************************
--
load data
APPEND
into table DL_HEM_INCOME_DETAIL_REQS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LINDR_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LINDR_DL_SEQNO                   RECNUM
,LINDR_DL_LOAD_STATUS             CONSTANT "L"
,LINDR_REFERENCE                  CHAR "rtrim(UPPER(:LINDR_REFERENCE))"
,LINDR_TYPE                       CHAR "rtrim(UPPER(:LINDR_TYPE))"
,LINDR_CREATED_BY                 CHAR "rtrim(UPPER(:LINDR_CREATED_BY))"
,LINDR_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LINDR_CREATED_DATE=blanks
,LINDR_PAR_REFERENCE_TYPE         CHAR "rtrim(UPPER(:LINDR_PAR_REFERENCE_TYPE))"
,LINDR_PAR_REFERENCE              CHAR "rtrim(UPPER(:LINDR_PAR_REFERENCE))"
,LINDR_SCO_CODE                   CHAR "rtrim(UPPER(:LINDR_SCO_CODE))"
,LINDR_STATUS_DATE                DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LINDR_STATUS_DATE=blanks
,LINDR_HRV_IDTY_CODE              CHAR "rtrim(UPPER(:LINDR_HRV_IDTY_CODE))"
,LINDR_PARTNER_IND                CHAR "rtrim(UPPER(:LINDR_PARTNER_IND))"
,LINDR_LEGACY_REFERENCE_TYPE      CHAR "rtrim(UPPER(:LINDR_LEGACY_REFERENCE_TYPE))"
,LINDR_LEGACY_REFERENCE           CHAR "rtrim(UPPER(:LINDR_LEGACY_REFERENCE))"
,LINDR_NUM_OF_CHILDREN            CHAR "rtrim(UPPER(:LINDR_NUM_OF_CHILDREN))"
,LINDR_REQUEST_PROCESS_DATE       DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LINDR_REQUEST_PROCESS_DATE=blanks
,LINDR_REQUEST_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LINDR_REQUEST_DATE=blanks
,LINDR_IFP_CODE                   CHAR "rtrim(UPPER(:LINDR_IFP_CODE))"
)
