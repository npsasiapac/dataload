--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.15      MJK  26-MAR-2018  Initial Creation.
--  1.1     6.15      UJO  02-MAY-2018  Corrected BY (Umesh Joshi) the bind variable for
--                                      LOOF_ALE_APP_LEGACY_REF AUS Support Team
--  1.2     6.15      AJ   03-MAY-2018  Date fields amended to allow only what is need so
--                                      time stamp left on those that have the option only
--                                      removed from LOOF_OFFER_DATE ,LOOF_RESPOND_BY_DATE
--                                      LOOF_HPS_START_DATE, LOOF_EXPECTED_TCY_START_DATE  
--
--***********************************************************************
--
load data
APPEND
into table DL_HAT_ORGANISATION_OFFERS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LOOF_DLB_BATCH_ID            	CONSTANT "$BATCH_NO"
,LOOF_DL_SEQNO                	RECNUM
,LOOF_DL_LOAD_STATUS          	CONSTANT "L"
,LOOF_OFFER_DATE                DATE "DD-MON-YYYY" NULLIF LOOF_OFFER_DATE=blanks
,LOOF_RESPOND_BY_DATE           DATE "DD-MON-YYYY" NULLIF LOOF_RESPOND_BY_DATE=blanks
,LOOF_PRO_PROPREF               CHAR "RTRIM(UPPER(:LOOF_PRO_PROPREF))"
,LOOF_HPS_START_DATE            DATE "DD-MON-YYYY" NULLIF LOOF_HPS_START_DATE=blanks
,LOOF_HPS_HPC_CODE              CHAR "RTRIM(UPPER(:LOOF_HPS_HPC_CODE))"
,LOOF_ALE_APP_LEGACY_REF        CHAR "RTRIM(UPPER(:LOOF_ALE_APP_LEGACY_REF))"
,LOOF_ALE_RLI_CODE              CHAR "RTRIM(UPPER(:LOOF_ALE_RLI_CODE))"
,LOOF_TTYP_HRV_CODE             CHAR "RTRIM(UPPER(:LOOF_TTYP_HRV_CODE))"
,LOOF_TTY_CODE                  CHAR "RTRIM(UPPER(:LOOF_TTY_CODE))"
,LOOF_OSG_OST_CODE              CHAR "RTRIM(UPPER(:LOOF_OSG_OST_CODE))"
,LOOF_EXPECTED_TCY_START_DATE   DATE "DD-MON-YYYY" NULLIF LOOF_EXPECTED_TCY_START_DATE=blanks
,LOOF_CASH_INCENTIVE            CHAR "RTRIM(UPPER(:LOOF_CASH_INCENTIVE))"
,LOOF_COMMENTS                  CHAR "RTRIM(UPPER(:LOOF_COMMENTS))"
,LOOF_TYPE                      CHAR "RTRIM(UPPER(:LOOF_TYPE))"
,LOOF_SCO_CODE                  CHAR "RTRIM(UPPER(:LOOF_SCO_CODE))"
,LOOF_CREATED_BY                CHAR "RTRIM(UPPER(:LOOF_CREATED_BY))"
,LOOF_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LOOF_CREATED_DATE=blanks
,LOOF_ACCEPTED_BY               CHAR "RTRIM(UPPER(:LOOF_ACCEPTED_BY))"
,LOOF_ACCEPTED_DATE             DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LOOF_ACCEPTED_DATE=blanks
)
