--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    VS   13-JAN-2009  Initial Creation.
--  1.1     6.10      AJ   14-DEC-2015  Control comments section added
--  1.2     6.13      AJ   24-FEB-2016  removed time stamp from review date
--                                      as not applicable
--
--
--***********************************************************************
--
load data
APPEND
into table DL_HAD_ADV_RSN_CASEWRK_EVTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LARCE_DLB_BATCH_ID              CONSTANT "$BATCH_NO"
,LARCE_DL_SEQNO                  RECNUM
,LARCE_DL_LOAD_STATUS            CONSTANT "L"
,LARCE_ACAS_ALTERNATE_REF        CHAR "rtrim(UPPER(:LARCE_ACAS_ALTERNATE_REF))"
,LARCE_ACRS_ARSN_CODE            CHAR "rtrim(UPPER(:LARCE_ACRS_ARSN_CODE))"
,LARCE_ACET_CODE                 CHAR "rtrim(UPPER(:LARCE_ACET_CODE))"
,LARCE_EVENT_DATETIME            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LARCE_EVENT_DATETIME=blanks
,LARCE_TEXT                      CHAR(2000) "rtrim(UPPER(:LARCE_TEXT))"
,LARCE_EVENT_DIRECTION_IND       CHAR "rtrim(UPPER(:LARCE_EVENT_DIRECTION_IND))"
,LARCE_CLIENT_INVOLVEMENT_IND    CHAR "rtrim(UPPER(:LARCE_CLIENT_INVOLVEMENT_IND))"
,LARCE_DIRECT_INTERVENTION_IND   CHAR "rtrim(UPPER(:LARCE_DIRECT_INTERVENTION_IND))"
,LARCE_CREATED_BY                CHAR "rtrim(UPPER(:LARCE_CREATED_BY))"
,LARCE_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LARCE_CREATED_DATE=blanks
,LARCE_DURATION                  CHAR "rtrim(UPPER(:LARCE_DURATION))"
,LARCE_REVIEW_DATE               DATE "DD-MON-YYYY" NULLIF LARCE_REVIEW_DATE=blanks
,LARCE_ACHO_REFERENCE            CHAR "rtrim(UPPER(:LARCE_ACHO_REFERENCE))"
,LARCE_AUN_CODE                  CHAR "rtrim(UPPER(:LARCE_AUN_CODE))"
,LARCE_REFNO                     INTEGER EXTERNAL "arce_refno_seq.nextval"
)


