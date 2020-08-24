--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    VS   13-JAN-2009  Initial Creation.
--  1.1     6.10      AJ   11=DEC-2015  Control comments section added
--  1.2     6.11      AJ   18-DEC-2015  LACRS_ARSS_HRV_ARST_CODE added this is 
--                                      the advice case reason stage
--
--
--***********************************************************************
--
load data
APPEND
into table DL_HAD_ADVICE_CASE_REASONS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LACRS_DLB_BATCH_ID              CONSTANT "$BATCH_NO"
,LACRS_DL_SEQNO                  RECNUM
,LACRS_DL_LOAD_STATUS            CONSTANT "L"
,LACRS_ACAS_ALTERNATE_REF        CHAR "rtrim(UPPER(:LACRS_ACAS_ALTERNATE_REF))"
,LACRS_ARSN_CODE                 CHAR "rtrim(UPPER(:LACRS_ARSN_CODE))"
,LACRS_MAIN_IND                  CHAR "rtrim(UPPER(:LACRS_MAIN_IND))"
,LACRS_SCO_CODE                  CHAR "rtrim(UPPER(:LACRS_SCO_CODE))"
,LACRS_STATUS_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACRS_STATUS_DATE=blanks
,LACRS_CREATED_BY                CHAR "rtrim(UPPER(:LACRS_CREATED_BY))"
,LACRS_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACRS_CREATED_DATE=blanks
,LACRS_OUTCOME_COMMENTS          CHAR(2000) "rtrim(:LACRS_OUTCOME_COMMENTS)"
,LACRS_PREV_SCO_CODE             CHAR "rtrim(UPPER(:LACRS_PREV_SCO_CODE))"
,LACRS_PREV_STATUS_DATE          DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACRS_PREV_STATUS_DATE=blanks
,LACRS_AUTHORISED_BY             CHAR "rtrim(UPPER(:LACRS_AUTHORISED_BY))"
,LACRS_AUTHORISED_DATE           DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACRS_AUTHORISED_DATE=blanks
,LAROC_OUTC_CODE                 CHAR "rtrim(UPPER(:LAROC_OUTC_CODE))"
,LAROC_PRIMARY_OUTCOME_IND       CHAR "rtrim(UPPER(:LAROC_PRIMARY_OUTCOME_IND))"
,LAROC_CURRENT_IND               CHAR "rtrim(UPPER(:LAROC_CURRENT_IND))"
,LAROC_CREATED_BY                CHAR "rtrim(UPPER(:LAROC_CREATED_BY))"
,LAROC_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LAROC_CREATED_DATE=blanks
,LAROC_SEQNO                     CHAR "rtrim(UPPER(:LAROC_SEQNO))"
,LACRS_ARSS_HRV_ARST_CODE        CHAR "rtrim(UPPER(:LACRS_ARSS_HRV_ARST_CODE))"
)



