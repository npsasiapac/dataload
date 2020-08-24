--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    VS   13-JAN-2009  Initial Creation.
--  1.1     6.10      AJ   11-DEC-2015  Control comments section added
--  1.2     6.11      AJ   18-DEC-2015  LACRS_ARSS_HRV_ARST_CODE added this is 
--                                      the advice case reason stage
--  1.3     6.13      AJ   24-FEB-2016  Checked dates and altered datetime 
--                                      where appropriate
--
--
--***********************************************************************
--
load data
APPEND
into table DL_HAD_ADVICE_CASES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LACAS_DLB_BATCH_ID              CONSTANT "$BATCH_NO"
,LACAS_DL_SEQNO                  RECNUM
,LACAS_DL_LOAD_STATUS            CONSTANT "L"
,LACAS_ALTERNATE_REF             CHAR "rtrim(UPPER(:LACAS_ALTERNATE_REF))"
,LACAS_APPROACH_DATE             DATE "DD-MON-YYYY" NULLIF LACAS_APPROACH_DATE=blanks
,LACAS_SCO_CODE                  CHAR "rtrim(UPPER(:LACAS_SCO_CODE))"
,LACAS_STATUS_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACAS_STATUS_DATE=blanks
,LACAS_CORRESPONDENCE_NAME       CHAR "rtrim(UPPER(:LACAS_CORRESPONDENCE_NAME))"
,LACAS_CREATED_BY                CHAR "rtrim(UPPER(:LACAS_CREATED_BY))"
,LACAS_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACAS_CREATED_DATE=blanks
,LACAS_HOMELESS_IND              CHAR "rtrim(UPPER(:LACAS_HOMELESS_IND))"
,LACAS_HRV_ACAM_CODE             CHAR "rtrim(UPPER(:LACAS_HRV_ACAM_CODE))"
,LACAS_HRV_CWTP_CODE             CHAR "rtrim(UPPER(:LACAS_HRV_CWTP_CODE))"
,LACAS_HRV_ACSP_CODE             CHAR "rtrim(UPPER(:LACAS_HRV_ACSP_CODE))"
,LACAS_EXPECTED_HOMELESS_DATE    DATE "DD-MON-YYYY" NULLIF LACAS_EXPECTED_HOMELESS_DATE=blanks
,LACAS_START_TIME_AT_RECEPTION   DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACAS_START_TIME_AT_RECEPTION=blanks
,LACAS_END_TIME_AT_RECEPTION     DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACAS_END_TIME_AT_RECEPTION=blanks
,LACAS_CASE_OPENED_DATE          DATE "DD-MON-YYYY" NULLIF LACAS_CASE_OPENED_DATE=blanks
,LACAS_PREV_SCO_CODE             CHAR "rtrim(UPPER(:LACAS_PREV_SCO_CODE))"
,LACAS_PREV_STATUS_DATE          DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACAS_PREV_STATUS_DATE=blanks
,LACAS_COMMENTS                  CHAR(2000) "rtrim(:LACAS_COMMENTS)"
,LACAS_AUN_CODE                  CHAR "rtrim(UPPER(:LACAS_AUN_CODE))"
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
,LACRS_ARSS_HRV_ARST_CODE        CHAR "rtrim(UPPER(:LACRS_ARSS_HRV_ARST_CODE))"
)




