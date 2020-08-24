load data
APPEND
into table DL_HEM_PERSON_PEO_ATT_HISTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPPAH_DLB_BATCH_ID                  CONSTANT "$BATCH_NO"
,LPPAH_DL_SEQNO                      RECNUM
,LPPAH_DL_LOAD_STATUS                CONSTANT "L"
,LPPAH_PAR_TYPE                      CHAR "rtrim(UPPER(:LPPAH_PAR_TYPE))"
,LPPAH_PAR_PER_ALT_REF               CHAR "rtrim(UPPER(:LPPAH_PAR_PER_ALT_REF))"
,LPPAH_PEAT_CODE                     CHAR "rtrim(UPPER(:LPPAH_PEAT_CODE))"
,LPPAH_ORIGINAL_CREATED_BY           CHAR "rtrim(UPPER(:LPPAH_ORIGINAL_CREATED_BY))"
,LPPAH_ORIGINAL_CREATED_DATE         DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPPAH_ORIGINAL_CREATED_DATE=blanks
,LPPAH_CREATED_BY                    CHAR "rtrim(UPPER(:LPPAH_CREATED_BY))"
,LPPAH_CREATED_DATE                  DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPPAH_CREATED_DATE=blanks
,LPPAH_PAAV_CODE                     CHAR "rtrim(UPPER(:LPPAH_PAAV_CODE))"
,LPPAH_DATE_VALUE                    DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPPAH_DATE_VALUE=blanks
,LPPAH_NUMERIC_VALUE                 CHAR "rtrim(:LPPAH_NUMERIC_VALUE)"
,LPPAH_TEXT_VALUE                    CHAR "rtrim(UPPER(:LPPAH_TEXT_VALUE))"
,LPPAH_COMMENTS                      CHAR "rtrim(UPPER(:LPPAH_COMMENTS))"
,LPPAH_YES_NO_VALUE                  CHAR "rtrim(UPPER(:LPPAH_YES_NO_VALUE))"
)
