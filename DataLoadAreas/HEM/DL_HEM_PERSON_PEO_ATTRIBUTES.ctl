load data
APPEND
into table DL_HEM_PERSON_PEO_ATTRIBUTES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPEPA_DLB_BATCH_ID                  CONSTANT "$BATCH_NO"
,LPEPA_DL_SEQNO                      RECNUM
,LPEPA_DL_LOAD_STATUS                CONSTANT "L"
,LPEPA_PAR_TYPE                      CHAR "rtrim(UPPER(:LPEPA_PAR_TYPE))"
,LPEPA_CLASS_CODE                    CHAR "rtrim(UPPER(:LPEPA_CLASS_CODE))"
,LPEPA_PAR_PER_ALT_REF               CHAR "rtrim(UPPER(:LPEPA_PAR_PER_ALT_REF))"
,LPEPA_CREATED_DATE                  DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPEPA_CREATED_DATE=blanks
,LPEPA_CREATED_BY                    CHAR "rtrim(UPPER(:LPEPA_CREATED_BY))"
,LPEPA_PEAT_CODE                     CHAR "rtrim(UPPER(:LPEPA_PEAT_CODE))"
,LPEPA_PAAV_CODE                     CHAR "rtrim(UPPER(:LPEPA_PAAV_CODE))"
,LPEPA_YES_NO_VALUE                  CHAR "rtrim(UPPER(:LPEPA_YES_NO_VALUE))"
,LPEPA_DATE_VALUE                    DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPEPA_DATE_VALUE=blanks
,LPEPA_NUMERIC_VALUE                 CHAR "rtrim(:LPEPA_NUMERIC_VALUE)"
,LPEPA_TEXT_VALUE                    CHAR "rtrim(UPPER(:LPEPA_TEXT_VALUE))"
,LPEPA_COMMENTS                      CHAR "rtrim(UPPER(:LPEPA_COMMENTS))"
,LPEPA_MODIFIED_DATE                 DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPEPA_MODIFIED_DATE=blanks
,LPEPA_MODIFIED_BY                   CHAR "rtrim(UPPER(:LPEPA_MODIFIED_BY))"
,LPEPA_REFNO                         INTEGER EXTERNAL "PEPA_REFNO_SEQ.NEXTVAL"
)
