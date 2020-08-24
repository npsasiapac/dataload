load data
APPEND
into table DL_HEM_PERSON_NAME_HISTORY
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPNH_DLB_BATCH_ID                  CONSTANT "$BATCH_NO"
,LPNH_DL_SEQNO                      RECNUM
,LPNH_DL_LOAD_STATUS                CONSTANT "L"
,LPNH_PAR_TYPE                      CHAR "rtrim(UPPER(:LPNH_PAR_TYPE))"
,LPNH_PAR_PER_ALT_REF               CHAR "rtrim(UPPER(:LPNH_PAR_PER_ALT_REF))"
,LPNH_START_DATE                    DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPNH_START_DATE=blanks
,LPNH_END_DATE                      DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPNH_END_DATE=blanks
,LPNH_FRV_FPH_CODE                  CHAR "rtrim(UPPER(:LPNH_FRV_FPH_CODE))"
,LPNH_USERNAME                      CHAR "rtrim(UPPER(:LPNH_USERNAME))"
,LPNH_SURNAME                       CHAR "rtrim(UPPER(:LPNH_SURNAME))"
,LPNH_FORENAME                      CHAR "rtrim(UPPER(:LPNH_FORENAME))"
,LPNH_INITIALS                      CHAR "rtrim(UPPER(:LPNH_INITIALS))"
,LPNH_TITLE                         CHAR "rtrim(UPPER(:LPNH_TITLE))"
,LPNH_OTHER_NAMES                   CHAR "rtrim(UPPER(:LPNH_OTHER_NAMES))"
,LPNH_SURNAME_PREFIX                CHAR "rtrim(UPPER(:LPNH_SURNAME_PREFIX))"
,LPNH_SURNAME_MLANG                 CHAR "rtrim(UPPER(:LPNH_SURNAME_MLANG))"
,LPNH_FORENAME_MLANG                CHAR "rtrim(UPPER(:LPNH_FORENAME_MLANG))"
,LPNH_INITIALS_MLANG                CHAR "rtrim(UPPER(:LPNH_INITIALS_MLANG))"
,LPNH_OTHER_NAMES_MLANG             CHAR "rtrim(UPPER(:LPNH_OTHER_NAMES_MLANG))"
)
